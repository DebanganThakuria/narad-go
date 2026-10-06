package narad

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Operation names of the batch forms, used in errors and events.
const (
	opAckBatch  = "ack batch"
	opNackBatch = "nack batch"
)

// WithBatch has each [Client.Consume] worker take up to n messages per
// request instead of one, and ack the ones its handler succeeded on
// together. It is off by default, and n is capped at [MaxBatch].
//
// The handler is unchanged: it still gets one message at a time, and
// everything [Message.Ack] and friends mean holds per message. What
// changes is the traffic. A worker asks for n messages, each with its
// own lease, handles them in order, and then acks every one its handler
// succeeded on in one request. The broker answers that ack with a status
// per message, and each is honoured: a lease that lapsed meanwhile is
// reported to [WithErrorHandler] as [ErrLeaseLost], just as a single ack
// would report it. A failed message is still handed back at once.
//
// While a message waits its turn its lease is renewed like that of one
// being handled, so a slow message early in a batch does not cost the
// rest theirs. On shutdown, messages not yet started are handed back.
//
// The trade: a message's ack goes out when its whole batch is done, not
// when it is, so a crash mid-batch redelivers messages already handled.
// Handlers must be idempotent anyway. And a broker with a per-identity
// cap on consumes in flight counts each poll as n, so workers times n is
// what that cap sees; past it the answer is [ErrThrottled]. It needs a 3.1.0 broker; an older
// one hands out one message at a time and refuses the batch ack, and the
// worker then acks one at a time.
func WithBatch(n int) ConsumeOption {
	return func(c *consumeConfig) { c.batch = min(n, MaxBatch) }
}

// pollBatch asks once for up to cfg.batch messages. It returns none when
// the wait elapsed with nothing available.
func (c *Client) pollBatch(ctx context.Context, topic string, cfg consumeConfig) ([]*Message, error) {
	if topic == "" {
		return nil, fmt.Errorf("narad: consume: %w: topic is required", ErrBadRequest)
	}
	// X-Narad-Client goes on every request; a batch consume without it
	// is refused with 400.
	extra := url.Values{"max": {strconv.Itoa(cfg.batch)}}
	res, err := c.do(ctx, consumeCall(topic, cfg, extra, c.cfg.timeout))
	if err != nil {
		return nil, err
	}
	if res.status == http.StatusNoContent {
		return nil, nil
	}
	return c.decodeMessages(res.body, topic)
}

// decodeMessages reads a batch consume reply, {"messages":[...]}.
//
// A node older than 3.1.0 ignores max and answers one message in the
// single shape, so that is read too.
func (c *Client) decodeMessages(body []byte, topic string) ([]*Message, error) {
	var batch struct {
		Messages *[]*Message `json:"messages"`
	}
	if err := json.Unmarshal(body, &batch); err != nil {
		return nil, fmt.Errorf("narad: consume %s: decode reply: %w", topic, err)
	}
	if batch.Messages == nil {
		msg, err := c.decodeMessage(body, topic)
		if err != nil {
			return nil, err
		}
		return []*Message{msg}, nil
	}
	msgs := *batch.Messages
	for _, msg := range msgs {
		msg.client = c
	}
	return msgs, nil
}

// dispatchBatch runs the handler over a batch of messages, one at a
// time, with every lease kept alive until its message is settled, and
// acks the successes together at the end.
func (c *Client) dispatchBatch(pollCtx, workCtx context.Context, h Handler, msgs []*Message, cfg consumeConfig) {
	type held struct {
		msg    *Message
		ctx    context.Context
		cancel context.CancelFunc
		keeper *leaseKeeper
	}
	items := make([]held, len(msgs))
	for i, msg := range msgs {
		ctx, cancel := context.WithCancel(workCtx)
		items[i] = held{msg: msg, ctx: ctx, cancel: cancel}
		if cfg.autoExtend && msg.Leased() {
			items[i].keeper = keepLease(ctx, cancel, msg, cfg.visibility)
		}
	}
	// release stops an item's renewals and reports whether its lease
	// went away while it was held.
	release := func(it *held) (lost bool) {
		if it.keeper != nil {
			it.keeper.stop()
			lost = it.keeper.lost()
		}
		it.cancel()
		return lost
	}
	reportLost := func(msg *Message, when string) {
		c.report(cfg, msg, fmt.Errorf("narad: %s: %w %s", msg.describe(), ErrLeaseLost, when))
		c.logger().Warn("narad: lease lost "+when,
			"topic", msg.Topic, "id", msg.ID(), "offset", msg.Offset)
	}

	var succeeded, unstarted []*held
	for i := range items {
		it := &items[i]
		if pollCtx.Err() != nil {
			// Shutting down: whatever has not started goes back now
			// rather than when its lease lapses.
			unstarted = append(unstarted, it)
			continue
		}
		if it.keeper != nil && it.keeper.lost() {
			release(it)
			reportLost(it.msg, "before the handler started")
			continue
		}

		err := safely(it.ctx, h, it.msg)

		if !it.msg.Leased() {
			release(it)
			continue
		}
		// As in dispatch, a message the handler settled itself is not
		// settled again.
		settled := it.msg.isSettled()
		if err == nil && !settled {
			// The lease stays renewed until the batch ack goes out.
			succeeded = append(succeeded, it)
			continue
		}
		if lost := release(it); lost && !settled {
			reportLost(it.msg, "while the handler was still running")
			continue
		}
		if err == nil {
			continue
		}
		c.report(cfg, it.msg, err)
		c.logger().Warn("narad: handler failed",
			"topic", it.msg.Topic, "id", it.msg.ID(), "offset", it.msg.Offset,
			"requeued", cfg.requeue && !settled, "err", err)
		if cfg.requeue && !settled {
			settleCtx, cancel := settleContext(workCtx)
			c.handBack(settleCtx, it.msg)
			cancel()
		}
	}

	settleCtx, cancel := settleContext(workCtx)
	defer cancel()

	// A lease lost before the ack could go out means the message is on
	// its way to somebody else, so it is reported rather than acked.
	var toAck []*Message
	for _, it := range succeeded {
		if release(it) {
			reportLost(it.msg, "before the ack could be sent")
			continue
		}
		toAck = append(toAck, it.msg)
	}
	for i, err := range c.settleMany(settleCtx, opAck, toAck) {
		if err == nil {
			continue
		}
		msg := toAck[i]
		c.report(cfg, msg, fmt.Errorf("narad: %s: the handler succeeded but the ack failed, so the message will be redelivered: %w",
			msg.describe(), err))
		c.logger().Error("narad: ack failed after successful handling",
			"topic", msg.Topic, "id", msg.ID(), "offset", msg.Offset, "err", err)
	}

	var toHandBack []*Message
	for _, it := range unstarted {
		if !release(it) {
			toHandBack = append(toHandBack, it.msg)
		}
	}
	for i, err := range c.settleMany(settleCtx, opNack, toHandBack) {
		if err != nil {
			c.logger().Warn("narad: could not hand an unstarted message back, it will wait out its visibility timeout",
				"topic", toHandBack[i].Topic, "id", toHandBack[i].ID(), "err", err)
		}
	}
}

// settleContext bounds a settle. As in dispatch, finishing it matters
// more than shutting down promptly, so it outlives a cancelled worker,
// but it gets its own short deadline so it cannot hang.
func settleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
}

// settleMany acks or nacks messages of one topic, with one request per
// round instead of one per message, and returns each message's outcome
// in order.
//
// The broker settles every receipt handle on its own and answers with
// the status a single ack of it would have got, so each message is
// treated exactly as [Message.Ack] or [Message.Nack] would treat it:
// 204 settles it, a retryable status is tried again, and a 410 is
// [ErrLeaseLost] unless an earlier round for that message may have
// landed, when, as for a single ack, it means that round spent the
// handle. A broker that refuses the batch form gets one request per
// message instead.
func (c *Client) settleMany(ctx context.Context, op string, msgs []*Message) []error {
	errs := make([]error, len(msgs))
	if len(msgs) == 0 {
		return errs
	}
	extra := map[string]string(nil)
	batchOp := opAckBatch
	if op == opNack {
		extra = map[string]string{"extend": "0"}
		batchOp = opNackBatch
	}
	if len(msgs) == 1 {
		errs[0] = msgs[0].settle(ctx, op, extra)
		return errs
	}

	topic := msgs[0].Topic
	pending := make([]int, len(msgs))
	for i := range pending {
		pending[i] = i
	}
	mayHaveLanded := make([]bool, len(msgs))
	attempts := max(c.cfg.attempts, 1)

	for attempt := 0; attempt < attempts && len(pending) > 0; attempt++ {
		if attempt > 0 {
			if !wait(ctx, c.cfg.backoff.delay(attempt-1)) {
				for _, i := range pending {
					errs[i] = ctx.Err()
				}
				return errs
			}
		}
		results, node, err := c.ackBatch(ctx, topic, batchOp, extra, msgs, pending)
		if err != nil {
			if errorIs(err, ErrBadRequest) {
				// A broker before 3.1.0 answers a batch ack 400
				// (receipt_handle required). One at a time still works,
				// and a 400 the batch earned for any other reason is
				// one each handle will earn on its own too.
				for _, i := range pending {
					errs[i] = msgs[i].settleFrom(ctx, op, extra, mayHaveLanded[i])
				}
				return errs
			}
			for _, i := range pending {
				errs[i] = err
			}
			if errors.Is(err, errBatchReply) {
				// The broker answered 200, so it processed every ack in
				// the batch; only the reply was unreadable. Send them
				// again, as acks that may have landed.
				for _, i := range pending {
					mayHaveLanded[i] = true
				}
				continue
			}
			if !Retryable(err) {
				return errs
			}
			if Uncertain(err) {
				for _, i := range pending {
					mayHaveLanded[i] = true
				}
			}
			continue
		}

		var next []int
		for j, i := range pending {
			status := results[j].Status
			if status == http.StatusNoContent ||
				(op == opAck && mayHaveLanded[i] && status == http.StatusGone) {
				msgs[i].settledBy(op)
				errs[i] = nil
				continue
			}
			handleErr := &Error{
				Op: op, Topic: topic, Status: status, Message: results[j].Error, Node: node,
				kind: errorKind(status, results[j].Error),
			}
			errs[i] = handleErr
			if Retryable(handleErr) {
				if Uncertain(handleErr) {
					mayHaveLanded[i] = true
				}
				next = append(next, i)
			}
		}
		pending = next
	}
	return errs
}

// ackResult is one handle's outcome in a batch ack reply.
type ackResult struct {
	Status int    `json:"status"`
	Error  string `json:"error"`
}

// ackBatch sends one batch ack for the pending messages and returns one
// result per pending message, in order, and the node that answered.
func (c *Client) ackBatch(ctx context.Context, topic, op string, extra map[string]string,
	msgs []*Message, pending []int) ([]ackResult, string, error) {
	handles := make([]string, len(pending))
	for j, i := range pending {
		handles[j] = msgs[i].Receipt
	}
	body, err := json.Marshal(struct {
		Handles []string `json:"receipt_handles"`
	}{handles})
	if err != nil {
		return nil, "", fmt.Errorf("narad: %s %s: encode request: %w", op, topic, err)
	}
	query := url.Values{}
	for name, value := range extra {
		query.Set(name, value)
	}
	res, err := c.do(ctx, call{
		method:      http.MethodPost,
		path:        "/v1/topics/" + url.PathEscape(topic) + "/ack",
		query:       query.Encode(),
		body:        body,
		contentType: "application/json",
		op:          op,
		topic:       topic,
		ok:          []int{http.StatusOK},
		once:        true,
	})
	if err != nil {
		return nil, "", err
	}
	var reply struct {
		Results []ackResult `json:"results"`
	}
	if err := json.Unmarshal(res.body, &reply); err != nil {
		return nil, "", fmt.Errorf("narad: %s %s: %w: %w", op, topic, errBatchReply, err)
	}
	if len(reply.Results) != len(pending) {
		return nil, "", fmt.Errorf("narad: %s %s: %w: %d results for %d receipt handles",
			op, topic, errBatchReply, len(reply.Results), len(pending))
	}
	return reply.Results, res.node, nil
}

// errBatchReply is a 200 batch ack reply that does not decode or does
// not match its request.
var errBatchReply = errors.New("reply does not match the request")
