package narad

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Operation names of the batch forms, used in errors and events.
const (
	opAckBatch    = "ack batch"
	opNackBatch   = "nack batch"
	opExtendBatch = "extend batch"
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
// Every lease in the batch is renewed from the poll until its message
// is settled, together, in one request per round, so a slow message
// early in a batch does not cost the rest theirs. That holds with
// [WithoutAutoExtend] too: it is for a handler that outruns its own
// lease, and in a batch a message also waits for the ones ahead of it
// and for the batch ack.
//
// On shutdown, the messages already handled are acked at once, so a
// handler abandoned at the end of [WithShutdownGrace] costs only its own
// message, and the messages not yet started are handed back.
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
//
// A record in the reply that cannot be decoded is reported to
// [WithErrorHandler] and left to wait out its visibility timeout, as it
// would be from a single consume; the rest of the batch is returned.
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
	msgs, bad, err := c.decodeMessages(res.body, topic)
	for _, u := range bad {
		c.report(cfg, u.msg, u.err)
		c.logger().Warn("narad: could not decode a message, it will wait out its visibility timeout",
			"topic", u.msg.Topic, "partition", u.msg.Partition, "offset", u.msg.Offset, "err", u.err)
	}
	return msgs, err
}

// undecodable is a record of a batch consume reply that could not be
// decoded.
type undecodable struct {
	// msg holds what could be read of it: where it is and its receipt.
	msg *Message
	err error
}

// decodeMessages reads a batch consume reply, {"messages":[...]}.
//
// Each record is decoded on its own, so one this client cannot read
// costs only that record and not the up to [MaxBatch] others reserved
// with it. Those it cannot read come back separately.
//
// A node older than 3.1.0 ignores max and answers one message in the
// single shape, so that is read too.
func (c *Client) decodeMessages(body []byte, topic string) ([]*Message, []undecodable, error) {
	var batch struct {
		Messages *[]json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(body, &batch); err != nil {
		return nil, nil, fmt.Errorf("narad: consume %s: decode reply: %w", topic, err)
	}
	if batch.Messages == nil {
		msg, err := c.decodeMessage(body, topic)
		if err != nil {
			return nil, nil, err
		}
		return []*Message{msg}, nil, nil
	}
	msgs := make([]*Message, 0, len(*batch.Messages))
	var bad []undecodable
	for _, raw := range *batch.Messages {
		msg := &Message{client: c}
		if err := json.Unmarshal(raw, msg); err != nil {
			bad = append(bad, c.partlyDecoded(raw, topic, err))
			continue
		}
		msgs = append(msgs, msg)
	}
	return msgs, bad, nil
}

// partlyDecoded reads what it can of a record that failed to decode: the
// fields that say where it is decode whatever the rest of it holds.
func (c *Client) partlyDecoded(raw json.RawMessage, topic string, cause error) undecodable {
	var where struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
		Receipt   string `json:"receipt_handle"`
	}
	_ = json.Unmarshal(raw, &where)
	if where.Topic == "" {
		where.Topic = topic
	}
	msg := &Message{
		Topic: where.Topic, Partition: where.Partition, Offset: where.Offset,
		Receipt: where.Receipt, client: c,
	}
	return undecodable{
		msg: msg,
		err: fmt.Errorf("narad: consume %s: decode %s: %w", topic, msg.describe(), cause),
	}
}

// dispatchBatch runs the handler over a batch of messages, one at a
// time, with every lease kept alive until its message is settled, and
// acks the successes together at the end, or as soon as shutdown
// begins.
func (c *Client) dispatchBatch(pollCtx, workCtx context.Context, h Handler, msgs []*Message, cfg consumeConfig) {
	leases := c.holdLeases(workCtx, msgs, cfg.visibility)
	defer leases.stop()

	reportLost := func(msg *Message, when string) {
		c.report(cfg, msg, fmt.Errorf("narad: %s: %w %s", msg.describe(), ErrLeaseLost, when))
		c.logger().Warn("narad: lease lost "+when,
			"topic", msg.Topic, "id", msg.ID(), "offset", msg.Offset)
	}

	// succeeded holds the messages handled successfully and not yet
	// acked. Shutdown acks them from another goroutine.
	var mu sync.Mutex
	var succeeded []int
	flush := func() {
		mu.Lock()
		held := succeeded
		succeeded = nil
		mu.Unlock()

		// A lease lost before the ack could go out means the message is
		// on its way to somebody else, so it is reported rather than
		// acked.
		var toAck []*Message
		for _, i := range held {
			if leases.release(i) {
				reportLost(msgs[i], "before the ack could be sent")
				continue
			}
			toAck = append(toAck, msgs[i])
		}
		settleCtx, cancel := settleContext(workCtx)
		defer cancel()
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
	}
	// Once shutdown begins, the message in hand may outlast the grace
	// period and be abandoned, and the messages already handled must not
	// go down with it, so they are acked at once.
	flushed := make(chan struct{})
	stopFlush := context.AfterFunc(pollCtx, func() {
		defer close(flushed)
		flush()
	})

	var unstarted []int
	for i, msg := range msgs {
		if pollCtx.Err() != nil {
			// Shutting down: whatever has not started goes back now
			// rather than when its lease lapses.
			unstarted = append(unstarted, i)
			continue
		}
		if leases.lost(i) {
			leases.release(i)
			reportLost(msg, "before the handler started")
			continue
		}

		err := safely(leases.context(i), h, msg)

		if !msg.Leased() {
			leases.release(i)
			continue
		}
		// As in dispatch, a message the handler settled itself is not
		// settled again.
		settled := msg.isSettled()
		if err == nil && !settled {
			// The lease stays renewed until the ack goes out.
			mu.Lock()
			succeeded = append(succeeded, i)
			mu.Unlock()
			continue
		}
		if lost := leases.release(i); lost && !settled {
			reportLost(msg, "while the handler was still running")
			continue
		}
		if err == nil {
			continue
		}
		c.report(cfg, msg, err)
		c.logger().Warn("narad: handler failed",
			"topic", msg.Topic, "id", msg.ID(), "offset", msg.Offset,
			"requeued", cfg.requeue && !settled, "err", err)
		if cfg.requeue && !settled {
			settleCtx, cancel := settleContext(workCtx)
			c.handBack(settleCtx, msg)
			cancel()
		}
	}

	if !stopFlush() {
		<-flushed
	}
	flush()

	var toHandBack []*Message
	for _, i := range unstarted {
		if !leases.release(i) {
			toHandBack = append(toHandBack, msgs[i])
		}
	}
	settleCtx, cancel := settleContext(workCtx)
	defer cancel()
	for i, err := range c.settleMany(settleCtx, opNack, toHandBack) {
		if err != nil {
			c.logger().Warn("narad: could not hand an unstarted message back, it will wait out its visibility timeout",
				"topic", toHandBack[i].Topic, "id", toHandBack[i].ID(), "err", err)
		}
	}
}

// batchLeases keeps the leases of a batch alive, with one extend request
// per round for all of them rather than one per message, until each
// message is released.
//
// Every lease is renewed, whatever [WithoutAutoExtend] says. That option
// is for a handler that reliably outruns its own lease, but in a batch a
// message also waits for the ones ahead of it and for the batch ack,
// which no handler's speed bounds.
type batchLeases struct {
	client *Client
	items  []batchLease
	// mu guards the items' held and gone flags.
	mu sync.Mutex
	// round is held for the whole of a renewal round, so a release can
	// wait out the round that may be renewing its message.
	round    sync.Mutex
	done     chan struct{}
	finished chan struct{}
}

// batchLease is one message's lease within a batch.
type batchLease struct {
	msg    *Message
	ctx    context.Context
	cancel context.CancelFunc
	// held is true while the lease is being renewed.
	held bool
	// gone is set when a renewal found the lease lost.
	gone bool
}

// holdLeases starts renewing the leases of msgs every third of the
// visibility timeout, as keepLease does for one message.
func (c *Client) holdLeases(ctx context.Context, msgs []*Message, visibility time.Duration) *batchLeases {
	l := &batchLeases{
		client:   c,
		items:    make([]batchLease, len(msgs)),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	for i, msg := range msgs {
		itemCtx, cancel := context.WithCancel(ctx)
		l.items[i] = batchLease{msg: msg, ctx: itemCtx, cancel: cancel, held: msg.Leased()}
	}
	every := max(visibility/3, time.Second)
	go func() {
		defer close(l.finished)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-l.done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				l.renew(ctx)
			}
		}
	}()
	return l
}

// renew extends every lease still held, in one request, and marks the
// ones the broker says are gone.
func (l *batchLeases) renew(ctx context.Context) {
	l.round.Lock()
	defer l.round.Unlock()

	var index []int
	var msgs []*Message
	l.mu.Lock()
	for i := range l.items {
		// A message its handler settled has no lease left to renew,
		// and a 410 for it would not mean it was lost.
		if it := &l.items[i]; it.held && !it.msg.isSettled() {
			index = append(index, i)
			msgs = append(msgs, it.msg)
		}
	}
	l.mu.Unlock()
	if len(msgs) == 0 {
		return
	}

	extendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for j, err := range l.client.settleMany(extendCtx, opExtend, msgs) {
		// Any other failure is not proof the lease is gone, and the
		// next round is still inside the window.
		if !errorIs(err, ErrLeaseLost) || msgs[j].isSettled() {
			continue
		}
		it := &l.items[index[j]]
		l.mu.Lock()
		it.held, it.gone = false, true
		l.mu.Unlock()
		// The message is back in the queue and may already be running
		// somewhere else, so its handler is told to stop.
		it.cancel()
	}
}

// context is the context the handler of message i runs under. It is
// cancelled when the lease is lost or the message released.
func (l *batchLeases) context(i int) context.Context { return l.items[i].ctx }

// lost reports whether message i's lease went away while it was held.
func (l *batchLeases) lost(i int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.items[i].gone
}

// release stops renewing message i's lease, once any round renewing it
// has finished, and reports whether the lease went away while it was
// held.
func (l *batchLeases) release(i int) (lost bool) {
	l.round.Lock()
	l.mu.Lock()
	it := &l.items[i]
	it.held = false
	lost = it.gone
	l.mu.Unlock()
	l.round.Unlock()
	it.cancel()
	return lost
}

// stop ends the renewals and waits for them to finish.
func (l *batchLeases) stop() {
	close(l.done)
	<-l.finished
	for i := range l.items {
		l.items[i].cancel()
	}
}

// settleContext bounds a settle. As in dispatch, finishing it matters
// more than shutting down promptly, so it outlives a cancelled worker,
// but it gets its own short deadline so it cannot hang.
func settleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
}

// settleMany acks, nacks or extends messages of one topic, with one request per
// round instead of one per message, and returns each message's outcome
// in order.
//
// The broker settles every receipt handle on its own and answers with
// the status a single ack of it would have got, so each message is
// treated exactly as [Message.Ack], [Message.Nack] or [Message.Extend]
// would treat it: 204 settles it, a retryable status is tried again,
// and a 410 is [ErrLeaseLost] unless an earlier round of an ack for that
// message may have landed, when, as for a single ack, it means that
// round spent the handle. A broker that refuses the batch form gets one request per
// message instead.
func (c *Client) settleMany(ctx context.Context, op string, msgs []*Message) []error {
	errs := make([]error, len(msgs))
	if len(msgs) == 0 {
		return errs
	}
	extra := map[string]string(nil)
	batchOp := opAckBatch
	switch op {
	case opNack:
		extra = map[string]string{"extend": "0"}
		batchOp = opNackBatch
	case opExtend:
		extra = map[string]string{"extend": "true"}
		batchOp = opExtendBatch
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
