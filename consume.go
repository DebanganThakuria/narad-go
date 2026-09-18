package narad

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// DefaultPollWait is how long a consume waits for a message before
// asking again. It matches the ceiling a stock broker applies; a longer
// wait is clamped by the server rather than refused.
const DefaultPollWait = 10 * time.Second

// A Handler processes one message.
//
// Returning nil acks it. Returning an error hands it back for
// redelivery, so a handler must be idempotent: Narad is at-least-once,
// and a message can arrive again after a broker restart even when the
// handler succeeded.
//
// The context is cancelled when the consumer is shutting down and, more
// importantly, when the message's lease is lost. A cancelled context
// means another consumer may already have this message, so stop rather
// than finish and write the result twice.
type Handler interface {
	Handle(ctx context.Context, msg *Message) error
}

// A HandlerFunc turns a function into a [Handler].
type HandlerFunc func(ctx context.Context, msg *Message) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error { return f(ctx, msg) }

// A ConsumeOption adjusts a receive or a consume loop.
type ConsumeOption func(*consumeConfig)

type consumeConfig struct {
	wait       time.Duration
	partition  int
	pinned     bool
	workers    int
	autoExtend bool
	visibility time.Duration
	requeue    bool
	onError    func(*Message, error)
	grace      time.Duration
}

func consumeDefaults() consumeConfig {
	return consumeConfig{
		wait:       DefaultPollWait,
		workers:    1,
		autoExtend: true,
		requeue:    true,
		grace:      30 * time.Second,
	}
}

// WithWait sets how long one poll waits for a message before it gives up
// and asks again. It defaults to [DefaultPollWait].
//
// Long polling is what keeps an idle consumer from spinning: the broker
// holds the request open until something arrives.
func WithWait(d time.Duration) ConsumeOption {
	return func(c *consumeConfig) { c.wait = d }
}

// FromPartition takes messages from one partition only, instead of
// whichever has work.
func FromPartition(partition int) ConsumeOption {
	return func(c *consumeConfig) {
		c.partition, c.pinned = partition, true
	}
}

// WithWorkers sets how many messages [Client.Consume] handles at once.
// It defaults to one.
//
// Each worker runs its own poll, because the broker hands out one
// message per request. Raising this raises the connections held open, so
// see [WithIdleConnections] if you go far above the default.
func WithWorkers(n int) ConsumeOption {
	return func(c *consumeConfig) { c.workers = n }
}

// WithoutAutoExtend stops [Client.Consume] renewing a message's lease
// while its handler runs.
//
// Only turn it off when handlers are reliably faster than the visibility
// timeout. Otherwise the message is redelivered underneath the handler
// and the work is done twice.
func WithoutAutoExtend() ConsumeOption {
	return func(c *consumeConfig) { c.autoExtend = false }
}

// WithoutRequeue leaves a failed message to wait out its visibility
// timeout instead of handing it back at once.
//
// Requeuing is the right default for a transient failure and the wrong
// one for a message that will always fail: Narad has no dead-letter
// queue, so a message handed back forever is redelivered forever.
func WithoutRequeue() ConsumeOption {
	return func(c *consumeConfig) { c.requeue = false }
}

// WithErrorHandler is called when a handler returns an error, when a
// lease is lost mid-handler, or when an ack fails after the work
// succeeded. The message is nil when the failure was not about one.
func WithErrorHandler(fn func(msg *Message, err error)) ConsumeOption {
	return func(c *consumeConfig) { c.onError = fn }
}

// WithShutdownGrace sets how long [Client.Consume] waits for in-flight
// handlers once its context ends. It defaults to thirty seconds.
//
// It is a bound, not a suggestion. A handler that ignores its context
// cannot be stopped, and waiting for it means Consume outlives the
// grace period its orchestrator gave the process, which gets the process
// killed and every in-flight ack lost. Abandoning the work costs one
// redelivery instead.
func WithShutdownGrace(d time.Duration) ConsumeOption {
	return func(c *consumeConfig) { c.grace = d }
}

// Receive takes one message, waiting until there is one.
//
// It keeps polling until a message arrives or ctx ends, so it returns
// either a message or an error and never both nil. Give ctx a deadline
// to bound the wait.
//
// The message holds a lease for the topic's visibility timeout. Settle
// it with [Message.Ack], [Message.Nack] or [Message.Extend]. For
// anything beyond one message, use [Client.Consume], which manages the
// lease for you.
func (c *Client) Receive(ctx context.Context, topic string, opts ...ConsumeOption) (*Message, error) {
	cfg := consumeDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	for {
		msg, err := c.poll(ctx, topic, cfg)
		if err != nil {
			return nil, err
		}
		if msg != nil {
			return msg, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

// ReadAt reads the record at an offset without taking it off the queue.
//
// This is replay: it reserves nothing and settles nothing, so the
// message holds no lease and must not be acked. It is how you re-read
// history that has already been consumed.
//
// It returns nil when the offset has not been written yet, which is how
// you know you have caught up with the end of the log, and
// [ErrOffsetGone] when the offset has aged out of retention.
func (c *Client) ReadAt(ctx context.Context, topic string, partition int, offset int64) (*Message, error) {
	if topic == "" {
		return nil, fmt.Errorf("narad: read: %w: topic is required", ErrBadRequest)
	}
	if offset < 0 {
		return nil, fmt.Errorf("narad: read %s: %w: offset must not be negative", topic, ErrBadRequest)
	}
	query := url.Values{
		"partition": {strconv.Itoa(partition)},
		"offset":    {strconv.FormatInt(offset, 10)},
	}
	res, err := c.do(ctx, call{
		method: http.MethodGet,
		path:   "/v1/topics/" + url.PathEscape(topic) + "/consume",
		query:  query.Encode(),
		op:     opRead,
		topic:  topic,
		ok:     []int{http.StatusOK, http.StatusNoContent},
	})
	if err != nil {
		// A 410 means something different here than on an ack: not a
		// lapsed lease, which a read never takes, but an offset that
		// aged out or cannot be read.
		if errorIs(err, ErrLeaseLost) {
			return nil, &goneError{topic: topic, offset: offset, cause: err}
		}
		return nil, err
	}
	if res.status == http.StatusNoContent {
		return nil, nil
	}
	return c.decodeMessage(res.body, topic)
}

// Consume takes messages and hands them to h until ctx ends.
//
// It is the call most programs want. It long-polls, runs a pool of
// workers, keeps each message's lease alive while its handler runs, acks
// on success and hands the message back on failure. A panicking handler
// is recovered so one bad message cannot take the consumer down.
//
//	err := client.Consume(ctx, "orders", narad.HandlerFunc(
//		func(ctx context.Context, msg *narad.Message) error {
//			return process(ctx, msg)
//		}))
//
// It returns nil on a clean shutdown, which is what a cancelled context
// is. In-flight handlers get [WithShutdownGrace] to finish and ack;
// anything still running then is abandoned, and its message is
// redelivered.
func (c *Client) Consume(ctx context.Context, topic string, h Handler, opts ...ConsumeOption) error {
	if topic == "" {
		return fmt.Errorf("narad: consume: %w: topic is required", ErrBadRequest)
	}
	if h == nil {
		return fmt.Errorf("narad: consume %s: %w: handler is required", topic, ErrBadRequest)
	}
	cfg := consumeDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.workers < 1 {
		cfg.workers = 1
	}
	if cfg.autoExtend && cfg.visibility <= 0 {
		cfg.visibility = c.visibilityOf(ctx, topic)
	}

	// Handlers outlive the caller's context by the grace period, so work
	// already in hand can finish and ack. Polling stops at once.
	work, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()

	var wg sync.WaitGroup
	for range cfg.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.worker(ctx, work, topic, h, cfg)
		}()
	}

	<-ctx.Done()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(cfg.grace):
		stopWork()
		// A moment for a handler that does watch its context to notice
		// and ack, then leave regardless.
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		return nil
	}
}

// visibilityOf reads the topic's visibility timeout, falling back to
// something short.
//
// Guessing short matters. The two errors are not symmetric: guess long
// and the first renewal arrives after the lease has already lapsed, so
// every message is quietly handled twice. Guess short and it costs a few
// extra renewals.
func (c *Client) visibilityOf(ctx context.Context, topic string) time.Duration {
	if info, err := c.Topic(ctx, topic); err == nil && info.VisibilityTimeout > 0 {
		return info.VisibilityTimeout
	}
	return 5 * time.Second
}

// worker runs one poll, handle and settle loop.
//
// pollCtx ends when the caller cancels, so polling stops at once.
// workCtx outlives it by the grace period, so a handler already holding
// a message can finish and ack it.
func (c *Client) worker(pollCtx, workCtx context.Context, topic string, h Handler, cfg consumeConfig) {
	failures := 0
	for pollCtx.Err() == nil {
		msg, err := c.poll(pollCtx, topic, cfg)
		if err != nil {
			if pollCtx.Err() != nil || errors.Is(err, ErrClosed) {
				return
			}
			failures++
			c.report(cfg, nil, err)
			c.logger().Warn("narad: poll failed",
				"topic", topic, "consecutive", failures, "err", err)
			// poll already retried inside the client. Getting here means
			// the cluster is not answering, so back off rather than
			// hot-looping on a broker that is struggling.
			if !wait(pollCtx, c.cfg.backoff.delay(failures-1)) {
				return
			}
			continue
		}
		failures = 0
		if msg != nil {
			c.dispatch(workCtx, h, msg, cfg)
		}
	}
}

// poll asks once for a message. It returns nil, nil when the wait
// elapsed with nothing available, which is an ordinary outcome.
func (c *Client) poll(ctx context.Context, topic string, cfg consumeConfig) (*Message, error) {
	if topic == "" {
		return nil, fmt.Errorf("narad: consume: %w: topic is required", ErrBadRequest)
	}
	query := url.Values{}
	if cfg.wait > 0 {
		query.Set("wait", cfg.wait.String())
	}
	if cfg.pinned {
		query.Set("partition", strconv.Itoa(cfg.partition))
	}

	// The request has to outlive the poll the server was asked to hold,
	// plus the round trip. The configured timeout would cancel it.
	timeout := c.cfg.timeout
	if cfg.wait > 0 {
		timeout = cfg.wait + 10*time.Second
	}

	res, err := c.do(ctx, call{
		method:  http.MethodGet,
		path:    "/v1/topics/" + url.PathEscape(topic) + "/consume",
		query:   query.Encode(),
		op:      opConsume,
		topic:   topic,
		ok:      []int{http.StatusOK, http.StatusNoContent},
		timeout: timeout,
	})
	if err != nil {
		return nil, err
	}
	if res.status == http.StatusNoContent {
		return nil, nil
	}
	return c.decodeMessage(res.body, topic)
}

func (c *Client) decodeMessage(body []byte, topic string) (*Message, error) {
	msg := &Message{client: c}
	if err := decode(body, msg, "consume "+topic); err != nil {
		return nil, err
	}
	return msg, nil
}

// dispatch runs the handler for one message with its lease managed
// around it.
func (c *Client) dispatch(ctx context.Context, h Handler, msg *Message, cfg consumeConfig) {
	handlerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var keeper *leaseKeeper
	if cfg.autoExtend && msg.Leased() {
		keeper = keepLease(handlerCtx, cancel, msg, cfg.visibility)
	}

	err := safely(handlerCtx, h, msg)

	if keeper != nil {
		keeper.stop()
	}
	if !msg.Leased() {
		return
	}
	if keeper != nil && keeper.lost() {
		// The message is already back in the queue. Acking now would
		// either fail or settle a lease somebody else is holding.
		lost := fmt.Errorf("narad: %s: %w while the handler was still running", msg.describe(), ErrLeaseLost)
		c.report(cfg, msg, lost)
		c.logger().Warn("narad: lease lost mid-handler",
			"topic", msg.Topic, "id", msg.ID(), "offset", msg.Offset)
		return
	}

	// Finishing the settle matters more than shutting down promptly: an
	// ack dropped here becomes a redelivery of work already done. Give
	// it its own short deadline so it cannot hang.
	settleCtx, cancelSettle := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancelSettle()

	if err != nil {
		c.report(cfg, msg, err)
		c.logger().Warn("narad: handler failed",
			"topic", msg.Topic, "id", msg.ID(), "offset", msg.Offset,
			"requeued", cfg.requeue, "err", err)
		if cfg.requeue {
			if nackErr := msg.Nack(settleCtx); nackErr != nil {
				c.logger().Warn("narad: could not hand the message back, it will wait out its visibility timeout",
					"topic", msg.Topic, "id", msg.ID(), "err", nackErr)
			}
		}
		return
	}
	if ackErr := msg.Ack(settleCtx); ackErr != nil {
		wrapped := fmt.Errorf("narad: %s: the handler succeeded but the ack failed, so the message will be redelivered: %w",
			msg.describe(), ackErr)
		c.report(cfg, msg, wrapped)
		c.logger().Error("narad: ack failed after successful handling",
			"topic", msg.Topic, "id", msg.ID(), "offset", msg.Offset, "err", ackErr)
	}
}

func (c *Client) report(cfg consumeConfig, msg *Message, err error) {
	if cfg.onError != nil {
		cfg.onError(msg, err)
	}
}

// safely runs the handler, turning a panic into an error so one bad
// message cannot take the consumer down.
func safely(ctx context.Context, h Handler, msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("narad: handler panicked: %v", r)
		}
	}()
	return h.Handle(ctx, msg)
}

// A leaseKeeper renews a message's lease while its handler runs.
type leaseKeeper struct {
	once     sync.Once
	done     chan struct{}
	finished chan struct{}
	gone     chan struct{}
}

// keepLease renews the lease on a timer until stopped.
//
// It renews every third of the visibility timeout, so two renewals can
// fail and the lease still survives to the third. Losing it cancels the
// handler's context, because the message has gone back to the queue and
// may already be running somewhere else.
func keepLease(ctx context.Context, cancelHandler context.CancelFunc, msg *Message, visibility time.Duration) *leaseKeeper {
	every := visibility / 3
	if every < time.Second {
		every = time.Second
	}
	k := &leaseKeeper{
		done:     make(chan struct{}),
		finished: make(chan struct{}),
		gone:     make(chan struct{}),
	}
	go func() {
		defer close(k.finished)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-k.done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				extendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				err := msg.Extend(extendCtx)
				cancel()
				if err == nil {
					continue
				}
				if errorIs(err, ErrLeaseLost) {
					close(k.gone)
					cancelHandler()
					return
				}
				// A transient failure is not proof the lease is gone.
				// The next tick is still inside the window.
			}
		}
	}()
	return k
}

// stop ends the renewals and waits for the goroutine to finish.
func (k *leaseKeeper) stop() {
	k.once.Do(func() { close(k.done) })
	<-k.finished
}

// lost reports whether the lease went away while the handler ran.
func (k *leaseKeeper) lost() bool {
	select {
	case <-k.gone:
		return true
	default:
		return false
	}
}

// goneError reports a read of an offset that is no longer readable,
// keeping the underlying Error reachable.
type goneError struct {
	topic  string
	offset int64
	cause  error
}

func (e *goneError) Error() string {
	return fmt.Sprintf("narad: read %s offset %d: %v", e.topic, e.offset, e.cause)
}

// Unwrap returns both the sentinel and the original error, so
// errors.Is(err, ErrOffsetGone) and errors.As(err, &apiErr) both work.
func (e *goneError) Unwrap() []error { return []error{ErrOffsetGone, e.cause} }

// errorIs is errors.Is, named here so the hot paths read cleanly.
func errorIs(err, target error) bool { return errors.Is(err, target) }

// ReadFrom streams a partition's records into h, starting at offset and
// stopping when it reaches the end of the log.
//
// Nothing is reserved and nothing is settled, so the messages hold no
// lease and must not be acked. This is the call for reading history
// back: auditing what was published, rebuilding a projection, or working
// out what a consumer did with a message last Tuesday.
//
// Offsets that have aged out of retention are skipped rather than
// reported, because a replay that starts before the retention window is
// the normal case and stopping on it would be useless. A handler error
// stops the read and is returned.
//
//	err := client.ReadFrom(ctx, "orders", 0, 0, narad.HandlerFunc(
//		func(ctx context.Context, msg *narad.Message) error {
//			return audit(msg)
//		}))
func (c *Client) ReadFrom(ctx context.Context, topic string, partition int, offset int64, h Handler) error {
	if h == nil {
		return fmt.Errorf("narad: read %s: %w: handler is required", topic, ErrBadRequest)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		msg, err := c.ReadAt(ctx, topic, partition, offset)
		switch {
		case errorIs(err, ErrOffsetGone):
			// Aged out. The next one may still be there.
			offset++
			continue
		case err != nil:
			return err
		case msg == nil:
			// Caught up with the end of this partition's log.
			return nil
		}
		if err := h.Handle(ctx, msg); err != nil {
			return fmt.Errorf("narad: read %s: %w", msg.describe(), err)
		}
		offset = msg.Offset + 1
	}
}

// Replay streams every record the topic still retains into h, one
// partition at a time, oldest first.
//
// It is the "something went wrong, let me look at everything" call.
// Nothing is reserved and nothing is settled, so a replay does not
// disturb consumers working the same topic and can be run against
// production without taking work away from anybody.
//
// It reads what exists when it starts and stops at the end of each
// partition, so it terminates on a live topic rather than following new
// writes. Records that aged out of retention are not there to be read:
// this is the log, not the whole history.
//
// There is no ordering across partitions. Within one, records come in
// offset order.
//
//	err := client.Replay(ctx, "orders", narad.HandlerFunc(
//		func(ctx context.Context, msg *narad.Message) error {
//			fmt.Println(msg.Partition, msg.Offset, msg.ID())
//			return nil
//		}))
func (c *Client) Replay(ctx context.Context, topic string, h Handler) error {
	if h == nil {
		return fmt.Errorf("narad: replay %s: %w: handler is required", topic, ErrBadRequest)
	}
	info, err := c.Topic(ctx, topic)
	if err != nil {
		return fmt.Errorf("narad: replay %s: %w", topic, err)
	}
	// The partition stats say where each log starts, so a replay begins
	// at the oldest record still retained rather than at offset zero and
	// a long walk through offsets that aged out years ago.
	if len(info.PartitionStats) == 0 {
		for partition := range info.Partitions {
			if err := c.ReadFrom(ctx, topic, partition, 0, h); err != nil {
				return err
			}
		}
		return nil
	}
	for _, stats := range info.PartitionStats {
		if err := c.ReadFrom(ctx, topic, stats.Index, stats.Oldest, h); err != nil {
			return err
		}
	}
	return nil
}
