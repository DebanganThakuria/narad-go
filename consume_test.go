package narad

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeBroker is just enough of Narad to drive a consumer: a queue of
// messages to hand out, and a record of what was done to them.
type fakeBroker struct {
	mu           sync.Mutex
	pending      []*Message
	acked        []string
	nacked       []string
	extended     []string
	visibilityMs int64
	// extendStatus overrides the reply to an extend, for the lease-lost
	// path.
	extendStatus int
}

func (b *fakeBroker) queue(msgs ...*Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = append(b.pending, msgs...)
}

func (b *fakeBroker) counts() (acked, nacked, extended int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.acked), len(b.nacked), len(b.extended)
}

func (b *fakeBroker) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/consume"):
			b.mu.Lock()
			if len(b.pending) == 0 {
				b.mu.Unlock()
				time.Sleep(5 * time.Millisecond)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next := b.pending[0]
			b.pending = b.pending[1:]
			b.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(next)

		case strings.HasSuffix(r.URL.Path, "/ack"):
			receipt := r.URL.Query().Get("receipt_handle")
			b.mu.Lock()
			switch r.URL.Query().Get("extend") {
			case "true":
				b.extended = append(b.extended, receipt)
				status := b.extendStatus
				b.mu.Unlock()
				if status != 0 {
					w.WriteHeader(status)
					return
				}
				w.WriteHeader(http.StatusNoContent)
				return
			case "0":
				b.nacked = append(b.nacked, receipt)
			default:
				b.acked = append(b.acked, receipt)
			}
			b.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)

		default: // topic lookup
			b.mu.Lock()
			visibility := b.visibilityMs
			b.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"name":"orders","partitions":3,"visibility_timeout_ms":%d}`, visibility)
		}
	}
}

func newBrokerClient(t *testing.T, b *fakeBroker) *Client {
	t.Helper()
	server := httptest.NewServer(b.handler())
	t.Cleanup(server.Close)
	c, err := New(server.URL, WithRetries(2),
		WithBackoff(time.Millisecond, time.Millisecond), WithoutBreaker())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func msgAt(offset int64, receipt string) *Message {
	return &Message{
		Topic: "orders", Partition: 1, Offset: offset,
		Payload: json.RawMessage(fmt.Sprintf(`{"id":"o%d"}`, offset)),
		Receipt: receipt,
	}
}

// waitFor polls a condition so a timing-sensitive assertion fails with a
// message rather than a deadlock.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestReceiveWaitsForAMessage(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 30_000}
	c := newBrokerClient(t, b)

	// Nothing queued yet: Receive keeps polling rather than returning an
	// empty answer the caller has to interpret.
	go func() {
		time.Sleep(50 * time.Millisecond)
		b.queue(msgAt(1, "1:1:11"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, err := c.Receive(ctx, "orders", WithWait(20*time.Millisecond))
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if msg == nil || msg.Offset != 1 {
		t.Fatalf("msg = %v", msg)
	}
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if acked, _, _ := b.counts(); acked != 1 {
		t.Errorf("acked = %d, want 1", acked)
	}
}

func TestReceiveStopsWhenTheContextEnds(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 30_000}
	c := newBrokerClient(t, b)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	msg, err := c.Receive(ctx, "orders", WithWait(10*time.Millisecond))
	if err == nil {
		t.Fatal("Receive should have failed once the context ended")
	}
	if msg != nil {
		t.Error("Receive returned both a message and an error")
	}
}

func TestConsumeHandlesAndAcks(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 30_000}
	b.queue(msgAt(1, "1:1:11"), msgAt(2, "1:2:22"), msgAt(3, "1:3:33"))
	c := newBrokerClient(t, b)

	var handled atomic.Int32
	done := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = c.Consume(ctx, "orders", HandlerFunc(func(context.Context, *Message) error {
			if handled.Add(1) == 3 {
				close(done)
			}
			return nil
		}), WithWorkers(2), WithWait(10*time.Millisecond))
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("handled only %d of 3", handled.Load())
	}
	waitFor(t, func() bool { a, _, _ := b.counts(); return a == 3 }, "all three to be acked")
}

// A failed handler must not ack. The message has to come back, which is
// the whole basis of at-least-once.
func TestConsumeHandsBackOnError(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 30_000}
	b.queue(msgAt(1, "1:1:11"))
	c := newBrokerClient(t, b)

	var reported atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_ = c.Consume(ctx, "orders", HandlerFunc(func(context.Context, *Message) error {
			return errors.New("downstream down")
		}), WithWait(10*time.Millisecond),
			WithErrorHandler(func(*Message, error) { reported.Add(1) }))
	}()

	waitFor(t, func() bool {
		acked, nacked, _ := b.counts()
		return nacked >= 1 && acked == 0
	}, "the message to be handed back and not acked")
	if reported.Load() == 0 {
		t.Error("the error handler never fired")
	}
}

func TestWithoutRequeueLeavesTheMessageToLapse(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 30_000}
	b.queue(msgAt(1, "1:1:11"))
	c := newBrokerClient(t, b)

	handled := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_ = c.Consume(ctx, "orders", HandlerFunc(func(context.Context, *Message) error {
			select {
			case handled <- struct{}{}:
			default:
			}
			return errors.New("nope")
		}), WithWait(10*time.Millisecond), WithoutRequeue())
	}()

	<-handled
	time.Sleep(200 * time.Millisecond)
	acked, nacked, _ := b.counts()
	if acked != 0 || nacked != 0 {
		t.Errorf("acked=%d nacked=%d, want the message simply left alone", acked, nacked)
	}
}

// One bad message must not take the consumer down.
func TestConsumeRecoversFromAPanic(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 30_000}
	b.queue(msgAt(1, "1:1:11"))
	c := newBrokerClient(t, b)

	var reported atomic.Value
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_ = c.Consume(ctx, "orders", HandlerFunc(func(context.Context, *Message) error {
			panic("boom")
		}), WithWait(10*time.Millisecond),
			WithErrorHandler(func(_ *Message, err error) { reported.Store(err) }))
	}()

	waitFor(t, func() bool { _, n, _ := b.counts(); return n >= 1 }, "the panicking message to be handed back")
	if err, _ := reported.Load().(error); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Errorf("reported %v, want a panic report", err)
	}
}

// Slow work must keep its message rather than have it redelivered
// underneath.
func TestConsumeRenewsTheLeaseForSlowWork(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 3_000} // renews every second
	b.queue(msgAt(1, "1:1:11"))
	c := newBrokerClient(t, b)

	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		_ = c.Consume(ctx, "orders", HandlerFunc(func(context.Context, *Message) error {
			<-release
			return nil
		}), WithWait(10*time.Millisecond))
	}()

	waitFor(t, func() bool { _, _, e := b.counts(); return e >= 2 }, "the lease to be renewed twice")
	close(release)
	waitFor(t, func() bool { a, _, _ := b.counts(); return a == 1 }, "the message to be acked after the work")
}

// Losing the lease means somebody else may have the message. The
// handler must be cancelled, and the message must not then be acked:
// that ack would settle another consumer's lease.
func TestConsumeCancelsTheHandlerWhenTheLeaseIsLost(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 3_000, extendStatus: http.StatusGone}
	b.queue(msgAt(1, "1:1:11"))
	c := newBrokerClient(t, b)

	cancelled := make(chan struct{})
	reported := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		_ = c.Consume(ctx, "orders", HandlerFunc(func(hctx context.Context, _ *Message) error {
			<-hctx.Done()
			close(cancelled)
			return hctx.Err()
		}), WithWait(10*time.Millisecond),
			WithErrorHandler(func(*Message, error) {
				select {
				case reported <- struct{}{}:
				default:
				}
			}))
	}()

	select {
	case <-cancelled:
	case <-time.After(8 * time.Second):
		t.Fatal("the handler was never cancelled after the lease was lost")
	}
	select {
	case <-reported:
	case <-time.After(3 * time.Second):
		t.Fatal("the lost lease was never reported")
	}

	time.Sleep(200 * time.Millisecond)
	acked, nacked, _ := b.counts()
	if acked != 0 || nacked != 0 {
		t.Errorf("acked=%d nacked=%d, want neither: the lease belongs to someone else now", acked, nacked)
	}
}

// ShutdownGrace is a bound, not a suggestion. A handler that ignores its
// context cannot be stopped, and waiting for it means Consume outlives
// the grace period the orchestrator gave the process.
func TestShutdownGraceIsHonouredByAnUncancellableHandler(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 30_000}
	b.queue(msgAt(1, "1:1:11"))
	c := newBrokerClient(t, b)

	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(done)
		_ = c.Consume(ctx, "orders", HandlerFunc(func(context.Context, *Message) error {
			close(started)
			<-release // deliberately ignores cancellation
			return nil
		}), WithWait(10*time.Millisecond), WithShutdownGrace(200*time.Millisecond))
	}()

	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Consume blocked on a handler that ignores its context")
	}
}

// Shutting down must not abandon work that is nearly finished: an
// unacked message is redelivered, and redoing it is what a graceful
// shutdown exists to avoid.
func TestConsumeFinishesInFlightWorkOnShutdown(t *testing.T) {
	t.Parallel()

	b := &fakeBroker{visibilityMs: 30_000}
	b.queue(msgAt(1, "1:1:11"))
	c := newBrokerClient(t, b)

	started := make(chan struct{})
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(done)
		_ = c.Consume(ctx, "orders", HandlerFunc(func(context.Context, *Message) error {
			close(started)
			time.Sleep(300 * time.Millisecond)
			return nil
		}), WithWait(10*time.Millisecond), WithShutdownGrace(3*time.Second))
	}()

	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return")
	}
	if acked, _, _ := b.counts(); acked != 1 {
		t.Errorf("acked = %d, want the in-flight message acked before shutdown finished", acked)
	}
}

func TestConsumeRejectsBadArguments(t *testing.T) {
	t.Parallel()

	c := newBrokerClient(t, &fakeBroker{})
	ctx := context.Background()
	if err := c.Consume(ctx, "", HandlerFunc(func(context.Context, *Message) error { return nil })); !errors.Is(err, ErrBadRequest) {
		t.Errorf("empty topic: %v", err)
	}
	if err := c.Consume(ctx, "orders", nil); !errors.Is(err, ErrBadRequest) {
		t.Errorf("nil handler: %v", err)
	}
}

// A read at an offset reserves nothing, so there is nothing to settle
// and trying to is a mistake worth naming.
func TestReadAtReturnsAnUnleasedMessage(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("offset"); got != "7" {
			t.Errorf("offset = %q, want 7", got)
		}
		if got := r.URL.Query().Get("partition"); got != "2" {
			t.Errorf("partition = %q, want 2", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"topic":"orders","partition":2,"offset":7,"payload":{"id":"o7"},"timestamp":1700000000}`))
	})

	msg, err := c.ReadAt(context.Background(), "orders", 2, 7)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if msg.Leased() {
		t.Error("a read at an offset must not hold a lease")
	}
	if err := msg.Ack(context.Background()); !errors.Is(err, ErrNoLease) {
		t.Errorf("Ack = %v, want ErrNoLease", err)
	}
	if got, want := msg.Time().UTC().Year(), 2023; got != want {
		t.Errorf("Time() year = %d, want %d: the timestamp is Unix seconds", got, want)
	}
}

func TestReadAtReportsTheEndOfTheLog(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	msg, err := c.ReadAt(context.Background(), "orders", 0, 99)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if msg != nil {
		t.Error("an unwritten offset should read as nil, not an error")
	}
}

// A 410 means a lapsed lease on an ack but an aged-out offset on a read.
// Reporting the first for the second sends a caller looking for a lease
// that never existed.
func TestReadAtReportsAnAgedOutOffset(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone)
		_, _ = w.Write([]byte(`{"error":"offset 5 aged out of retention"}`))
	})

	_, err := c.ReadAt(context.Background(), "orders", 1, 5)
	if !errors.Is(err, ErrOffsetGone) {
		t.Fatalf("err = %v, want ErrOffsetGone", err)
	}
	// The status and node must survive the wrapping, or the SDK breaks
	// its own promise that Error carries them.
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want the *Error still reachable", err)
	}
	if apiErr.Status != http.StatusGone || apiErr.Node == "" {
		t.Errorf("error = %+v", apiErr)
	}
	if Retryable(err) {
		t.Error("an aged-out offset is permanent")
	}
}

func TestSettleModes(t *testing.T) {
	t.Parallel()

	var modes []string
	var receipt string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		modes = append(modes, r.URL.Query().Get("extend"))
		receipt = r.URL.Query().Get("receipt_handle")
		w.WriteHeader(http.StatusNoContent)
	})

	ctx := context.Background()
	msg := &Message{Topic: "orders", Receipt: "2:17:99", client: c}
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := msg.Extend(ctx); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if err := msg.Nack(ctx); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	for i, want := range []string{"", "true", "0"} {
		if modes[i] != want {
			t.Errorf("request %d extend = %q, want %q", i, modes[i], want)
		}
	}
	if receipt != "2:17:99" {
		t.Errorf("receipt = %q, want it echoed verbatim", receipt)
	}
}

func TestAckOnAFirstAttemptGoneIsALostLease(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	msg := &Message{Topic: "orders", Receipt: "2:17:99", client: c}
	if err := msg.Ack(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Ack = %v, want ErrLeaseLost", err)
	}
}

// The ambiguous ack: the first reply was lost, the retry finds the
// receipt already spent. That 410 almost certainly means our own first
// attempt landed, and calling it a failure makes the caller redo work
// that was already committed.
func TestAckResolvesTheAmbiguousGoneAsSuccess(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			// Kill the connection after the server has the request:
			// exactly the shape of an ack whose reply never arrived.
			panic(http.ErrAbortHandler)
		}
		w.WriteHeader(http.StatusGone)
	})
	msg := &Message{Topic: "orders", Receipt: "2:17:99", client: c}
	if err := msg.Ack(context.Background()); err != nil {
		t.Fatalf("Ack = %v, want success: the first attempt had almost certainly landed", err)
	}
}

// A 503 is the router declining before anything reached the owner, so
// nothing happened and the following 410 is a genuinely lost lease.
func TestAckDoesNotResolveUnavailableThenGone(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "partition owner is down; retry later", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusGone)
	})
	msg := &Message{Topic: "orders", Receipt: "2:17:99", client: c}
	if err := msg.Ack(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Ack = %v, want ErrLeaseLost", err)
	}
}

// A dropped ack becomes a redelivery of work already done, so a 503 has
// to be retried rather than reported.
func TestAckRetriesUnavailable(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 2 {
			http.Error(w, "owner down", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	msg := &Message{Topic: "orders", Receipt: "2:17:99", client: c}
	if err := msg.Ack(context.Background()); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("attempts = %d, want 2", calls.Load())
	}
}

func TestPayloadDecoding(t *testing.T) {
	t.Parallel()

	t.Run("json stays verbatim", func(t *testing.T) {
		t.Parallel()
		m := &Message{Payload: json.RawMessage(`{"id":"o1","amount":5}`)}
		raw, err := m.Bytes()
		if err != nil || string(raw) != `{"id":"o1","amount":5}` {
			t.Fatalf("Bytes() = %q, err %v", raw, err)
		}
		var got order
		if err := m.Into(&got); err != nil || got.ID != "o1" {
			t.Errorf("Into gave %+v, err %v", got, err)
		}
	})

	t.Run("text loses its transport quotes", func(t *testing.T) {
		t.Parallel()
		m := &Message{Payload: json.RawMessage(`"hello world"`)}
		if got := m.Text(); got != "hello world" {
			t.Errorf("Text() = %q", got)
		}
	})

	t.Run("binary is decoded", func(t *testing.T) {
		t.Parallel()
		m := &Message{Payload: json.RawMessage(`"AAECg/8="`), Encoding: "base64"}
		raw, err := m.Bytes()
		if err != nil {
			t.Fatalf("Bytes: %v", err)
		}
		if string(raw) != string([]byte{0, 1, 2, 0x83, 0xff}) {
			t.Errorf("Bytes() = %v", raw)
		}
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		for _, payload := range []string{"", "null"} {
			m := &Message{Payload: json.RawMessage(payload)}
			raw, err := m.Bytes()
			if err != nil || raw != nil {
				t.Errorf("Bytes(%q) = %q, err %v", payload, raw, err)
			}
		}
	})

	t.Run("corrupt base64 is an error", func(t *testing.T) {
		t.Parallel()
		m := &Message{Payload: json.RawMessage(`"!!!"`), Encoding: "base64"}
		if _, err := m.Bytes(); err == nil {
			t.Error("Bytes should fail on a payload marked base64 that is not")
		}
	})
}

// logBroker serves ReadAt against an in-memory log, so replay can be
// tested against something that behaves like a partitioned topic.
type logBroker struct {
	// records is partition index to the offsets it holds.
	records map[int]map[int64]string
	// oldest is the first offset each partition still retains.
	oldest map[int]int64
}

func (b *logBroker) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/consume") {
			// Topic lookup, with the stats replay uses to find the start
			// of each log.
			w.Header().Set("Content-Type", "application/json")
			stats := make([]string, 0, len(b.records))
			for partition := range b.records {
				stats = append(stats, fmt.Sprintf(
					`{"index":%d,"oldest_offset":%d}`, partition, b.oldest[partition]))
			}
			fmt.Fprintf(w, `{"name":"orders","partitions":%d,"partition_stats":[%s]}`,
				len(b.records), strings.Join(stats, ","))
			return
		}

		partition, _ := strconv.Atoi(r.URL.Query().Get("partition"))
		offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		body, ok := b.records[partition][offset]
		if !ok {
			// Below the retained window means gone; at or above the end
			// means not written yet.
			if offset < b.oldest[partition] {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusGone)
				_, _ = w.Write([]byte(`{"error":"aged out of retention"}`))
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"topic":"orders","partition":%d,"offset":%d,"payload":%q}`,
			partition, offset, body)
	}
}

func newLogClient(t *testing.T, b *logBroker) *Client {
	t.Helper()
	server := httptest.NewServer(b.handler())
	t.Cleanup(server.Close)
	c, err := New(server.URL, WithRetries(1), WithoutBreaker())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestReadFromStreamsToTheEndOfTheLog(t *testing.T) {
	t.Parallel()

	b := &logBroker{
		records: map[int]map[int64]string{0: {5: "a", 6: "b", 7: "c"}},
		oldest:  map[int]int64{0: 5},
	}
	c := newLogClient(t, b)

	var seen []int64
	err := c.ReadFrom(context.Background(), "orders", 0, 5, HandlerFunc(
		func(_ context.Context, msg *Message) error {
			seen = append(seen, msg.Offset)
			if msg.Leased() {
				t.Error("a replayed message must hold no lease")
			}
			return nil
		}))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(seen) != 3 || seen[0] != 5 || seen[2] != 7 {
		t.Errorf("offsets = %v, want 5,6,7 in order", seen)
	}
}

// Starting before the retention window is the normal case for a replay,
// so aged-out offsets are skipped rather than ending the read.
func TestReadFromSkipsAgedOutOffsets(t *testing.T) {
	t.Parallel()

	b := &logBroker{
		records: map[int]map[int64]string{0: {10: "a", 11: "b"}},
		oldest:  map[int]int64{0: 10},
	}
	c := newLogClient(t, b)

	var seen []int64
	err := c.ReadFrom(context.Background(), "orders", 0, 0, HandlerFunc(
		func(_ context.Context, msg *Message) error {
			seen = append(seen, msg.Offset)
			return nil
		}))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(seen) != 2 {
		t.Errorf("offsets = %v, want the two that are still retained", seen)
	}
}

func TestReadFromStopsOnAHandlerError(t *testing.T) {
	t.Parallel()

	b := &logBroker{
		records: map[int]map[int64]string{0: {0: "a", 1: "b", 2: "c"}},
		oldest:  map[int]int64{0: 0},
	}
	c := newLogClient(t, b)

	var count int
	boom := errors.New("stop here")
	err := c.ReadFrom(context.Background(), "orders", 0, 0, HandlerFunc(
		func(context.Context, *Message) error {
			count++
			return boom
		}))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the handler's error", err)
	}
	if count != 1 {
		t.Errorf("handled %d records, want it to stop at the first failure", count)
	}
}

// The "something went wrong, show me everything" call: every partition,
// starting where each log actually starts.
func TestReplayCoversEveryPartition(t *testing.T) {
	t.Parallel()

	b := &logBroker{
		records: map[int]map[int64]string{
			0: {100: "a", 101: "b"},
			1: {0: "c"},
			2: {7: "d", 8: "e", 9: "f"},
		},
		oldest: map[int]int64{0: 100, 1: 0, 2: 7},
	}
	c := newLogClient(t, b)

	perPartition := map[int]int{}
	err := c.Replay(context.Background(), "orders", HandlerFunc(
		func(_ context.Context, msg *Message) error {
			perPartition[msg.Partition]++
			return nil
		}))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	want := map[int]int{0: 2, 1: 1, 2: 3}
	for partition, count := range want {
		if perPartition[partition] != count {
			t.Errorf("partition %d gave %d records, want %d", partition, perPartition[partition], count)
		}
	}
}

func TestReplayAndReadFromNeedAHandler(t *testing.T) {
	t.Parallel()

	c := newLogClient(t, &logBroker{records: map[int]map[int64]string{}, oldest: map[int]int64{}})
	ctx := context.Background()
	if err := c.ReadFrom(ctx, "orders", 0, 0, nil); !errors.Is(err, ErrBadRequest) {
		t.Errorf("ReadFrom with no handler = %v", err)
	}
	if err := c.Replay(ctx, "orders", nil); !errors.Is(err, ErrBadRequest) {
		t.Errorf("Replay with no handler = %v", err)
	}
}

func TestReplayStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	records := map[int64]string{}
	for i := int64(0); i < 1000; i++ {
		records[i] = "x"
	}
	b := &logBroker{
		records: map[int]map[int64]string{0: records},
		oldest:  map[int]int64{0: 0},
	}
	c := newLogClient(t, b)

	ctx, cancel := context.WithCancel(context.Background())
	var count int
	err := c.ReadFrom(ctx, "orders", 0, 0, HandlerFunc(
		func(context.Context, *Message) error {
			count++
			if count == 5 {
				cancel()
			}
			return nil
		}))
	if err == nil {
		t.Fatal("ReadFrom should report the cancellation")
	}
	if count > 10 {
		t.Errorf("handled %d records after cancellation", count)
	}
}
