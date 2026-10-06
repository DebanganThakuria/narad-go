package narad

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// batchConsumeBroker serves batch consume and batch ack the way the
// broker does (handlers/messaging/consume_batch.go and ack_batch.go):
// GET /consume?max=N answers {"messages":[...]} or 204 and refuses a
// request without X-Narad-Client; POST /ack with a receipt_handles body
// answers 200 with one {"status","error"} per handle, in order.
type batchConsumeBroker struct {
	mu           sync.Mutex
	pending      []*Message
	visibilityMs int64

	// statuses scripts the answers for a handle, one per settle of it;
	// once used up, a handle settles with 204.
	statuses map[string][]int
	// extendStatus is the answer to an extend of a handle, 204 if unset.
	extendStatus map[string]int
	// singleShape answers a consume in the one-message shape, as a node
	// before 3.1.0 does whatever max says.
	singleShape bool
	// noBatchAck refuses a batch ack the way a node before 3.1.0 does.
	noBatchAck bool
	// failFirstBatchAck applies the first batch ack and then answers it
	// 500, as a broker whose reply was lost.
	failFirstBatchAck bool
	// garbleFirstBatchAck applies the first batch ack and then answers
	// 200 with this body, as a proxy that mangled the reply would.
	garbleFirstBatchAck *string

	maxes        []string
	batchAcks    [][]string // the handles of each batch ack, by request
	batchNacks   [][]string
	singleAcks   []string
	singleNacks  []string
	extended     []string
	acked        map[string]bool
	missingGuard bool
}

func (b *batchConsumeBroker) queue(msgs ...*Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = append(b.pending, msgs...)
}

// settle applies one settle of a handle and returns its status.
func (b *batchConsumeBroker) settle(handle string) int {
	if script := b.statuses[handle]; len(script) > 0 {
		b.statuses[handle] = script[1:]
		return script[0]
	}
	if b.acked == nil {
		b.acked = map[string]bool{}
	}
	if b.acked[handle] {
		return http.StatusGone
	}
	b.acked[handle] = true
	return http.StatusNoContent
}

func (b *batchConsumeBroker) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		defer b.mu.Unlock()
		query := r.URL.Query()
		switch {
		case strings.HasSuffix(r.URL.Path, "/consume"):
			max := query.Get("max")
			b.maxes = append(b.maxes, max)
			if max != "" && r.Header.Get("X-Narad-Client") == "" {
				b.missingGuard = true
				writeBrokerError(w, http.StatusBadRequest, "batch consume requires an X-Narad-Client header")
				return
			}
			n, _ := strconv.Atoi(max)
			if n < 1 || b.singleShape {
				n = 1
			}
			if len(b.pending) == 0 {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			n = min(n, len(b.pending))
			out := b.pending[:n]
			b.pending = b.pending[n:]
			w.Header().Set("Content-Type", "application/json")
			if max == "" || b.singleShape {
				_ = json.NewEncoder(w).Encode(out[0])
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"messages": out})

		case strings.HasSuffix(r.URL.Path, "/ack"):
			extend := query.Get("extend")
			if handle := query.Get("receipt_handle"); handle != "" {
				switch extend {
				case "true":
					b.extended = append(b.extended, handle)
					if status := b.extendStatus[handle]; status != 0 {
						writeBrokerError(w, status, "receipt handle is no longer valid")
						return
					}
					w.WriteHeader(http.StatusNoContent)
				case "0":
					b.singleNacks = append(b.singleNacks, handle)
					w.WriteHeader(http.StatusNoContent)
				default:
					b.singleAcks = append(b.singleAcks, handle)
					if status := b.settle(handle); status != http.StatusNoContent {
						writeBrokerError(w, status, "receipt handle is no longer valid")
						return
					}
					w.WriteHeader(http.StatusNoContent)
				}
				return
			}
			if b.noBatchAck {
				writeBrokerError(w, http.StatusBadRequest, "receipt_handle required")
				return
			}
			raw, _ := io.ReadAll(r.Body)
			var body struct {
				Handles []string `json:"receipt_handles"`
			}
			if err := json.Unmarshal(raw, &body); err != nil || len(body.Handles) == 0 {
				writeBrokerError(w, http.StatusBadRequest, "receipt_handles required")
				return
			}
			if extend == "0" {
				b.batchNacks = append(b.batchNacks, body.Handles)
			} else {
				b.batchAcks = append(b.batchAcks, body.Handles)
			}
			results := make([]map[string]any, len(body.Handles))
			for i, handle := range body.Handles {
				status := http.StatusNoContent
				if extend != "0" {
					status = b.settle(handle)
				}
				results[i] = map[string]any{"status": status}
				if status != http.StatusNoContent {
					results[i]["error"] = "receipt handle is no longer valid"
				}
			}
			if b.failFirstBatchAck && len(b.batchAcks) == 1 && extend != "0" {
				writeBrokerError(w, http.StatusInternalServerError, "ack failed")
				return
			}
			if b.garbleFirstBatchAck != nil && len(b.batchAcks) == 1 && extend != "0" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, *b.garbleFirstBatchAck)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"results": results})

		default: // topic lookup
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"name":"orders","partitions":3,"visibility_timeout_ms":%d}`, b.visibilityMs)
		}
	}
}

func (b *batchConsumeBroker) snapshot(read func(*batchConsumeBroker)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	read(b)
}

func newBatchConsumeClient(t *testing.T, b *batchConsumeBroker) *Client {
	t.Helper()
	server := httptest.NewServer(b.handler())
	t.Cleanup(server.Close)
	c, err := New(server.URL, WithRetries(3),
		WithBackoff(time.Millisecond, time.Millisecond), WithoutBreaker())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// consumeInBackground runs Consume until the test ends and returns a
// function that stops it and waits for it to return.
func consumeInBackground(t *testing.T, c *Client, h HandlerFunc, opts ...ConsumeOption) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Consume(ctx, "orders", h, append([]ConsumeOption{WithWait(10 * time.Millisecond)}, opts...)...)
	}()
	stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Consume did not return")
		}
	}
	t.Cleanup(stop)
	return stop
}

// errorLog collects what Consume reports to WithErrorHandler.
type errorLog struct {
	mu   sync.Mutex
	errs map[string][]error // by receipt handle
}

func (l *errorLog) handler(msg *Message, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.errs == nil {
		l.errs = map[string][]error{}
	}
	key := ""
	if msg != nil {
		key = msg.Receipt
	}
	l.errs[key] = append(l.errs[key], err)
}

func (l *errorLog) of(receipt string) []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.errs[receipt]...)
}

func TestConsumeWithBatchTakesManyAndAcksThemTogether(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{}
	for i := range 5 {
		b.queue(msgAt(int64(i), fmt.Sprintf("1:%d:9", i)))
	}
	c := newBatchConsumeClient(t, b)

	var mu sync.Mutex
	var handled []int64
	consumeInBackground(t, c, func(_ context.Context, msg *Message) error {
		mu.Lock()
		defer mu.Unlock()
		handled = append(handled, msg.Offset)
		return nil
	}, WithBatch(10))

	waitFor(t, func() bool {
		var acks int
		b.snapshot(func(b *batchConsumeBroker) { acks = len(b.acked) })
		return acks == 5
	}, "the batch to be acked")

	b.snapshot(func(b *batchConsumeBroker) {
		if b.maxes[0] != "10" {
			t.Errorf("max = %q, want 10", b.maxes[0])
		}
		if b.missingGuard {
			t.Error("a batch consume went out without X-Narad-Client")
		}
		if len(b.batchAcks) != 1 || len(b.batchAcks[0]) != 5 {
			t.Errorf("batch acks = %v, want one carrying all 5 handles", b.batchAcks)
		}
		if len(b.singleAcks) != 0 {
			t.Errorf("single acks = %v, want none", b.singleAcks)
		}
	})
	mu.Lock()
	defer mu.Unlock()
	for i, offset := range handled {
		if offset != int64(i) {
			t.Fatalf("handled %v, want the batch in order", handled)
		}
	}
}

func TestWithBatchIsCappedAtTheBrokersLimit(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{}
	b.queue(msgAt(1, "1:1:9"), msgAt(2, "1:2:9"))
	c := newBatchConsumeClient(t, b)
	consumeInBackground(t, c, func(context.Context, *Message) error { return nil }, WithBatch(500))

	waitFor(t, func() bool {
		var n int
		b.snapshot(func(b *batchConsumeBroker) { n = len(b.maxes) })
		return n > 0
	}, "a consume")
	b.snapshot(func(b *batchConsumeBroker) {
		if b.maxes[0] != "100" {
			t.Errorf("max = %q, want 100", b.maxes[0])
		}
	})
}

// Every handle's status in a batch ack reply counts on its own: a 410
// is a lost lease for that message alone, a 503 is retried for that
// message alone, and a 400 is reported for that message alone.
func TestBatchAckHonoursEveryHandlesStatus(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{statuses: map[string][]int{
		"1:1:9": {http.StatusGone},
		"1:2:9": {http.StatusServiceUnavailable},
		"1:3:9": {http.StatusBadRequest},
	}}
	c := newBatchConsumeClient(t, b)
	msgs := []*Message{msgAt(0, "1:0:9"), msgAt(1, "1:1:9"), msgAt(2, "1:2:9"), msgAt(3, "1:3:9")}
	for _, msg := range msgs {
		msg.client = c
	}

	errs := c.settleMany(context.Background(), opAck, msgs)

	if errs[0] != nil {
		t.Errorf("handle 0: %v, want acked", errs[0])
	}
	if !errors.Is(errs[1], ErrLeaseLost) {
		t.Errorf("handle 1: %v, want ErrLeaseLost", errs[1])
	}
	if errs[2] != nil {
		t.Errorf("handle 2: %v, want acked on the retry", errs[2])
	}
	if !errors.Is(errs[3], ErrBadRequest) {
		t.Errorf("handle 3: %v, want ErrBadRequest", errs[3])
	}
	var apiErr *Error
	if errors.As(errs[1], &apiErr) && (apiErr.Op != opAck || apiErr.Message == "") {
		t.Errorf("a handle's error should read as an ack's, with the broker's message: %+v", apiErr)
	}
	for i, msg := range msgs {
		if settled := msg.isSettled(); settled != (errs[i] == nil) {
			t.Errorf("message %d settled = %v with error %v", i, settled, errs[i])
		}
	}
	b.snapshot(func(b *batchConsumeBroker) {
		if len(b.batchAcks) != 2 || len(b.batchAcks[1]) != 1 || b.batchAcks[1][0] != "1:2:9" {
			t.Errorf("batch acks = %v, want a retry carrying only the 503 handle", b.batchAcks)
		}
	})
}

// A batch ack whose reply was lost may have landed, so a 410 on the
// retry means that ack spent the handle, exactly as for a single ack.
// A 410 the first time round is a lost lease.
func TestBatchAckResolvesAGoneAfterALostReplyAsSuccess(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{failFirstBatchAck: true}
	c := newBatchConsumeClient(t, b)
	msgs := []*Message{msgAt(0, "1:0:9"), msgAt(1, "1:1:9")}
	for _, msg := range msgs {
		msg.client = c
	}
	for i, err := range c.settleMany(context.Background(), opAck, msgs) {
		if err != nil {
			t.Errorf("message %d: %v, want acked", i, err)
		}
	}

	again := []*Message{msgAt(0, "1:0:9"), msgAt(1, "1:1:9")}
	for _, msg := range again {
		msg.client = c
	}
	for i, err := range c.settleMany(context.Background(), opAck, again) {
		if !errors.Is(err, ErrLeaseLost) {
			t.Errorf("message %d acked twice: %v, want ErrLeaseLost", i, err)
		}
	}
}

// A 200 means the broker processed the batch ack, so a reply that does
// not decode, or does not match the request, leaves every ack possibly
// landed. The retry's 410s then mean those acks spent the handles.
func TestBatchAckWithAnUnreadableReplyIsRetriedAsPossiblyLanded(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"undecodable":     `{"results":[{"status":204},`,
		"too few results": `{"results":[{"status":204}]}`,
		"empty":           ``,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := &batchConsumeBroker{garbleFirstBatchAck: &body}
			c := newBatchConsumeClient(t, b)
			msgs := []*Message{msgAt(0, "1:0:9"), msgAt(1, "1:1:9")}
			for _, msg := range msgs {
				msg.client = c
			}
			for i, err := range c.settleMany(context.Background(), opAck, msgs) {
				if err != nil {
					t.Errorf("message %d: %v, want acked", i, err)
				}
				if !msgs[i].isSettled() {
					t.Errorf("message %d is not marked settled", i)
				}
			}
			b.snapshot(func(b *batchConsumeBroker) {
				if len(b.batchAcks) != 2 {
					t.Errorf("batch acks = %v, want the first and one retry", b.batchAcks)
				}
			})
		})
	}
}

func TestBatchAckFallsBackToSingleAcksOnAnOlderBroker(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{noBatchAck: true}
	c := newBatchConsumeClient(t, b)
	msgs := []*Message{msgAt(0, "1:0:9"), msgAt(1, "1:1:9"), msgAt(2, "1:2:9")}
	for _, msg := range msgs {
		msg.client = c
	}
	for i, err := range c.settleMany(context.Background(), opAck, msgs) {
		if err != nil {
			t.Errorf("message %d: %v", i, err)
		}
	}
	b.snapshot(func(b *batchConsumeBroker) {
		if len(b.singleAcks) != 3 {
			t.Errorf("single acks = %v, want one per message", b.singleAcks)
		}
	})
}

// A node before 3.1.0 ignores max and answers one message in the
// single shape.
func TestBatchConsumeReadsAnOlderNodesSingleMessage(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{singleShape: true}
	b.queue(msgAt(1, "1:1:9"), msgAt(2, "1:2:9"))
	c := newBatchConsumeClient(t, b)

	var mu sync.Mutex
	var handled []string
	consumeInBackground(t, c, func(_ context.Context, msg *Message) error {
		mu.Lock()
		defer mu.Unlock()
		handled = append(handled, msg.Receipt)
		return nil
	}, WithBatch(10))

	waitFor(t, func() bool {
		var acks int
		b.snapshot(func(b *batchConsumeBroker) { acks = len(b.acked) })
		return acks == 2
	}, "both messages to be acked")
	mu.Lock()
	defer mu.Unlock()
	if len(handled) != 2 || handled[0] != "1:1:9" || handled[1] != "1:2:9" {
		t.Errorf("handled %v", handled)
	}
}

func TestBatchConsumeHandsBackAFailureAtOnceAndAcksTheRest(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{}
	b.queue(msgAt(1, "1:1:9"), msgAt(2, "1:2:9"), msgAt(3, "1:3:9"))
	c := newBatchConsumeClient(t, b)
	var log errorLog
	boom := errors.New("boom")
	consumeInBackground(t, c, func(_ context.Context, msg *Message) error {
		if msg.Offset == 2 {
			return boom
		}
		return nil
	}, WithBatch(3), WithErrorHandler(log.handler))

	waitFor(t, func() bool {
		var acks, nacks int
		b.snapshot(func(b *batchConsumeBroker) { acks, nacks = len(b.acked), len(b.singleNacks) })
		return acks == 2 && nacks == 1
	}, "two acks and one hand-back")
	b.snapshot(func(b *batchConsumeBroker) {
		if b.singleNacks[0] != "1:2:9" {
			t.Errorf("handed back %v, want the failed message", b.singleNacks)
		}
		if b.acked["1:2:9"] {
			t.Error("the failed message was acked")
		}
	})
	if errs := log.of("1:2:9"); len(errs) != 1 || !errors.Is(errs[0], boom) {
		t.Errorf("reported %v for the failed message", errs)
	}
}

// A handler that settles its own message must not have it settled
// again: the second ack would find the handle spent and report a
// failure that did not happen.
func TestAMessageTheHandlerSettledIsNotSettledAgain(t *testing.T) {
	t.Parallel()

	for _, batch := range []int{0, 3} {
		t.Run(fmt.Sprintf("batch %d", batch), func(t *testing.T) {
			t.Parallel()
			b := &batchConsumeBroker{}
			b.queue(msgAt(1, "1:1:9"), msgAt(2, "1:2:9"), msgAt(3, "1:3:9"))
			c := newBatchConsumeClient(t, b)
			var log errorLog
			consumeInBackground(t, c, func(ctx context.Context, msg *Message) error {
				switch msg.Offset {
				case 1:
					return msg.Ack(ctx)
				case 2:
					// Settled, then reported as failed: neither an ack
					// nor a second hand-back may follow.
					if err := msg.Nack(ctx); err != nil {
						return err
					}
					return errors.New("handed back by the handler")
				}
				return nil
			}, WithBatch(batch), WithErrorHandler(log.handler))

			waitFor(t, func() bool {
				var acks int
				b.snapshot(func(b *batchConsumeBroker) { acks = len(b.acked) })
				return acks == 2
			}, "messages 1 and 3 to be acked")
			time.Sleep(50 * time.Millisecond)

			b.snapshot(func(b *batchConsumeBroker) {
				acks := len(b.singleAcks)
				for _, handles := range b.batchAcks {
					acks += len(handles)
				}
				if acks != 2 {
					t.Errorf("acks sent = %d (single %v, batch %v), want 2", acks, b.singleAcks, b.batchAcks)
				}
				if len(b.singleNacks) != 1 || len(b.batchNacks) != 0 {
					t.Errorf("nacks = %v and %v, want only the handler's own", b.singleNacks, b.batchNacks)
				}
			})
			for _, receipt := range []string{"1:1:9", "1:3:9"} {
				if errs := log.of(receipt); len(errs) != 0 {
					t.Errorf("%s reported %v, want nothing", receipt, errs)
				}
			}
			// The handler's own error is still reported.
			if errs := log.of("1:2:9"); len(errs) != 1 {
				t.Errorf("1:2:9 reported %v, want the handler's error once", errs)
			}
		})
	}
}

// A message waiting its turn in a batch holds a lease like one being
// handled, so it is renewed rather than left to lapse behind slow work.
func TestBatchRenewsTheLeasesOfWaitingMessages(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{visibilityMs: 3_000} // renews every second
	b.queue(msgAt(1, "1:1:9"), msgAt(2, "1:2:9"))
	c := newBatchConsumeClient(t, b)
	release := make(chan struct{})
	consumeInBackground(t, c, func(_ context.Context, msg *Message) error {
		if msg.Offset == 1 {
			<-release
		}
		return nil
	}, WithBatch(2))

	waitFor(t, func() bool {
		renewed := false
		b.snapshot(func(b *batchConsumeBroker) {
			for _, handle := range b.extended {
				renewed = renewed || handle == "1:2:9"
			}
		})
		return renewed
	}, "the waiting message's lease to be renewed")
	close(release)
	waitFor(t, func() bool {
		var acks int
		b.snapshot(func(b *batchConsumeBroker) { acks = len(b.acked) })
		return acks == 2
	}, "both messages to be acked")
}

// A message handled early in a batch whose lease is lost before the
// batch ack goes out is on its way to somebody else: it is reported as
// a lost lease, and not acked.
func TestBatchDoesNotAckAMessageWhoseLeaseWasLostMeanwhile(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{
		visibilityMs: 3_000,
		extendStatus: map[string]int{"1:1:9": http.StatusGone},
	}
	b.queue(msgAt(1, "1:1:9"), msgAt(2, "1:2:9"), msgAt(3, "1:3:9"))
	c := newBatchConsumeClient(t, b)
	var log errorLog
	consumeInBackground(t, c, func(_ context.Context, msg *Message) error {
		if msg.Offset == 2 {
			// Long enough for message 1's first renewal to find its
			// lease gone.
			time.Sleep(1500 * time.Millisecond)
		}
		return nil
	}, WithBatch(3), WithErrorHandler(log.handler))

	waitFor(t, func() bool {
		var acks int
		b.snapshot(func(b *batchConsumeBroker) { acks = len(b.acked) })
		return acks == 2
	}, "messages 2 and 3 to be acked")
	b.snapshot(func(b *batchConsumeBroker) {
		if b.acked["1:1:9"] {
			t.Error("a message whose lease was lost was acked")
		}
	})
	if errs := log.of("1:1:9"); len(errs) != 1 || !errors.Is(errs[0], ErrLeaseLost) {
		t.Errorf("reported %v for the lost message, want ErrLeaseLost", errs)
	}
}

// Shutting down mid-batch finishes the message in hand and hands the
// unstarted rest back at once, in one request, rather than leaving them
// to wait out their visibility timeout.
func TestBatchHandsBackUnstartedMessagesOnShutdown(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{}
	b.queue(msgAt(1, "1:1:9"), msgAt(2, "1:2:9"), msgAt(3, "1:3:9"), msgAt(4, "1:4:9"))
	c := newBatchConsumeClient(t, b)

	var stop func()
	started := make(chan struct{})
	finish := make(chan struct{})
	var mu sync.Mutex
	var handled []int64
	stop = consumeInBackground(t, c, func(_ context.Context, msg *Message) error {
		mu.Lock()
		handled = append(handled, msg.Offset)
		mu.Unlock()
		if msg.Offset == 1 {
			close(started)
			<-finish
		}
		return nil
	}, WithBatch(4))

	<-started
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(finish)
	}()
	stop()

	mu.Lock()
	defer mu.Unlock()
	if len(handled) != 1 {
		t.Errorf("handled %v, want only the message already in hand", handled)
	}
	b.snapshot(func(b *batchConsumeBroker) {
		if !b.acked["1:1:9"] {
			t.Error("the message in hand was not acked")
		}
		if len(b.batchNacks) != 1 || len(b.batchNacks[0]) != 3 {
			t.Errorf("batch nacks = %v, want the three unstarted messages in one", b.batchNacks)
		}
	})
}

// Once the handler has settled its message there is no lease to renew,
// and a renewal would find the receipt spent and take that for a lost
// lease.
func TestRenewalsStopOnceTheHandlerSettles(t *testing.T) {
	t.Parallel()

	b := &batchConsumeBroker{visibilityMs: 3_000} // renews every second
	b.queue(msgAt(1, "1:1:9"))
	c := newBatchConsumeClient(t, b)
	var log errorLog
	finished := make(chan struct{})
	consumeInBackground(t, c, func(ctx context.Context, msg *Message) error {
		defer close(finished)
		if err := msg.Ack(ctx); err != nil {
			return err
		}
		select {
		case <-time.After(1300 * time.Millisecond):
		case <-ctx.Done():
			t.Error("the handler was cancelled after settling its own message")
		}
		return nil
	}, WithErrorHandler(log.handler))

	<-finished
	time.Sleep(50 * time.Millisecond)
	b.snapshot(func(b *batchConsumeBroker) {
		if len(b.extended) != 0 {
			t.Errorf("renewed %v after the handler acked", b.extended)
		}
		if len(b.singleAcks) != 1 {
			t.Errorf("acks = %v, want only the handler's", b.singleAcks)
		}
	})
	if errs := log.of("1:1:9"); len(errs) != 0 {
		t.Errorf("reported %v, want nothing", errs)
	}
}
