package narad

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// writeBrokerError answers the way the broker's WriteError does: a JSON
// body {"error": msg}.
func writeBrokerError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// The produce 503s a 3.1.0 broker answers with nothing stored: a node
// being decommissioned (with Retry-After: 1), and a schema validation
// that never ran because no slot freed up, either within the node's 5s
// wait or before the request's own deadline.
var nothingStored503s = []struct {
	name       string
	retryAfter string
	message    string
}{
	{"draining node", "1", "this node is being decommissioned and takes no new produce; send it to another node"},
	{"busy schema validator", "", "schema: validation capacity busy, retry: no schema validation slot on this node freed up within 5s, so the payload was not validated; retry, preferably through another node"},
	{"schema validation wait cut short", "", "schema: payload not validated: context deadline exceeded while waiting for a schema validation slot"},
}

func TestProduceRefusedByOneNodeLandsOnAnotherWithoutWaiting(t *testing.T) {
	t.Parallel()

	for _, tc := range nothingStored503s {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			refused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				writeBrokerError(w, http.StatusServiceUnavailable, tc.message)
			}))
			defer refused.Close()
			var accepted atomic.Int32
			healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				accepted.Add(1)
				w.WriteHeader(http.StatusAccepted)
			}))
			defer healthy.Close()

			c, err := New(refused.URL+","+healthy.URL, WithRetries(2),
				WithBackoff(time.Millisecond, time.Millisecond), WithoutBreaker())
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer c.Close()

			// Whichever node goes first, the produce must end on the
			// healthy one, and the refusing node's Retry-After must not
			// hold up a retry that goes elsewhere.
			for range 4 {
				start := time.Now()
				if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); err != nil {
					t.Fatalf("Produce: %v", err)
				}
				if took := time.Since(start); took > 500*time.Millisecond {
					t.Fatalf("produce took %s, it waited out a Retry-After meant for the other node", took)
				}
			}
			if accepted.Load() != 4 {
				t.Errorf("healthy node accepted %d produces, want 4", accepted.Load())
			}
		})
	}
}

// A produce 503 the broker sends with nothing stored is certain, so
// WithCautiousRetries moves on to the next node rather than stop and
// leave the caller to reconcile a message that was never written. This
// is what lets a cautious producer ride through a decommission.
func TestCautiousRetriesMoveOnFromA503ThatStoredNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range nothingStored503s {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			refused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				writeBrokerError(w, http.StatusServiceUnavailable, tc.message)
			}))
			defer refused.Close()
			b := &batchBroker{}
			healthy := httptest.NewServer(b.handler())
			defer healthy.Close()

			c, err := New(refused.URL+","+healthy.URL, WithRetries(2), WithCautiousRetries(),
				WithBackoff(time.Millisecond, time.Millisecond), WithoutBreaker())
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer c.Close()

			// Whichever node goes first, every batch must end on the
			// healthy one.
			for range 4 {
				var batch Batch
				_ = batch.Add(order{ID: "o1"})
				_ = batch.Add(order{ID: "o2"})
				if err := c.ProduceBatch(context.Background(), "orders", &batch); err != nil {
					t.Fatalf("ProduceBatch: %v", err)
				}
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			if len(b.stored) != 8 {
				t.Errorf("healthy node stored %d messages, want all 8", len(b.stored))
			}

			apiErr := &Error{Op: opProduce, Status: http.StatusServiceUnavailable, Message: tc.message}
			if Uncertain(apiErr) || !Retryable(apiErr) {
				t.Errorf("got retryable=%v uncertain=%v, want a retryable 503 that stored nothing",
					Retryable(apiErr), Uncertain(apiErr))
			}
		})
	}
}

// Any other produce 503, such as one from a proxy that may have passed
// the request on before it gave up, may have stored the message, and
// cautious retries stop there.
func TestCautiousRetriesStopAtAnUnrecognisedProduce503(t *testing.T) {
	t.Parallel()

	for _, op := range []string{opProduce, opProduceBatch} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				http.Error(w, "upstream connect error or disconnect/reset before headers", http.StatusServiceUnavailable)
			}, WithRetries(4), WithCautiousRetries())

			var err error
			if op == opProduce {
				err = c.Produce(context.Background(), "orders", map[string]int{"a": 1})
			} else {
				var batch Batch
				_ = batch.Add(order{ID: "o1"})
				err = c.ProduceBatch(context.Background(), "orders", &batch)
			}
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("err = %v, want ErrUnavailable", err)
			}
			if !Uncertain(err) || !Retryable(err) {
				t.Errorf("got retryable=%v uncertain=%v, want both", Retryable(err), Uncertain(err))
			}
			if got := calls.Load(); got != 1 {
				t.Errorf("attempts = %d, want 1", got)
			}
		})
	}
}

// With nowhere else to go, a Retry-After on a 503 is still honoured: the
// retry goes back to the node that asked for the wait.
func TestRetryAfterOn503IsHonouredWhenNoOtherNodeRemains(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var first time.Time
	var gap time.Duration
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			first = time.Now()
			w.Header().Set("Retry-After", "1")
			writeBrokerError(w, http.StatusServiceUnavailable, nothingStored503s[0].message)
			return
		}
		gap = time.Since(first)
		w.WriteHeader(http.StatusAccepted)
	}, WithRetries(2), WithBackoff(time.Microsecond, time.Microsecond))

	if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if gap < 900*time.Millisecond {
		t.Errorf("retry gap = %s, want about the 1s the server asked for", gap)
	}
}

func TestConflictsAreToldApart(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		message string
		want    error
		notWant []error
	}{
		{
			name:    "plain conflict",
			message: "topic already exists",
			want:    ErrExists,
			notWant: []error{ErrNameTaken, ErrTopicChanged},
		},
		{
			name:    "name differs only in letter case",
			message: `topic already exists: "Orders" differs only in letter case from the existing topic "orders"; on a case-insensitive filesystem both would share one directory, so choose another name`,
			want:    ErrNameTaken,
			notWant: []error{ErrTopicChanged},
		},
		{
			name:    "topic changed under the request",
			message: "topic changed since it was read",
			want:    ErrTopicChanged,
			notWant: []error{ErrNameTaken},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				writeBrokerError(w, http.StatusConflict, tc.message)
			})

			_, err := c.CreateTopic(context.Background(), "Orders")
			if !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
			// Every conflict still matches ErrExists, so code written
			// before the finer sentinels keeps working.
			if !errors.Is(err, ErrExists) {
				t.Errorf("err = %v, want it to match ErrExists too", err)
			}
			for _, other := range tc.notWant {
				if errors.Is(err, other) {
					t.Errorf("err = %v, should not match %v", err, other)
				}
			}
			var apiErr *Error
			if !errors.As(err, &apiErr) || apiErr.Message != tc.message {
				t.Errorf("the server's message was not kept: %+v", apiErr)
			}
		})
	}
}

func TestNewRefusalsMapToTheirSentinels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		status    int
		message   string
		want      error
		retryable bool
	}{
		{"payload nested too deep", http.StatusBadRequest, "payload nests deeper than 256 levels", ErrBadRequest, false},
		{"topic name too long", http.StatusBadRequest, "topic name is 201 bytes; new topic names are limited to 200 bytes", ErrBadRequest, false},
		{"per-identity produce cap", http.StatusTooManyRequests, "too many in-flight produce requests for this identity (limit 64 per node)", ErrThrottled, true},
		{"failed-login budget", http.StatusTooManyRequests, "too many failed authentication attempts", ErrThrottled, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				writeBrokerError(w, tc.status, tc.message)
			}, WithRetries(1))

			err := c.Produce(context.Background(), "orders", map[string]int{"a": 1})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if Retryable(err) != tc.retryable {
				t.Errorf("Retryable = %v, want %v", Retryable(err), tc.retryable)
			}
			if nodeFault(err) {
				t.Error("this refusal says nothing about the node's health")
			}
		})
	}
}

// outcomeUnknownReply is the 503 a 3.1.0 broker sends when the leader
// appended a topic write and then lost its leadership: a later leader
// may still commit it.
const outcomeUnknownReply = "control plane temporarily unavailable: the change may still be applied, read it back before retrying: leadership lost while committing log"

// topicWrites runs each topic write the way a caller would.
var topicWrites = []struct {
	op  string
	run func(c *Client) error
}{
	{"create topic", func(c *Client) error {
		_, err := c.CreateTopic(context.Background(), "orders")
		return err
	}},
	{"set schema", func(c *Client) error {
		_, err := c.SetSchema(context.Background(), "orders",
			WithSchema(json.RawMessage(`{"type":"object"}`)), WithSchemaBaseVersion(1))
		return err
	}},
	{"delete topic", func(c *Client) error {
		return c.DeleteTopic(context.Background(), "orders")
	}},
}

// A topic write the leader may still apply must not be sent again by a
// caller who asked for cautious retries: the resend would find its own
// change and report a conflict for a write that worked.
func TestCautiousRetriesStopAtATopicWriteWhoseOutcomeIsUnknown(t *testing.T) {
	t.Parallel()

	for _, tc := range topicWrites {
		t.Run(tc.op, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					writeBrokerError(w, http.StatusServiceUnavailable, outcomeUnknownReply)
					return
				}
				writeBrokerError(w, http.StatusConflict, "topic already exists")
			}, WithRetries(3), WithCautiousRetries())

			err := tc.run(c)
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("err = %v, want the uncertain ErrUnavailable", err)
			}
			if !Uncertain(err) {
				t.Errorf("Uncertain(%v) = false, want true", err)
			}
			if got := calls.Load(); got != 1 {
				t.Errorf("attempts = %d, want 1", got)
			}
		})
	}
}

// A topic write's 503 that does not carry the broker's own text, such as
// a follower's forward to the leader that got no answer, is undecided
// too, and so is the outcome-unknown text on any operation.
func TestTopicWrite503sAreUncertain(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  *Error
	}{
		{"forward failure on create topic", &Error{Op: "create topic", Status: 503, Message: "forward to leader: connection reset by peer"}},
		{"forward failure on set schema", &Error{Op: "set schema", Status: 503, Message: "no leader known"}},
		{"forward failure on delete topic", &Error{Op: "delete topic", Status: 503}},
		{"outcome unknown on any operation", &Error{Op: "set retention", Status: 503, Message: outcomeUnknownReply}},
	}
	for _, tc := range cases {
		if !Uncertain(tc.err) || !Retryable(tc.err) {
			t.Errorf("%s: retryable=%v uncertain=%v, want both", tc.name, Retryable(tc.err), Uncertain(tc.err))
		}
	}
}

// Without cautious retries the write is still retried, but the retry
// event says it may duplicate, and a Retry-After on the uncertain 503 is
// honoured rather than cut short by moving to another node: the change
// may still land, and the wait gives it time to.
func TestUncertainTopicWrite503IsRetriedAfterTheWaitItAskedFor(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var first time.Time
	var gap time.Duration
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			first = time.Now()
			w.Header().Set("Retry-After", "1")
			writeBrokerError(w, http.StatusServiceUnavailable, outcomeUnknownReply)
			return
		}
		gap = time.Since(first)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"name":"orders","partitions":3}`))
	})
	one := httptest.NewServer(handler)
	defer one.Close()
	two := httptest.NewServer(handler)
	defer two.Close()

	var uncertainRetry atomic.Bool
	c, err := New(one.URL+","+two.URL, WithRetries(2),
		WithBackoff(time.Millisecond, time.Millisecond), WithoutBreaker(),
		WithEvents(func(e Event) {
			if e.Kind == EventRetry && e.Uncertain {
				uncertainRetry.Store(true)
			}
		}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if _, err := c.CreateTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if !uncertainRetry.Load() {
		t.Error("the retry event did not say the first attempt may have applied")
	}
	if gap < 900*time.Millisecond {
		t.Errorf("retry gap = %s, want about the 1s the server asked for", gap)
	}
}

// A produce to a delayed fan-out child is refused with 409, but nothing
// about it already exists: it will be refused however often it is sent,
// so it must not read as the benign conflict EnsureTopic treats as
// success.
func TestProduceToADelayedChildIsABadRequest(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		message string
		send    func(c *Client) error
	}{
		{"produce", "direct produce to a delayed child topic is not allowed", func(c *Client) error {
			return c.Produce(context.Background(), "orders-delayed", map[string]int{"a": 1})
		}},
		{"produce batch", "direct produce to a delayed child topic is not allowed", func(c *Client) error {
			var batch Batch
			_ = batch.Add(order{ID: "o1"})
			return c.ProduceBatch(context.Background(), "orders-delayed", &batch)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				writeBrokerError(w, http.StatusConflict, tc.message)
			})

			err := tc.send(c)
			if !errors.Is(err, ErrBadRequest) {
				t.Errorf("err = %v, want ErrBadRequest", err)
			}
			if errors.Is(err, ErrExists) {
				t.Errorf("err = %v, should not match ErrExists", err)
			}
			if Retryable(err) {
				t.Errorf("err = %v, should not be retryable", err)
			}
		})
	}
}

// A read that never got its answer changed nothing, so its error must
// not suggest the server applied it. A produce in the same state may
// have been stored, and still says so.
func TestReadCutOffMidFlightDoesNotClaimItWasApplied(t *testing.T) {
	t.Parallel()

	read := &ConnError{Op: opRead, Topic: "orders", Err: context.DeadlineExceeded, reached: true}
	if strings.Contains(read.Error(), "may have applied") {
		t.Errorf("read error = %q, want no claim that it was applied", read.Error())
	}
	produce := &ConnError{Op: opProduce, Topic: "orders", Err: context.DeadlineExceeded, reached: true}
	if !strings.Contains(produce.Error(), "may have applied") {
		t.Errorf("produce error = %q, want it to say it may have been applied", produce.Error())
	}
}
