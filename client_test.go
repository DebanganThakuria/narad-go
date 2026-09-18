package narad

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// newTestClient builds a client against a handler, with retries fast
// enough that a test does not spend its time asleep and the breaker off
// unless the test wants it.
func newTestClient(t *testing.T, h http.HandlerFunc, opts ...Option) *Client {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)

	base := []Option{
		WithRetries(3),
		WithBackoff(time.Millisecond, 2*time.Millisecond),
		WithoutBreaker(),
	}
	c, err := New(server.URL, append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestParseAddresses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want []string
	}{
		{"localhost:7942", []string{"http://localhost:7942"}},
		{"http://a:1/", []string{"http://a:1"}},
		{"https://a:1", []string{"https://a:1"}},
		{"a:1,b:2, c:3 ", []string{"http://a:1", "http://b:2", "http://c:3"}},
	}
	for _, tc := range cases {
		got, err := parseAddresses(tc.in)
		if err != nil {
			t.Errorf("parseAddresses(%q): %v", tc.in, err)
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("parseAddresses(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("parseAddresses(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
	for _, bad := range []string{"", "  ", ",,", "ftp://a"} {
		if _, err := parseAddresses(bad); !errors.Is(err, ErrBadRequest) {
			t.Errorf("parseAddresses(%q) = %v, want ErrBadRequest", bad, err)
		}
	}
}

func TestNewRejectsAnEmptyAddress(t *testing.T) {
	t.Parallel()

	if _, err := New(""); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("New(\"\") = %v, want ErrBadRequest", err)
	}
}

// The server refuses a state-changing request that carries neither an
// API content type nor this header, and a body-less ack has no content
// type to offer. Sending it on everything is what keeps acks working.
func TestEveryRequestCarriesTheGuardHeader(t *testing.T) {
	t.Parallel()

	var got http.Header
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}, WithAuth("admin", "secret"))

	msg := &Message{Topic: "orders", Receipt: "1:2:3", client: c}
	if err := msg.Ack(context.Background()); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got.Get("X-Narad-Client") == "" {
		t.Error("X-Narad-Client was not sent")
	}
	if got.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want application/json on a body-less POST", got.Get("Content-Type"))
	}
	user, pass, ok := (&http.Request{Header: got}).BasicAuth()
	if !ok || user != "admin" || pass != "secret" {
		t.Errorf("basic auth = %q/%q ok=%v", user, pass, ok)
	}
}

// An empty user agent would send an empty header, which the guard
// rejects, so the option ignores it.
func TestEmptyUserAgentIsIgnored(t *testing.T) {
	t.Parallel()

	var got string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-Narad-Client")
		w.WriteHeader(http.StatusAccepted)
	}, WithUserAgent(""))

	if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if got == "" {
		t.Error("an empty user agent produced an empty guard header")
	}
}

func TestErrorBodyIsDecoded(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"topic not found"}`))
	})

	err := c.Produce(context.Background(), "missing", map[string]int{"a": 1})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want an *Error", err)
	}
	if apiErr.Message != "topic not found" || apiErr.Status != http.StatusNotFound {
		t.Errorf("error = %+v", apiErr)
	}
	if apiErr.Node == "" {
		t.Error("the error should name the node that answered")
	}
}

// The cluster routing layer answers some failures with plain text, so a
// client that insisted on JSON would lose the explanation exactly when
// it is most useful.
func TestPlainTextErrorBodyIsKept(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "partition owner is down; retry later", http.StatusServiceUnavailable)
	})

	_, err := c.Receive(context.Background(), "orders")
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want an *Error", err)
	}
	if apiErr.Message != "partition owner is down; retry later" {
		t.Errorf("message = %q", apiErr.Message)
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Error("503 should map to ErrUnavailable")
	}
}

func TestRetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestPermanentErrorsAreNotRetried(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	})

	if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err = %v, want ErrBadRequest", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1: a 400 fails the same way forever", got)
	}
}

func TestRetryAfterIsHonoured(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var first time.Time
	var gap time.Duration
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			first = time.Now()
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
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

func TestRetryAfterParsing(t *testing.T) {
	t.Parallel()

	if got := retryAfter("5"); got != 5*time.Second {
		t.Errorf("seconds = %s, want 5s", got)
	}
	for _, bad := range []string{"", "nonsense", "-3", "0"} {
		if got := retryAfter(bad); got != 0 {
			t.Errorf("retryAfter(%q) = %s, want 0", bad, got)
		}
	}
	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	if got := retryAfter(future); got <= 0 {
		t.Errorf("date form = %s, want positive", got)
	}
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	if got := retryAfter(past); got != 0 {
		t.Errorf("past date = %s, want 0", got)
	}
}

// 503 means opposite things depending on the operation, which is why
// Uncertain has to know which one ran.
func TestClassification(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		err       error
		retryable bool
		uncertain bool
	}{
		{"throttled", &Error{Status: 429}, true, false},
		{"misdirected", &Error{Status: 421}, true, false},
		{"server error", &Error{Status: 500}, true, true},
		{"bad gateway", &Error{Status: 502}, true, true},
		{"bad request", &Error{Status: 400}, false, false},
		{"not found", &Error{Status: 404}, false, false},
		{"lease lost", &Error{Status: 410}, false, false},
		{"too large", &Error{Status: 413}, false, false},
		{"dial failure", &ConnError{Err: errors.New("refused")}, true, false},
		{"cut off mid-flight", &ConnError{Err: errors.New("reset"), reached: true}, true, true},
		{"no nodes", ErrNoNodes, true, false},
		{"nil", nil, false, false},

		{"unavailable on ack", &Error{Status: 503, Op: opAck}, true, false},
		{"unavailable on consume", &Error{Status: 503, Op: opConsume}, true, false},
		{"unavailable on produce", &Error{Status: 503, Op: opProduce}, true, true},
	}
	for _, tc := range cases {
		if got := Retryable(tc.err); got != tc.retryable {
			t.Errorf("Retryable(%s) = %v, want %v", tc.name, got, tc.retryable)
		}
		if got := Uncertain(tc.err); got != tc.uncertain {
			t.Errorf("Uncertain(%s) = %v, want %v", tc.name, got, tc.uncertain)
		}
	}
}

// Retrying an uncertain produce can duplicate. The caller chooses in
// advance, and the choice has to be honoured.
func TestCautiousRetriesStopAtUncertainty(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}, WithRetries(4), WithCautiousRetries())

	err := c.Produce(context.Background(), "orders", map[string]int{"a": 1})
	if err == nil {
		t.Fatal("Produce should have failed")
	}
	if !Uncertain(err) {
		t.Error("a 500 should be uncertain")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
}

func TestRetryMovesToAnotherNode(t *testing.T) {
	t.Parallel()

	var healthy atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthy.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer good.Close()

	c, err := New(bad.URL+","+good.URL, WithRetries(2),
		WithBackoff(time.Microsecond, time.Microsecond), WithoutBreaker())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	// Whichever node is picked first, the retry must land on the other.
	if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if healthy.Load() == 0 {
		t.Error("the retry never reached the healthy node")
	}
}

func TestBreakerTakesADeadNodeOutOfRotation(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}, WithBreaker(2, time.Hour), WithRetries(2))

	ctx := context.Background()
	_ = c.Produce(ctx, "orders", map[string]int{"a": 1})
	before := calls.Load()

	if err := c.Produce(ctx, "orders", map[string]int{"a": 1}); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("err = %v, want ErrNoNodes once the breaker is open", err)
	}
	if calls.Load() != before {
		t.Error("a request reached a node whose breaker is open")
	}
	if health := c.Health(); health[0].Healthy {
		t.Error("Health should report the node as unhealthy")
	}
}

// Narad's 429 is a per-identity cap on consumes in flight, so every node
// answers the same way. Counting it would let one over-concurrent
// consumer take out the whole cluster for produce traffic too.
func TestThrottlingDoesNotCountAgainstANode(t *testing.T) {
	t.Parallel()

	cases := map[error]bool{
		nil:                              false,
		&ConnError{Err: errors.New("x")}: true,
		&Error{Status: 429}:              false,
		&Error{Status: 404}:              false,
		&Error{Status: 403}:              false,
		&Error{Status: 410}:              false,
		&Error{Status: 400}:              false,
		&Error{Status: 500}:              true,
		&Error{Status: 502}:              true,
		&Error{Status: 503}:              true,
	}
	for err, want := range cases {
		if got := nodeFault(err); got != want {
			t.Errorf("nodeFault(%v) = %v, want %v", err, got, want)
		}
	}
}

func TestClosedClientRefusesWork(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close is not idempotent: %v", err)
	}
}

func TestContextCancellationStopsRetrying(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}, WithRetries(100), WithBackoff(50*time.Millisecond, 50*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := c.Produce(ctx, "orders", map[string]int{"a": 1}); err == nil {
		t.Fatal("Produce should have failed")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %s: the retry loop ignored the context", elapsed)
	}
}

// A probe has to name its node, and must not open that node's breaker:
// polling readiness during a rolling restart would otherwise take the
// data plane down.
func TestPingTargetsOneNodeAndDoesNotTripTheBreaker(t *testing.T) {
	t.Parallel()

	var ready atomic.Bool
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			t.Errorf("ping asked for %q", r.URL.Path)
		}
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}, WithBreaker(1, time.Hour))

	ctx := context.Background()
	node := c.Nodes()[0]

	if err := c.Ping(ctx, node); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ping = %v, want ErrUnavailable while not ready", err)
	}
	if health := c.Health(); !health[0].Healthy {
		t.Error("a not-ready answer opened the breaker")
	}
	ready.Store(true)
	if err := c.Ping(ctx, node); err != nil {
		t.Fatalf("Ping once ready: %v", err)
	}
	if err := c.Ping(ctx, "http://elsewhere:7942"); !errors.Is(err, ErrBadRequest) {
		t.Errorf("Ping to an unknown node = %v, want ErrBadRequest", err)
	}
}

func TestEventsAndLoggerFire(t *testing.T) {
	t.Parallel()

	var requests, retries atomic.Int32
	var warnings atomic.Int32
	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	},
		WithEvents(func(e Event) {
			switch e.Kind {
			case EventRequest:
				requests.Add(1)
			case EventRetry:
				retries.Add(1)
			}
		}),
		WithLogger(countingLogger{warn: &warnings}),
	)

	if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if requests.Load() != 2 {
		t.Errorf("request events = %d, want 2", requests.Load())
	}
	if retries.Load() != 1 {
		t.Errorf("retry events = %d, want 1", retries.Load())
	}
	if warnings.Load() == 0 {
		t.Error("the retry was not logged")
	}
}

type countingLogger struct{ warn *atomic.Int32 }

func (l countingLogger) Debug(string, ...any) {}
func (l countingLogger) Info(string, ...any)  {}
func (l countingLogger) Warn(string, ...any)  { l.warn.Add(1) }
func (l countingLogger) Error(string, ...any) {}

func TestBackoffIsBoundedAndSpread(t *testing.T) {
	t.Parallel()

	b := backoff{base: 100 * time.Millisecond, max: time.Second}
	seen := make(map[time.Duration]bool)
	for range 200 {
		d := b.delay(0)
		if d < 0 || d > 100*time.Millisecond {
			t.Fatalf("delay = %s, want within (0, 100ms]", d)
		}
		seen[d] = true
	}
	if len(seen) < 50 {
		t.Errorf("only %d distinct delays over 200 draws; the spread is missing", len(seen))
	}
	// Whatever the attempt, never above the cap, and never a garbage
	// value from an overflowed exponent.
	for _, attempt := range []int{-1, 0, 5, 63, 1000, 1 << 30} {
		if d := b.delay(attempt); d < 0 || d > time.Second {
			t.Errorf("delay(%d) = %s, outside the cap", attempt, d)
		}
	}
}
