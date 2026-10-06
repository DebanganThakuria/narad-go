package narad

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

// The two produce 503s the broker answers with nothing stored: a node
// being decommissioned (with Retry-After: 1) and a schema validator that
// stayed busy for 5s.
var nothingStored503s = []struct {
	name       string
	retryAfter string
	message    string
}{
	{"draining node", "1", "this node is being decommissioned and takes no new produce; send it to another node"},
	{"busy schema validator", "", "schema: validation capacity busy, retry"},
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

// The broker's changelog promises that WithCautiousRetries stops at a
// produce 503, since the client reads every produce 503 as possibly
// applied. Keep that true.
func TestCautiousRetriesStopAtEveryProduce503(t *testing.T) {
	t.Parallel()

	for _, tc := range nothingStored503s {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				writeBrokerError(w, http.StatusServiceUnavailable, tc.message)
			}, WithRetries(4), WithCautiousRetries())

			err := c.Produce(context.Background(), "orders", map[string]int{"a": 1})
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("err = %v, want ErrUnavailable", err)
			}
			if !Uncertain(err) || !Retryable(err) {
				t.Errorf("a produce 503 should be retryable and uncertain, got retryable=%v uncertain=%v",
					Retryable(err), Uncertain(err))
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
