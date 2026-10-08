package narad

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A caller that stops waiting says nothing about the node. Stopping a
// Consume cancels every long-poll it has in flight, and those
// cancellations used to count as node failures: they opened every
// breaker, so the next call, typically cleanup right after the
// consumer stopped, failed with ErrNoNodes.
func TestCallerCancellationDoesNotTripTheBreaker(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/consume") {
			<-r.Context().Done() // a long-poll with nothing to deliver
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}, WithBreaker(2, time.Hour), WithRetries(1))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Receive(ctx, "orders", WithWait(10*time.Second))
		}()
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()

	if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); err != nil {
		t.Fatalf("Produce after the caller cancelled its polls = %v, want success", err)
	}
	if health := c.Health(); !health[0].Healthy {
		t.Error("the node reads as unhealthy after its callers merely stopped waiting")
	}
}

// A half-open breaker lets one probe through and waits for its answer.
// If the probe's caller gives up first, there is no answer: the probe
// must be handed back, or the node waits for an answer that never
// comes and stays out of rotation for good.
func TestCancelledHalfOpenProbeDoesNotStrandTheNode(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable) // opens the breaker
		case 2:
			// The probe, which its caller abandons. Bounded, so a client
			// that keeps the request open cannot hang the test.
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}, WithBreaker(1, 20*time.Millisecond), WithRetries(1))

	if err := c.Produce(context.Background(), "orders", map[string]int{"a": 1}); err == nil {
		t.Fatal("the first produce should fail and open the breaker")
	}
	time.Sleep(30 * time.Millisecond) // past the cooldown: the next call is the probe

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_ = c.Produce(ctx, "orders", map[string]int{"a": 1})
	cancel()

	time.Sleep(30 * time.Millisecond)
	err := c.Produce(context.Background(), "orders", map[string]int{"a": 1})
	if errors.Is(err, ErrNoNodes) {
		t.Fatal("the node stayed out of rotation after its probe was abandoned")
	}
	if err != nil {
		t.Fatalf("Produce = %v, want success", err)
	}
}
