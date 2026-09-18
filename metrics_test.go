package narad

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metricNames collects the metric names a registry holds.
func metricNames(t *testing.T, reg *prometheus.Registry) []string {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make([]string, 0, len(families))
	for _, family := range families {
		out = append(out, family.GetName())
	}
	return out
}

func TestDefaultMetricNames(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.Observe(Event{Kind: EventRequest, Op: "produce", Node: "n1", Status: 202, Took: time.Millisecond})
	m.Observe(Event{Kind: EventRetry, Op: "produce", Node: "n1"})
	m.Observe(Event{Kind: EventNode, Node: "n1", State: "open"})

	want := map[string]bool{
		"narad_requests_total":           false,
		"narad_request_duration_seconds": false,
		"narad_retries_total":            false,
		"narad_node_up":                  false,
	}
	for _, name := range metricNames(t, reg) {
		if _, ok := want[name]; !ok {
			t.Errorf("unexpected metric %q", name)
			continue
		}
		want[name] = true
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("%q was never reported", name)
		}
	}
}

// The prefix keeps "narad" in the name rather than replacing it, so the
// metrics stay recognizable on a dashboard that mixes sources.
func TestMetricsPrefixIsPrepended(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg, WithMetricsPrefix("payments"))
	m.Observe(Event{Kind: EventRequest, Op: "produce", Node: "n1", Status: 202})

	got := metricNames(t, reg)
	if len(got) == 0 {
		t.Fatal("nothing was registered")
	}
	for _, name := range got {
		if !strings.HasPrefix(name, "payments_narad_") {
			t.Errorf("metric %q does not carry the prefix", name)
		}
	}
}

// Two clients in one process collide on a registry unless they are given
// different prefixes, and that has to be reportable rather than fatal.
func TestMetricsPrefixSeparatesTwoClients(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	if _, err := NewMetricsWithError(reg, WithMetricsPrefix("orders")); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := NewMetricsWithError(reg, WithMetricsPrefix("payments")); err != nil {
		t.Fatalf("second with its own prefix: %v", err)
	}
	if _, err := NewMetricsWithError(reg, WithMetricsPrefix("orders")); err == nil {
		t.Fatal("registering the same prefix twice should be reported")
	}
}

func TestInvalidMetricsPrefixIsReported(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"has space", "1leading-digit", "dash-es", "dot.ted", "$"} {
		if _, err := NewMetricsWithError(prometheus.NewRegistry(), WithMetricsPrefix(bad)); err == nil {
			t.Errorf("prefix %q should have been rejected", bad)
		}
	}
	for _, good := range []string{"payments", "my_app", "_leading_underscore", "app2"} {
		if _, err := NewMetricsWithError(prometheus.NewRegistry(), WithMetricsPrefix(good)); err != nil {
			t.Errorf("prefix %q should have been accepted: %v", good, err)
		}
	}
}

func TestNewMetricsPanicsOnABadPrefix(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Error("NewMetrics should panic on an invalid prefix")
		}
	}()
	NewMetrics(prometheus.NewRegistry(), WithMetricsPrefix("not valid"))
}

// The outcome label is what makes the counter actionable: throttling
// calls for less concurrency, unavailability for patience, and a
// rejection for a fix in the caller.
func TestOutcomeLabels(t *testing.T) {
	t.Parallel()

	cases := map[string]error{
		"ok":           nil,
		"throttled":    ErrThrottled,
		"unavailable":  ErrUnavailable,
		"lease_lost":   ErrLeaseLost,
		"not_found":    ErrNotFound,
		"denied":       ErrForbidden,
		"rejected":     ErrBadRequest,
		"server_error": ErrServer,
		"unreachable":  &ConnError{Err: errors.New("refused")},
		"error":        errors.New("something else"),
	}
	for want, err := range cases {
		if got := outcome(err); got != want {
			t.Errorf("outcome(%v) = %q, want %q", err, got, want)
		}
	}
}

func TestNodeUpTracksTheBreaker(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.Observe(Event{Kind: EventNode, Node: "n1", State: "open"})

	value := gaugeValue(t, reg, "narad_node_up")
	if value != 0 {
		t.Errorf("node_up = %v while the breaker is open, want 0", value)
	}
	m.Observe(Event{Kind: EventNode, Node: "n1", State: "closed"})
	if value := gaugeValue(t, reg, "narad_node_up"); value != 1 {
		t.Errorf("node_up = %v once the breaker closed, want 1", value)
	}
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		metrics := family.GetMetric()
		if len(metrics) == 0 {
			t.Fatalf("%s has no samples", name)
		}
		return metrics[0].GetGauge().GetValue()
	}
	t.Fatalf("%s was not registered", name)
	return 0
}

// A request with no status is one that never got a reply, and it must
// still be counted rather than dropped.
func TestRequestWithNoStatusIsCounted(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.Observe(Event{
		Kind: EventRequest, Op: "produce", Node: "n1",
		Err: &ConnError{Err: errors.New("reset")},
	})

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "narad_requests_total" {
			continue
		}
		for _, label := range family.GetMetric()[0].GetLabel() {
			if label.GetName() == "status" && label.GetValue() != "none" {
				t.Errorf("status label = %q, want none", label.GetValue())
			}
		}
		return
	}
	t.Fatal("the request was not counted")
}
