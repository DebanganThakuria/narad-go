package narad

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// metricsSubsystem keeps "narad" in every metric name whatever prefix is
// set, so the metrics stay recognizable in a dashboard that mixes sources.
const metricsSubsystem = "narad"

// validMetricsPrefix is Prometheus's own rule for a name part.
var validMetricsPrefix = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// A MetricsOption adjusts the metrics.
type MetricsOption func(*metricsConfig)

type metricsConfig struct {
	namespace string
}

// WithMetricsPrefix puts a prefix in front of every metric name, so
// narad_requests_total becomes <prefix>_narad_requests_total.
//
// Use it to scope the client's metrics to your service when one registry
// collects from several sources, or when two Narad clients in the same
// process would otherwise collide on a registry that refuses duplicate
// names.
//
// The prefix must be a valid Prometheus name part: letters, digits and
// underscores, not starting with a digit. An invalid one is reported by
// [NewMetricsWithError] and panics in [NewMetrics].
func WithMetricsPrefix(prefix string) MetricsOption {
	return func(c *metricsConfig) { c.namespace = prefix }
}

// Metrics reports a Narad client's activity to Prometheus.
//
// Create one per client and pass [Metrics.Observe] to [WithEvents]. It is
// safe for concurrent use. Four metrics come out of it:
//
//	narad_requests_total{op,node,status,outcome}   counter
//	narad_request_duration_seconds{op,node}        histogram
//	narad_retries_total{op,node,uncertain}         counter
//	narad_node_up{node}                            gauge
//
// The gauge is 1 while a node is being used and 0 while its circuit
// breaker holds it out.
type Metrics struct {
	requests  *prometheus.CounterVec
	durations *prometheus.HistogramVec
	retries   *prometheus.CounterVec
	nodeUp    *prometheus.GaugeVec
}

// NewMetrics registers the metrics and returns them.
//
// Pass prometheus.DefaultRegisterer for the usual case, or your own
// registry:
//
//	metrics := narad.NewMetrics(prometheus.DefaultRegisterer)
//	client, err := narad.New(addr, narad.WithEvents(metrics.Observe))
//
// It panics if registration fails, which follows MustRegister and suits
// a call made once at startup. Use [NewMetricsWithError] where a panic
// is not wanted, such as when the prefix comes from configuration.
func NewMetrics(reg prometheus.Registerer, opts ...MetricsOption) *Metrics {
	m, err := NewMetricsWithError(reg, opts...)
	if err != nil {
		panic(err)
	}
	return m
}

// NewMetricsWithError is [NewMetrics], returning an error instead of
// panicking.
//
// It reports an invalid prefix, and a registry that already holds these
// metrics, which is what happens when a process builds two clients
// without giving them different prefixes.
func NewMetricsWithError(reg prometheus.Registerer, opts ...MetricsOption) (*Metrics, error) {
	var cfg metricsConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.namespace != "" && !validMetricsPrefix.MatchString(cfg.namespace) {
		return nil, fmt.Errorf("narad: metrics prefix %q is not a valid metric name part", cfg.namespace)
	}

	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Subsystem: metricsSubsystem,
			Name:      "requests_total",
			Help:      "Narad requests by operation, node, status and outcome.",
		}, []string{"op", "node", "status", "outcome"}),

		durations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.namespace,
			Subsystem: metricsSubsystem,
			Name:      "request_duration_seconds",
			Help:      "How long Narad requests took.",
			// Reaching out to 30s because a long-polling consume that
			// waits the whole time is normal, not slow, and the default
			// buckets would put every one of them in +Inf.
			Buckets: []float64{
				.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30,
			},
		}, []string{"op", "node"}),

		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Subsystem: metricsSubsystem,
			Name:      "retries_total",
			Help:      "Narad retries, labelled by whether the failure may already have been applied.",
		}, []string{"op", "node", "uncertain"}),

		nodeUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cfg.namespace,
			Subsystem: metricsSubsystem,
			Name:      "node_up",
			Help:      "1 while the client is sending a node requests, 0 while its circuit breaker holds it out.",
		}, []string{"node"}),
	}
	for _, collector := range []prometheus.Collector{m.requests, m.durations, m.retries, m.nodeUp} {
		if err := reg.Register(collector); err != nil {
			return nil, fmt.Errorf("narad: register metrics: %w", err)
		}
	}
	return m, nil
}

// Observe records one client event. Give it to [WithEvents].
func (m *Metrics) Observe(e Event) {
	switch e.Kind {
	case EventRequest:
		status := "none"
		if e.Status != 0 {
			status = strconv.Itoa(e.Status)
		}
		m.requests.WithLabelValues(e.Op, e.Node, status, outcome(e.Err)).Inc()
		m.durations.WithLabelValues(e.Op, e.Node).Observe(e.Took.Seconds())

	case EventRetry:
		m.retries.WithLabelValues(e.Op, e.Node, strconv.FormatBool(e.Uncertain)).Inc()

	case EventNode:
		up := 0.0
		if e.State == "closed" {
			up = 1
		}
		m.nodeUp.WithLabelValues(e.Node).Set(up)
	}
}

// outcome classifies a request for the counter.
//
// The point of separating these is that they call for different
// responses. "throttled" means send less, "unavailable" means a node or
// a partition owner is down, "rejected" is the caller's own bug, and
// "unreachable" is the network. One "error" label would hide all of it.
func outcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrThrottled):
		return "throttled"
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	case errors.Is(err, ErrLeaseLost):
		return "lease_lost"
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrExists):
		return "not_found"
	case errors.Is(err, ErrForbidden), errors.Is(err, ErrUnauthenticated):
		return "denied"
	case errors.Is(err, ErrBadRequest):
		return "rejected"
	case errors.Is(err, ErrServer):
		return "server_error"
	}
	var connErr *ConnError
	if errors.As(err, &connErr) {
		return "unreachable"
	}
	return "error"
}
