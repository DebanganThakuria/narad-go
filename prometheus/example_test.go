package prometheus_test

import (
	"log"

	"github.com/debanganthakuria/narad-go"
	naradprom "github.com/debanganthakuria/narad-go/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

// Register the metrics once, then hand Observe to the client.
func ExampleNewMetrics() {
	metrics := naradprom.NewMetrics(prometheus.DefaultRegisterer)
	client, err := narad.New("localhost:7942", narad.WithEvents(metrics.Observe))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

// A prefix scopes the metrics to one service, and keeps two clients in
// one process from colliding on the same registry.
func ExampleWithMetricsPrefix() {
	reg := prometheus.NewRegistry()
	payments := naradprom.NewMetrics(reg, naradprom.WithMetricsPrefix("payments"))
	orders := naradprom.NewMetrics(reg, naradprom.WithMetricsPrefix("orders"))
	_, _ = payments, orders
	// payments_narad_requests_total, orders_narad_requests_total, and so on
}
