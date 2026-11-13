# narad-go

[![Go Reference](https://pkg.go.dev/badge/github.com/debanganthakuria/narad-go.svg)](https://pkg.go.dev/github.com/debanganthakuria/narad-go)
[![CI](https://github.com/DebanganThakuria/narad-go/actions/workflows/ci.yaml/badge.svg)](https://github.com/DebanganThakuria/narad-go/actions/workflows/ci.yaml)
[![Release](https://img.shields.io/github/v/release/DebanganThakuria/narad-go?sort=semver)](https://github.com/DebanganThakuria/narad-go/releases/latest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)

The Go client for [Narad](https://github.com/DebanganThakuria/narad), a
queue-first message broker. You write against a work queue (take a message,
hold it under a lease, ack it or let it come back); underneath, each topic is
a partitioned log, which is what makes replay and fan-out cheap.

```sh
go get github.com/debanganthakuria/narad-go@latest
```

Needs Go 1.26 or later. The core `narad` package imports only the standard
library.

## Quick start

```go
client, err := narad.New("n1:7942,n2:7942,n3:7942",
    narad.WithAuth("svc", os.Getenv("NARAD_PASSWORD")))
if err != nil {
    return err
}
defer client.Close()

// A struct becomes JSON; []byte and string go as they are.
err = client.Produce(ctx, "orders", order)

// Long-polls, keeps the lease alive while the handler runs,
// acks on success and hands the message back on error.
err = client.Consume(ctx, "orders", narad.HandlerFunc(
    func(ctx context.Context, msg *narad.Message) error {
        var o Order
        if err := msg.Into(&o); err != nil {
            return err
        }
        return process(ctx, o)
    }))
```

List every node, comma separated: the client spreads requests across them
and routes around failing ones, so a node restart is invisible to your code.

## What you get without configuring anything

- Long polling, lease renewal for slow handlers, and handler cancellation if
  the lease is lost.
- Ack on success, immediate hand-back on error, panic recovery, and a
  30-second graceful shutdown.
- Retries with full jitter, per-node circuit breakers, and a connection pool
  sized for long polling.

Tune any of it with options such as `WithWorkers`, `WithTimeout`,
`WithRetries` and `WithBreaker`; every option is documented on
[pkg.go.dev](https://pkg.go.dev/github.com/debanganthakuria/narad-go).

## Handlers must be idempotent

Narad delivers at least once and does not deduplicate, so a message can
arrive more than once; it does not guarantee order either. A produce whose
reply was lost may also have been stored; the client retries it by default,
because a duplicate is recoverable and a lost message is not. Use
`WithCautiousRetries()` when a duplicate is the worse outcome, and check
errors without matching strings:

```go
narad.Retryable(err)              // worth trying again
narad.Uncertain(err)              // may have happened anyway
errors.Is(err, narad.ErrNotFound) // which assumption was wrong
```

## More

- **Batches.** `ProduceBatch` stores up to 100 messages in one request, all or
  none; `WithBatch(n)` has each consumer worker fetch and ack `n` at a time.
  Both need Narad 3.1.0.
- **Envelopes and retries.** `WithEnvelope()` wraps a message with an id, a
  timestamp, an attempt count and the last error. `client.Retry` republishes
  with the count raised: point it at a delay child for backoff, or at a
  parking topic as a dead letter.
- **Topics and replay.** `EnsureTopic`, `SetSchema`, fan-out and delay
  children, and `ReadAt`, `ReadFrom` and `Replay` to read from an offset.
- **Logging and metrics.** `WithLogger(slog.Default())`, and Prometheus
  metrics from the `prometheus` package:

  ```go
  import naradprom "github.com/debanganthakuria/narad-go/prometheus"

  metrics := naradprom.NewMetrics(prometheus.DefaultRegisterer)
  client, err := narad.New(addr, narad.WithEvents(metrics.Observe))
  ```

  Only programs that import it compile the Prometheus client.

The [Go client guide](https://debanganthakuria.github.io/narad/build/go-sdk/)
in the Narad docs walks through all of it, and
[pkg.go.dev](https://pkg.go.dev/github.com/debanganthakuria/narad-go) has the
full API.

## Compatibility

Narad's `/v1` HTTP API is stable, so the client keeps working across broker
upgrades. Batches, keep-forever retention and partial topic answers need
Narad 3.1.0; everything else works with earlier brokers. The module is below
v1.0.0, so a minor release may change the API; the
[changelog](./CHANGELOG.md) says when.

## Contributing and support

See [CONTRIBUTING.md](./CONTRIBUTING.md), [SUPPORT.md](./SUPPORT.md) and, for
vulnerabilities, [SECURITY.md](./SECURITY.md).

## License

[Apache 2.0](./LICENSE).
