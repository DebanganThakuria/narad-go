# narad-go

The Go client for [Narad](https://github.com/DebanganThakuria/narad).

```sh
go get github.com/debanganthakuria/narad-go
```

**The front door is SQS; the inside is Kafka.** You write against a work
queue: take a message, hold it under a visibility timeout, ack it or let
it come back. No consumer groups, no partition assignment, no offsets to
commit. Underneath, a topic is a partitioned append-only log with offsets
and retention, which is what pays for replay, cheap fan-out, and messages
that survive being consumed.

It depends on **nothing but the standard library**.

## The whole of it

```go
client, err := narad.New("localhost:7942")
if err != nil {
    return err
}
defer client.Close()

// Produce. A struct becomes JSON, []byte and string go as they are.
err = client.Produce(ctx, "orders", order)

// Consume. Long-polls, runs a worker pool, manages the lease, acks.
err = client.Consume(ctx, "orders", narad.HandlerFunc(
    func(ctx context.Context, msg *narad.Message) error {
        var order Order
        if err := msg.Into(&order); err != nil {
            return err
        }
        return process(ctx, order)
    }))
```

That is the API. Everything below is optional.

## Defaults you do not have to think about

| | |
|---|---|
| Long polling | An idle consumer waits on the broker instead of spinning |
| Lease renewal | Slow work keeps its message instead of losing it mid-handler |
| Lease-loss cancellation | If the lease is lost, the handler's context is cancelled, because somebody else may have the message now |
| Ack on success, hand back on failure | A handler error returns the message at once rather than waiting out the timeout |
| Panic recovery | One bad message does not take the consumer down |
| Graceful shutdown | In-flight handlers get 30s to finish and ack |
| Retries with full jitter | Four attempts, spread so a struggling broker is not hit by every client at once |
| Per-node circuit breakers | One dead node costs you that node, not the cluster |
| Connection pooling | Sized for long polling, where Go's default of two idle connections per host is far too few |

## Options, when you need them

```go
client, err := narad.New("n1:7942,n2:7942,n3:7942",
    narad.WithAuth("svc", os.Getenv("NARAD_PASSWORD")),
    narad.WithLogger(slog.Default()),
    narad.WithTimeout(10*time.Second),
    narad.WithRetries(5),
    narad.WithBackoff(50*time.Millisecond, 5*time.Second),
    narad.WithBreaker(5, 2*time.Second),
)

err = client.Consume(ctx, "orders", handler,
    narad.WithWorkers(16),
    narad.WithWait(10*time.Second),
    narad.WithErrorHandler(func(msg *narad.Message, err error) { ... }),
)
```

Give it every node you have, comma separated. The client spreads work
across them and routes around the ones that are failing, which is what
makes a node restart invisible to your code.

## Envelopes

Publish plainly, or wrap the message so it carries an id, a timestamp,
an attempt count and the last error:

```go
err := client.Produce(ctx, "orders", order,
    narad.WithEnvelope(),
    narad.WithHeaders(map[string]string{"trace": traceID}),
)
```

The id is a UUIDv7, so ids sort in the order they were created, and it
appears in every error and log line about that message. `msg.Into(&v)`
unwraps for you; `msg.Envelope()` gives you the rest.

**Attempts advance only when you republish.** The broker never rewrites a
stored payload, so ordinary redelivery hands back the same bytes and the
same count. `client.Retry` is what moves it: it publishes a fresh copy
with the count raised and the error recorded, then acks the original.
Point it at a delay child and you have backoff; point it at another topic
and you have a parking queue.

```go
if env.Attempts >= 5 {
    return client.Retry(ctx, msg, err, "orders-parked")
}
return client.Retry(ctx, msg, err, "orders-retry-30s")
```

### Envelopes and schemas together

A topic schema describes your message, and an envelope is a different
shape, so producing one to such a topic is rejected. `WithEnvelopeSchema`
fixes that by wrapping your schema in the envelope's, so the broker
validates both:

```go
_, err := client.EnsureTopic(ctx, "orders",
    narad.WithPartitionCount(6),
    narad.WithEnvelopeSchema(schema),
)
```

Your `$defs` are lifted to the top of the result so internal `$ref`s keep
resolving. `EnvelopeSchema` returns what will be stored, if you want to
look.

## Errors

Three questions, answered without matching on strings:

```go
narad.Retryable(err)              // worth trying again
narad.Uncertain(err)              // might have happened anyway
errors.Is(err, narad.ErrNotFound) // which assumption was wrong
```

`ErrBadRequest`, `ErrUnauthenticated`, `ErrForbidden`, `ErrNotFound`,
`ErrExists`, `ErrLeaseLost`, `ErrNoLease`, `ErrOffsetGone`,
`ErrTooLarge`, `ErrThrottled`, `ErrUnavailable`, `ErrServer`,
`ErrNoNodes`, `ErrClosed`, `ErrNoEnvelope`.

`*narad.Error` carries the status, the server's message and the node that
answered. `*narad.ConnError` covers requests that never got a reply.

## At-least-once, and what it asks of you

**Handlers must be idempotent.** Narad delivers at least once and does
not deduplicate. A broker restart can redeliver a message whose ack was
still in the batch being persisted. This is not something to plan around
later.

The same property shapes producing. A produce whose reply was lost may
still be committed, so retrying it can duplicate. The client retries by
default, because a duplicate is recoverable and a lost message is not.
When a duplicate is the worse outcome, `WithCautiousRetries()` hands the
choice back and `Uncertain(err)` tells you when it matters.

## Logging

Any logger with slog's method set works, which means `*slog.Logger` needs
no adapter:

```go
narad.WithLogger(slog.Default())
```

The client logs sparingly: retries and node state changes at warn, and
the failures a consumer cannot report any other way at error. For
anything finer, use `WithEvents`.

## Metrics

Prometheus lives in a separate module so the client itself stays free of
dependencies:

```sh
go get github.com/debanganthakuria/narad-go/prometheus
```

```go
metrics := naradprom.New(prometheus.DefaultRegisterer)
client, err := narad.New(addr, narad.WithEvents(metrics.Observe))
```

`narad_requests_total`, `narad_request_duration_seconds`,
`narad_retries_total` and `narad_node_up`.

Give it a prefix to scope the metrics to your service, or to tell two
clients in one process apart on a registry that refuses duplicate names:

```go
metrics := naradprom.New(reg, naradprom.WithPrefix("payments"))
// payments_narad_requests_total, and so on
```

`NewWithError` is the same thing without the panic, for when the prefix
comes from configuration. Or skip the module and write your own handler
for `narad.Event`, which reports anywhere you like.

## Also here

Topics (`CreateTopic`, `EnsureTopic`, `Topic`, `Topics`, `DeleteTopic`,
`SetSchema`), replay (`ReadAt`), and health (`Ping`, `Health`).

`Ping` takes the node to probe, because a probe sent through the client's
own load balancing would answer for whichever node came next. It does not
count against that node's breaker either, so polling readiness during a
rolling restart cannot take the cluster out of rotation.

## One sharp edge

**Payload round-trips are not always exact.** The broker keeps valid JSON
verbatim, wraps other UTF-8 text in a JSON string, and base64-encodes
binary with a flag. Only the third is marked on the wire, so a payload
arriving as a JSON string is either text the broker quoted or a JSON
string you really sent. `msg.Bytes()` unquotes it, which is right for the
first and loses the quotes on the second. Send a struct and read it with
`msg.Into` and the question never comes up.

## Compatibility

Go 1.23 or later. Narad's `/v1` HTTP surface is stable, so a client built
against it keeps working across broker upgrades.

## License

Apache 2.0.
