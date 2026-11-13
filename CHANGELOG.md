# Changelog

All notable changes to narad-go are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is below 1.0.0, a minor release may change the API.

## [Unreleased]

## [0.1.0] - 2026-11-13

The first tagged release. Until now `go get` recorded a pseudo-version of
`main`.

### Added
- `Produce` and `ProduceBatch`, keyed or not, and `Consume`, one message at
  a time or in batches with `WithBatch`, with long polling, lease renewal
  while a handler runs, cancellation of the handler's context when its lease
  is lost, ack on success and hand-back on failure, panic recovery, and a
  graceful shutdown that gives in-flight handlers 30 seconds to finish.
- Optional envelopes (`WithEnvelope`, `WithHeaders`) that wrap a message
  with an id, a timestamp, an attempt count, the last error and your own
  headers, and work with topics that enforce a schema.
- Topics (`CreateTopic`, `EnsureTopic`, `Topic`, `Topics`, `DeleteTopic`,
  `SetSchema`), with fan-out and delay children, retention by age or
  forever, and partial answers while a partition's owner is down.
- Replay (`ReadAt`, `ReadFrom`, `Replay`), with `Replay` waiting out a
  partition it cannot read rather than skipping it.
- Multi-node addresses with per-node circuit breakers, retries with full
  jitter that honour `Retry-After`, and a connection pool sized for long
  polling. `Ping` probes a chosen node without tripping its breaker, and
  `Health` reports what the client believes about each node.
- Typed errors (`ErrExists`, `ErrUnavailable` and the rest) and an optional
  `slog` logger.
- `github.com/debanganthakuria/narad-go/prometheus`, a package that exports
  the client's metrics to Prometheus. It is part of this module; until now it
  was a module of its own.

The core `narad` package imports the Go standard library alone, so a program
that never imports the `prometheus` package never compiles the Prometheus
client. The module supports Go 1.23 or later. Batches, keep-forever retention and partial topic answers need
Narad 3.1.0; everything else works against earlier brokers too.

[Unreleased]: https://github.com/DebanganThakuria/narad-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/DebanganThakuria/narad-go/releases/tag/v0.1.0
