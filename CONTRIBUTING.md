# Contributing to narad-go

Thanks for contributing to the Go client for
[Narad](https://github.com/DebanganThakuria/narad).

## Before you start

- Open an issue before a large change, so the work can be scoped before
  implementation starts.
- Keep pull requests focused on one change.
- A change in behaviour comes with a test in the same pull request. A bug fix
  comes with a test that fails without the fix.
- Participation is covered by the [Code of Conduct](./CODE_OF_CONDUCT.md).
- Security issues go through [SECURITY.md](./SECURITY.md), never a public issue.

## Two modules, one rule

The repository holds two Go modules:

- the client, at the root, which depends on **the Go standard library and
  nothing else**. CI fails if the root module gains a dependency or a `go.sum`.
- `prometheus`, the optional metrics adapter, with its own `go.mod`.

A feature that needs a third-party package belongs in its own module next to
`prometheus`, not in the root.

The client supports the Go version in `go.mod` (currently 1.23). Do not use
language or standard library features newer than that.

## Development

```sh
gofmt -l .
go vet ./...
go test -race -count=1 ./...

cd prometheus
go vet ./...
go test -race -count=1 ./...
```

Run with `GOWORK=off` if you keep a `go.work` that includes this repository:
CI does, so that the stdlib-only check sees the root module alone.

The tests that talk to a real broker sit behind the `integration` build tag
and skip unless `NARAD_ADDR` points at one. With a broker running
(`narad server start --dev`):

```sh
NARAD_ADDR=localhost:7942 go test -tags integration -count=1 ./...
```

`NARAD_USER` (default `admin`) and `NARAD_PASSWORD` supply credentials when the
broker has security on.

## Requirements for acceptance

A pull request is merged when:

- CI passes: format, the stdlib-only check, `go mod tidy`, vet and race tests
  for both modules, govulncheck and CodeQL
- it follows the test rule above
- user-visible changes have an entry under `## [Unreleased]` in
  `CHANGELOG.md`, and the README is updated where it describes the behaviour
- it can be licensed under the Apache License 2.0

## Commit style

Conventional prefixes, as in the history: `feat:`, `fix:`, `docs:`, `test:`,
`refactor:`, `chore:`, `build:`, `ci:`. Scope the prometheus module as
`fix(prometheus): ...`. The subject says what changes for a user, in plain
words.

## Releases

The maintainer tags the client as `vX.Y.Z`, then the metrics module as
`prometheus/vX.Y.Z` on a commit whose `prometheus/go.mod` requires a tagged
client release.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](./LICENSE).
