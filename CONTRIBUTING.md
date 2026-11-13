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

## One module, one rule

The repository is a single Go module with two packages:

- `narad`, the client, at the root, which imports **the Go standard library
  and nothing else**. CI fails if it gains any other import.
- `prometheus`, the optional metrics adapter, which is the only package that
  imports the Prometheus client.

A feature that needs a third-party package belongs in its own package next to
`prometheus`, never in `narad`.

The client supports the Go version in `go.mod` (currently 1.23). Do not use
language or standard library features newer than that.

## Development

```sh
gofmt -l .
go mod tidy -diff
go vet ./...
go test -race -count=1 ./...
```

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

- CI passes: format, the stdlib-only check on `narad`, `go mod tidy`, vet,
  race tests, govulncheck and CodeQL
- it follows the test rule above
- user-visible changes have an entry under `## [Unreleased]` in
  `CHANGELOG.md`, and the README is updated where it describes the behaviour
- it can be licensed under the Apache License 2.0

## Commit style

Conventional prefixes, as in the history: `feat:`, `fix:`, `docs:`, `test:`,
`refactor:`, `chore:`, `build:`, `ci:`. Scope changes to the metrics package
as `fix(prometheus): ...`. The subject says what changes for a user, in plain
words.

## Releases

The maintainer tags releases as `vX.Y.Z`, after moving the `## [Unreleased]`
entries in `CHANGELOG.md` under the new version and setting `narad.Version`
in `client.go` to match.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](./LICENSE).
