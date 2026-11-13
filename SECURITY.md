# Security Policy

## Reporting a vulnerability

Please do not open a public GitHub issue for security vulnerabilities.

Use GitHub's private vulnerability reporting flow:

https://github.com/DebanganThakuria/narad-go/security/advisories/new

Include:

- a description of the vulnerability
- affected versions or commit SHAs
- reproduction steps or proof of concept
- impact assessment if known
- any suggested remediation

A vulnerability in the Narad broker itself belongs to the broker's
repository instead: https://github.com/DebanganThakuria/narad/security/advisories/new

## Supported versions

narad-go follows semantic versioning. Security fixes land on the most recent
minor line and ship as a patch release on that line. There are no long-term
support branches: upgrading to the latest release is the supported path.

| Version | Supported |
|---|---|
| 0.2.x | Yes |
| 0.1.x | No, upgrade to 0.2.x |

While the module is below v1.0.0, a minor release may change the API; the
release notes say what changed.

## Response targets

narad-go has a single maintainer, so these are goals rather than a
contractual SLA:

| Stage | Target |
|---|---|
| Acknowledge a report | 5 business days |
| Initial assessment and severity | 10 business days |
| Fix released for a confirmed high-severity issue | 30 days |

Every report gets an initial response within 14 days of arriving, even when
the assessment takes longer.

## Disclosure process

The project acknowledges reports, assesses impact, develops a fix, and
coordinates disclosure once affected users have a reasonable opportunity to
upgrade. Fixes are published as a GitHub Security Advisory with a CVE where
one is warranted, and named in the `### Security` section of that release's
`CHANGELOG.md` entry and in its GitHub release notes.

## Scope

The core `narad` package imports the Go standard library alone, so a standard
library advisory is fixed by building with a patched Go toolchain, not by a
narad-go release. Advisories in the module's dependencies (today the
Prometheus client, used by the `prometheus` package) are in scope.

An address with no scheme means `http://`, and `WithAuth` credentials travel
as HTTP Basic auth, so over plain HTTP they are readable on the network. That
is documented behaviour, not a vulnerability: give `https://` addresses in
production.

## Non-security issues

For bugs, documentation issues, or questions that do not expose a
vulnerability, use public issues or the Narad Discussions board instead of the
private advisory flow.
