# Security

This document describes the security properties of MongrelDB Kit and how to
report vulnerabilities.

## Overview

MongrelDB Kit is the application-facing persistence layer for MongrelDB. It
ships four surfaces - Rust crates, a TypeScript package, a Python facade,
and a CLI - all of which talk to `mongreldb-server` over HTTP and/or peer on
the `@visorcraft/mongreldb` NAPI addon for embedded access. The Kit itself
holds no encryption keys and stores no data at rest; it is a client and
schema/migration layer.

## Client security properties

- The Kit communicates with `mongreldb-server` over plain HTTP. The daemon
  binds to `127.0.0.1` by default - traffic stays on the loopback interface.
  For remote or multi-tenant deployments, terminate TLS in a reverse proxy
  (nginx, Caddy) in front of the daemon.
- The Kit supports Bearer token and HTTP Basic auth, matching the daemon's
  `--auth-token` and `--auth-users` modes. Tokens are sent only in the
  `Authorization` header and are never logged by the Kit.
- The native Condition API and transaction builder accept typed parameters
  (column IDs, value bytes, typed column buffers) - no string interpolation,
  no SQL injection surface. User-supplied values are serialized as typed
  JSON, not concatenated into queries.
- SQL is sent to the daemon's DataFusion-backed `/sql` endpoint, which
  parses and parameterizes it server-side. The Kit never interprets SQL
  locally.
- Idempotency keys are caller-supplied opaque strings; the Kit does not
  derive or store them.

## Embedded access (NAPI addon)

The TypeScript package peers on the `@visorcraft/mongreldb` NAPI addon for
embedded, in-process access to `mongreldb-core`. When embedded, the Kit runs
inside the host process and inherits that process's filesystem and memory
permissions. There is no network hop and no separate authentication boundary
- secure the host process accordingly.

## Daemon security (mongreldb-server)

The Kit is a client of `mongreldb-server`. The daemon's security posture:

- Binds to `127.0.0.1` only - not accessible from other machines.
- **No authentication by default** - any local process can query, write, or
  delete data. Enable `--auth-token` or `--auth-users` for any shared host.
- No TLS - traffic is plaintext on the loopback interface.
- No rate limiting or request size caps.

For remote access or multi-tenant environments, place a reverse proxy
(nginx, Caddy) in front with TLS termination and authentication. Do not
expose the daemon directly to a network.

## Input validation

- Schema, migration, and constraint definitions are typed - invalid column
  types, bad foreign-key references, and malformed migrations are rejected
  before any request is sent to the daemon.
- Bulk-load paths accept typed buffers (`NativeColumn`) - invalid buffer
  lengths are rejected by the `validate()` method on deserialization.
- User/role/credential management is executed through SQL against the
  daemon; the Kit does not store or hash credentials itself.

## Dependency security

MongrelDB Kit's direct dependencies are the MongrelDB engine crates
(`mongreldb-core`, `mongreldb-query`, `mongreldb-server`), the
`@visorcraft/mongreldb` NAPI addon, and standard per-language tooling
(rust, Node.js, maturin/PyO3). All are MIT or Apache-2.0 licensed. Report
dependency vulnerabilities through GitHub's Dependabot alerts or the
private vulnerability reporting flow below.

## Reporting a vulnerability

**Do not file a public GitHub issue, discussion, or pull request for
security problems.** Report privately through **GitHub's private
vulnerability reporting**:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability**.
3. Fill in the advisory form with the details below.

This keeps the report confidential between you and the maintainers
until a fix is ready. Please include as much as you can:

- a description of the issue and its impact,
- step-by-step reproduction steps,
- the MongrelDB Kit version, OS, and toolchain versions,
- the relevant configuration, error output, or a proof-of-concept,
- a suggested fix or mitigation, if you have one.

### What to expect

- **Acknowledgement** of your report within a few days.
- An initial assessment and, where confirmed, a remediation plan.
- Progress updates through the private advisory thread until the
  issue is resolved.
- Credit for your responsible disclosure in the advisory, unless you
  prefer to remain anonymous.

We ask that you give us a reasonable opportunity to ship a fix before
any public disclosure.
