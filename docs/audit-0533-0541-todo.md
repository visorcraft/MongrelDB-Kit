# 0.53.3 cursor and 0.54.1 cancellation audit todo

This ledger tracks the two implementation audits supplied on 2026-07-14.
Checkboxes cover implementation, tests, documentation, and qualification.

## Cursor and generation audit

- [x] Bind cursors to one result manifest, table/schema/index generation,
  principal, security version, query-time TTL, canonical request fingerprint,
  and expiration.
- [x] Protect cursors with a process-local MAC and reject tampering, unknown,
  expired, cross-user, cross-instance, and stale cursors with typed errors.
- [x] Preserve deterministic global ranks, ties, exact reranking, and no
  duplicate or missing rows across pages.
- [x] Test data changes, index rebuild, flush, compaction, reopen, TTL,
  security, masks, RLS, schema/index changes, tampering, and replay.
- [x] Reauthorize Admin inside every scored-search retry before returning
  explain data. Test direct role revocation and dropped users.
- [x] Replace whole-table read-generation cloning with structurally shared
  generations. Bound retained cursor state.
- [x] Measure generation clone bytes/nanos, 1M overlapping read/write RSS,
  commit p99, and cursor lifetime behavior.
- [x] Rename misleading NAPI approximate and incremental aggregate methods so
  names, docs, and actual full-scan/recompute behavior agree.
- [x] Apply and test the ANN raw-candidate ceiling, cap trace, selective RLS,
  exact rerank, and bounded memory.
- [x] Document canonical fingerprint versioning and cross-instance behavior.
- [x] Run the 100k, concurrency, and 1M cursor qualification tests from a clean
  exact source tree.

## Write-path follow-ups

- [x] Track UPDATE `changed_columns` separately from the complete post-image.
- [x] Cover triggers, cascades, upsert, RLS, constraints, and final revocation
  checks with the correct representation.
- [x] Remove catalog filesystem refresh from the WAL critical section.
- [x] Share the process-wide security-version coordinator across handles and
  fail closed on refresh errors.
- [x] Test zero unchanged-security catalog reads, refresh, dropped users,
  revoked roles, credentialless use, and shared-handle observation.
- [x] Benchmark authenticated 10k-row batch latency and catalog-read count.

## SQL control core

- [x] Add `ExecutionControl` with cancellation reason, deadline, parent-child
  propagation, checked error conversion, and AI-context compatibility.
- [x] Add cryptographic strict `QueryId`, bounded active/finished registries,
  duplicate rejection, ownership, session metadata, phases, fingerprints, and
  RAII cleanup.
- [x] Make cancellation and commit fence one atomic race. Preserve durable
  commit truth.
- [x] Register before SQL/session queue waits. Use cancellable permits and one
  control through multi-statement execution.
- [x] Give cached plans and prepared execution fresh controls. Avoid cancelled
  result-cache insertion.

## SQL execution and transactions

- [x] Build a per-query DataFusion task context and managed execution stream.
- [x] Propagate checkpoints through materialized, cursor, prebuilt-batch,
  residual-filter, Arrow conversion, native dispatch, native aggregate, FK
  join, scored ANN/Sparse/MinHash, and external-module paths.
- [x] Drop execution streams on cancellation. Reject or document
  non-cooperative modules.
- [x] Add statement staging savepoints, aborted transaction state, commit
  fence, cancel-vs-commit tests, and multi-statement outcome metadata.
- [x] Document SELECT, autocommit DML/DDL, explicit transaction, disconnect,
  and retry semantics.

## Server

- [x] Extend `/sql` with validated timeout/query ID and response header.
- [x] Add owner/admin status and cancel routes with 404 existence hiding.
- [x] Bound timeout, registry, SQL concurrency, output rows, and output bytes.
- [x] Make session queueing, prepared planning/execution, buffered
  serialization, and Arrow streaming cancellable.
- [x] Cancel on stream drop, session close, and graceful shutdown. Keep active
  sessions safe from the idle reaper.
- [x] Emit typed errors, cancellation/latency/commit-race/output metrics, and no
  raw SQL in registry or status.
- [x] Advertise SQL cancellation capability version 1.

## Native and Kit APIs

- [x] Add NAPI async query handles/options and C FFI explicit query handles.
- [x] Add Rust HTTP client start/cancel/status handles.
- [x] Add Kit Rust embedded and remote options, handles, typed errors,
  capability negotiation, separate transport timeout, and best-effort cancel.
- [x] Add TypeScript embedded/remote `AbortSignal`, async query handles, typed
  errors, listener cleanup, and responsive event-loop behavior.
- [x] Add TypeScript migration and SQL DDL helpers with explicit timeout,
  signal, and query-ID options.
- [x] Add Python embedded thread-safe handles, GIL release, asyncio
  cancellation, remote controls, separate transport timeout, and typed errors.
- [x] Add CLI timeout/query ID flags and Ctrl-C cancellation.
- [x] Preserve uncontrolled remote SQL compatibility with older servers. Fail
  controlled requests clearly when v1 capability is absent.

## Testing, performance, and documentation

- [x] Cover queue, planning, scans, native paths, DataFusion, scored SQL,
  external modules, cache/plan reuse, duplicate IDs, and cleanup.
- [x] Cover pre-fence INSERT/UPDATE/DELETE, commit winner, cancel winner,
  explicit transaction abort/rollback, and completed statement count.
- [x] Cover server IDs, ownership, prepared queries, session lifecycle,
  shutdown, stream drop, serialization, output limit, and responsive control
  endpoints.
- [x] Cover Rust embedded/remote, TypeScript embedded/remote, Python
  embedded/remote, NAPI, C FFI, and CLI behavior.
- [x] Benchmark unused-control overhead and cancellation latency. Record stuck
  query count and registry cleanup.
- [x] Document compatibility, errors, transaction semantics, migration/DDL
  timeout guidance, and AI-agent use.
- [ ] Pin MongrelDB Kit patches to the final exact MongrelDB SHA.
- [ ] Run final Rust, server, NAPI/TypeScript, C FFI, Python, remote
  cross-repository, formatting, and lint gates on clean trees.
- [ ] Record final exact SHAs and benchmark artifact in the qualification
  documents.

## Final re-audit

- [ ] Re-read every point in both audit documents against source and tests.
- [ ] Search for old uncontrolled SQL entry points and raw catalog reads under
  WAL lock.
- [ ] Confirm no raw SQL/parameters leak through query status, logs, or errors.
- [ ] Confirm both repositories are clean and all commits are pushed.
