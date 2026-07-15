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
- [x] Pin MongrelDB Kit patches to the final exact MongrelDB SHA.
- [x] Run final Rust, server, NAPI/TypeScript, C FFI, Python, remote
  cross-repository, formatting, and lint gates on clean trees.
- [x] Record final exact SHAs and benchmark artifact in the qualification
  documents.

## Final re-audit

- [x] Re-read every point in both audit documents against source and tests.
- [x] Search for old uncontrolled SQL entry points and raw catalog reads under
  WAL lock.
- [x] Confirm no raw SQL/parameters leak through query status, logs, or errors.
- [x] Confirm both repositories are clean and all commits are pushed.

## Final qualification evidence

Implementation and qualification source:

- MongrelDB: `ab67f9a042671fed57c1e7e9f350641b0aa32b2c`.
- MongrelDB Kit: `b15370f0a3cd97af67bbfc976eadd2f99372d98e`.
- At qualification time, root Kit patches and `crates/kit-perf/Cargo.toml`
  pinned the exact MongrelDB revision above. Release bumps strip those dev-only
  patches and resolve the published engine version.
- Both implementation trees were clean and pushed before this evidence-only
  ledger update.

Final gates:

- MongrelDB formatting, workspace clippy with all targets/features, and
  workspace tests passed. Workspace result: 1,061 passed, 1 ignored.
- MongrelDB server, client, Node Rust, C FFI, Kit FFI, and JNI suites passed.
- MongrelDB release NAPI build and test passed.
- Final focused cursor, cancellation, security, scored-query, and transaction
  suites passed.
- MongrelDB Kit formatting, workspace clippy with all targets/features, and
  workspace tests passed. Workspace result: 179 passed.
- TypeScript release-addon build, check, and tests passed: 308 tests.
- Python build, tests, and conformance passed: 159 tests.
- TypeScript used the local release-built addon from the exact sibling source
  because the `0.54.1` npm peer was not published during qualification.

Benchmark and characterization evidence:

- Strict clean 100k AI qualification passed.
- Strict clean 1M AI structural qualification passed with 1,000,000 rows and
  10 measured queries.
- AI concurrency, 1M read-generation, and ANN candidate-cap validators passed.
- Authenticated 10,000-row batch committed all rows in 15 ms with zero catalog
  disk reads.
- Controlled point query: 2.1795 to 2.2743 microseconds.
- Controlled 100k scan: 27.314 to 27.662 milliseconds.
- Accepted cancellation to scan completion: 85.665 to 93.687 microseconds.
- Accepted cancellation to queued completion: 3.9783 to 4.0002 microseconds.
- Criterion reported no statistically significant regression for all four
  cancellation measurements.

Final source re-audit confirmed:

- security catalog refresh occurs before the security gate, commit lock, and
  shared WAL critical section;
- final UPDATE authorization uses `changed_columns`, while constraints, RLS,
  triggers, WAL, and post-images retain the complete row;
- SQL registration precedes queue waits and one control reaches planning,
  execution, native/scored/external paths, serialization, and commit fencing;
- status and slow-query logs expose query IDs, fingerprints, phases, and safe
  operation names, never raw SQL or parameters;
- cursor continuation is bound to its manifest, principal, security/schema/
  index generations, query time, expiry, canonical request hash, and MAC;
- no audit-specific `todo!` or `unimplemented!` remains.
