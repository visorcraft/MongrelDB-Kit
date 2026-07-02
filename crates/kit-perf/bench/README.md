# Cross-language benchmark suite (Kit P0)

One workload (single-row insert/update/delete + bulk-ingest throughput)
exercised through **Rust Kit**, **core-direct** (raw `mongreldb-core`),
**TypeScript Kit**, **Python Kit**, and **SQLite** for apples-to-apples
comparison. This is measurement infrastructure, not a speedup.

## Rust (Kit vs core-direct vs SQLite)

From the repo root:

```bash
cd crates/kit-perf && cargo run --release --bin compare
```

Outputs a markdown table: single-insert/update/delete latency (median of 7,
µs/ms/s) + bulk-ingest throughput (Melem/s) for all three engines at
N=100 and N=1,000,000.

## TypeScript Kit

Build the TS package first, then run the bench script:

```bash
cd packages/kit && npm run build
npx tsx ../../crates/kit-perf/bench/ts/bench.ts [N]
```

## Python Kit

Build the Python extension first, then run:

```bash
cd python/mongreldb_kit && maturin develop
python ../../crates/kit-perf/bench/py/bench.py [N]
```

## What it measures

- **Single-row insert + commit** — the hot path: `begin → insert → commit`
  with full per-row validation and PK/unique/FK guard checks (Kit) vs raw
  `Table::put → commit` (core-direct).
- **Single-row update + commit** — Kit's update is delete+reinsert at the
  storage layer; core-direct is a PK upsert.
- **Single-row delete + commit**.
- **Bulk-ingest throughput** — `insert_many` (Kit) vs `put_batch` (core),
  measuring Melem/s over a single transaction.
