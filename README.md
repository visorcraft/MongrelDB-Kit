<p align="center">
  <img src="assets/mongrel.png" alt="MongrelDB logo" width="250" />
</p>

<h1 align="center">MongrelDB Kit</h1>

<p align="center">
  <b>The application-facing persistence layer for MongrelDB - schema-aware query builder, migrations, relational constraints, and stable semantics across TypeScript, Rust, Python, and PHP.</b>
</p>

<p align="center">
  <a href="https://crates.io/crates/mongreldb-kit"><img src="https://img.shields.io/crates/v/mongreldb-kit" alt="crates.io" /></a>
  <a href="https://www.npmjs.com/package/@visorcraft/mongreldb-kit"><img src="https://img.shields.io/npm/v/@visorcraft/mongreldb-kit" alt="npm" /></a>
  <a href="https://pypi.org/project/mongreldb-kit/"><img src="https://img.shields.io/pypi/v/mongreldb-kit" alt="PyPI" /></a>
</p>

## Packages And Tools

| Surface | Package / crate | Install or run |
|---|---|---|
| TypeScript | `@visorcraft/mongreldb-kit` | `npm install @visorcraft/mongreldb-kit @visorcraft/mongreldb` |
| Rust | `mongreldb-kit` | `cargo add mongreldb-kit` |
| Python | `mongreldb-kit` | `pip install mongreldb-kit` |
| CLI | `mongreldb-kit-cli` (`mongreldb-kit` binary) | `cargo run -p mongreldb-kit-cli -- --help` |

## What It Provides

- Schema helpers for typed tables, stable table/column ids, defaults, indexes, checks, unique constraints, and foreign keys. Full type set: int64, float64, bool, text, bytes (BLOB), JSON, timestamp, date, date64, time64, interval, decimal128, UUID, JSON (native), and array columns.
- Synchronous TypeScript CRUD/query builder with predicates, ordering, projections, aggregates, joins, subqueries, CTEs, batch inserts, updates, and deletes.
- Rust and Python APIs backed by the same Rust core and verified with cross-language conformance fixtures.
- Migration runner with content-addressed checksums, stored schema catalog, table renames, and SQL views.
- Embedded SQL surface (sql / sqlArrow / sqlRows) with recursive CTEs, window functions, CREATE TABLE AS SELECT, materialized views, multi-statement execution, and a mongreldb_fts_rank relevance-scoring UDF.
- Storage tuning (spill thresholds, compaction zstd, result-cache sizing, index build policy), trigger config, and per-table introspection (run count, page-cache stats, memtable/cache lengths).
- Non-blocking async I/O variants (`putAsync` / `queryAsync` / `countAsync` / …) and `WriteBuffer` micro-batching for high-throughput ingest (TypeScript).
- Engine-side trigger management plus SQL-backed virtual/external table helpers.
- Extended SQL Function helpers for JSON, date/time, aggregate, and math-style SQL calls.
- User/role/credentials management with optional storage-layer enforcement: Argon2id-hashed catalog users, roles, `GRANT`/`REVOKE` table-level permissions, daemon HTTP Basic + Bearer auth, and opt-in `require_auth` credential enforcement (credentialed open/create constructors, `enable_auth`/`disable_auth`, offline recovery) - exposed through every language API, the embedded SQL surface, and the CLI (`user` / `role` / `auth` subcommands).
- Relational constraint enforcement on top of MongrelDB transactions: not-null, type/range/string validation, unique/composite unique, foreign keys, and cascade/set-null/restrict deletes.
- Multi-process file locking, replication, and change-data-capture via the daemon.

## Documentation

- [Overview](docs/README.md)
- [TypeScript quickstart](docs/typescript.md)
- [Rust quickstart](docs/rust.md)
- [Python quickstart](docs/python.md)
- [CLI](docs/cli.md)
- [Schema DSL](docs/schema.md)
- [Types](docs/types.md)
- [Defaults](docs/defaults.md)
- [Query builder](docs/query-builder.md)
- [Migrations](docs/migrations.md)
- [Triggers](docs/triggers.md)
- [Extended SQL & virtual tables](docs/extended-sql-and-virtual-tables.md)
- [Constraints](docs/constraints.md)
- [Transactions](docs/transactions.md)
- [Errors](docs/errors.md)
- [Internal tables](docs/internal-tables.md)
- [Testing](docs/testing.md)
- [Production checklist](docs/production-checklist.md)

## History retention and time-travel reads

Both the embedded `KitDatabase` and the daemon client `RemoteDatabase` expose
history-retention controls:

```ts
// TypeScript
db.setHistoryRetentionEpochs(100);   // embedded: number argument
remote.setHistoryRetentionEpochs(100n); // remote: bigint argument
console.log(db.historyRetentionEpochs());     // bigint
console.log(remote.historyRetentionEpochs()); // bigint
console.log(db.earliestRetainedEpoch());      // bigint
```

```python
# Python (embedded)
db.set_history_retention_epochs(100)
print(db.history_retention_epochs())  # int
print(db.earliest_retained_epoch())   # int

# Python (remote)
remote.set_history_retention_epochs(100)
print(remote.history_retention_epochs())
print(remote.earliest_retained_epoch())
```

Set retention **before** writing the data you want to time-travel back to. The
engine default keeps only the latest epoch, so older snapshots are pruned
unless retention is raised first. Increasing retention later cannot restore
history that has already been removed. Read past snapshots with
`db.rowsAtEpoch('table', epoch)` (embedded) or `SELECT ... AS OF EPOCH <epoch>`
(embedded SQL and the daemon).

## Quick Example

Minimal TypeScript schema and CRUD flow:

**TypeScript**
```ts
import {
  KitDatabase,
  Schema,
  table,
  int,
  text,
  sequenceDefault,
  unique,
  eq
} from '@visorcraft/mongreldb-kit';

const users = table('users', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('users_id_seq') }),
    text('email', { nullable: false }),
    text('name', { nullable: true })
  ],
  primaryKey: 'id',
  unique: [unique(['email'], { name: 'users_email_uq' })]
});

const schema = new Schema([users]);
const db = KitDatabase.openSync('./app-data', schema);

db.migrateSync(schema, [
  {
    version: 1,
    name: 'initial',
    up({ ensureTable }) {
      ensureTable(users);
    }
  }
]);

const alice = db.insertInto(users)
  .values({ email: 'alice@example.com', name: 'Alice' })
  .executeSync();

const [row] = db.selectFrom(users)
  .where(eq(users.id, alice.id))
  .executeSync();

console.log(row);
db.close();
```

See the language docs for complete runnable examples in TypeScript, Rust, and Python.

## Development Notes

- TypeScript requires Node.js 22+ and the native `@visorcraft/mongreldb` peer dependency.
- A MongrelDB database path is a data directory, not a single database file.
- In this mono-repo checkout, the TypeScript package loads the native addon from the sibling MongrelDB repo. Build `crates/mongreldb-node` there with `npm run build` in release mode before benchmarking; stale debug `.node` builds make bulk paths much slower.

## Building and testing

```sh
# Rust
rtk cargo check --workspace
rtk cargo test --workspace

# TypeScript
cd packages/kit
rtk npm ci
rtk npm run build
rtk npm run check
rtk npm test

# Python
cd python/mongreldb_kit
rtk python -m venv .venv
rtk .venv/bin/pip install maturin
rtk maturin develop
rtk .venv/bin/pytest ../../python/tests ../../tests/conformance/python

# CLI
rtk cargo run -p mongreldb-kit-cli -- --help
```

## License

MIT OR Apache-2.0
