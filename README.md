# MongrelDB Kit

MongrelDB Kit is the application-facing persistence layer for [MongrelDB](https://www.MongrelDB.com). It gives TypeScript, Rust, and Python applications a schema-aware query builder, migrations, relational constraints, batch writes, auto-increment ids, and stable semantics across languages.

The repository also ships the `mongreldb-kit` CLI for schema validation, migration planning/status, drift checks, fixture import/export, and type generation.

[![crates.io](https://img.shields.io/crates/v/mongreldb-kit)](https://crates.io/crates/mongreldb-kit)
[![npm](https://img.shields.io/npm/v/@mongreldb/kit)](https://www.npmjs.com/package/@mongreldb/kit)
[![PyPI](https://img.shields.io/pypi/v/mongreldb-kit)](https://pypi.org/project/mongreldb-kit/)

## Packages And Tools

| Surface | Package / crate | Install or run |
|---|---|---|
| TypeScript | `@mongreldb/kit` | `npm install @mongreldb/kit mongreldb` |
| Rust | `mongreldb-kit` | `cargo add mongreldb-kit` |
| Python | `mongreldb-kit` | `pip install mongreldb-kit` |
| CLI | `mongreldb-kit-cli` (`mongreldb-kit` binary) | `cargo run -p mongreldb-kit-cli -- --help` |

## What It Provides

- Schema helpers for typed tables, stable table/column ids, defaults, indexes, checks, unique constraints, and foreign keys.
- Synchronous TypeScript CRUD/query builder with predicates, ordering, projections, aggregates, joins, subqueries, CTEs, batch inserts, updates, and deletes.
- Rust and Python APIs backed by the same Rust core and verified with cross-language conformance fixtures.
- Migration runner with content-addressed checksums and stored schema catalog.
- Relational constraint enforcement on top of MongrelDB transactions: not-null, type/range/string validation, unique/composite unique, foreign keys, and cascade/set-null/restrict deletes.

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
- [Constraints](docs/constraints.md)
- [Transactions](docs/transactions.md)
- [Errors](docs/errors.md)
- [Internal tables](docs/internal-tables.md)
- [Testing](docs/testing.md)
- [Production checklist](docs/production-checklist.md)
- [Specification](docs/spec.md)

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
} from '@mongreldb/kit';

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

- TypeScript requires Node.js 22+ and the native `mongreldb` peer dependency.
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
