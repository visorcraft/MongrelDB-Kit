# MongrelDB Kit

Multi-language application persistence layer for [MongrelDB](https://github.com/visorcraft/mongreldb). MongrelDB Kit gives TypeScript, Rust, and Python applications a schema-aware query-builder API with migrations, relational constraints, batch writes, auto-increment ids, and stable semantics across languages.

[![crates.io](https://img.shields.io/crates/v/mongreldb-kit)](https://crates.io/crates/mongreldb-kit)
[![npm](https://img.shields.io/npm/v/@mongreldb/kit)](https://www.npmjs.com/package/@mongreldb/kit)
[![PyPI](https://img.shields.io/pypi/v/mongreldb-kit)](https://pypi.org/project/mongreldb-kit/)

## Packages

| Language | Package | Install |
|---|---|---|
| TypeScript | `@mongreldb/kit` | `npm install @mongreldb/kit` |
| Rust | `mongreldb-kit` | `cargo add mongreldb-kit` |
| Python | `mongreldb-kit` | `pip install mongreldb-kit` |

## Documentation

- [Specification](docs/spec.md) — layers, internal tables, encoding, error codes, concurrency
- [TypeScript quickstart](docs/typescript.md)
- [Rust quickstart](docs/rust.md)
- [Python quickstart](docs/python.md)
- [Migrations](docs/migrations.md)
- [Constraints](docs/constraints.md)
- [Query builder](docs/query-builder.md)
- [Testing](docs/testing.md)
- [Production checklist](docs/production-checklist.md)

## Quick Example

The same schema and CRUD flow in TypeScript, Rust, and Python:

**TypeScript**
```ts
import { KitDatabase, Schema, table, int, text, sequenceDefault, unique } from '@mongreldb/kit';

const users = table('users', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('users_id_seq') }),
    text('email'),
    text('name', { nullable: true })
  ],
  primaryKey: 'id',
  unique: [unique(['email'], { name: 'users_email_uq' })]
});

const db = KitDatabase.openSync('./data', new Schema([users]));
const inserted = db.insertInto(users).values({ email: 'alice@example.com' }).executeSync();
const many = db.insertInto(users).valuesMany([
  { email: 'bob@example.com' },
  { email: 'cleo@example.com' }
]).executeSync();
```

See the language docs for complete runnable examples.

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
```

## License

MIT OR Apache-2.0
