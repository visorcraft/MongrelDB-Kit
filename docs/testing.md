# Testing

MongrelDB Kit provides test helpers and a shared conformance suite so all language implementations can prove identical behavior.

## Test structure

```text
tests/
  conformance/
    fixtures/       # JSON fixtures shared by all languages
      schema.json
      inserts.json
      updates.json
      deletes.json
      queries.json
      migrations.json
    typescript/     # TypeScript runner
    rust/           # Rust runner
    python/         # Python runner
```

## Conformance fixtures

Fixtures describe inputs and expected outcomes in a language-neutral format. Each language binding loads the same JSON files and asserts the same results.

Coverage includes:

- Schema serialization
- Key encoding
- Defaults (static, now, UUID, sequence)
- Validation (not-null, type, enum, min/max, length, regex, checks)
- Unique constraints, including nullable unique semantics
- Primary-key operations
- Sequence conflicts
- Foreign-key existence
- Parent delete vs child insert conflict
- Cascade delete
- Set-null delete
- Restrict delete
- Migration apply and failure
- Query AST serialization
- Query result shape

## Running the conformance suite

### TypeScript

```sh
cd packages/kit
rtk npm test
```

This runs both package tests and `tests/conformance/typescript`.

### Rust

```sh
rtk cargo test --workspace
```

The conformance crate is a workspace member at `tests/conformance/rust`.

### Python

```sh
cd python/mongreldb_kit
rtk maturin develop
rtk .venv/bin/pytest ../../python/tests ../../tests/conformance/python
```

## Unit tests

Each crate and package has focused unit tests for its layer:

- `crates/mongreldb-kit-core` - key encoding, validation, migration planning, delete planning, query AST
- `crates/mongreldb-kit` - database open/create, transactions, CRUD, constraints, migrations
- `packages/kit` - TypeScript DSL, query builder, migration runner
- `python/tests` - Python schema builders and CRUD

Run them individually:

```sh
# Rust core
rtk cargo test -p mongreldb-kit-core

# Rust storage-backed crate
rtk cargo test -p mongreldb-kit

# TypeScript
cd packages/kit
rtk npm test

# Python
cd python/mongreldb_kit
rtk .venv/bin/pytest ../../python/tests
```

## Test fixture helpers

### TypeScript

Kit tests are fast because they run against a **real** on-disk MongrelDB in a throwaway temp
directory - no mocks. The pattern is always the same: `mkdtempSync` a unique directory,
`KitDatabase.openSync` it, run migrations, use it, then `db.close()` and `rmSync` the directory.

> `KitDatabase.openSync(dir, schema)` takes a **data directory**, not a single file. Give each test
> a fresh directory from `mkdtempSync` so the OS guarantees a unique, non-colliding path.

#### A reusable `freshDb()` helper

Define the shared "store" schema once, then hand every test its own isolated database:

```ts
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  KitDatabase, Schema, table, int, text, timestamp,
  sequenceDefault, nowDefault, staticDefault, unique, index, foreignKey,
} from '@visorcraft/mongreldb-kit';
import type { Migration } from '@visorcraft/mongreldb-kit';

const customers = table('customers', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('customers_id_seq') }),
    text('email', { nullable: false }),
    text('name', { nullable: false }),
    text('tier', { enumValues: ['free', 'pro'], default: staticDefault('free') }),
    timestamp('created_at', { default: nowDefault() }),
  ],
  primaryKey: 'id',
  unique: [unique(['email'])],
});

const orders = table('orders', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('orders_id_seq') }),
    int('customer_id', { nullable: false }),
    timestamp('placed_at', { default: nowDefault() }),
  ],
  primaryKey: 'id',
  indexes: [index(['customer_id'])],
  foreignKeys: [
    foreignKey(['customer_id'], { table: 'customers', columns: ['id'] }, { onDelete: 'cascade' }),
  ],
});

export const schema = new Schema([customers, orders]);
export const migrations: Migration[] = [{ version: 1, name: 'init', up: () => {} }];

/** Open a brand-new migrated database in a unique temp directory. */
export function freshDb() {
  const dir = mkdtempSync(join(tmpdir(), 'kit-test-'));
  const db = KitDatabase.openSync(dir, schema);
  db.migrateSync(schema, migrations);
  return {
    db,
    cleanup() {
      db.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

export { customers, orders };
```

#### Using it - one database per test

Give **every** test its own `freshDb()`. Sharing a database across tests lets ids, unique guards,
and prior rows leak between cases; a fresh directory per test keeps them hermetic and order-
independent. Always tear down in a `finally` (or an `afterEach`) so the temp directory does not
linger:

```ts
import { describe, it, expect } from 'vitest';
import { freshDb, customers } from './fixtures';

describe('customers', () => {
  it('assigns a 1-based id and applies defaults', () => {
    const { db, cleanup } = freshDb();
    try {
      const ada = db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada' }).executeSync();
      expect(ada.id).toBe(1n);     // sequences start at 1 (never 0) in a fresh DB
      expect(ada.tier).toBe('free'); // staticDefault applied
    } finally {
      cleanup();
    }
  });

  it('starts from 1 again in an independent database', () => {
    const { db, cleanup } = freshDb();
    try {
      const first = db.insertInto(customers).values({ email: 'a@example.com', name: 'A' }).executeSync();
      expect(first.id).toBe(1n);   // not affected by the previous test
    } finally {
      cleanup();
    }
  });
});
```

If you prefer hooks over `try/finally`, capture the handle in `beforeEach` and tear down in
`afterEach`:

```ts
import { beforeEach, afterEach } from 'vitest';

let ctx: ReturnType<typeof freshDb>;
beforeEach(() => { ctx = freshDb(); });
afterEach(() => { ctx.cleanup(); });
```

#### Notes

- **Sequences are 1-based and per-database.** The first auto-increment id is `1n`, never `0n`, and a
  fresh `freshDb()` restarts every sequence from 1. Assert on `1n`/`2n`, not `0n`.
- **int64 ids are `bigint`.** Compare against `1n`, and pass `bigint` foreign keys (`{ customer_id: ada.id }`).
- **Migrations must run before any reads/writes.** Call `db.migrateSync(schema, migrations)` inside
  the fixture (as above) so the schema catalog and `__kit_*` tables are present.
- **Always `rmSync` the directory.** Temp dirs are not auto-removed; clean up to avoid filling
  `os.tmpdir()` across thousands of test runs.

### Rust

```rust
use mongreldb_kit::{Database, Schema};
use std::path::PathBuf;

fn temp_db(schema: Schema) -> (Database, PathBuf) {
    let dir = tempfile::tempdir().unwrap().keep();
    let db = Database::create(&dir, schema).unwrap();
    (db, dir)
}
```

### Python

```python
import os
import tempfile
from mongreldb_kit import Database

def fresh_db(schema):
    path = os.path.join(tempfile.mkdtemp(), "test.kitdb")
    return Database.create(path, schema)
```

## Writing new conformance tests

1. Add a JSON fixture under `tests/conformance/fixtures/` describing the input and expected result.
2. Update the TypeScript, Rust, and Python runners to load the fixture and assert the outcome.
3. Run all three suites before committing.

## Continuous integration

CI should run:

- `cargo fmt --check`
- `cargo clippy --workspace`
- `cargo test --workspace`
- TypeScript typecheck (`npm run check`)
- TypeScript tests (`npm test`)
- Python tests (`pytest`)
- Conformance suite for all languages
- Package build checks
- Documentation link checks

## See also

- [TypeScript](./typescript.md) - the CRUD surface exercised in these fixtures.
- [Defaults & sequences](./defaults.md) - why fresh-DB ids start at `1`.
- [Errors](./errors.md) - asserting on typed errors and stable `.code`s in tests.
- [Migrations](./migrations.md) - the `migrateSync` step every fixture runs.
