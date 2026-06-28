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

- `crates/mongreldb-kit-core` — key encoding, validation, migration planning, delete planning, query AST
- `crates/mongreldb-kit` — database open/create, transactions, CRUD, constraints, migrations
- `packages/kit` — TypeScript DSL, query builder, migration runner
- `python/tests` — Python schema builders and CRUD

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

Use a temporary directory and `KitDatabase.openSync`:

```ts
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KitDatabase, Schema, table, int, text } from '@mongreldb/kit';

function freshDb(schema: Schema) {
  const path = join(tmpdir(), `kit-test-${Date.now()}.kitdb`);
  return KitDatabase.openSync(path, schema);
}
```

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
