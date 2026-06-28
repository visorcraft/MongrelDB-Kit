# Migrations

MongrelDB Kit records schema changes in a versioned migration list. The runner applies pending migrations in order, records them in the internal `_migrations` table, and updates the schema catalog.

## Migration JSON format

Each migration is an object with `version`, `name`, and `ops`:

```json
[
  {
    "version": 1,
    "name": "initial",
    "ops": [
      { "create_table": { "name": "users" } }
    ]
  },
  {
    "version": 2,
    "name": "add_posts",
    "ops": [
      { "create_table": { "name": "posts" } },
      { "add_column": { "table": "users", "column": "bio" } }
    ]
  }
]
```

## Operations

| Operation | JSON | Notes |
|---|---|---|
| Create table | `{"create_table": {"name": "users"}}` | Creates the table from the current schema |
| Drop table | `{"drop_table": {"name": "users"}}` | Destructive |
| Add column | `{"add_column": {"table": "users", "column": "bio"}}` | Column must be nullable or have a default |
| Drop column | `{"drop_column": {"table": "users", "column": "bio"}}` | Destructive |
| Add index | `{"add_index": {"table": "users", "index": "idx_email"}}` | |
| Drop index | `{"drop_index": {"table": "users", "index": "idx_email"}}` | |
| Add unique | `{"add_unique": {"table": "users", "constraint": "uq_email"}}` | Backfills guards |
| Drop unique | `{"drop_unique": {"table": "users", "constraint": "uq_email"}}` | Cleans guards |
| Add foreign key | `{"add_foreign_key": {"table": "posts", "constraint": "fk_posts_user"}}` | Verifies existing rows |
| Drop foreign key | `{"drop_foreign_key": {"table": "posts", "constraint": "fk_posts_user"}}` | |
| Add check | `{"add_check": {"table": "users", "constraint": "chk_email"}}` | Validates existing rows |
| Drop check | `{"drop_check": {"table": "users", "constraint": "chk_email"}}` | |
| Raw SQL | `{"raw_sql": "SELECT 1"}` | Not supported by all adapters |

## How migrations work

1. The runner acquires the advisory lock in `__kit_migration_locks`.
2. It reads already-applied migrations from `__kit_schema_migrations`.
3. It computes pending migrations: those with a version greater than the maximum applied version.
4. Each pending migration runs inside a transaction.
5. Successes are recorded as `applied`; failures are recorded as `failed` and the error is raised.
6. The schema catalog is rewritten and the lock is released.

The runner is idempotent: running it twice applies nothing the second time.

## Migration checksums

Each migration is checksummed as `sha256(version:name)`. This is stored in the migration record and is the same across all languages.

## CLI commands

The `mongreldb-kit` CLI reads migrations from JSON files.

Apply migrations:
```sh
mongreldb-kit migrate apply ./app.kitdb ./migrations.json
```

Show status:
```sh
mongreldb-kit migrate status ./app.kitdb
```

Show pending migrations without applying:
```sh
mongreldb-kit migrate plan ./app.kitdb ./migrations.json
```

Generate a skeleton migration from drift:
```sh
mongreldb-kit generate migration ./schema.json --from ./app.kitdb
```

Validate a schema file:
```sh
mongreldb-kit schema validate ./schema.json
```

Print the stored schema:
```sh
mongreldb-kit schema print ./app.kitdb
```

## TypeScript migrations

```ts
db.migrateSync(schema, [
  {
    version: 1,
    name: 'initial',
    up({ ensureTable }) {
      ensureTable(users);
    }
  },
  {
    version: 2,
    name: 'add_bio',
    up({ addColumn }) {
      addColumn('users', text('bio', { nullable: true }));
    }
  }
]);
```

The `MigrationContext` provides:
- `kit` — the `KitDatabase`
- `db` — the native MongrelDB database object
- `ensureTable(table)` — create a table from the schema if it does not exist
- `addColumn(tableName, column)` — add a column, backfilling non-nullable columns with their default
- `sql(sql)` — run raw SQL (async migrations only)

## Rust migrations

```rust
use mongreldb_kit::{Migration, MigrationOp};

let migrations = vec![
    Migration {
        version: 1,
        name: "initial".into(),
        ops: vec![MigrationOp::CreateTable { name: "users".into() }],
    },
    Migration {
        version: 2,
        name: "add_posts".into(),
        ops: vec![
            MigrationOp::CreateTable { name: "posts".into() },
            MigrationOp::AddColumn { table: "users".into(), column: "bio".into() },
        ],
    },
];

mongreldb_kit::migrate(&mut db, &migrations)?;
```

## Python migrations

```python
db.migrate([
    {"version": 1, "name": "initial", "ops": [{"create_table": {"name": "users"}}]},
    {"version": 2, "name": "add_posts", "ops": [{"create_table": {"name": "posts"}}]},
])
```

## Backfilling

When adding a non-nullable column, the column must have a default. The runner scans the existing table and writes the default value into every row before committing.

## Failed migrations

If a migration fails, the transaction is rolled back and the migration record is marked `failed`. Fix the migration, then run the runner again. The runner only considers migrations whose version is greater than the maximum applied version, so a failed migration must be repaired or removed before later migrations can proceed.
