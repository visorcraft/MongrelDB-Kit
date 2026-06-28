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
| Drop table | `{"drop_table": {"name": "users"}}` | Destructive; also clears the table's unique-key and row guards |
| Add column | `{"add_column": {"table": "users", "column": "bio"}}` | Column must be nullable or have a default |
| Drop column | `{"drop_column": {"table": "users", "column": "bio"}}` | Destructive |
| Add index | `{"add_index": {"table": "users", "index": "idx_email"}}` | |
| Drop index | `{"drop_index": {"table": "users", "index": "idx_email"}}` | |
| Add unique | `{"add_unique": {"table": "users", "constraint": "uq_email"}}` | Backfills `__kit_unique_keys` guards for existing rows; **fails** if existing data already violates the constraint |
| Drop unique | `{"drop_unique": {"table": "users", "constraint": "uq_email"}}` | Cleans the constraint's guards |
| Add foreign key | `{"add_foreign_key": {"table": "posts", "constraint": "fk_posts_user"}}` | Touches parent `__kit_row_guards` for existing children; **fails** if a child references a missing parent |
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

Each migration carries a **content-aware** SHA-256 checksum computed over a single
canonical serialization of its `version`, `name`, and ordered `ops` list:

```
sha256('{"version":<n>,"name":<json>,"ops":[<op>,...]}')
```

The canonical form fixes the key order (`op` first, then the op's fields) and uses
standard JSON string escaping, so TypeScript, Rust, and Python produce **byte-identical**
checksums for the same logical migration. Editing or reordering a migration's ops changes
its checksum, which is how drift detection rejects a tampered or re-meaning'd migration
that was already applied. (In TypeScript the `ops` array is optional; when omitted the
checksum covers the version and name with an empty op list.)

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
- `sql(sql)` — run raw SQL (async migrations only; throws in `migrateSync`)

For the other schema changes, import the standalone helpers and call them from `up()` with
`ctx.kit` (these are async, so use the async `migrate()` runner):

```ts
import { dropTable, addUnique, addForeignKey, addIndex } from '@mongreldb/kit';

await migrate(db, schema, [
  {
    version: 3,
    name: 'tighten_accounts',
    ops: [{ kind: 'addUnique', table: 'accounts', constraint: 'accounts_email_uq' }],
    async up({ kit }) {
      await addUnique(kit, 'accounts', unique(['email'], { name: 'accounts_email_uq' }));
    }
  }
]);
```

- `dropTable(kit, tableName)` — drop a table and clear its unique-key/row guards.
- `addUnique(kit, tableName, uniqueSpec)` — add a unique constraint, backfilling guards
  (rejects existing data that already violates it).
- `addForeignKey(kit, tableName, fkSpec)` — add a foreign key, touching parent row guards
  (rejects existing children with a missing parent).
- `addIndex(kit, tableName, indexSpec)` — add an index by rebuilding the table.

Listing the matching declarative `ops` on the migration keeps its content-aware checksum
in sync with the imperative `up()` so a later edit is detected as drift.

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

## Backfilling and guard maintenance

Several ops scan existing rows and reconcile the kit's guard tables
(`__kit_unique_keys`, `__kit_row_guards`) so that constraints added after data already
exists stay consistent. All backfills are idempotent — re-running leaves existing guards
untouched.

- **Add column (non-nullable).** The column must have a default. The runner scans the
  table and writes the default into every row before committing.
- **Add unique.** For each existing row whose unique columns are all non-null, the runner
  reserves a `__kit_unique_keys` guard. If two rows produce the same key the existing data
  already violates the constraint and the migration **fails** (rejecting the change).
  Rows with a null in any unique column are skipped (nulls never collide).
- **Add foreign key.** For each existing child row with a non-null foreign key, the runner
  verifies the referenced parent exists and touches the parent's `__kit_row_guards` entry
  (so a later concurrent parent delete conflicts). A missing parent **fails** the
  migration.
- **Drop table / drop unique.** The runner removes the unique-key and row guards owned by
  the dropped table or constraint.

## Failed migrations

If a migration fails, the transaction is rolled back and the migration record is marked `failed`. Fix the migration, then run the runner again. The runner only considers migrations whose version is greater than the maximum applied version, so a failed migration must be repaired or removed before later migrations can proceed.
