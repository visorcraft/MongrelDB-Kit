# Migrations

MongrelDB Kit evolves a schema through a versioned, ordered list of migrations.
A runner applies the pending ones in version order, records each in the internal
`__kit_schema_migrations` table, and rewrites the schema catalog. Re-running is a
no-op, and a content-aware checksum detects after-the-fact edits to an
already-applied migration.

This guide uses the shared "store" schema (`customers`, `products`, `orders`,
`order_items`) defined in [Schema DSL](./schema.md).

## The migration object

A migration is a small object. In TypeScript:

```ts
interface Migration {
  version: number;          // unique, ordered; first applied is the lowest
  name: string;             // human label, recorded alongside the version
  ops?: MigrationOp[];      // optional declarative description (see below)
  up: (ctx: MigrationContext) => Promise<void> | void;
}
```

- **`version`** orders the history. The runner applies migrations whose version
  is greater than the highest already applied, in ascending order.
- **`name`** is stored with the record and shown by the CLI.
- **`up()`** performs the change. In TypeScript this is **imperative** — you call
  context methods and helper functions to do the work (see
  [Two execution models](#two-execution-models)).
- **`ops`** is an optional, declarative list describing what the migration
  changes. The TypeScript runner does **not** execute `ops`; they exist only to
  fold the migration's intent into its [checksum](#checksums-and-drift-detection)
  so a later edit is caught as drift.

## Running migrations

There are two runners. Both live on the kit surface; pick by whether your `up()`
callbacks are synchronous.

```ts
import { KitDatabase, migrate } from '@visorcraft/mongreldb-kit';
import { schema } from './schema.js';

const db = KitDatabase.openSync('./data', schema);

// Synchronous: a method on the database. up() must be synchronous.
db.migrateSync(schema, migrations);

// Asynchronous: a standalone helper. Use it when up() is async or calls ctx.sql().
await migrate(db, schema, migrations);
```

Both:

1. Acquire the advisory lock row in `__kit_migration_locks` (a single `default`
   lock with a 5-minute TTL). A live lock raises `KitMigrationError`
   (`migration lock is already held`); an expired lock is reclaimed.
2. Sort the supplied migrations by version.
3. Read applied records from `__kit_schema_migrations` and
   [verify their checksums](#checksums-and-drift-detection).
4. Compute the pending set: every migration whose version is greater than the
   maximum applied version.
5. For each pending migration: write a record with status `in_progress`, run
   `up()` inside a native transaction, then on success commit and mark the record
   `applied`. On failure the transaction is rolled back, the record is marked
   `failed`, and a `KitMigrationError` is raised.
6. Rewrite the schema catalog (`__kit_schema_catalog`) and release the lock.

Because step 4 only ever selects versions above the high-water mark, **running
the runner twice applies nothing the second time** — migrations are idempotent.

## Two execution models

The same `MigrationOp` vocabulary is shared across languages, but it is used two
different ways. Knowing which you are in avoids confusion.

### TypeScript: imperative `up()`

The TypeScript runner calls your `up(ctx)` and you make the change with context
methods and the exported migration helpers. The `ops` array is metadata only —
it never drives the work.

`MigrationContext` provides:

| Member | What it does |
| --- | --- |
| `ctx.kit` | The `KitDatabase`. |
| `ctx.db` | The native MongrelDB database. |
| `ctx.ensureTable(table)` | Create a table from its `TableSpec` if it does not exist. |
| `ctx.addColumn(tableName, column)` | Add a column, backfilling non-nullable columns with their default; no-ops if the physical column already exists. |
| `ctx.dropColumn(tableName, columnName)` | Drop a non-primary-key column by rebuilding the table; removes indexes/unique constraints/FKs owned by that column. |
| `ctx.alterColumn(tableName, oldName, column)` | Alter an existing column through native MongrelDB validation. |
| `ctx.addIndex(tableName, index)` | Add an index by rebuilding the table. |
| `ctx.dropIndex(tableName, indexName)` | Drop an index by rebuilding the table. |
| `ctx.createTrigger(trigger)` | Install a declarative engine trigger from a `TriggerSpec`. |
| `ctx.replaceTrigger(trigger)` | Replace an existing trigger with the same name. |
| `ctx.dropTrigger(name)` | Drop an installed trigger. |
| `ctx.createVirtualTable(table)` | Run `CREATE VIRTUAL TABLE ... USING ...`. Available in async migrations only. |
| `ctx.dropVirtualTable(name)` | Run `DROP TABLE ...` for a virtual table. Available in async migrations only. |
| `ctx.sql(sql)` | Run raw SQL. Available in async migrations only; throws in `migrateSync`. |

For changes beyond adding a table or column, import the standalone helpers and
call them from `up()` with `ctx.kit`. They are async, so use the async `migrate`
runner:

```ts
import {
  migrate, dropTable, addColumn, addIndex, addUnique, addForeignKey, unique,
} from '@visorcraft/mongreldb-kit';

await migrate(db, schema, [
  {
    version: 5,
    name: 'tighten_products',
    ops: [{ kind: 'addUnique', table: 'products', constraint: 'products_sku_uq' }],
    async up({ kit }) {
      await addUnique(kit, 'products', unique(['sku'], { name: 'products_sku_uq' }));
    },
  },
]);
```

| Helper | Effect |
| --- | --- |
| `createTable(kit, table)` / `ctx.ensureTable` | Create the table from its spec if missing. |
| `addColumn(kit, t, column)` / `ctx.addColumn` | Add a column; non-nullable columns are backfilled with their default; existing physical columns are skipped. |
| `dropColumn(kit, t, column)` / `ctx.dropColumn` | Drop a non-primary-key column by rebuilding the table; indexes, unique constraints, and outgoing foreign keys on that column are removed. |
| `alterColumn(kit, t, oldName, column)` / `ctx.alterColumn` | Alter an existing column through native MongrelDB validation. Supports renames, native type changes that do not require stored-row conversion, and nullability changes that existing data satisfies. |
| `addIndex(kit, t, indexSpec)` / `ctx.addIndex` | Add an index by rebuilding the table (MongrelDB has no add-index-in-place). Unique indexes also backfill unique guards. |
| `dropIndex(kit, t, indexName)` / `ctx.dropIndex` | Drop an index by rebuilding the table; dropping a unique index also clears its guards. |
| `ctx.createTrigger(trigger)` / `ctx.replaceTrigger(trigger)` / `ctx.dropTrigger(name)` | Manage engine-side triggers. Include matching `createTrigger` / `replaceTrigger` / `dropTrigger` ops when you want checksum drift protection. |
| `createVirtualTable(kit, table)` / `ctx.createVirtualTable` | Create a virtual table through SQL; async migrations only. |
| `dropVirtualTable(kit, name)` / `ctx.dropVirtualTable` | Drop a virtual table through SQL; async migrations only. |
| `addUnique(kit, t, uniqueSpec)` | Add a unique constraint and backfill its guards; rejects data that already violates it. |
| `addForeignKey(kit, t, fkSpec)` | Add a foreign key and touch parent row guards; rejects a child with a missing parent. |
| `dropTable(kit, tableName)` | Drop a table and clear its unique-key and row guards. |

There is intentionally **no** TypeScript helper for `dropUnique`,
`dropForeignKey`, `addCheck`, or `dropCheck`: check and foreign-key *enforcement*
is driven by the schema definition you pass when opening the database and
re-persisting the catalog, so those need no row-level backfill step.

### Declarative (JSON / Rust / CLI)

When migrations come from a JSON file (the CLI) or the Rust kit, there is no
`up()` — the **`ops` list is the migration body**, and the runner executes each
op. JSON migrations use serde's snake_case shape:

```json
[
  {
    "version": 1,
    "name": "initial",
    "ops": [
      { "create_table": { "name": "customers" } },
      { "create_table": { "name": "products" } }
    ]
  },
  {
    "version": 2,
    "name": "add_orders",
    "ops": [
      { "create_table": { "name": "orders" } },
      { "add_foreign_key": { "table": "orders", "constraint": "orders_customer_id_fk" } }
    ]
  }
]
```

The Rust/CLI runner resolves each op against the database's **stored** schema, so
the referenced tables, columns, and constraints must already be present in that
schema. See [CLI](./cli.md) for how `mongreldb-kit migrate apply` is wired.

## Supported operations

The op vocabulary is identical across languages, but coverage differs by runner.
Verify against this matrix rather than assuming symmetry.

| Op | TypeScript helper | Rust / CLI op runner |
| --- | --- | --- |
| `createTable` | `createTable` / `ctx.ensureTable` | implemented |
| `dropTable` | `dropTable` (clears guards) | implemented (clears guards) |
| `addColumn` | `addColumn` / `ctx.addColumn` (backfills non-nullable defaults) | implemented (adds the column) |
| `alterColumn` | `alterColumn` / `ctx.alterColumn` (native ALTER COLUMN validation) | implemented (native ALTER COLUMN validation) |
| `addUnique` | `addUnique` (backfill + reject violations) | implemented (backfill + reject violations) |
| `dropUnique` | — | implemented (deletes the constraint's guards) |
| `addForeignKey` | `addForeignKey` (backfill + reject missing parent) | implemented (backfill + reject missing parent) |
| `dropForeignKey` | — | metadata-only no-op (enforcement follows the re-persisted schema) |
| `addCheck` / `dropCheck` | — | metadata-only no-op (enforcement follows the re-persisted schema) |
| `addIndex` | `addIndex` / `ctx.addIndex` (table rebuild) | implemented (table rebuild) |
| `dropColumn` | `dropColumn` / `ctx.dropColumn` (table rebuild) | implemented (table rebuild) |
| `dropIndex` | `dropIndex` / `ctx.dropIndex` (table rebuild) | implemented (table rebuild) |
| `createTrigger` | `ctx.createTrigger` | implemented |
| `replaceTrigger` | `ctx.replaceTrigger` | implemented |
| `dropTrigger` | `ctx.dropTrigger` | implemented |
| `createVirtualTable` | `createVirtualTable` / `ctx.createVirtualTable` (async migrations only) | implemented (runs SQL via the embedded session) |
| `dropVirtualTable` | `dropVirtualTable` / `ctx.dropVirtualTable` (async migrations only) | implemented (runs SQL via the embedded session) |
| `createView` / `replaceView` | `createView` / `replaceView` / `ctx.createView` / `ctx.replaceView` (async migrations only) | implemented (runs `CREATE VIEW` via the embedded session) |
| `dropView` | `dropView` / `ctx.dropView` (async migrations only) | implemented (runs `DROP VIEW IF EXISTS` via the embedded session) |
| `rawSql` | `ctx.sql` (async migrations only) | implemented (runs the SQL via the embedded session) |

Remaining asymmetry: `dropUnique`, `dropForeignKey`, and check ops are handled
(or safely no-op'd) by the Rust/CLI runner but have no TypeScript helper because
the schema definition already carries their enforcement. SQL-backed ops (views,
virtual tables, raw SQL) run in both surfaces: TypeScript requires async
migrations (the SQL path is async), while the Rust/CLI runner executes them
through its own embedded `MongrelSession`. See
[SQL views](#sql-views) below for view-specific semantics.

## Checksums and drift detection

Every migration carries a **content-aware** SHA-256 checksum over a single
canonical serialization of its `version`, `name`, and ordered `ops` list:

```
sha256('{"version":<n>,"name":<json>,"ops":[<op>,...]}')
```

The canonical form fixes the key order (`op` first, then the op's fields) and
uses standard JSON string escaping, so TypeScript, Rust, and Python produce
**byte-identical** checksums for the same logical migration. Two verified
examples (asserted by the cross-language conformance tests):

```
{"version":1,"name":"init","ops":[{"op":"create_table","name":"users"}]}
  -> fe2f521793591207bd4d8645c2631e4b7ce43e30fe7ea5691a2846c74ea71cc3

{"version":1,"name":"init","ops":[]}
  -> 6408373a4372a2c49859db2a4548ea43308e5ba7dd3609998ca376606cf09757
```

When `ops` is omitted (TypeScript), the checksum covers the version and name
with an empty op list — the second form above.

**Drift detection.** When the TypeScript runner reads the applied records, it
recomputes the checksum of the supplied migration with the same version and
compares it (and the name) against what was stored. A mismatch — or an applied
version that is missing from the supplied list — raises `KitSchemaDriftError`.
That is how an edited or reordered historical migration is caught before it can
silently change the meaning of the schema. Records left in `failed` status are
skipped by drift detection; repair them before re-running.

> Because the checksum is content-aware, **adding or editing an `op` on an
> already-applied migration changes its checksum and trips drift detection**.
> Keep the `ops` you list in sync with what `up()` actually did so the checksum
> stays stable.

## Backfilling and guard maintenance

Several operations scan existing rows and reconcile the kit's
[internal guard tables](./internal-tables.md) so constraints added *after* data
exists stay consistent. All backfills are idempotent — re-running leaves existing
guards untouched.

- **Add column (nullable or already present).** A nullable column needs no row backfill. In
  TypeScript, `addColumn` first asks the engine for the current column names and returns without
  mutation when the column already exists; this keeps reruns and partially repaired migrations from
  adding the same column twice.
- **Add column (non-nullable).** The column must have a default. The runner
  scans the table and writes the default into every row before committing.
  A `sequence` / `AUTO_INCREMENT` default cannot be backfilled and raises `KitMigrationError`.
- **Add unique.** For each existing row whose unique columns are all non-null,
  the runner reserves a `__kit_unique_keys` guard. If two rows produce the same
  key, the existing data already violates the constraint and the migration
  **fails**. Rows with a null in any unique column are skipped (nulls never
  collide).
- **Add foreign key.** For each existing child row with a non-null foreign key,
  the runner verifies the referenced parent exists and touches the parent's
  `__kit_row_guards` entry, so a later concurrent parent delete conflicts. A
  missing parent **fails** the migration.
- **Drop table / drop unique.** The runner removes the unique-key and row guards
  owned by the dropped table or constraint.
- **Add/drop index and drop column.** The runner rebuilds the physical table with
  the target schema and copies visible rows. Dropping a column also removes stale
  unique guards for constraints that no longer exist in the target schema.

## Renaming tables

All three language surfaces expose a durable table rename:
- TypeScript: `KitDatabase.renameTable(oldName, newName)`
- Rust: `Database::rename_table(&mut self, from, to)`
- Python: `Database.rename_table(old_name, new_name)`

Each is a direct wrapper over the engine's durable table rename. It preserves the table id, rows,
indexes, and handles, and rejects names that start with the reserved `__kit_` prefix. The Rust and
Python kits additionally update the in-memory kit schema catalog (and persist it to
`kit_schema.json`) and retarget foreign keys that referenced the old name, so the new name works
end-to-end for subsequent transactions. The TypeScript kit reads table names from the engine, so it
reflects the rename immediately; update your code-defined schema to match so constraint checking
stays aligned.

Use it in the transition migration with an open handle that can still see the old table, then pass
the new schema to `migrate` so the schema catalog is rewritten to the renamed table. Future
opens should use the new schema. There is no declarative `renameTable` migration op, so record the
intent in the migration name and, if you use `ops` for checksums, an equivalent `rawSql`
description:

```ts
const db = KitDatabase.openSync('./data', oldSchema);
db.migrateSync(newSchema, [
  {
    version: 3,
    name: 'rename_widgets_to_things',
    ops: [{ kind: 'rawSql', sql: 'ALTER TABLE widgets RENAME TO things' }],
    up({ kit }) {
      kit.renameTable('widgets', 'things');
    },
  },
]);
```

## SQL views

Views (`CREATE VIEW <name> AS <select>`) are **session-scoped** in the engine — they are not
persisted to the catalog. The kit holds one long-lived SQL session per `Database` handle for the
database's lifetime (mirroring how the daemon and any long-lived app use MongrelDB), so a view
created via a migration or direct `sql()` call persists across subsequent `sql()` / `sqlRows()` /
`sqlArrow()` calls on that same handle. Closing and reopening the database loses the view —
re-apply the migration to restore it.

Three migration ops manage views; all run SQL through the embedded session:

| Op | Effect |
|---|---|
| `createView` / `replaceView` | `CREATE VIEW <name> AS <select>`. The engine overwrites any existing view, so create and replace are the same SQL. |
| `dropView` | `DROP VIEW IF EXISTS <name>` (idempotent). |

```ts
// TypeScript — view ops are async-only (they run SQL).
await db.migrate(schema, [
  {
    version: 4,
    name: 'add_active_users_view',
    ops: [{ kind: 'createView', name: 'active_users', view: { name: 'active_users', sql: 'SELECT id, email FROM users WHERE active = TRUE' } }],
    async up({ createView }) {
      await createView({ name: 'active_users', sql: 'SELECT id, email FROM users WHERE active = TRUE' });
    },
  },
]);
```

```python
# Python / Rust — views are created by the migration runner directly.
db.migrate([{
    "version": 4,
    "name": "add_active_users_view",
    "ops": [{"create_view": {"name": "active_users", "view": {"name": "active_users", "sql": "SELECT id, email FROM users WHERE active = TRUE"}}}],
}])
# Query the view via the SQL surface:
db.sql_rows("SELECT * FROM active_users ORDER BY id")
```

> **Note:** `CREATE OR REPLACE VIEW` is not supported by the engine (the `OR REPLACE` keyword is
> gated off). Re-issue `CREATE VIEW` for replace semantics — it overwrites.

## Walkthrough: evolving the store schema

Start from the base store schema and grow it. Each migration lists the `ops`
that match what `up()` does, so the checksum stays content-aware.

```ts
import {
  KitDatabase, migrate, text, unique, addUnique,
} from '@visorcraft/mongreldb-kit';
import { schema, customers, products, orders, orderItems } from './schema.js';

const db = KitDatabase.openSync('./data', schema);

// v1 is the initial create. Synchronous: ensureTable + addColumn are sync.
db.migrateSync(schema, [
  {
    version: 1,
    name: 'init',
    ops: [
      { kind: 'createTable', name: 'customers' },
      { kind: 'createTable', name: 'products' },
      { kind: 'createTable', name: 'orders' },
      { kind: 'createTable', name: 'order_items' },
    ],
    up({ ensureTable }) {
      ensureTable(customers);
      ensureTable(products);
      ensureTable(orders);
      ensureTable(orderItems);
    },
  },
]);
```

**Add a column.** Give customers an optional phone number. A nullable column
needs no backfill; a non-nullable one would need a default.

```ts
db.migrateSync(schema, [
  /* ...v1... */
  {
    version: 2,
    name: 'add_customer_phone',
    ops: [{ kind: 'addColumn', table: 'customers', column: 'phone' }],
    up({ addColumn }) {
      addColumn('customers', text('phone', { nullable: true }));
    },
  },
]);
```

**Add a unique constraint.** Suppose products gain a `barcode`. Add the column
(nullable, so existing rows are fine), then add the unique constraint, which
backfills `__kit_unique_keys` for the rows that have a barcode and rejects the
migration if two rows already share one. `addUnique` is async, so use `migrate`:

```ts
await migrate(db, schema, [
  /* ...v1, v2... */
  {
    version: 3,
    name: 'add_product_barcode',
    ops: [
      { kind: 'addColumn', table: 'products', column: 'barcode' },
      { kind: 'addUnique', table: 'products', constraint: 'products_barcode_uq' },
    ],
    async up({ addColumn, kit }) {
      addColumn('products', text('barcode', { nullable: true }));
      await addUnique(kit, 'products', unique(['barcode'], { name: 'products_barcode_uq' }));
    },
  },
]);
```

After v3, `db.migrateSync`/`migrate` re-runs apply nothing, and editing any of
the listed `ops` on v1–v3 would be rejected as drift on the next run.

## Failed migrations

If a migration's `up()` throws, the transaction is rolled back and the record is
marked `failed`; the runner raises `KitMigrationError`. Because the runner only
considers versions above the maximum applied, a failed migration blocks the ones
after it. Repair (or remove) the failed migration, then run the runner again.
Failed records are excluded from drift verification so a fixed migration can be
re-applied cleanly.

## Rust migrations

```rust
use mongreldb_kit::{migrate, Migration, MigrationOp};

let migrations = vec![
    Migration {
        version: 1,
        name: "init".into(),
        ops: vec![MigrationOp::CreateTable { name: "customers".into() }],
    },
    Migration {
        version: 2,
        name: "add_orders".into(),
        ops: vec![
            MigrationOp::CreateTable { name: "orders".into() },
            MigrationOp::AddForeignKey {
                table: "orders".into(),
                constraint: "orders_customer_id_fk".into(),
            },
        ],
    },
];

migrate(&mut db, &migrations)?;
```

The Rust runner is op-driven (`version` is an `i64`) and resolves every op
against the database's stored schema.

## Python migrations

```python
db.migrate([
    {"version": 1, "name": "init", "ops": [{"create_table": {"name": "customers"}}]},
    {"version": 2, "name": "add_orders", "ops": [{"create_table": {"name": "orders"}}]},
])
```

## See also

- [Schema DSL](./schema.md) — the table/column/constraint specs migrations apply.
- [Constraints](./constraints.md) — what unique, check, and foreign-key backfills enforce.
- [Internal tables](./internal-tables.md) — the `__kit_*` tables migrations read and write.
- [CLI](./cli.md) — `migrate apply`/`status`/`plan` and `generate migration`.
- [Errors](./errors.md) — `KitMigrationError`, `KitSchemaDriftError`, `KitForeignKeyError`.
