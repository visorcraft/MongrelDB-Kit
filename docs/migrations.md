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
import { KitDatabase, migrate } from '@mongreldb/kit';
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
| `ctx.sql(sql)` | Run raw SQL. Available in async migrations only; throws in `migrateSync`. |

For changes beyond adding a table or column, import the standalone helpers and
call them from `up()` with `ctx.kit`. They are async, so use the async `migrate`
runner:

```ts
import {
  migrate, dropTable, addColumn, addIndex, addUnique, addForeignKey, unique,
} from '@mongreldb/kit';

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
| `addIndex(kit, t, indexSpec)` | Add an index by rebuilding the table (MongrelDB has no add-index-in-place). |
| `addUnique(kit, t, uniqueSpec)` | Add a unique constraint and backfill its guards; rejects data that already violates it. |
| `addForeignKey(kit, t, fkSpec)` | Add a foreign key and touch parent row guards; rejects a child with a missing parent. |
| `dropTable(kit, tableName)` | Drop a table and clear its unique-key and row guards. |

There is intentionally **no** TypeScript helper for `dropColumn`, `dropUnique`,
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
| `addUnique` | `addUnique` (backfill + reject violations) | implemented (backfill + reject violations) |
| `dropUnique` | — | implemented (deletes the constraint's guards) |
| `addForeignKey` | `addForeignKey` (backfill + reject missing parent) | implemented (backfill + reject missing parent) |
| `dropForeignKey` | — | metadata-only no-op (enforcement follows the re-persisted schema) |
| `addCheck` / `dropCheck` | — | metadata-only no-op (enforcement follows the re-persisted schema) |
| `addIndex` | `addIndex` (table rebuild) | **not supported** — raises a migration error |
| `dropColumn` | — | **not supported** — raises a migration error |
| `dropIndex` | — | **not supported** — raises a migration error |
| `rawSql` | `ctx.sql` (async migrations only) | **not supported** — raises a migration error |

Note the asymmetry: `addIndex` works in TypeScript (it rebuilds the table) but
the Rust/CLI runner rejects it; conversely `dropUnique`, `dropForeignKey`, and
check ops are handled (or safely no-op'd) by the Rust/CLI runner but have no
TypeScript helper because the schema definition already carries their
enforcement.

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

## Renaming tables

TypeScript exposes `KitDatabase.renameTable(oldName, newName)`, a direct wrapper over the engine's
durable table rename. It preserves the table id, rows, indexes, and handles, and rejects names that
start with the reserved `__kit_` prefix.

Use it in the transition migration with an open handle that can still see the old table, then pass
the new schema to `migrateSync` so the schema catalog is rewritten to the renamed table. Future
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

## Walkthrough: evolving the store schema

Start from the base store schema and grow it. Each migration lists the `ops`
that match what `up()` does, so the checksum stays content-aware.

```ts
import {
  KitDatabase, migrate, text, unique, addUnique,
} from '@mongreldb/kit';
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
