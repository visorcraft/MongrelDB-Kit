# TypeScript Quickstart

This guide shows how to define a schema, run migrations, and perform CRUD with `@visorcraft/mongreldb-kit`.

## Installation

```sh
npm install @visorcraft/mongreldb-kit @visorcraft/mongreldb
```

`@visorcraft/mongreldb` is a peer dependency providing the native database bindings. In a local checkout, build
the sibling `crates/mongreldb-node` addon with `npm run build` (release mode) before benchmarking;
debug builds make bulk writes and pushed-down queries look much slower than they are.

## Complete example

```ts
import {
  KitDatabase,
  Schema,
  table,
  int,
  text,
  bool,
  foreignKey,
  check,
  index,
  staticDefault,
  sequenceDefault,
  eq,
  desc
} from '@visorcraft/mongreldb-kit';

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const users = table('users', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('users_id_seq') }),
    text('email'),
    text('name', { nullable: true })
  ],
  primaryKey: 'id',
  indexes: [index(['email'], { unique: true, name: 'uq_user_email' })]
});

const posts = table('posts', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('posts_id_seq') }),
    int('user_id'),
    text('title'),
    text('body', { nullable: true }),
    bool('published', { default: staticDefault(false) }),
    text('created_at', { generated: 'now' })
  ],
  primaryKey: 'id',
  foreignKeys: [
    foreignKey(['user_id'], { table: 'users', columns: ['id'] }, { onDelete: 'cascade' })
  ],
  checks: [check('title_not_empty', (row) => (row.title as string).length > 0 || 'title must not be empty')]
});

const schema = new Schema([users, posts]);

// ---------------------------------------------------------------------------
// Open or create the database and run migrations
// ---------------------------------------------------------------------------

const db = KitDatabase.openSync('./app-data', schema);

db.migrateSync(schema, [
  {
    version: 1,
    name: 'initial',
    up({ ensureTable }) {
      ensureTable(users);
      ensureTable(posts);
    }
  }
]);

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

// `id` is omitted: the sequence assigns a 1-based id (the first row is 1n, never 0n).
// Columns with a default (and nullable columns) are optional in `.values(...)`; only
// non-nullable, no-default columns are required. int64 columns are `bigint` (alice.id === 1n).
const alice = db.insertInto(users).values({ email: 'alice@example.com', name: 'Alice' }).executeSync();
const bob = db.insertInto(users).values({ email: 'bob@example.com' }).executeSync();

const post = db.insertInto(posts)
  .values({ user_id: alice.id, title: 'Hello Kit', body: 'First post.' })
  .executeSync();

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

const publishedPosts = db
  .selectFrom(posts)
  .where(eq(posts.published, false))
  .orderBy(desc(posts.created_at))
  .limit(10)
  .executeSync();

const titles = db.selectFrom(posts).select([posts.title]).executeSync();

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

db.updateTable(posts)
  .set({ published: true })
  .where(eq(posts.id, post.id))
  .executeSync();

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

// Deleting Alice cascades to her posts because of the FK onDelete action.
const deleted = db.deleteFrom(users).where(eq(users.id, alice.id)).executeSync();
console.log('deleted users:', deleted);

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

db.close();
```

## Column helpers

- `int(name, opts?)`
- `text(name, opts?)`
- `real(name, opts?)` — `float64`
- `bool(name, opts?)`
- `timestamp(name, opts?)`
- `date(name, opts?)`
- `json(name, opts?)`
- `blob(name, opts?)` — bytes

## Column options

| Option | Effect |
|---|---|
| `nullable?: boolean` | Allow `null` values |
| `primaryKey?: boolean` | Mark as part of the primary key |
| `default?: DefaultValue` | Static, now, UUID, sequence, or custom default |
| `generated?: 'uuid' \| 'now'` | Auto-generate on insert/update |
| `enumValues?: string[]` | Restrict string values |
| `min?: number`, `max?: number` | Numeric range |
| `minLength?: number`, `maxLength?: number` | String/bytes length |
| `regex?: RegExp` | Pattern match |
| `check?: (value) => boolean \| string` | Per-column custom check |

## Query builder

Select:
```ts
db.selectFrom(table)
  .where(predicate)
  .orderBy(asc(column), desc(column2))
  .limit(n)
  .offset(n)
  .select([col1, col2])
  .executeSync();
```

Insert:
```ts
db.insertInto(table).values({ ... }).executeSync();
db.insertInto(table).valuesMany([{ ... }, { ... }]).executeSync();
```

Update:
```ts
db.updateTable(table).set({ ... }).where(predicate).executeSync();
```

Delete:
```ts
db.deleteFrom(table).where(predicate).executeSync();
```

## Predicates

- `eq(column, value)`
- `ne(column, value)`
- `gt(column, value)`, `gte(column, value)`, `lt(column, value)`, `lte(column, value)`
- `isNull(column)`, `isNotNull(column)`
- `inList(column, values)`, `notInList(column, values)`
- `like(column, pattern)`, `contains(column, substring)`
- `bytesPrefix(column, prefix)` — anchored `LIKE 'prefix%'` on a bitmap-indexed Bytes column (exact pushdown; see [Query builder](./query-builder.md#bytesprefix--anchored-prefix-on-bytes-columns))
- `and(...predicates)`, `or(...predicates)`, `not(predicate)`

Joins, aggregates, `groupBy`/`having`, `distinct`, subqueries, `exists`, and CTEs are part of the
same builder — see the [Query builder](./query-builder.md) guide for the full surface.

## Database helpers

- `db.tableNames()` returns application tables and hides the reserved `__kit_*` namespace.
- `db.renameTable(oldName, newName)` durably renames a live table. Pair it with a matching schema
  update in a migration; it rejects `__kit_` names.
- `db.createTriggerSync(spec)`, `db.createOrReplaceTriggerSync(spec)`, `db.dropTriggerSync(name)`,
  `db.triggers()`, and `db.trigger(name)` manage engine-side triggers.
- `await db.sql(sql)` returns an Apache Arrow table. `await db.sqlRows(sql)` decodes SQL results to
  plain objects.
- `await db.createVirtualTable(spec)` and `await db.dropVirtualTable(name)` run the SQL DDL for
  module-backed virtual/external tables.
- `await db.createView(spec)` / `await db.dropView(name)` create/drop a SQL view (`CREATE VIEW` /
  `DROP VIEW IF EXISTS`). Views are session-scoped — see [SQL views](./migrations.md#sql-views).
- `db.updateWhere(table, patch, predicate)` and `db.deleteWhere(table, predicate)` are one-shot
  convenience twins of Rust/Python `update_where`/`delete_where`, wrapping the
  `updateTable`/`deleteFrom` builders. `updateWhere` returns the updated rows; `deleteWhere`
  returns the deleted count as a `bigint`.
- `await db.analyze()` and `await db.vacuum()` rebuild index statistics and reclaim space (the
  engine's `ANALYZE` / `VACUUM` equivalents), routing through the SQL surface.
- **Async / non-blocking I/O:** the Kit wraps the addon's `spawn_blocking` async variants so hot
  read/write paths don't block the Node event loop: `db.putAsync(table, cells)`,
  `db.getAsync(table, rowId)`, `db.queryAsync(table, conditions)`, `db.countAsync(table)`,
  `db.countWhereAsync(table, conditions)`, `db.queryArrowAsync(table, conditions)`,
  `db.setSimilarityAsync(...)`, plus async twins of the maintenance methods (`flushAsync`,
  `compactAllAsync`, `compactTableAsync`, `snapshotEpochAsync`, `approxAggregateAsync`).
  **Caveat:** the maintenance twins where the addon has no native async variant
  (`compactAllAsync`/`compactTableAsync`/`approxAggregateAsync`/`snapshotEpochAsync`) wrap the sync
  call in a `Promise` — they match the async signature but still block; the `TableHandle` async
  methods (`putAsync`/`queryAsync`/…) are genuinely non-blocking. See
  [runTxn](./typescript.md#transactions) for an async transaction helper.
- **Bulk ingest:** `db.bulkLoadTyped(table, columns)` is the fastest ingest path —
  column-major `Int64`/`Float64`/`Bool` buffers laid out little-endian. Commits internally (returns
  the epoch); not transactional; bypasses Kit constraints. For typed columnar loads of numeric
  tables it beats `insertMany`. Re-exported types: `TypedColumn`, `PutResult`, `RowJs`,
  `ConditionSpec`, `CommitResultJs`.
- `db.nativeDb` exposes the underlying `mongreldb` database for raw operations that intentionally
  bypass Kit validation, defaults, and relational guards.

> The kit's SQL session is held for the database's lifetime, so views (`CREATE
> VIEW`) created via `db.sql()` persist across subsequent `sql()` / `sqlRows()`
> calls. See [Migrations → SQL views](./migrations.md#sql-views).

## Triggers and SQL helpers

Assuming `users`, `audit`, and `events` table specs already exist:

```ts
import {
  groupConcat,
  newColumn,
  percentileCont,
  textValue,
  trigger,
  virtualTable,
} from '@visorcraft/mongreldb-kit';

db.createTriggerSync(trigger({
  name: 'users_ai',
  target: { kind: 'table', name: 'users' },
  timing: 'after',
  event: 'insert',
  program: {
    steps: [{
      kind: 'insert',
      table: 'audit',
      cells: [
        { column_id: audit.user_id.id, value: newColumn(users.id.id) },
        { column_id: audit.note.id, value: textValue('created') },
      ],
    }],
  },
}));

await db.sqlRows(`SELECT ${percentileCont(events.latency_ms, 0.95).sql} AS p95 FROM events`);
await db.sqlRows(`SELECT ${groupConcat(events.tag, '|').sql} AS tags FROM events`);
await db.createVirtualTable(virtualTable('docs_fts', 'fts_docs', ['content=docs']));
```

See [Triggers](./triggers.md) and
[Extended SQL & virtual tables](./extended-sql-and-virtual-tables.md) for the full API.

## Migrations

Call `db.migrateSync(schema, migrations)` to apply pending migrations in version order. The runner acquires an advisory lock, records each migration in `__kit_schema_migrations`, and updates `__kit_schema_catalog`.

## Error handling

Catch typed errors by name:

```ts
import { KitDuplicateError, KitForeignKeyError, KitRestrictError, KitValidationError } from '@visorcraft/mongreldb-kit';

try {
  db.insertInto(users).values({ email: 'alice@example.com' }).executeSync();
} catch (err) {
  if (err instanceof KitDuplicateError) {
    console.error('duplicate email');
  }
}
```

## Running this example

Save the file as `kit-demo.ts` and run it with Node 22+:

```sh
npx tsx kit-demo.ts
```

The first run creates `./app-data`. Subsequent runs open the existing database directory.

## See also

- [Schema DSL](./schema.md) and [Types](./types.md) — column/table specs and `Row`/`Insert`/`Update` inference.
- [Defaults & sequences](./defaults.md) — defaults and 1-based auto-increment ids.
- [Query builder](./query-builder.md) — the complete query surface.
- [Triggers](./triggers.md) and [Extended SQL & virtual tables](./extended-sql-and-virtual-tables.md).
- [Constraints](./constraints.md) · [Errors](./errors.md) — enforcement and the typed failures.
- [Transactions](./transactions.md) · [Migrations](./migrations.md) · [Testing](./testing.md).
