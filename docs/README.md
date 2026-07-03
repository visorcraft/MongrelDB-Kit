# MongrelDB Kit

`@mongreldb/kit` is the application persistence layer for [MongrelDB](https://github.com/visorcraft/mongreldb).
MongrelDB is a fast storage engine with typed tables, snapshots, transactions, and SQL reads; the
Kit adds the relational application layer on top:

- a **schema DSL** with typed rows, inserts, and updates,
- a synchronous **query builder** (CRUD, batch inserts, predicates, ordering, projections,
  aggregates, joins, group/having, subqueries, CTEs),
- a **migration runner** with content-addressed checksums,
- **relational constraints the engine does not enforce natively** — not-null, checks, unique and
  composite-unique, foreign keys, and cascade / set-null / restrict deletes,
- **auto-increment ids**, defaults, table rename support, and a stable error taxonomy.

The same semantics are available from **TypeScript, Rust, and Python**, backed by a shared Rust
core and validated by a cross-language conformance suite.

> The Kit deliberately does **not** expose MongrelDB's internal storage `RowId` as your primary
> key. Your application ids live in your own columns and are assigned by the Kit's sequences, so
> they stay stable and portable.

## Install

```sh
npm install @mongreldb/kit
```

`mongreldb` (the native engine binding) is a peer dependency. Node 22+ is required. In local
development, build the sibling engine addon in release mode before benchmarking TypeScript paths.
See the [TypeScript guide](./typescript.md) for build/runtime details, and [Rust](./rust.md) /
[Python](./python.md) for those ecosystems.

## Quickstart

Define a schema, open a database directory, run migrations, and use typed CRUD:

```ts
import {
  Schema, table, int, text, timestamp,
  sequenceDefault, nowDefault, staticDefault, unique,
  KitDatabase, eq, desc,
} from '@mongreldb/kit';

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

const schema = new Schema([customers]);
const migrations = [{ version: 1, name: 'init', up: () => {} }];

// A MongrelDB data *directory* (created if missing) — not a single file.
const db = KitDatabase.openSync('./data', schema);
db.migrateSync(schema, migrations);

// Insert: omit `id` and the sequence assigns a 1-based id; `tier`/`created_at` use defaults.
const ada = db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada' }).executeSync();
console.log(ada.id);   // 1n  (bigint — int64 columns are bigint in TS)
console.log(ada.tier); // 'free'

// Read back, newest first.
const recent = db.selectFrom(customers).orderBy(desc(customers.created_at)).limit(10).executeSync();

// Unique violation throws a typed error.
try {
  db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada II' }).executeSync();
} catch (err) {
  // err instanceof KitDuplicateError
}

db.close();
```

Everything above — schema, defaults, the 1-based id, the unique constraint, typed rows — is
explained in depth in the topic guides below.

## Documentation map

Start here, then dive into the topic that fits your task.

| Guide | What it covers |
| --- | --- |
| [Schema DSL](./schema.md) | Tables, columns, types, column options, indexes, and assembling a `Schema`. |
| [Types](./types.md) | `Row<T>`, `Insert<T>`, `Update<T>` inference and typed CRUD. |
| [Defaults & sequences](./defaults.md) | Static / now / uuid / sequence / custom defaults and auto-increment ids. |
| [Query builder](./query-builder.md) | Select, insert (single and batch), update, delete, predicates, ordering, pagination, projections, aggregates, distinct, joins, group/having, subqueries, exists, CTEs, and the raw escape hatch. |
| [Constraints](./constraints.md) | Not-null, checks, unique / composite-unique, foreign keys, and delete actions (cascade / set-null / restrict). |
| [Transactions](./transactions.md) | `begin`/`commit`/`rollback`, the retrying `transaction()` helper, conflicts, and the concurrency model. |
| [Migrations](./migrations.md) | Migration files, the runner, checksums, supported operations, idempotent column adds, and table renames. |
| [Stored procedures](./stored-procedures.md) | Declarative routines callable from embedded, remote, and CLI Kit clients. |
| [Errors](./errors.md) | The error taxonomy, codes, and how to handle each category. |
| [Internal tables](./internal-tables.md) | The reserved `__kit_*` tables and what each one stores. |
| [CLI](./cli.md) | The `mongreldb-kit` command line: `check`, `diff`, `generate`, `migrate`, and more. |
| [Testing](./testing.md) | Temp-directory fixtures and patterns for fast, isolated tests. |
| [Production checklist](./production-checklist.md) | What to verify before shipping. |
| [Specification](./spec.md) | Deep internals: type model, key encoding, and the concurrency model. |

### Language guides

| Guide | Package |
| --- | --- |
| [TypeScript](./typescript.md) | `@mongreldb/kit` |
| [Rust](./rust.md) | `mongreldb-kit` |
| [Python](./python.md) | `mongreldb-kit` (import `mongreldb_kit`) |

## A note on examples

All examples use a generic "store" domain — `customers`, `products`, `orders`, and `order_items` —
so the relationships (a customer has orders, an order has items, items reference products) exercise
foreign keys, cascade deletes, joins, and aggregates. The full schema appears in
[Schema DSL](./schema.md) and is reused throughout.
