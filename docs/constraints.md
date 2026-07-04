# Constraints

MongrelDB enforces snapshots, transactions, and per-table indexes; it does **not**
enforce relational constraints. MongrelDB Kit adds them on top — not-null and column
validators, table-level checks, unique and composite-unique keys, foreign keys, and
cascade / set-null / restrict delete actions — with the same semantics in TypeScript,
Rust, and Python.

All examples use the generic "store" schema from [Schema DSL](./schema.md): `customers`,
`products`, `orders`, and `order_items`. Every violation throws a typed error from the
[error taxonomy](./errors.md); the relevant one is named with each rule below.

## How enforcement runs

Every constrained mutation runs inside a single MongrelDB transaction (see
[Transactions](./transactions.md)):

- **Insert** — apply defaults, validate the whole row, enforce foreign keys, stage unique
  / primary-key guards, then write and commit.
- **Update** — load the row, merge the patch, re-validate, re-check changed foreign keys,
  rewrite the row, and restage guards.
- **Delete** — plan cascade / set-null / restrict actions across all children, apply them,
  delete the row, and commit atomically.

Uniqueness, primary-key, and foreign-key integrity are backed by reserved guard tables
(`__kit_unique_keys`, `__kit_row_guards`) so the checks hold transactionally even under
snapshot isolation. That machinery is described in [Internal tables](./internal-tables.md)
and the [concurrency model](./transactions.md#concurrency-model); the rest of this page is
about the rules you declare and the errors they raise.

## Not-null and column validators

Column options on the [Schema DSL](./schema.md) declare per-value rules. The Kit checks
them with `validateRow` before every insert and update, in this order: not-null, type,
`enumValues`, `min`/`max`, `minLength`/`maxLength`, `regex`, then the column `check`
function. The first failure throws **`KitValidationError`** with `.table` and `.column`
set and a human-readable `.message`.

```ts
import { table, int, text } from '@visorcraft/mongreldb-kit';

const products = table('products', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('products_id_seq') }),
    text('sku', { nullable: false }),
    text('name', { nullable: false, minLength: 1, maxLength: 120 }),
    int('price_cents', { nullable: false, min: 0 }),
    text('tier', { enumValues: ['standard', 'premium'], default: staticDefault('standard') }),
    text('slug', { nullable: false, regex: /^[a-z0-9-]+$/ }),
    int('stock', { nullable: false, check: (v) => (v as bigint) <= 10_000n || 'stock too high' }),
  ],
  primaryKey: 'id',
});
```

Each rule and the error it throws:

| Violation | Example value | `KitValidationError.message` |
| --- | --- | --- |
| Not-null | `name: null` | `Column "name" cannot be null` |
| Wrong type | `price_cents: 5` (number, not `bigint`) | `Column "price_cents" must be a bigint` |
| Enum | `tier: 'gold'` | `Value "gold" for "tier" must be one of standard, premium` |
| `min` | `price_cents: -1n` | `Value for "price_cents" must be at least 0` |
| `max` | `price_cents: 99n` with `max: 50` | `Value for "price_cents" must be at most 50` |
| `minLength` | `name: ''` | `Value for "name" must have length at least 1` |
| `maxLength` | 200-char `name` | `Value for "name" must have length at most 120` |
| `regex` | `slug: 'Not A Slug'` | `Value for "slug" does not match required pattern` |
| Column `check` | `stock: 20_000n` | `stock too high` (the string you returned) |

```ts
try {
  db.insertInto(products).values({ sku: 'W-1', name: '', price_cents: 500n, slug: 'w-1', stock: 1n }).executeSync();
} catch (err) {
  // err instanceof KitValidationError
  // err.table === 'products', err.column === 'name'
}
```

> **`int64` columns are `bigint` in TypeScript.** Pass `500n`, not `500`; a plain `number`
> trips the type validator. `real`/`float` are `number`, `bool` is `boolean`, and
> `text`/`json`/`timestamp`/`date` are `string`.

A **column `check`** is a predicate `(value) => boolean | string`. Return `true` to pass;
return a `string` (used as the message) or `false` to fail. `false` yields the generic
message `Value for "<column>" failed custom check`.

## Table-level checks

Use `check(name, fn)` for rules that span more than one column or need the whole row. In
TypeScript `fn` is a **predicate function** `(row) => boolean | string`, run after every
column validator passes:

```ts
import { table, int, check } from '@visorcraft/mongreldb-kit';

const orderItems = table('order_items', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('order_items_id_seq') }),
    int('order_id', { nullable: false }),
    int('product_id', { nullable: false }),
    int('quantity', { nullable: false }),
    int('unit_price_cents', { nullable: false }),
  ],
  primaryKey: 'id',
  checks: [check('qty_positive', (row) => (row.quantity as bigint) > 0n)],
});
```

A failing table check throws **`KitValidationError`** with `.table` set and `.column`
undefined. As with column checks, return a `string` for a custom message; returning
`false` produces `Table check "qty_positive" failed`.

```ts
db.insertInto(orderItems)
  .values({ order_id: 1n, product_id: 1n, quantity: 0n, unit_price_cents: 500n })
  .executeSync();
// throws KitValidationError — table check "qty_positive"
```

> The string-expression grammar you may see elsewhere (e.g. `"quantity > 0"`) is the
> **cross-language serialized form** of a check — the representation stored in the schema
> catalog and evaluated by the Rust/Python engine and the conformance suite. In TypeScript
> you always pass a function, never a raw string.

**In Rust/Python:** the predicate is the same idea — a closure / callable returning a
bool-or-message. The serialized `"quantity > 0"` expression is what crosses the language
boundary. See [rust.md](./rust.md) / [python.md](./python.md).

## Unique constraints

Declare uniqueness with `unique(columns, { name? })`. The name defaults to
`uq_<col1>_<col2>`. A violation throws **`KitDuplicateError`** carrying `.table` and the
`.constraint` name.

### Single-column unique

```ts
const customers = table('customers', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('customers_id_seq') }),
    text('email', { nullable: false }),
    text('name', { nullable: false }),
  ],
  primaryKey: 'id',
  unique: [unique(['email'])],
});

db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada' }).executeSync();
db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada II' }).executeSync();
// throws KitDuplicateError (constraint "uq_email")
```

### Composite unique

A composite unique key is unique over the **tuple** of columns:

```ts
const orderItems = table('order_items', {
  columns: [/* … */],
  primaryKey: 'id',
  unique: [unique(['order_id', 'product_id'])],
});

// Same product twice on one order:
db.insertInto(orderItems).values({ order_id: 7n, product_id: 3n, quantity: 1n, unit_price_cents: 500n }).executeSync();
db.insertInto(orderItems).values({ order_id: 7n, product_id: 3n, quantity: 9n, unit_price_cents: 500n }).executeSync();
// throws KitDuplicateError (constraint "uq_order_id_product_id")
```

### Nullable unique semantics

A row whose unique key has a `null` component does **not** consume a guard key, so multiple
rows may share `null` in a nullable unique column without conflicting — matching SQL's
treatment of `NULL` in unique indexes.

> **Gotcha — a unique *index* does not enforce uniqueness.** Only `unique(...)` specs are
> guard-checked. `index(['email'], { unique: true })` builds a lookup index for query
> performance but the Kit does **not** reject duplicates for it. To enforce a unique value,
> add it to `unique`.

Uniqueness is stored in `__kit_unique_keys`: each constrained insert/update encodes a key
per constraint and rejects the write if another row already owns it. See
[Internal tables](./internal-tables.md).

## Primary keys

A `primaryKey` may be a single column or a tuple. Composite primary keys are enforced with
the same guard mechanism as `unique`, so a duplicate tuple throws **`KitDuplicateError`**
(constraint `__pk_<table>`):

```ts
const memberships = table('memberships', {
  columns: [int('user_id'), int('group_id'), text('role', { nullable: false })],
  primaryKey: ['user_id', 'group_id'],
});

db.insertInto(memberships).values({ user_id: 1n, group_id: 2n, role: 'owner' }).executeSync();
db.insertInto(memberships).values({ user_id: 1n, group_id: 2n, role: 'member' }).executeSync();
// throws KitDuplicateError (constraint "__pk_memberships")
```

> **How single-column primary keys are checked.** In normal use the id comes from a
> sequence default (`sequenceDefault(...)`), and an auto-assigned id is unique by
> construction, so the Kit skips the duplicate check entirely — keeping single inserts and
> bulk loads cheap. If you instead supply an **explicit** id that already exists, the insert
> throws **`KitDuplicateError`** (constraint `__pk_<table>`); the Kit checks the id against
> the existing rows directly rather than reserving a guard row. A non-primary-key scalar
> column still needs `unique(...)` to reject duplicates.

## Foreign keys

`foreignKey(columns, { table, columns }, { onDelete })` guarantees a child row references an
existing parent. Existence is enforced on insert and whenever an update changes a
foreign-key column; a dangling reference throws **`KitForeignKeyError`** (`.table`,
`.constraint`, default name `fk_<cols>_<parent>`). `onDelete` is `'cascade' | 'set null' |
'restrict'` and defaults to `'restrict'`.

```ts
const orders = table('orders', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('orders_id_seq') }),
    int('customer_id', { nullable: false }),
    text('status', { enumValues: ['pending', 'paid', 'shipped', 'cancelled'], default: staticDefault('pending') }),
  ],
  primaryKey: 'id',
  indexes: [index(['customer_id'])],
  foreignKeys: [
    foreignKey(['customer_id'], { table: 'customers', columns: ['id'] }, { onDelete: 'cascade' }),
  ],
});

db.insertInto(orders).values({ customer_id: 999n }).executeSync();
// throws KitForeignKeyError — no customer with id 999 (constraint "fk_customer_id_customers")
```

### Nullable and composite foreign keys

A `null` foreign-key value skips the parent check (the reference is simply absent):

```ts
int('manager_id', { nullable: true }),
foreignKey(['manager_id'], { table: 'employees', columns: ['id'] }),
// manager_id: null inserts fine; a non-null value must point at an existing employee.
```

A composite foreign key references a composite parent key, column-for-column:

```ts
foreignKey(['region', 'warehouse_no'], { table: 'warehouses', columns: ['region', 'no'] });
```

## Delete actions

When a parent row is deleted, each child foreign key's `onDelete` decides what happens:

| Action | Behavior | On block |
| --- | --- | --- |
| `restrict` (default) | Reject the delete while any child references the row. | `KitRestrictError` |
| `cascade` | Recursively delete the referencing child rows. | — |
| `set null` | Clear the child foreign-key columns (must be nullable) and keep the rows. | — |

### Restrict

```ts
// order_items reference products with onDelete: 'restrict'
foreignKey(['product_id'], { table: 'products', columns: ['id'] }, { onDelete: 'restrict' });

db.deleteFrom(products).where(eq(products.id, widget.id)).executeSync();
// throws KitRestrictError when any order_item still references that product
// (.table === 'order_items', .constraint === the fk name)
```

### Cascade

```ts
// orders reference customers with onDelete: 'cascade';
// order_items reference orders with onDelete: 'cascade'.
db.deleteFrom(customers).where(eq(customers.id, ada.id)).executeSync();
// deletes Ada, her orders, and those orders' items — one atomic transaction.
```

### Set null

The child foreign-key columns **must be nullable**. Deleting the parent clears them and
leaves the child rows in place:

```ts
const catalog = table('catalog', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('catalog_id_seq') }),
    text('name', { nullable: false }),
    int('category_id', { nullable: true }),
  ],
  primaryKey: 'id',
  foreignKeys: [
    foreignKey(['category_id'], { table: 'categories', columns: ['id'] }, { onDelete: 'set null' }),
  ],
});

db.deleteFrom(categories).where(eq(categories.id, cat.id)).executeSync();
const item = db.selectFrom(catalog).where(eq(catalog.id, hammer.id)).executeSync()[0];
// item still exists; item.category_id is now null.
```

> If a `set null` target column is **not** nullable, the cleared row fails re-validation and
> the delete throws `KitValidationError` instead.

## Gotchas

- **Null round-trips as `null`.** A column written with `null` — omitted on insert, set to
  `null`, or cleared by a `set null` cascade — reads back as a JavaScript `null`, and
  `isNull(column)` matches it (`isNotNull(column)` excludes it). Nullable foreign keys and
  `set null` results behave as expected: a cleared `category_id` reads back as `null`.
- **Unique indexes are not enforced** — use `unique(...)`, not `index(..., { unique: true })`.
- **Single-column primary keys reject explicit duplicates** — supplying an id that already
  exists throws `KitDuplicateError`; an omitted id is filled from the sequence and is unique
  by construction. Use `unique(...)` for non-PK scalar uniqueness.
- **Validators run in-process**, in declaration order, on the full row — they are not pushed
  into the storage engine. A column `check`/table `check` is your own predicate function.

## Cross-language consistency

Constraint behavior is exercised by a shared conformance suite: the same fixtures run
inserts, updates, deletes, unique and foreign-key violations, and cascade / set-null /
restrict actions across TypeScript, Rust, and Python, so the rules and errors above behave
identically in every binding.

## See also

- [Schema DSL](./schema.md) — declaring columns, `unique`, `foreignKey`, `check`, and assembling a `Schema`.
- [Transactions](./transactions.md) — how each constrained mutation commits, and conflict handling.
- [Errors](./errors.md) — `KitValidationError`, `KitDuplicateError`, `KitForeignKeyError`, `KitRestrictError`, and codes.
- [Internal tables](./internal-tables.md) — the `__kit_unique_keys` / `__kit_row_guards` guard tables.
- [Query builder](./query-builder.md) — the CRUD calls that run these checks.
