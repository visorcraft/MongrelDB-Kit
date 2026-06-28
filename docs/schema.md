# Schema DSL

A MongrelDB Kit schema is plain TypeScript. You declare **tables** made of **columns**, attach
table-level constraints (primary key, indexes, unique, foreign keys, checks), and assemble them
into a `Schema`. The same declarations drive type inference (see [Types](./types.md)), validation,
constraint enforcement, and migrations.

Everything on this page is imported from `@mongreldb/kit`:

```ts
import {
  Schema, table,
  int, text, real, bool, json, timestamp, date, blob, column,
  index, unique, foreignKey, check,
  sequenceDefault, nowDefault, staticDefault,
} from '@mongreldb/kit';
```

## The running example

The docs use a generic **store** domain — `customers`, `products`, `orders`, `order_items`. Here is
the full schema; later sections dissect each piece.

```ts
export const customers = table('customers', {
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

export const products = table('products', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('products_id_seq') }),
    text('sku', { nullable: false }),
    text('name', { nullable: false }),
    int('price_cents', { nullable: false }),
  ],
  primaryKey: 'id',
  unique: [unique(['sku'])],
  checks: [check('price_nonneg', (row) => (row.price_cents as bigint) >= 0n)],
});

export const orders = table('orders', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('orders_id_seq') }),
    int('customer_id', { nullable: false }),
    text('status', {
      enumValues: ['pending', 'paid', 'shipped', 'cancelled'],
      default: staticDefault('pending'),
    }),
    timestamp('placed_at', { default: nowDefault() }),
  ],
  primaryKey: 'id',
  indexes: [index(['customer_id'])],
  foreignKeys: [
    foreignKey(['customer_id'], { table: 'customers', columns: ['id'] }, { onDelete: 'cascade' }),
  ],
});

export const orderItems = table('order_items', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('order_items_id_seq') }),
    int('order_id', { nullable: false }),
    int('product_id', { nullable: false }),
    int('quantity', { nullable: false }),
    int('unit_price_cents', { nullable: false }),
  ],
  primaryKey: 'id',
  foreignKeys: [
    foreignKey(['order_id'], { table: 'orders', columns: ['id'] }, { onDelete: 'cascade' }),
    foreignKey(['product_id'], { table: 'products', columns: ['id'] }, { onDelete: 'restrict' }),
  ],
  unique: [unique(['order_id', 'product_id'])],
  checks: [check('qty_positive', (row) => (row.quantity as bigint) > 0n)],
});

export const schema = new Schema([customers, products, orders, orderItems]);
```

## `table(name, options)`

`table()` returns a `TableSpec`. Each column is also exposed as a property on the returned object —
`customers.email`, `orders.customer_id` — which is exactly what the query builder wants for
predicates and ordering (`eq(customers.email, ...)`, `desc(orders.id)`).

```ts
const t = table('customers', {
  columns: [ /* ColumnSpec[] — at least one */ ],
  primaryKey: 'id',          // string | string[]  (required)
  indexes:     [/* IndexSpec[]      */],  // optional
  unique:      [/* UniqueSpec[]     */],  // optional
  foreignKeys: [/* ForeignKeySpec[] */],  // optional
  checks:      [/* CheckSpec[]      */],  // optional
  id: 1,                     // optional explicit table id (see Notes)
});
```

`table()` validates eagerly and throws a plain `Error` if a declaration is inconsistent: duplicate
column names or ids, a `primaryKey` / index / unique / foreign-key column that does not exist on the
table, etc. These are programming errors surfaced at construction time, not row-level validation.

### Column ids and table ids

- Every column receives a **stable 1-based id by declaration order** (`id` 1, 2, 3, …). The id is
  the on-disk address for that column's cells, so treat declaration order as significant: append new
  columns rather than reordering existing ones. (You can pin an id by setting `id` on a `ColumnSpec`
  before passing it in; otherwise it is positional.)
- Each table receives a `tableId`. Without an explicit `id` option it is auto-assigned from a
  process-global counter; pass `id` in the table options to pin it.

## Columns

A column has a name, a **storage type**, and optional `ColumnOptions`. Use the typed constructors —
they are thin wrappers over the generic `column()`:

| Constructor | Storage type | `Row<T>` TS type |
| --- | --- | --- |
| `int(name, opts?)` | `int64` | `bigint` |
| `real(name, opts?)` | `float64` | `number` |
| `bool(name, opts?)` | `bool` | `boolean` |
| `text(name, opts?)` | `text` | `string` |
| `timestamp(name, opts?)` | `timestamp` | `string` (ISO 8601) |
| `date(name, opts?)` | `date` | `string` (`YYYY-MM-DD`) |
| `json(name, opts?)` | `json` | `unknown` — stored as text; see below |
| `blob(name, opts?)` | `bytes` | inferred `unknown`; runtime value is `Uint8Array` |
| `column(name, storageType, opts?)` | any of the above | per storage type |

`column()` is the generic escape hatch the others delegate to; pass the storage type string
explicitly (`column('raw', 'int64', { nullable: true })`). Prefer the named constructors.

A nullable column widens its `Row<T>` type with `| null` — `text('note', { nullable: true })` infers
`string | null`.

### `int64` is `bigint`, not `number`

Integer columns map to JavaScript `bigint`. Write literals with the `n` suffix and read them back as
`bigint`:

```ts
const c = db.insertInto(customers).values({ email: 'a@b.c', name: 'Ada' }).executeSync();
c.id;            // 1n   (bigint)
c.id === 1n;     // true
c.id === 1;      // false — never compare a bigint id against a number
```

Use `real()` (a `float64` / `number`) only for genuinely fractional quantities. Money is best kept
as integer minor units — note `price_cents` and `unit_price_cents` are `int` in the store schema.

### `json` columns are stored as text

`json` validates that the value is JSON-serializable, but storage writes the value through a **text**
cell. In practice you supply (and read back) a **string** — serialize yourself:

```ts
text('meta', { nullable: true });        // or:
json('meta');                            // inferred `unknown`; pass a string

db.insertInto(settings).values({ meta: JSON.stringify({ theme: 'dark' }) }).executeSync();
const row = db.selectFrom(settings).executeSync()[0];
const meta = JSON.parse(row.meta as string);
```

### `blob` columns are `Uint8Array` at runtime

A `blob` (storage `bytes`) is validated as, stored from, and read back as a `Uint8Array`. The
inferred `Row<T>` type is currently `unknown`, so you will usually cast on the way out:

```ts
db.insertInto(files).values({ data: new Uint8Array([1, 2, 3]) }).executeSync();
const data = row.data as Uint8Array;
```

## `ColumnOptions`

Every constructor accepts the same options object:

```ts
type ColumnOptions = {
  nullable?: boolean;                          // default false
  primaryKey?: boolean;                        // marker; the table primaryKey is authoritative
  default?: DefaultValue;                      // see ./defaults.md
  generated?: 'uuid' | 'now';                  // shorthand default; see below
  enumValues?: string[];                       // allowed text values
  check?: (value: unknown) => boolean | string;
  min?: number;                                // numeric lower bound (int64 & float64)
  max?: number;                                // numeric upper bound
  minLength?: number;                          // length floor for text / bytes
  maxLength?: number;                          // length ceiling for text / bytes
  regex?: RegExp;                              // pattern for text
};
```

How each one behaves on a row (validation runs on every insert and update):

- **`nullable`** — when `false` (the default), a missing/`null` value is rejected with
  `KitValidationError`. When `true`, the column may be `null` and becomes optional on insert (see
  [Types](./types.md)).
- **`primaryKey`** — a per-column marker. The table's `primaryKey` option is what actually defines
  the key; keep them consistent (`int('id', { primaryKey: true })` plus `primaryKey: 'id'`).
- **`default` / `generated`** — supply a value when the column is omitted on insert. `generated:
  'uuid'` and `generated: 'now'` are shorthands for `default: uuidDefault()` / `nowDefault()`. If
  both are set, `default` wins. Either one makes the column optional on insert. Full coverage in
  [Defaults & sequences](./defaults.md).
- **`enumValues`** — for text columns, the value must be one of the listed strings, else
  `KitValidationError`. The inferred type stays `string`.
- **`check`** — a **predicate function** `(value) => boolean | string`. Return `true` to pass;
  return `false` (generic message) or a `string` (used as the error message) to fail. This is the
  TypeScript surface — do **not** pass a SQL-like string expression here; that grammar is the
  cross-language serialized form. Table-level `check()` (below) works the same way over a whole row.

  ```ts
  text('slug', { check: (v) => (v as string).length > 0 || 'slug required' })
  ```
- **`min` / `max`** — numeric bounds, checked for both `int64` (`bigint`) and `float64` (`number`)
  columns. `int('age', { min: 0, max: 150 })`.
- **`minLength` / `maxLength`** — length bounds for `text` (string length) and `bytes` (byte
  length). `text('handle', { minLength: 3, maxLength: 20 })`.
- **`regex`** — a `RegExp` the string value must match. `text('handle', { regex: /^[a-z]+$/ })`.

Bounds and patterns only apply to compatible types (e.g. `regex` is ignored for non-strings), and
they are skipped entirely when the value is `null` on a nullable column.

## Table-level constraints

These are passed in the `table()` options and are enforced by the Kit (the storage engine does not
enforce them natively). See [Constraints](./constraints.md) for the full behavior; here is how each
is constructed.

### `primaryKey` — single and composite

`primaryKey` is required and accepts a column name or an array of names for a composite key:

```ts
// single-column key
table('customers', { columns: [...], primaryKey: 'id' });

// composite key
table('memberships', {
  columns: [int('user_id', { nullable: false }), int('group_id', { nullable: false }), /* … */],
  primaryKey: ['user_id', 'group_id'],
});
```

Internally `primaryKey` is normalized to a string array, so `'id'` and `['id']` are equivalent.

### `index(columns, { name?, unique? })`

A non-unique secondary index by default; pass `unique: true` for a unique index. The generated name
is `idx_<col1>_<col2>_…` unless you pass `name`.

```ts
indexes: [index(['customer_id']), index(['email'], { unique: true, name: 'idx_email' })]
```

The query builder pushes equality/range predicates down to the native engine only for indexed
columns and `int64` columns; everything else is filtered in memory. Index the columns you filter on
hot paths.

### `unique(columns, { name? })`

A unique constraint over one or more columns (composite uniqueness). Generated name is
`uq_<col1>_<col2>_…`. Inserting/updating a duplicate throws a typed duplicate error.

```ts
unique: [unique(['email'])]                  // single column
unique: [unique(['order_id', 'product_id'])] // composite — one product per order
```

### `foreignKey(columns, { table, columns }, { onDelete?, name? })`

A foreign key from local `columns` to a parent table's `columns`. `onDelete` is `'cascade'`,
`'set null'`, or `'restrict'` (the **default** is `'restrict'`). Generated name is
`fk_<col1>_…_<referencedTable>`.

```ts
foreignKeys: [
  foreignKey(['customer_id'], { table: 'customers', columns: ['id'] }, { onDelete: 'cascade' }),
  foreignKey(['product_id'],  { table: 'products',  columns: ['id'] }, { onDelete: 'restrict' }),
]
```

Inserts/updates verify the referenced row exists; deletes apply the `onDelete` action (cascade the
delete, null out the child columns, or block the delete).

### `check(name, fn)`

A **table-level** check evaluated against the whole row. `fn` is `(row) => boolean | string`: return
`true` to pass, or `false` / a `string` message to fail with `KitValidationError`. Use it for
multi-column invariants or anything a single-column `check` cannot express.

```ts
checks: [
  check('price_nonneg', (row) => (row.price_cents as bigint) >= 0n),
  check('qty_positive', (row) => (row.quantity as bigint) > 0n || 'quantity must be positive'),
]
```

The string-expression form (e.g. `"price_cents >= 0"`) is the cross-language serialized
representation used by the Rust/Python evaluator and the conformance suite — in TypeScript you always
pass a predicate function.

## Assembling a `Schema`

`new Schema([...tables])` collects the tables and is what you hand to `KitDatabase.openSync` /
`migrateSync`. It rejects duplicate table names or table ids at construction time.

```ts
export const schema = new Schema([customers, products, orders, orderItems]);

const db = KitDatabase.openSync('./data', schema);   // a data *directory*, not a file
db.migrateSync(schema, [{ version: 1, name: 'init', up: () => {} }]);
```

Useful instance methods:

| Method | Returns |
| --- | --- |
| `schema.tablesList()` | `TableSpec[]` — all tables, in insertion order |
| `schema.table(name)` | the `TableSpec` (throws if absent) |
| `schema.hasTable(name)` | `boolean` |

## Notes

- `table()` and `new Schema()` throw on inconsistent declarations (missing pk column, duplicate
  names/ids, unknown index/unique/fk columns) — these are construction-time programming errors,
  distinct from per-row `KitValidationError`.
- Column ids are positional and load-bearing; append columns, do not reorder them. See
  [Migrations](./migrations.md) for evolving a schema safely.
- The Kit does **not** use MongrelDB's internal storage `RowId` as your primary key. Your ids live
  in your own columns and are assigned by [defaults / sequences](./defaults.md).

## See also

- [Types](./types.md) — `Row<T>`, `Insert<T>`, `Update<T>` inferred from these declarations.
- [Defaults & sequences](./defaults.md) — `default` / `generated` and auto-increment ids.
- [Constraints](./constraints.md) — how unique, foreign-key, check, and not-null are enforced.
- [Query builder](./query-builder.md) — using the column properties in selects and predicates.
- [Migrations](./migrations.md) — evolving a schema over time.
