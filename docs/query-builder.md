# Query builder

MongrelDB Kit ships a small, typed query builder for reads and writes. It is **synchronous**
(`.executeSync()`) with an async wrapper (`.execute()`), returns typed rows, and pushes the
predicates it can down to the storage engine while computing everything else — joins, grouping,
aggregates, subqueries, CTEs — in memory. The whole surface is exposed as methods on a
`KitDatabase` plus a handful of helper functions imported from `@mongreldb/kit`.

This guide uses the shared "store" schema (`customers`, `products`, `orders`, `order_items`); the
full definition lives in [Schema DSL](./schema.md). The snippets below assume an open database and a
small amount of seed data:

```ts
import {
  KitDatabase, Schema,
  eq, ne, gt, gte, lt, lte, inList, notInList, isNull, isNotNull,
  like, contains, and, or, not, asc, desc,
  inSubquery, exists, notExists, count, sum, min, max, avg,
} from '@mongreldb/kit';
import { customers, products, orders, orderItems, schema } from './store-schema.js';

const db = KitDatabase.openSync('./data', schema);
db.migrateSync(schema, [{ version: 1, name: 'init', up: () => {} }]);

// int64 columns are bigint in TS — values are written as bigint literals (500n).
const ada  = db.insertInto(customers).values({ email: 'ada@example.com',  name: 'Ada'  }).executeSync();
const bob  = db.insertInto(customers).values({ email: 'bob@example.com',  name: 'Bob'  }).executeSync();
const cleo = db.insertInto(customers).values({ email: 'cleo@example.com', name: 'Cleo' }).executeSync();
const widget = db.insertInto(products).values({ sku: 'W-1', name: 'Widget', price_cents: 500n  }).executeSync();
const gadget = db.insertInto(products).values({ sku: 'G-1', name: 'Gadget', price_cents: 1200n }).executeSync();
const o1 = db.insertInto(orders).values({ customer_id: ada.id, status: 'paid'    }).executeSync();
const o2 = db.insertInto(orders).values({ customer_id: ada.id, status: 'pending' }).executeSync();
const o3 = db.insertInto(orders).values({ customer_id: bob.id, status: 'paid'    }).executeSync();
```

> **Column accessors.** A `table(...)` value carries its columns as properties, so you reference a
> column as `orders.status`, `customers.email`, `products.price_cents`, and so on. A few names are
> reserved by the table object itself — see [Gotchas](#gotchas).

## Reads

### Select, filter, order, paginate

`db.selectFrom(table)` returns a `SelectBuilder`. Chain modifiers, then call `.executeSync()`.
With no modifiers it returns every row.

```ts
// All rows -> Row<typeof customers>[]
const everyone = db.selectFrom(customers).executeSync();

// Filtered: one predicate goes in .where(...)
const paid = db.selectFrom(orders).where(eq(orders.status, 'paid')).executeSync();

// Ordering, limit, offset (a page). orderBy is variadic and stable across keys.
const page = db
  .selectFrom(orders)
  .orderBy(desc(orders.placed_at), asc(orders.id))
  .limit(20)
  .offset(40)
  .executeSync();
```

`.where(...)` takes a **single** predicate; calling it again replaces the previous one. Combine
conditions with `and(...)` / `or(...)`:

```ts
const adasPaid = db
  .selectFrom(orders)
  .where(and(eq(orders.customer_id, ada.id), eq(orders.status, 'paid')))
  .executeSync();
```

`limit` and `offset` are plain JS `number`s.

### Column projection — `.select([...])`

`.select([col, ...])` narrows each result row to only the named columns. The result type narrows
too, to `Pick<Row<T>, names>[]`:

```ts
const slim = db.selectFrom(customers).select([customers.id, customers.email]).executeSync();
// slim: Array<{ id: bigint; email: string }>
```

### A single row

The builder always returns an array; index `[0]` for one row (or `undefined` when empty):

```ts
const found = db.selectFrom(customers).where(eq(customers.id, ada.id)).executeSync()[0];
// found: Row<typeof customers> | undefined
```

### `.executeSync()` vs `.execute()`

Every terminal builder exposes both. `.executeSync()` runs the query and returns the result
directly; `.execute()` returns a `Promise` that resolves to the same value. The implementation is
synchronous either way — `execute()` is purely an `async` convenience for callers that prefer
awaiting.

```ts
const rows = await db.selectFrom(orders).where(eq(orders.status, 'paid')).execute();
```

## Predicates

Predicate helpers are imported from `@mongreldb/kit` and passed to `.where(...)`. The comparison
helpers are typed: the value must match the column's application type (e.g. `bigint` for an `int`
column, `string` for `text`).

| Helper | Signature | Meaning |
| --- | --- | --- |
| `eq` | `eq(column, value)` | `column = value` |
| `ne` | `ne(column, value)` | `column <> value` |
| `gt` / `gte` | `gt(column, value)` | `column > value` / `>=` |
| `lt` / `lte` | `lt(column, value)` | `column < value` / `<=` |
| `isNull` | `isNull(column)` | `column IS NULL` |
| `isNotNull` | `isNotNull(column)` | `column IS NOT NULL` |
| `inList` | `inList(column, values[])` | `column IN (...)` |
| `notInList` | `notInList(column, values[])` | `column NOT IN (...)` |
| `like` | `like(column, pattern)` | SQL `LIKE` (see below) |
| `contains` | `contains(column, substr)` | case-sensitive substring |
| `and` | `and(...predicates)` | logical AND (variadic) |
| `or` | `or(...predicates)` | logical OR (variadic) |
| `not` | `not(predicate)` | logical NOT |
| `inSubquery` | `inSubquery(column, sub)` | `column IN (subquery)` |
| `exists` / `notExists` | `exists(sub)` | `EXISTS` / `NOT EXISTS` |

```ts
db.selectFrom(orders).where(ne(orders.status, 'cancelled')).executeSync();
db.selectFrom(products).where(gte(products.price_cents, 1000n)).executeSync();
db.selectFrom(orders).where(inList(orders.status, ['paid', 'shipped'])).executeSync();
db.selectFrom(orders).where(notInList(orders.status, ['cancelled'])).executeSync();
db.selectFrom(customers).where(isNotNull(customers.email)).executeSync();

// Nested logic.
db.selectFrom(orders)
  .where(or(eq(orders.status, 'paid'), and(eq(orders.status, 'pending'), gt(orders.id, 1n))))
  .executeSync();

db.selectFrom(orders).where(not(eq(orders.status, 'paid'))).executeSync();
```

`asc(column)` and `desc(column)` build the order terms used by `.orderBy(...)`.

### `like` and `contains`

`like(column, pattern)` is SQL `LIKE`: `%` matches any run of characters, `_` matches exactly one,
every other character is literal, and the match is **case-sensitive** and anchored to the whole
value. `contains(column, substr)` is a case-sensitive substring test with no wildcards.

```ts
db.selectFrom(customers).where(like(customers.email, '%@example.com')).executeSync(); // suffix
db.selectFrom(customers).where(like(customers.email, 'a_a@%')).executeSync();          // a?a@...
db.selectFrom(customers).where(contains(customers.email, 'cleo')).executeSync();       // substring
```

Both run as a full table scan with the match evaluated in JavaScript — see
[Performance & limits](#performance--limits).

## Writes

### Insert — returns the inserted `Row`

`insertInto(table).values(row).executeSync()` validates the row, applies defaults (including the
sequence-assigned primary key), enforces foreign keys and unique/PK guards in a transaction, and
returns the **single** stored `Row<T>` with every column populated.

```ts
const row = db.insertInto(customers).values({ email: 'dan@example.com', name: 'Dan' }).executeSync();
row.id;   // 4n   — bigint, assigned by the customers_id_seq sequence (1-based)
row.tier; // 'free' — staticDefault applied
```

`.values(...)` is required; omitting it throws. Columns that are nullable or have a default may be
omitted (see [Types](./types.md) for the `Insert<T>` shape). Remember that `int64` values are
`bigint`: `price_cents: 500n`, not `500`.

### Insert many — one transaction for a batch

`insertInto(table).valuesMany(rows).executeSync()` inserts an array of rows in a **single
transaction** and returns the stored `Row<T>[]` in input order. It is the same as calling
`.values(row).executeSync()` in a loop — defaults, validation, sequence-assigned ids, and
foreign-key / unique / PK guards all still run per row — but it commits once instead of once per
row, which is dramatically faster for bulk loads.

```ts
const rows = db.insertInto(products).valuesMany([
  { sku: 'A-1', name: 'Anvil',  price_cents: 2500n },
  { sku: 'B-1', name: 'Bucket', price_cents: 400n  },
  { sku: 'C-1', name: 'Cog',    price_cents: 150n  },
]).executeSync();
rows.map((r) => r.id); // [1n, 2n, 3n] — sequence ids assigned in order
```

Because the whole batch is one transaction, it is **all-or-nothing**: if any row fails a guard or
validator the transaction rolls back and nothing is inserted. For tables with a single-column
primary key the batch pre-loads existing keys once, so duplicate detection stays O(1) per row
rather than a scan per row. An empty array inserts nothing and returns `[]`.

### Update — returns the updated `Row[]`

`updateTable(table).set(patch).where(predicate).executeSync()` merges `patch` into every matched
row and returns the updated rows as `Row<T>[]` (full rows, not just the changed columns).

```ts
const updated = db
  .updateTable(orders)
  .set({ status: 'shipped' })
  .where(eq(orders.customer_id, ada.id))
  .executeSync();
// updated: Row<typeof orders>[]  — every matched order, now status: 'shipped'
```

`.set(...)` is required. `.where(...)` is **optional** — omitting it updates every row in the
table. Columns produced by `nowDefault()` are refreshed on update unless you set them
explicitly. Unique, primary-key, and foreign-key guards are re-checked for the new values.

### Delete — returns a `bigint` count

`deleteFrom(table).where(predicate).executeSync()` returns the number of matched rows as a
`bigint`. Configured `onDelete` actions (cascade / set null / restrict) run inside the same
transaction; the count reflects the rows matched at the top level, not cascaded rows.

```ts
const removed = db.deleteFrom(orders).where(eq(orders.id, o2.id)).executeSync();
// removed: 1n  (bigint). Its order_items are cascade-deleted by the FK.
```

`.where(...)` is optional — omitting it deletes every row in the table.

## Aggregates

Whole-table scalar aggregates are terminal methods on `SelectBuilder`. They honor `.where(...)` but
ignore ordering, projection, and pagination.

| Method | Return type | Empty set |
| --- | --- | --- |
| `selectCount()` | `bigint` | `0n` |
| `selectSum(col)` | `bigint` for an `int` column, `number` for a `real`/`float` column | `0n` / `0` (never null) |
| `selectAvg(col)` | `number \| null` (always a float) | `null` |
| `selectMin(col)` | `ColumnValue<col> \| null` | `null` |
| `selectMax(col)` | `ColumnValue<col> \| null` | `null` |

`NULL` values are skipped before aggregating. `selectCount()` with no `.where(...)` uses the
engine's fast row count.

```ts
const n     = db.selectFrom(orders).selectCount().executeSync();                    // bigint
const units = db.selectFrom(orderItems).selectSum(orderItems.quantity).executeSync(); // bigint (int column)
const cheap = db.selectFrom(products).selectMin(products.price_cents).executeSync();   // bigint | null
const dear  = db.selectFrom(products).selectMax(products.price_cents).executeSync();   // bigint | null
const mean  = db.selectFrom(products).selectAvg(products.price_cents).executeSync();   // number | null

// Filtered + empty-set behavior
const cancelled = db.selectFrom(orders).where(eq(orders.status, 'cancelled'));
cancelled.selectCount().executeSync();             // 0n
cancelled.selectSum(orders.id).executeSync();      // 0n  (int sum of nothing is 0n, not null)
cancelled.selectAvg(orders.id).executeSync();      // null
cancelled.selectMin(orders.id).executeSync();      // null
```

> `selectSum` over a `real`/`float` column returns a `number` and an empty float sum is `0`. Only
> `avg`, `min`, and `max` return `null` for an empty set.

## Distinct

`.distinct()` removes duplicate result rows. With a projection it dedupes over the selected
columns; without one, over the full row. Any `limit`/`offset` is applied **after** the dedupe.

```ts
const statuses = db.selectFrom(orders).select([orders.status]).distinct().executeSync();
// statuses: Array<{ status: string }> — one row per distinct status
```

## Joins

Start a join from a base `selectFrom(...)`. `innerJoin(table, on)`, `leftJoin(table, on)`, and
`crossJoin(table)` return a `JoinBuilder`. The result of `.executeSync()` is `JoinRow[]`, where a
`JoinRow` is **keyed by table name** and each side is a row object or `null`:

```ts
type JoinRow = Record<string, Record<string, unknown> | null>;
```

The `on` callback receives the assembled `JoinRow` and returns a boolean. For a `LEFT JOIN` with no
match, the joined side is `null`.

```ts
// INNER JOIN: orders with their customer
const joined = db
  .selectFrom(orders)
  .innerJoin(customers, (r) => r.orders!.customer_id === r.customers!.id)
  .where((r) => r.customers!.email === 'ada@example.com') // post-join filter over the JoinRow
  .executeSync();
// joined: Array<{ orders: Row; customers: Row }>
joined[0].orders;    // the order row
joined[0].customers; // the matched customer row

// LEFT JOIN: every customer, with their orders (null when none)
const withOrders = db
  .selectFrom(customers)
  .leftJoin(orders, (r) => r.orders!.customer_id === r.customers!.id)
  .executeSync();
// A customer with no orders -> { customers: {...}, orders: null }

// CROSS JOIN: cartesian product (no predicate)
const pairs = db.selectFrom(products).crossJoin(customers).executeSync();
// pairs.length === products.length * customers.length
```

The base table honors the `.where(...)` you set on `selectFrom(...)`; `JoinBuilder` adds its own
`.where(joinPredicate)` (a post-join filter over the `JoinRow`), plus `.limit(n)` / `.offset(n)`.
Joined tables are fully scanned — see [Performance & limits](#performance--limits).

## Grouping — `groupBy` / `aggregate` / `having`

`selectFrom(table).where(...).groupBy(...columns).aggregate({...}).having(...).executeSync()`
produces one `GroupRow` per distinct combination of the group columns. Each row carries the
group-key column values (by name) plus one entry per named aggregate. Aggregate descriptors are the
`count()`, `sum(col)`, `min(col)`, `max(col)`, and `avg(col)` helpers.

```ts
const byStatus = db
  .selectFrom(orders)
  .groupBy(orders.status)
  .aggregate({
    n:     count(),         // -> bigint
    minId: min(orders.id),  // -> bigint | null
    maxId: max(orders.id),  // -> bigint | null
    sumId: sum(orders.id),  // -> bigint  (int column)
    avgId: avg(orders.id),  // -> number | null
  })
  .having((g) => (g.n as bigint) >= 2n) // filter groups after aggregation
  .executeSync();
// byStatus: Array<{ status: string; n: bigint; minId; maxId; sumId; avgId }>
```

`count()` aliases resolve to `bigint`; the others follow the same return/empty rules as the scalar
aggregates above (within a group there is always at least one row). `having(...)` filters the
already-assembled group rows. `groupBy` keeps the base `.where(...)` but not ordering — sort the
returned array in JS if you need a specific group order.

## Subqueries

`inSubquery`, `exists`, and `notExists` take a row-returning `SelectBuilder`. They are
**uncorrelated**: the subquery is evaluated once, up front, and cannot reference the outer row.

- `inSubquery(column, sub)` — the subquery must project exactly one column (via `.select([col])`).
  If you don't project, it falls back to a single-column primary key, then to the first column. An
  aggregate/count subquery is rejected.
- `exists(sub)` / `notExists(sub)` — true/false for whether the subquery matches any row; it gates
  the entire outer scan.

```ts
// Customers who placed at least one paid order
const paidCustomerIds = db
  .selectFrom(orders)
  .where(eq(orders.status, 'paid'))
  .select([orders.customer_id]); // exactly one column

const buyers = db.selectFrom(customers).where(inSubquery(customers.id, paidCustomerIds)).executeSync();

// EXISTS / NOT EXISTS — note these gate the whole outer query (uncorrelated)
const anyPending = db.selectFrom(orders).where(eq(orders.status, 'pending'));
db.selectFrom(customers).where(exists(anyPending)).executeSync();    // all customers, iff a pending order exists
db.selectFrom(customers).where(notExists(anyPending)).executeSync(); // all customers, iff none exists
```

## Common table expressions (CTEs)

`db.with(name, builder)` runs a row-returning select eagerly, materializes its rows in memory, and
returns a `CteScope` whose `selectFrom(name)` reads them as if they were a table. Chain `.with(...)`
to declare additional CTEs in the same scope.

```ts
const scope = db.with('paid_orders', db.selectFrom(orders).where(eq(orders.status, 'paid')));

// Read the CTE like a table — full SelectBuilder surface applies.
const rows = scope.selectFrom('paid_orders').orderBy(asc(orders.id)).executeSync();
// rows: Record<string, unknown>[]  — CTE rows are untyped records

// Aggregate over a CTE
const paidCount = db
  .with('paid', db.selectFrom(orders).where(eq(orders.status, 'paid')))
  .selectFrom('paid')
  .selectCount()
  .executeSync(); // bigint

// Chained CTEs in one scope
const products2 = db
  .with('paid', db.selectFrom(orders).where(eq(orders.status, 'paid')))
  .with('all_products', db.selectFrom(products))
  .selectFrom('all_products')
  .executeSync();
```

The `builder` passed to `with` must be a select that returns rows — handing it a `selectCount()` or
other aggregate throws (`Only a row-returning select can back a CTE`). CTEs are not lazy and not
recursive; each one is computed once and cached for the life of the scope. Rows read from a CTE are
typed as `Record<string, unknown>[]`, not `Row<T>[]`, because the source is synthetic.

## Raw escape hatch — `db.nativeDb`

When the builder does not expose what you need, drop to the underlying MongrelDB `Database` via
`db.nativeDb`. This bypasses kit constraints (validation, unique/FK guards, defaults), so use it
deliberately.

```ts
const total = db.nativeDb.table('orders').count(); // bigint, straight from the engine
```

## Performance & limits

The builder pushes the predicates it can into the storage engine and computes the rest in
JavaScript. Know where the line is:

- **Predicate pushdown is narrow.** `eq` and the range operators (`gt`/`gte`/`lt`/`lte`) push down
  for `int64` (and `float`) columns, and `eq`/`inList` push down for **indexed** `text` columns
  (via a bitmap index). Everything else — `ne`, `isNull`/`isNotNull`, `like`, `contains`,
  `notInList`, and `eq` on a non-indexed text column — runs as a full table scan with the match
  evaluated in JS. `inList` only pushes down when *every* value is individually pushable.
- **Joins are in-memory nested loops.** Each joined table is fully scanned once and re-evaluated for
  every combination; there is no predicate pushdown into a joined table. Intended for modest working
  sets.
- **Aggregates, grouping, and CTEs compute in memory** over the matched rows.
- **Subqueries are uncorrelated.** `inSubquery`/`exists`/`notExists` evaluate their subquery exactly
  once; they cannot reference the outer row, so there is no per-row re-binding.
- **CTEs are eagerly materialized**, not lazy or recursive — each `with` is computed up front and
  cached for the scope's lifetime.

## Gotchas

- **`.where(...)` replaces, it doesn't accumulate.** Two `.where(...)` calls keep only the last;
  combine with `and(...)` / `or(...)`.
- **Reserved column names aren't exposed as accessors.** The table value reuses keys like `name`,
  `columns`, `primaryKey`, `indexes`, `foreignKeys`, `unique`, and `checks` for its own metadata, so
  a column literally named `name` is **not** reachable as `customers.name` (that returns the table
  name string). Reach such a column through the columns array instead:
  `const nameCol = customers.columns.find((c) => c.name === 'name')!;` and pass `nameCol` to a
  predicate. Prefer column names that don't collide.
- **Aggregates ignore ordering and pagination.** `selectCount()`/`selectSum(...)`/etc. carry only
  the `.where(...)`; any `.orderBy`/`.limit`/`.offset` set before them is dropped.
- **`distinct()` paginates after deduping**, so `.limit(n).distinct()` returns up to `n` *distinct*
  rows, not `n` rows then deduped.
- **`int64` is `bigint`.** Write integer literals as `500n`, compare with `gt(col, 0n)`, and expect
  `bigint` back from `selectSum`/`selectCount` and integer columns.

## Other languages

The same query surface is available from Rust and Python with language-idiomatic APIs; the
predicate set and the in-memory/uncorrelated/materialized ceilings are identical. See the
[Rust](./rust.md) and [Python](./python.md) guides for the full APIs. A quick orientation:

```rust
// Rust: a language-neutral AST from mongreldb_kit_core::query, re-exported by mongreldb-kit.
use mongreldb_kit::{Query, Select, Expr, Literal, OrderBy, Direction};

let query = Query::Select(Select {
    table: "orders".into(),
    columns: vec![Expr::Column("id".into())],
    filter: Some(Expr::Eq(
        Box::new(Expr::Column("status".into())),
        Box::new(Expr::Literal(Literal::Text("paid".into()))),
    )),
    order_by: vec![OrderBy { expr: Expr::Column("id".into()), direction: Direction::Desc }],
    limit: Some(10),
    offset: Some(0),
});
let rows = txn.select(&query)?;
```

Rust also exposes `txn.select_distinct`, `txn.aggregate(&AggregateQuery { .. })` (count/sum/min/max/avg
with optional `group_by`/`having`), `txn.join(&JoinQuery { .. })` with `JoinKind::Inner`/`Left`/`Cross`,
and `txn.select_with(&ctes, &body)`. `Expr` carries `In`, `NotIn`, `Like`, `Contains`, `Not`,
`InSubquery`, `Exists`, and `NotExists`.

```python
# Python: object filters plus an order string.
rows = txn.select("orders", filter={"status": {"eq": "paid"}}, order="-id", limit=10, offset=0)
rows = txn.select("customers", filter={"email": {"like": "%@example.com"}}, distinct=True)

# Aggregates with group-by / having (use the agg() helper)
txn.aggregate(
    "orders",
    [kit.agg("count", "n")],
    group_by=["status"],
    having={"n": {"gte": 2}},
)

# Inner/left/cross joins; each row is keyed by table/alias (None for an unmatched left side)
txn.join("orders", [{"kind": "inner", "table": "customers",
                     "on": kit.on_eq("orders.customer_id", "customers.id")}])

# Uncorrelated subqueries and materialized CTEs
txn.select("customers", filter={"exists": {"table": "orders", "filter": {"status": {"eq": "pending"}}}})
```

### Cross-language predicate reference

| Operation | TypeScript | Rust | Python filter |
|---|---|---|---|
| Equal | `eq(col, val)` | `Expr::Eq(...)` | `{"col": {"eq": val}}` |
| Not equal | `ne(col, val)` | `Expr::Ne(...)` | `{"col": {"ne": val}}` |
| Greater than | `gt(col, val)` | `Expr::Gt(...)` | `{"col": {"gt": val}}` |
| Greater or equal | `gte(col, val)` | `Expr::Gte(...)` | `{"col": {"gte": val}}` |
| Less than | `lt(col, val)` | `Expr::Lt(...)` | `{"col": {"lt": val}}` |
| Less or equal | `lte(col, val)` | `Expr::Lte(...)` | `{"col": {"lte": val}}` |
| Null | `isNull(col)` | `Expr::IsNull(...)` | `{"col": {"is_null": true}}` |
| Not null | `isNotNull(col)` | `Expr::IsNotNull(...)` | `{"col": {"is_not_null": true}}` |
| In list | `inList(col, [...])` | `Expr::In(col, [...])` | `{"col": {"in": [...]}}` |
| Not in list | `notInList(col, [...])` | `Expr::NotIn(col, [...])` | `{"col": {"not_in": [...]}}` |
| Like | `like(col, "%x%")` | `Expr::Like(col, "%x%")` | `{"col": {"like": "%x%"}}` |
| Contains | `contains(col, "x")` | `Expr::Contains(col, "x")` | `{"col": {"contains": "x"}}` |
| In subquery | `inSubquery(col, sub)` | `Expr::InSubquery(col, Box<Select>)` | `{"col": {"in_subquery": {...}}}` |
| Exists / not exists | `exists(sub)` / `notExists(sub)` | `Expr::Exists(...)` / `Expr::NotExists(...)` | top-level `exists` / `not_exists` |
| And | `and(a, b)` | `Expr::And(vec![...])` | multiple keys, or top-level `and` |
| Or | `or(a, b)` | `Expr::Or(vec![...])` | top-level `or` |
| Not | `not(pred)` | `Expr::Not(Box::new(...))` | top-level `not` |

Raw escape hatches per language: TypeScript `db.nativeDb`, Rust `db.inner` (core `Database`), Python
`db._handle`. These bypass kit constraints.

## See also

- [Schema DSL](./schema.md) — the `customers` / `products` / `orders` / `order_items` schema used above.
- [Types](./types.md) — `Row<T>`, `Insert<T>`, and `Update<T>` inference.
- [Defaults & sequences](./defaults.md) — sequence-assigned ids and column defaults.
- [Constraints](./constraints.md) — the unique / foreign-key / check guards writes enforce.
- [Transactions](./transactions.md) — how writes commit and retry on conflict.
- [TypeScript](./typescript.md) · [Rust](./rust.md) · [Python](./python.md) — the language guides.
