# Query Builder

MongrelDB Kit provides a query builder for common CRUD. The supported feature set is identical in concept across languages but exposed in language-idiomatic APIs.

## TypeScript

### Select

```ts
import { eq, gt, gte, lt, lte, ne, isNull, isNotNull, inList, and, or, asc, desc } from '@mongreldb/kit';

// All rows
const all = db.selectFrom(users).executeSync();

// Filtered
const active = db.selectFrom(users).where(eq(users.email, 'alice@example.com')).executeSync();

// Combined predicates
const filtered = db
  .selectFrom(posts)
  .where(and(gt(posts.id, 0n), eq(posts.published, true)))
  .executeSync();

// Ordering, limit, offset
const page = db
  .selectFrom(posts)
  .orderBy(desc(posts.created_at))
  .limit(10)
  .offset(20)
  .executeSync();

// Projection
const titles = db.selectFrom(posts).select([posts.title, posts.id]).executeSync();

// Count
const count = db.selectFrom(posts).selectCount().executeSync();
```

### Insert

```ts
const row = db.insertInto(users).values({ email: 'alice@example.com', name: 'Alice' }).executeSync();
```

### Update

```ts
const updated = db.updateTable(users).set({ name: 'Alice Smith' }).where(eq(users.id, 1n)).executeSync();
```

### Delete

```ts
const deleted = db.deleteFrom(users).where(eq(users.id, 1n)).executeSync();
```

### Aggregates

Whole-table aggregates return a scalar. `selectCount` returns a `bigint`; `selectSum`
preserves the column type (`bigint` for integer columns, `number` for floats);
`selectAvg` returns `number | null`; `selectMin`/`selectMax` return the column value
or `null` for an empty set.

```ts
const n = db.selectFrom(orders).selectCount().executeSync();
const total = db.selectFrom(orders).where(eq(orders.status, 'paid')).selectSum(orders.amount).executeSync();
const lo = db.selectFrom(orders).selectMin(orders.amount).executeSync();
const hi = db.selectFrom(orders).selectMax(orders.amount).executeSync();
const mean = db.selectFrom(orders).selectAvg(orders.amount).executeSync();
```

### Distinct

```ts
const statuses = db.selectFrom(orders).select([orders.status]).distinct().executeSync();
```

### Text and set predicates

```ts
import { like, contains, notInList, not, inList } from '@mongreldb/kit';

db.selectFrom(users).where(like(users.email, '%@example.com')).executeSync();   // SQL LIKE pattern
db.selectFrom(users).where(contains(users.email, 'bob')).executeSync();          // substring match
db.selectFrom(users).where(inList(users.id, [1n, 2n])).executeSync();
db.selectFrom(users).where(notInList(users.id, [1n, 2n])).executeSync();
db.selectFrom(users).where(not(eq(users.role, 'admin'))).executeSync();
```

### Joins

`innerJoin`, `leftJoin`, and `crossJoin` produce rows shaped as `{ [tableName]: row }`,
with the right side `null` for an unmatched `LEFT JOIN`. The `on` predicate is a plain
JS function evaluated over the combined row.

```ts
const rows = db
  .selectFrom(users)
  .innerJoin(orders, (r) => r.orders?.userId === r.users?.id)
  .where((r) => r.users?.id === 1n)
  .executeSync();
// rows: Array<{ users: Row; orders: Row | null }>

const everyPair = db.selectFrom(tags).crossJoin(orders).executeSync();
```

### Group by and having

`groupBy(...columns).aggregate({...}).having(...)` computes one row per group made of
the group-key columns plus each named aggregate. `having` filters the assembled group
rows.

```ts
import { count, sum, min, max, avg } from '@mongreldb/kit';

const byStatus = db
  .selectFrom(orders)
  .groupBy(orders.status)
  .aggregate({ n: count(), total: sum(orders.amount), lo: min(orders.amount), hi: max(orders.amount), mean: avg(orders.amount) })
  .having((g) => (g.total as bigint) >= 100n)
  .executeSync();
// byStatus: Array<{ status: string; n: bigint; total: bigint; lo: ...; hi: ...; mean: number | null }>
```

### Subqueries

`inSubquery`, `exists`, and `notExists` take a select builder. They are **uncorrelated**:
the subquery is evaluated once (not per outer row). An `inSubquery` builder must project
exactly one column (or fall back to the single-column primary key).

```ts
import { inSubquery, exists, notExists } from '@mongreldb/kit';

const bigSpenders = db.selectFrom(orders).where(gt(orders.amount, 80n)).select([orders.userId]);
db.selectFrom(users).where(inSubquery(users.id, bigSpenders)).executeSync();

const pending = db.selectFrom(orders).where(eq(orders.status, 'pending'));
db.selectFrom(users).where(exists(pending)).executeSync();
db.selectFrom(users).where(notExists(pending)).executeSync();
```

### Common table expressions (CTEs)

`db.with(name, builder)` runs `builder` eagerly, materializes its rows in memory, and
returns a scope whose `selectFrom(name)` reads them as if they were a table. Chain
`.with(...)` to declare more.

```ts
const scope = db.with('big_orders', db.selectFrom(orders).where(gt(orders.amount, 80n)));
const rows = scope.selectFrom('big_orders').orderBy(asc(orders.id)).executeSync();

const paidCount = db
  .with('paid', db.selectFrom(orders).where(eq(orders.status, 'paid')))
  .selectFrom('paid')
  .selectCount()
  .executeSync();
```

### Ceilings

These advanced features trade scale for portability and run entirely in JS:

- **In-memory, no pushdown.** Joins, group-by, aggregates, and CTEs fully scan their
  inputs and evaluate predicates in JavaScript; there is no index or predicate pushdown
  into the storage engine. They are intended for modest working sets.
- **Uncorrelated subqueries only.** `inSubquery`/`exists`/`notExists` evaluate their
  subquery once; they cannot reference the outer row.
- **CTEs are materialized, not lazy or recursive.** Each `with` is computed up front and
  cached for the life of the scope.

## Rust

Rust exposes a language-neutral AST from `mongreldb_kit_core::query`, re-exported by `mongreldb-kit`.

### Select

```rust
use mongreldb_kit::{Query, Select, Expr, Literal, OrderBy, Direction};

let query = Query::Select(Select {
    table: "posts".into(),
    columns: vec![Expr::Column("title".into())],
    filter: Some(Expr::And(vec![
        Expr::Eq(
            Box::new(Expr::Column("published".into())),
            Box::new(Expr::Literal(Literal::Bool(true))),
        ),
        Expr::Gt(
            Box::new(Expr::Column("id".into())),
            Box::new(Expr::Literal(Literal::Int(0))),
        ),
    ])),
    order_by: vec![OrderBy {
        expr: Expr::Column("id".into()),
        direction: Direction::Desc,
    }],
    limit: Some(10),
    offset: Some(0),
});

let rows = txn.select(&query)?;
```

### Insert

```rust
use serde_json::{json, Map};

let mut row = Map::new();
row.insert("id".into(), json!(1));
row.insert("title".into(), json!("Hello"));
let inserted = txn.insert("posts", row)?;
```

### Update

```rust
let mut patch = Map::new();
patch.insert("published".into(), json!(true));
let updated = txn.update("posts", &json!(1), patch)?;
```

### Delete

```rust
txn.delete("posts", &json!(1))?;
```

### Aggregates, joins, and CTEs

The same advanced surface is available through the AST and `Transaction`:

- `txn.select_distinct(&query)` — `SELECT DISTINCT`.
- `txn.aggregate(&AggregateQuery { table, aggregates, filter, group_by, having, .. })` —
  `count`/`sum`/`min`/`max`/`avg` with optional `group_by` and `having`.
- `txn.join(&JoinQuery { .. })` with `JoinKind::Inner`/`Left`/`Cross`; each `JoinRow`
  maps an alias to its matched row (`None` for an unmatched left side).
- `txn.select_with(&ctes, &body)` — materialize `Cte`s, then run a `Select` whose `table`
  may name a CTE.

`Expr` also carries `In`, `NotIn`, `Like`, `Contains`, `Not`, `InSubquery`, `Exists`, and
`NotExists`. Subqueries are uncorrelated and joins/aggregates/CTEs evaluate in memory.

## Python

Python uses a simple object filter and an order string.

### Select

```python
# All rows
rows = txn.select("users")

# Filter with operators
rows = txn.select("posts", filter={"published": {"eq": True}, "user_id": {"gt": 0}})

# Ordering, limit, offset
rows = txn.select("posts", order="-created_at,+id", limit=10, offset=20)
```

### Insert

```python
row = txn.insert("posts", {"id": 1, "user_id": 1, "title": "Hello"})
```

### Update

```python
row = txn.update("posts", 1, {"published": True})
```

### Delete

```python
txn.delete("posts", 1)
```

### Aggregates, joins, and CTEs

```python
# Distinct + richer filters (like / contains / in / not_in / is_null / and / or / not)
txn.select("users", filter={"email": {"like": "%@example.com"}}, distinct=True)

# Aggregates with group-by and having (use the agg() helper)
txn.aggregate(
    "orders",
    [kit.agg("count", "n"), kit.agg("sum", "total", "amount")],
    group_by=["status"],
    having={"total": {"gte": 100}},
)

# Inner/left/cross joins; each row is keyed by table/alias (None for an unmatched left side)
txn.join("users", [{"kind": "inner", "table": "orders", "on": kit.on_eq("orders.user_id", "users.id")}])

# Uncorrelated subqueries and materialized CTEs
txn.select("users", filter={"exists": {"table": "orders", "filter": {"status": {"eq": "pending"}}}})
txn.select("paid", ctes=[{"name": "paid", "table": "orders", "filter": {"status": {"eq": "paid"}}}])
```

## Predicate reference

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
| And | `and(a, b)` | `Expr::And(vec![...])` | implicit with multiple keys, or top-level `and` |
| Or | `or(a, b)` | `Expr::Or(vec![...])` | top-level `or` |
| Not | `not(pred)` | `Expr::Not(Box::new(...))` | top-level `not` |

## Raw escape hatches

When the query builder does not expose an operation, drop down to the native database. These paths bypass kit constraints.

- TypeScript: `db.nativeDb`
- Rust: `db.inner` (core `Database`)
- Python: `db._handle`
