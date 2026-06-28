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

## Predicate reference

| Operation | TypeScript | Rust | Python filter |
|---|---|---|---|
| Equal | `eq(col, val)` | `Expr::Eq(...)` | `{"col": {"eq": val}}` |
| Not equal | `ne(col, val)` | `Expr::Ne(...)` | `{"col": {"ne": val}}` |
| Greater than | `gt(col, val)` | `Expr::Gt(...)` | `{"col": {"gt": val}}` |
| Greater or equal | `gte(col, val)` | `Expr::Gte(...)` | `{"col": {"gte": val}}` |
| Less than | `lt(col, val)` | `Expr::Lt(...)` | `{"col": {"lt": val}}` |
| Less or equal | `lte(col, val)` | `Expr::Lte(...)` | `{"col": {"lte": val}}` |
| Null | `isNull(col)` | `Expr::IsNull(...)` | not supported |
| Not null | `isNotNull(col)` | `Expr::IsNotNull(...)` | not supported |
| In list | `inList(col, [...])` | `Expr::In(col, [...])` | not supported |
| Like | not exposed | `Expr::Like(col, "%x%")` | not supported |
| And | `and(a, b)` | `Expr::And(vec![...])` | implicit with multiple keys |
| Or | `or(a, b)` | `Expr::Or(vec![...])` | not supported |
| Not | not exposed | `Expr::Not(Box::new(...))` | not supported |

## Raw escape hatches

When the query builder does not expose an operation, drop down to the native database. These paths bypass kit constraints.

- TypeScript: `db.nativeDb`
- Rust: `db.inner` (core `Database`)
- Python: `db._handle`
