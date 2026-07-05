# Rust Quickstart

This guide shows how to define a schema, run migrations, and perform CRUD with the `mongreldb-kit` crate.

## Add the dependency

```toml
[dependencies]
mongreldb-kit = "0.1"
serde_json = "1"
```

## Complete example

```rust
use mongreldb_kit::{
    Column, ColumnType, Database, DefaultKind, ForeignKey, ForeignKeyAction, Index, Migration,
    MigrationOp, Schema, Table, UniqueConstraint,
};
use serde_json::{json, Map};
use std::path::Path;

fn users_table() -> Table {
    Table {
        id: 1,
        name: "users".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "email", ColumnType::Text),
            Column {
                nullable: true,
                ..Column::new(3, "name", ColumnType::Text)
            },
        ],
        primary_key: vec!["id".into()],
        indexes: vec![Index {
            name: "uq_user_email".into(),
            columns: vec!["email".into()],
            unique: true,
        }],
        foreign_keys: vec![],
        unique_constraints: vec![UniqueConstraint {
            name: "uq_user_email".into(),
            columns: vec!["email".into()],
        }],
        check_constraints: vec![],
    }
}

fn posts_table() -> Table {
    let mut published = Column::new(5, "published", ColumnType::Bool);
    published.default = Some(mongreldb_kit::DefaultKind::Static(json!(false)));

    Table {
        id: 2,
        name: "posts".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "user_id", ColumnType::Int64),
            Column::new(3, "title", ColumnType::Text),
            Column {
                nullable: true,
                ..Column::new(4, "body", ColumnType::Text)
            },
            published,
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![ForeignKey {
            name: "fk_posts_user".into(),
            columns: vec!["user_id".into()],
            references_table: "users".into(),
            references_columns: vec!["id".into()],
            on_delete: ForeignKeyAction::Cascade,
        }],
        unique_constraints: vec![],
        check_constraints: vec![],
    }
}

fn schema() -> Schema {
    Schema::new(vec![users_table(), posts_table()]).unwrap()
}

fn main() -> mongreldb_kit::Result<()> {
    let dir = std::env::temp_dir().join("kit-rust-demo");
    let _ = std::fs::remove_dir_all(&dir);

    // Create the database.
    let mut db = Database::create(&dir, schema())?;

    // Run migrations.
    let migrations = vec![
        Migration {
            version: 1,
            name: "initial".into(),
            ops: vec![
                MigrationOp::CreateTable { name: "users".into() },
                MigrationOp::CreateTable { name: "posts".into() },
            ],
        },
    ];
    mongreldb_kit::migrate(&mut db, &migrations)?;

    // Insert users.
    let mut txn = db.begin()?;
    let mut alice = Map::new();
    alice.insert("id".into(), json!(1));
    alice.insert("email".into(), json!("alice@example.com"));
    alice.insert("name".into(), json!("Alice"));
    txn.insert("users", alice)?;

    let mut bob = Map::new();
    bob.insert("id".into(), json!(2));
    bob.insert("email".into(), json!("bob@example.com"));
    txn.insert("users", bob)?;
    txn.commit()?;

    // Insert a post.
    let mut txn = db.begin()?;
    let mut post = Map::new();
    post.insert("id".into(), json!(1));
    post.insert("user_id".into(), json!(1));
    post.insert("title".into(), json!("Hello Kit"));
    post.insert("body".into(), json!("First post."));
    txn.insert("posts", post)?;
    txn.commit()?;

    // Query posts by user, ordered by id descending.
    let txn = db.begin()?;
    let query = mongreldb_kit::Query::Select(mongreldb_kit::Select {
        table: "posts".into(),
        columns: vec![
            mongreldb_kit::Expr::Column("id".into()),
            mongreldb_kit::Expr::Column("title".into()),
        ],
        filter: Some(mongreldb_kit::Expr::Eq(
            Box::new(mongreldb_kit::Expr::Column("user_id".into())),
            Box::new(mongreldb_kit::Expr::Literal(mongreldb_kit::Literal::Int(1))),
        )),
        order_by: vec![mongreldb_kit::OrderBy {
            expr: mongreldb_kit::Expr::Column("id".into()),
            direction: mongreldb_kit::Direction::Desc,
        }],
        limit: Some(10),
        offset: None,
    });
    let rows = txn.select(&query)?;
    for row in &rows {
        println!("{:?}", row.values);
    }

    // Update the post.
    let mut txn = db.begin()?;
    let mut patch = Map::new();
    patch.insert("published".into(), json!(true));
    txn.update("posts", &json!(1), patch)?;
    txn.commit()?;

    // Deleting Alice cascades to her posts.
    let mut txn = db.begin()?;
    txn.delete("users", &json!(1))?;
    txn.commit()?;

    Ok(())
}
```

## Schema construction

A [`Table`](https://docs.rs/mongreldb-kit/latest/mongreldb_kit/schema/struct.Table.html) is built from [`Column`](https://docs.rs/mongreldb-kit/latest/mongreldb_kit/schema/struct.Column.html) values and assembled into a validated [`Schema`](https://docs.rs/mongreldb-kit/latest/mongreldb_kit/schema/struct.Schema.html) with `Schema::new`.

```rust
let mut col = Column::new(1, "name", ColumnType::Text);
col.nullable = true;
col.default = Some(DefaultKind::Uuid);
```

## Transactions

```rust
let mut txn = db.begin()?;
txn.insert("users", row)?;
let row = txn.get_by_pk("users", &json!(1))?;
let rows = txn.select(&query)?;
txn.update("users", &json!(1), patch)?;
txn.delete("users", &json!(1))?;
txn.commit()?;
```

Use `txn.rollback()` to abort.

### Batch insert

`txn.insert_many(table, rows)` stages a whole `Vec` of rows in the open transaction and returns
the stored `Vec<Row>` in order — the same per-row defaults, validation, sequence ids, and guards
as `insert`, but staged in one pass so a single `commit()` writes the batch. For a single-column
primary key it preloads the existing keys once, so the per-row duplicate check stays O(1).

```rust
let mut a = Map::new();
a.insert("sku".into(), json!("A-1"));
a.insert("name".into(), json!("Anvil"));
let mut b = Map::new();
b.insert("sku".into(), json!("B-1"));
b.insert("name".into(), json!("Bucket"));

let mut txn = db.begin()?;
let rows = txn.insert_many("products", vec![a, b])?; // returns Vec<Row> in order
txn.commit()?; // all-or-nothing: any row erroring rolls the batch back
```

## Query AST

The kit exposes a language-neutral AST in `mongreldb_kit_core::query`:

```rust
use mongreldb_kit::{Expr, Literal, Query, Select, Direction, OrderBy};

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
    offset: None,
});
```

`Expr` also carries the full predicate set: `Like`, `Contains`, `BytesPrefix`
(anchored prefix on a bitmap-indexed `Bytes` column — exact pushdown, no
residual), `IsNull`/`IsNotNull`, `In`/`NotIn`, `InSubquery`, `Exists`/
`NotExists`, and the logical combinators. The engine pushes `BytesPrefix` down
to a bitmap key-prefix scan when the column has a bitmap index:

```rust
// Find events whose `key` (a Bytes column with a bitmap index) starts with the
// bytes of "user:". Resolves to an exact bitmap-prefix lookup — no full scan.
let query = Query::Select(Select {
    table: "events".into(),
    columns: vec![],
    filter: Some(Expr::BytesPrefix(
        Box::new(Expr::Column("key".into())),
        "user:".into(),
    )),
    order_by: vec![],
    limit: None,
    offset: None,
});
let matched: Vec<Row> = txn.select(&query)?;
```

## Migrations

```rust
let migrations = vec![
    Migration {
        version: 1,
        name: "initial".into(),
        ops: vec![MigrationOp::CreateTable { name: "users".into() }],
    },
];
mongreldb_kit::migrate(&mut db, &migrations)?;
```

The Rust runner executes every migration op directly: table/column/index/
constraint changes run against the core engine, while procedure/trigger ops call
the engine DDL, and SQL-backed ops — `CreateView`/`ReplaceView`/`DropView`,
`CreateVirtualTable`/`DropVirtualTable`, and `RawSql` — run through the embedded
`MongrelSession` (see [Embedded SQL surface](#embedded-sql-surface-and-maintenance)
below). There is no longer any "requires a SQL-capable Kit surface" caveat — the
Rust Kit *is* that surface.

## Triggers and remote SQL

Engine-side triggers use `TriggerSpec`, a JSON wrapper that keeps the Kit API
aligned with the engine trigger schema:

```rust
use mongreldb_kit::TriggerSpec;
use serde_json::json;

let spec = TriggerSpec::new(json!({
    "name": "users_ai",
    "target": { "kind": "table", "name": "users" },
    "timing": "after",
    "event": "insert",
    "program": { "steps": [] }
}));

db.create_trigger(&spec)?;
db.replace_trigger(&spec)?;
db.drop_trigger("users_ai")?;
```

With the `remote` feature, `RemoteDatabase::sql_rows` runs daemon SQL and
`VirtualTableSpec` generates module-backed virtual-table DDL:

```rust
use mongreldb_kit::VirtualTableSpec;

remote.create_virtual_table(&VirtualTableSpec::new(
    "docs_fts",
    "fts_docs",
    ["content=docs"],
))?;
remote.drop_virtual_table("docs_fts")?;
```

## Embedded SQL surface and maintenance

`Database::sql`, `sql_arrow`, and `sql_rows` run statements through the kit's
embedded `MongrelSession` (the engine's DataFusion SQL frontend). The session is
held for the database's lifetime, so session-scoped objects (views, prepared
statements, the result cache) persist across calls — mirroring a long-lived
database connection. After a migration that creates or drops tables, call
`refresh_sql_session` so the session sees the new table set.

```rust
use mongreldb_kit::{Database, ViewSpec};
use mongreldb_kit_core::MigrationOp;

// Read path: returns Arrow RecordBatch, raw IPC bytes, or JSON-style rows.
let batches = db.sql("SELECT id, email FROM users ORDER BY id")?;
let ipc: Vec<u8> = db.sql_arrow("SELECT id FROM users ORDER BY id")?;
let rows = db.sql_rows("SELECT id, email FROM users ORDER BY id")?;

// DDL/DML return empty; views live in the session.
db.sql("CREATE VIEW active AS SELECT id FROM users WHERE active = TRUE")?;
db.sql_rows("SELECT * FROM active")?; // queries the view

// Maintenance (the engine's ANALYZE / VACUUM equivalents).
db.analyze()?;          // ensure_indexes_complete() on every table
let reclaimed = db.vacuum()?; // compact_all() + gc()

// Rename a table (engine + kit schema catalog + persisted).
db.rename_table("widgets", "things")?;

// SQL views (session-scoped — live in the kit's long-lived MongrelSession).
use mongreldb_kit::ViewSpec;
db.create_view(&ViewSpec::new("active", "SELECT id FROM users WHERE active = TRUE"))?;
db.drop_view("active")?;

// Reserve the next AUTO_INCREMENT id (parity with TS reserveAutoIncSync).
let next_id: Option<i64> = db.reserve_auto_inc("orders")?;
```

> Writes through `sql()` bypass kit-level constraints (defaults, enums, min/max,
> length, regex, triggers) — use the `Transaction` API for constrained writes.
> The engine's own declarative constraints (unique, FK, check) still apply.

## Sequences and defaults

A column whose `DefaultKind::Sequence(name)` default is set is auto-assigned from a named sequence
when the inserted row omits it. Sequences are **1-based** (the first value is `1`, matching SQL
`AUTO_INCREMENT`). You can also draw values directly:

```rust
let first = db.allocate_sequence("orders_id_seq", 1)?; // 1 on a fresh sequence
let block = db.allocate_sequence("orders_id_seq", 10)?; // reserve 10, returns the first
```

`DefaultKind` also covers `Static(value)`, `Now`, `Uuid`, and `CustomName(name)`.

## Error handling

`KitError` is a flat enum of stable categories: `Validation`, `Duplicate`, `ForeignKey`, `Restrict`,
`TriggerValidation`, `Migration`, `Conflict`, `Storage`, and `Integrity`. Match on the variant you handle:

```rust
use mongreldb_kit::KitError;

match db.begin() {
    Ok(mut txn) => {
        if let Err(KitError::Duplicate(_)) = txn.insert("users", row) {
            println!("duplicate");
        }
    }
    Err(e) => eprintln!("{e}"),
}
```

`Conflict` is the retryable category; see [Errors](./errors.md) for the cross-language mapping.

## Running this example

```sh
cargo new kit-demo --bin
cd kit-demo
# Add mongreldb-kit and serde_json to Cargo.toml, then paste the code above.
cargo run
```

## See also

- [Query builder](./query-builder.md) — the query model the `Query`/`Select`/`Expr` AST serializes.
- [Constraints](./constraints.md) · [Errors](./errors.md) — enforcement and the `KitError` categories.
- [Migrations](./migrations.md) — migration ops and the runner.
- [TypeScript](./typescript.md) · [Python](./python.md) — the sibling language surfaces.
