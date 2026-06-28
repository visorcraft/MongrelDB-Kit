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

## Error handling

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

## Running this example

```sh
cargo new kit-demo --bin
cd kit-demo
# Add mongreldb-kit and serde_json to Cargo.toml, then paste the code above.
cargo run
```
