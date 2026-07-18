# Rust Quickstart

This guide shows how to define a schema, run migrations, and perform CRUD with the `mongreldb-kit` crate.

## Add the dependency

```toml
[dependencies]
mongreldb-kit = "0.59.1"
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
the stored `Vec<Row>` in order - the same per-row defaults, validation, sequence ids, and guards
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
(anchored prefix on a bitmap-indexed `Bytes` column - exact pushdown, no
residual), `IsNull`/`IsNotNull`, `In`/`NotIn`, `InSubquery`, `Exists`/
`NotExists`, and the logical combinators. The engine pushes `BytesPrefix` down
to a bitmap key-prefix scan when the column has a bitmap index:

```rust
// Find events whose `key` (a Bytes column with a bitmap index) starts with the
// bytes of "user:". Resolves to an exact bitmap-prefix lookup - no full scan.
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
the engine DDL, and SQL-backed ops - `CreateView`/`ReplaceView`/`DropView`,
`CreateVirtualTable`/`DropVirtualTable`, and `RawSql` - run through the embedded
`MongrelSession` (see [Embedded SQL surface](#embedded-sql-surface-and-maintenance)
below). There is no longer any "requires a SQL-capable Kit surface" caveat - the
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

With the `remote` feature, `RemoteDatabase::sql_rows` runs daemon SQL. Configure
Bearer or Basic authentication once so capabilities, SQL, cancellation, status,
and every other route share the same credentials:

```rust
use mongreldb_kit::{RemoteAuth, RemoteDatabase, RemoteOptions, SecretString};

let remote = RemoteDatabase::connect_with_options(
    "https://db.example.com",
    RemoteOptions {
        auth: Some(RemoteAuth::Bearer(SecretString::from(
            std::env::var("MONGRELDB_TOKEN")?,
        ))),
        ..RemoteOptions::default()
    },
)?;
```

`RemoteSqlQueryHandle::cancel` returns `RemoteCancelOutcome`, distinguishing
accepted, already-cancelling, too-late, already-finished, not-found, and
pre-cancelled results. `status` returns the durable statement, commit epoch,
terminal error, and retryability fields. If transport fails after submission,
the client checks status before cancelling; it returns `KitError::CommitOutcome`
when the server proves a commit and `KitError::OutcomeUnknown` when no terminal
outcome can be established.

The same typed outcome contract covers remote `/kit/txn`, procedure, and
trigger writes. A `COMMIT_OUTCOME` response remains `KitError::CommitOutcome`
with `committed` and exact `last_commit_epoch` available through
`KitError::query_outcome()`, plus `retryable` through
`KitError::query_metadata()`.
`QUERY_OUTCOME_UNKNOWN` remains `KitError::OutcomeUnknown`; do not replay it.

`RemoteQueryStatus::durable_commit_state()` returns `None` for that unknown
case. Its commit and counter fields remain `Option` values. Never treat `None`
as `false` or zero.

Remote SQL always requires cancellation capability version 2. This lets the
client assign a query ID even for default `sql_rows` calls and recover a durable
status if Arrow or JSON decoding fails after a commit.

Use owner-bound, bounded pagination for large read-only results:

```rust
use mongreldb_kit::RemoteSqlPaginationOptions;

let page = remote.sql_page(
    "SELECT id, title FROM documents ORDER BY id",
    RemoteSqlPaginationOptions {
        page_size_rows: 500,
        projection: vec!["id".into(), "title".into()],
        query_id: None,
        timeout: None,
        max_page_bytes: Some(1_000_000),
        max_page_tokens: Some(100_000),
        max_output_rows: None,
        max_output_bytes: None,
    },
)?;
if let Some(cursor) = page.next_cursor.as_deref() {
    let next = remote.continue_sql_page(cursor)?;
}
```

For a retry-safe single write, call `execute_idempotent_sql` with
`RemoteIdempotentSqlOptions`. Reuse the same idempotency key after transport
loss. The returned `RemoteSqlWriteReceipt` contains the original query ID,
replay state, committed statement count, and exact `last_commit_epoch`.
It also preserves the first and last committed statement indexes.

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
statements, the result cache) persist across calls - mirroring a long-lived
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

// SQL views (session-scoped - live in the kit's long-lived MongrelSession).
use mongreldb_kit::ViewSpec;
db.create_view(&ViewSpec::new("active", "SELECT id FROM users WHERE active = TRUE"))?;
db.drop_view("active")?;

// Reserve the next AUTO_INCREMENT id (parity with TS reserveAutoIncSync).
let next_id: Option<i64> = db.reserve_auto_inc("orders")?;
```

> Writes through `sql()` bypass kit-level constraints (defaults, enums, min/max,
> length, regex, triggers) - use the `Transaction` API for constrained writes.
> The engine's own declarative constraints (unique, FK, check) still apply.

### Advanced SQL (recursive CTEs, windows, regex, catalog, ATTACH, SAVEPOINTs)

The embedded DataFusion 54 session supports the full SQL stdlib:

```rust
// Recursive CTE.
db.sql_rows("WITH RECURSIVE tree AS (
    SELECT id, 0 AS depth FROM nodes WHERE parent IS NULL
    UNION ALL
    SELECT n.id, t.depth + 1 FROM nodes n JOIN tree t ON n.parent = t.id
) SELECT id, depth FROM tree ORDER BY id")?;

// Window function.
db.sql_rows("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users")?;

// Regex.
db.sql_rows("SELECT id FROM users WHERE regexp('^admin.*', name) = 1")?;

// Catalog.
db.sql_rows("SELECT type, name FROM information_schema.tables ORDER BY name")?;

// ATTACH (cross-database).
db.sql("ATTACH './other' AS other")?;
db.sql_rows("SELECT id FROM other_items")?;

// SAVEPOINTs.
db.sql("BEGIN")?;
db.sql("INSERT INTO logs VALUES (1, 'hello')")?;
db.sql("SAVEPOINT sp1")?;
db.sql("INSERT INTO logs VALUES (2, 'world')")?;
db.sql("ROLLBACK TO sp1")?; // discards 'world'
db.sql("COMMIT")?;
```

Savepoints require an explicit transaction. `ROLLBACK TO name` keeps the target
active, removes savepoints created after it, and can recover an aborted
transaction so work can continue.

## Storage tuning & introspection

The Kit surfaces the engine's power-user knobs for production sizing and
observability. Database-wide tunables (`set_spill_threshold`,
`set_recursive_triggers`, `trigger_config` / `set_trigger_config`) are atomic
`&self`; per-table tunables and introspection go through the engine's per-table
`Mutex`:

```rust
// Database-wide.
db.set_spill_threshold(1_000_000);
db.set_recursive_triggers(true);
let cfg = db.trigger_config(); // TriggerConfig { recursive_triggers, max_depth, max_loop_iterations }
db.set_trigger_config(TriggerConfig {
    recursive_triggers: true, max_depth: 16, max_loop_iterations: 5000
})?;

// Per-table tuning.
db.set_table_compaction_zstd_level("widgets", 3)?;
db.set_table_result_cache_max_bytes("widgets", 64_000_000)?;
db.set_table_index_build_policy("widgets", IndexBuildPolicy::Eager)?;

// Per-table introspection (read-only).
let runs = db.table_run_count("widgets")?;          // compaction target: 1
let stats = db.table_page_cache_stats("widgets")?;  // CacheStats { hits, misses, try_lock_misses }
let memtable = db.table_memtable_len("widgets")?;
```

All methods are also reachable via the `Database::raw()` escape hatch
(`db.raw().table(name)?.lock()`) for the full engine surface.

If a second process may briefly hold the database lock, opt in to retrying opens:

```rust
use std::path::Path;
use mongreldb_kit::{Database, OpenOptions};

let opts = OpenOptions::new().with_lock_timeout_ms(5_000);
let db = Database::open_with_options(Path::new("./store.kitdb"), opts)?;
```

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

`KitError` is a flat enum of stable categories, including `Validation`,
`Duplicate`, `ForeignKey`, `Restrict`, `TriggerValidation`, `Migration`,
`Conflict`, `Storage`, `DatabaseLocked`, and `Integrity`. Match on the variant
you handle. `DatabaseLocked` identifies a database already owned by another
live handle or process without parsing its message.

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

## Users, roles & permissions

The Kit forwards the engine's catalog-stored auth model - Argon2id-hashed
users, roles that bundle permissions, and `GRANT`/`REVOKE` table-level
access control. The `Permission` enum is re-exported from the kit crate so
you do not need a direct `mongreldb-core` dependency.

```rust
use mongreldb_kit::{Database, Permission};

let db = Database::open("./store.kitdb")?;

// Users
db.create_user("alice", "s3cret-pw")?;
db.alter_user_password("alice", "new-pw")?;
assert!(db.verify_user("alice", "new-pw")?.is_some());
db.set_user_admin("alice", true)?; // admin bypasses all permission checks
let names: Vec<String> = db.users(); // ["alice"]

// Roles + permissions
db.create_role("analyst")?;
db.grant_permission("analyst", Permission::Select { table: "orders".into() })?;
db.grant_permission("analyst", Permission::Insert { table: "orders".into() })?;
db.grant_role("alice", "analyst")?;
let roles: Vec<String> = db.roles(); // ["analyst"]

// Reverse
db.revoke_role("alice", "analyst")?;
db.revoke_permission("analyst", Permission::Insert { table: "orders".into() })?;
db.drop_role("analyst")?;
db.drop_user("alice")?;
```

The full model (including SQL DDL like `CREATE USER` / `GRANT` and the HTTP
daemon's Bearer + Basic auth modes) is documented in the engine
[Users, Roles & Permissions](https://github.com/visorcraft/MongrelDB/blob/master/docs/14-auth.md)
guide. The Kit CLI exposes the same operations as
[`user` and `role` subcommands](./cli.md#user--manage-catalog-users).

### Credential enforcement

A database with `require_auth` set rejects every open that does not supply
valid credentials. Use the credentialed constructors to create or open such a
database, and `enable_auth`/`disable_auth` to flip the flag in code.

```rust
use mongreldb_kit::Database;

// Create a new database with require_auth on, bootstrapping the first admin.
let db = Database::create_with_credentials(
    "./store.kitdb",
    schema,
    "alice",
    "s3cret-pw",
)?;

// Open an existing require_auth database.
let db = Database::open_with_credentials("./store.kitdb", "alice", "s3cret-pw")?;

assert!(db.require_auth_enabled());

// Turn require_auth on for an existing credentialless database.
db.enable_auth("alice", "s3cret-pw")?;

// Recovery: clear require_auth (needs an open handle).
db.disable_auth()?;
```

```rust
// Encrypted + credentialed: both layers in one call.
let db = Database::create_encrypted_with_credentials(
    "./store.kitdb", schema, "passphrase", "admin", "s3cret-pw",
)?;

// Long-lived handles call refresh_principal after a REVOKE to pick up
// permission changes made by other handles.
db.refresh_principal()?;
```

The full model and recovery flow are documented in the engine
[credential enforcement guide](https://github.com/visorcraft/MongrelDB/blob/master/docs/15-credential-enforcement.md).

## Running this example

```sh
cargo new kit-demo --bin
cd kit-demo
# Add mongreldb-kit and serde_json to Cargo.toml, then paste the code above.
cargo run
```

## See also

- [Query builder](./query-builder.md) - the query model the `Query`/`Select`/`Expr` AST serializes.
- [Constraints](./constraints.md) · [Errors](./errors.md) - enforcement and the `KitError` categories.
- [Migrations](./migrations.md) - migration ops and the runner.
- [TypeScript](./typescript.md) · [Python](./python.md) - the sibling language surfaces.

## History retention and time-travel reads

MongrelDB retains a configurable window of committed epochs for MVCC
time-travel reads. Embedded databases initially keep only the latest epoch.
The daemon defaults to 1024 epochs unless
`MONGRELDB_HISTORY_RETENTION_EPOCHS` overrides it.

### Embedded mode

```rust
use mongreldb_kit::Database;

let db = Database::open(&path, schema()).unwrap();

// Raise the window before writing if you need to read past the default.
db.set_history_retention_epochs(10_000).unwrap();

// Current window and earliest retained epoch.
let window = db.history_retention_epochs();
let earliest = db.earliest_retained_epoch();

// Insert and capture the epoch.
let mut tx = db.begin().unwrap();
tx.insert("t", [("id".into(), json!(1))].into_iter().collect()).unwrap();
tx.commit().unwrap();
let e1 = db.snapshot_epoch();

// Read a past snapshot — `rows_at_epoch` returns rows as of that epoch.
let past = db.rows_at_epoch("t", e1).unwrap();
```

Increasing the window cannot restore history that was already pruned, so
`earliest_retained_epoch` never moves backward.

### Remote mode

When using `RemoteDatabase` (the `remote` cargo feature), the same three
controls are forwarded to the daemon's `GET`/`PUT /history/retention` endpoints:

```rust
use mongreldb_kit::RemoteDatabase;

let remote = RemoteDatabase::connect("http://127.0.0.1:8453").unwrap();
remote.set_history_retention_epochs(10_000).unwrap();
let window = remote.history_retention_epochs().unwrap();
let earliest = remote.earliest_retained_epoch().unwrap();
```

For SQL time-travel, use `AS OF EPOCH` in a query string:

```rust
let rows = remote.sql_rows(&format!("SELECT * FROM t AS OF EPOCH {e1}")).unwrap();
```
