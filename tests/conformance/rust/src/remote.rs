//! Remote conformance mode (PLAN.md #3).
//!
//! Boots a real `mongreldb-server` daemon (engine v0.19.0+) on an ephemeral port
//! and drives the Kit's `RemoteDatabase` against it, asserting that the typed
//! write path commits and that the engine's declarative constraints are enforced
//! authoritatively server-side. This is the cross-repo proof that the typed
//! remote surface matches the daemon's contract.
//!
//! The conformance runner enables `mongreldb-kit`'s `remote` feature, so
//! `RemoteDatabase` is always available here.

use std::sync::Arc;
use std::thread;

use mongreldb_core::constraint::{
    CheckConstraint, CheckExpr, FkAction, ForeignKey, TableConstraints, UniqueConstraint,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, Value};
use mongreldb_kit::{KitError, RemoteDatabase};
use mongreldb_server::build_app;
use serde_json::{json, Map, Value as JVal};

type KitResult<T> = std::result::Result<T, KitError>;

struct Server {
    base_url: String,
    _dir: tempfile::TempDir,
    _join: thread::JoinHandle<()>,
}

fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,
    }
}

fn users_schema() -> Schema {
    let mut cons = TableConstraints::default();
    cons.uniques.push(UniqueConstraint {
        id: 1,
        name: "users_email_unique".into(),
        columns: vec![1],
    });
    cons.checks.push(CheckConstraint {
        id: 2,
        name: "age_nonneg".into(),
        expr: CheckExpr::Or(
            Box::new(CheckExpr::IsNull(2)),
            Box::new(CheckExpr::Ge(
                Box::new(CheckExpr::Col(2)),
                Box::new(CheckExpr::Lit(Value::Int64(0))),
            )),
        ),
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(
                0,
                "id",
                TypeId::Int64,
                ColumnFlags::empty()
                    .with(ColumnFlags::PRIMARY_KEY)
                    .with(ColumnFlags::AUTO_INCREMENT),
            ),
            col(
                1,
                "email",
                TypeId::Bytes,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
            col(
                2,
                "age",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
        clustered: false,
    }
}

fn orders_schema() -> Schema {
    let mut cons = TableConstraints::default();
    cons.foreign_keys.push(ForeignKey {
        id: 3,
        name: "orders_uid_fk".into(),
        columns: vec![11],
        ref_table: "users".into(),
        ref_columns: vec![0],
        on_delete: FkAction::Restrict,
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(
                10,
                "oid",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            ),
            col(
                11,
                "uid",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
        clustered: false,
    }
}

fn boot_server() -> Result<Server, String> {
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let dir_path = dir.path().to_path_buf();
    let db = Database::create(&dir_path).map_err(|e| format!("db create: {e}"))?;
    db.create_table("users", users_schema())
        .map_err(|e| format!("create users: {e}"))?;
    db.create_table("orders", orders_schema())
        .map_err(|e| format!("create orders: {e}"))?;
    let app = build_app(Arc::new(db));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime: {e}"))?;
    let listener = rt
        .block_on(async { tokio::net::TcpListener::bind(("127.0.0.1", 0)).await })
        .map_err(|e| format!("bind: {e}"))?;
    let addr = listener.local_addr().map_err(|e| format!("addr: {e}"))?;
    let join = thread::spawn(move || {
        rt.block_on(async {
            let _ = axum::serve(listener, app).await;
        });
    });
    Ok(Server {
        base_url: format!("http://{addr}"),
        _dir: dir,
        _join: join,
    })
}

fn row(pairs: &[(&str, JVal)]) -> Map<String, JVal> {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    m
}

/// Alias used in later scenarios (kept for readability alongside `row`).
fn row_pairs(pairs: &[(&str, JVal)]) -> Map<String, JVal> {
    row(pairs)
}

/// Run the remote conformance scenarios. Returns `Err(message)` on the first
/// mismatch so `main.rs` / the `#[test]` can report it.
pub fn run_remote() -> Result<(), String> {
    inner().map_err(|e| e.to_string())
}

fn inner() -> KitResult<()> {
    let srv = boot_server().map_err(KitError::Storage)?;
    let mut db = RemoteDatabase::connect(&srv.base_url)?;

    // Schema loaded from the daemon.
    let names = db.table_names();
    if !names.contains(&"users".to_string()) || !names.contains(&"orders".to_string()) {
        return Err(KitError::Storage(format!(
            "remote schema missing tables: {names:?}"
        )));
    }

    // 1. insert_returning commits; the post-image row carries the auto-inc id.
    let batch = db
        .begin()
        .insert_returning("users", row(&[("email", json!("a@x")), ("age", json!(30))]))?
        .commit()?;
    let put_row = batch.results[0]
        .row_ref()
        .ok_or_else(|| KitError::Storage("no returning row on insert_returning".into()))?;
    let id = put_row
        .get("id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| KitError::Storage("no id in returned row".into()))?;
    if id < 1 {
        return Err(KitError::Storage(format!(
            "auto_inc id should be >= 1, got {id}"
        )));
    }

    // 2. duplicate unique → DuplicateError.
    match db
        .begin()
        .insert("users", row(&[("email", json!("a@x"))]))?
        .commit()
    {
        Err(KitError::Duplicate(msg)) if msg.contains("users_email_unique") => {}
        other => {
            return Err(KitError::Storage(format!(
                "expected DuplicateError, got {other:?}"
            )));
        }
    }

    // 3. CHECK violation → Validation error.
    match db
        .begin()
        .insert("users", row(&[("email", json!("b@x")), ("age", json!(-5))]))?
        .commit()
    {
        Err(KitError::Validation(msg)) if msg.contains("age_nonneg") => {}
        other => {
            return Err(KitError::Storage(format!(
                "expected CHECK Validation error, got {other:?}"
            )));
        }
    }

    // 4. FK violation: order referencing a nonexistent user → ForeignKeyError.
    match db
        .begin()
        .insert("orders", row(&[("oid", json!(1)), ("uid", json!(9999))]))?
        .commit()
    {
        Err(KitError::ForeignKey(_)) => {}
        other => {
            return Err(KitError::Storage(format!(
                "expected ForeignKeyError, got {other:?}"
            )));
        }
    }

    // 5. Valid FK: create the user, then an order referencing it → commits.
    db.begin()
        .insert(
            "users",
            row(&[
                ("id", json!(5)),
                ("email", json!("u@x")),
                ("age", json!(21)),
            ]),
        )?
        .commit()?;
    db.begin()
        .insert("orders", row(&[("oid", json!(50)), ("uid", json!(5))]))?
        .commit()?;

    // 6. delete_by_pk of the referenced user → ForeignKeyError (RESTRICT).
    match db.begin().delete_by_pk("users", json!(5))?.commit() {
        Err(KitError::ForeignKey(_)) => {}
        other => {
            return Err(KitError::Storage(format!(
                "expected FK restrict on delete, got {other:?}"
            )));
        }
    }

    // 7. idempotency: replay returns the cached epoch.
    let r1 = db
        .begin()
        .with_idempotency_key("k-remote-1")
        .insert(
            "users",
            row(&[("email", json!("idem@x")), ("age", json!(1))]),
        )?
        .commit()?;
    let r2 = db
        .begin()
        .with_idempotency_key("k-remote-1")
        .insert(
            "users",
            row(&[("email", json!("idem@x")), ("age", json!(1))]),
        )?
        .commit()?;
    if r1.epoch != r2.epoch {
        return Err(KitError::Storage(format!(
            "idempotency mismatch: {} vs {}",
            r1.epoch, r2.epoch
        )));
    }

    // 8. native typed query: PK lookup returns the row with its physical row id.
    let rows = db.query("users", vec![json!({"pk": {"value": id}})], None, None)?;
    if rows.len() != 1 {
        return Err(KitError::Storage(format!(
            "pk query returned {} rows",
            rows.len()
        )));
    }
    if rows[0].row_id.parse::<u64>().is_err() {
        return Err(KitError::Storage("query row_id not numeric".into()));
    }
    if rows[0].values.get("email") != Some(&json!("a@x")) {
        return Err(KitError::Storage(format!(
            "query row email mismatch: {:?}",
            rows[0].values.get("email")
        )));
    }

    // 9. remote DDL: provision a constraint-bearing table entirely over HTTP,
    //    then exercise its unique constraint through /kit/txn.
    let body = json!({
        "name": "tags",
        "columns": [
            {"id": 0, "name": "id", "ty": "int64", "primary_key": true, "auto_increment": true},
            {"id": 1, "name": "label", "ty": "bytes", "nullable": true},
        ],
        "constraints": {
            "uniques": [{"id": 5, "name": "tag_label_unique", "columns": [1]}]
        }
    });
    let tid = db.create_table(&body)?;
    if tid == 0 {
        return Err(KitError::Storage("create_table returned table_id 0".into()));
    }
    if !db.table_names().contains(&"tags".to_string()) {
        return Err(KitError::Storage(
            "create_table did not refresh schema cache".into(),
        ));
    }
    db.begin()
        .insert("tags", row_pairs(&[("label", json!("alpha"))]))?
        .commit()?;
    // Duplicate label on the remotely-created table → DuplicateError.
    match db
        .begin()
        .insert("tags", row_pairs(&[("label", json!("alpha"))]))?
        .commit()
    {
        Err(KitError::Duplicate(msg)) if msg.contains("tag_label_unique") => {}
        other => {
            return Err(KitError::Storage(format!(
                "expected DuplicateError on remote-created table, got {other:?}"
            )));
        }
    }

    Ok(())
}
