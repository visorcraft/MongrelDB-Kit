//! P6: bulk-write guard batching. `insert_many` preloads the committed
//! unique-guard rows once per batch and caches FK parent-existence probes,
//! so the per-row constraint checks are O(1) against in-memory sets instead
//! of re-scanning the guard tables for every row.

use mongreldb_kit::{
    AggregateQuery, Aggregate as Agg, AggFunc, Column, ColumnType, Database, ForeignKey, KitError,
    Schema, Table, UniqueConstraint,
};
use serde_json::{json, Map, Value};
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    tempfile::tempdir().unwrap().keep()
}

fn col(id: u32, name: &str, ty: ColumnType) -> Column {
    Column::new(id, name, ty)
}

fn row(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn db_with_users_and_orders() -> Database {
    let dir = temp_dir();
    let schema = Schema::new(vec![
        Table {
            id: 1,
            name: "users".into(),
            columns: vec![
                col(1, "id", ColumnType::Int64),
                col(2, "email", ColumnType::Text),
            ],
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![UniqueConstraint {
                name: "uq_email".into(),
                columns: vec!["email".into()],
            }],
            check_constraints: vec![],
        },
        Table {
            id: 2,
            name: "orders".into(),
            columns: vec![
                col(1, "id", ColumnType::Int64),
                col(2, "user_id", ColumnType::Int64),
            ],
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![ForeignKey {
                name: "fk_user".into(),
                columns: vec!["user_id".into()],
                references_table: "users".into(),
                references_columns: vec!["id".into()],
                on_delete: mongreldb_kit::ForeignKeyAction::Restrict,
            }],
            unique_constraints: vec![],
            check_constraints: vec![],
        },
    ])
    .unwrap();
    Database::create(&dir, schema).unwrap()
}

/// A bulk insert with a unique constraint must detect a duplicate against a
/// committed row (the preloaded guard set must include pre-existing guards).
#[test]
fn bulk_insert_detects_committed_unique_conflict() {
    let db = db_with_users_and_orders();

    let mut txn = db.begin().unwrap();
    txn.insert("users", row(&[("id", json!(1)), ("email", json!("a@x.com"))]))
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let err = txn
        .insert_many(
            "users",
            vec![
                row(&[("id", json!(2)), ("email", json!("b@x.com"))]),
                row(&[("id", json!(3)), ("email", json!("a@x.com"))]), // conflicts with committed
            ],
        )
        .unwrap_err();
    assert!(matches!(err, KitError::Duplicate(_)), "{err:?}");
}

/// A bulk insert must detect an intra-batch unique-key duplicate.
#[test]
fn bulk_insert_detects_intra_batch_unique_duplicate() {
    let db = db_with_users_and_orders();

    let mut txn = db.begin().unwrap();
    let err = txn
        .insert_many(
            "users",
            vec![
                row(&[("id", json!(1)), ("email", json!("dup@x.com"))]),
                row(&[("id", json!(2)), ("email", json!("dup@x.com"))]),
            ],
        )
        .unwrap_err();
    assert!(matches!(err, KitError::Duplicate(_)), "{err:?}");
}

/// A clean bulk insert must succeed and all rows must be visible after commit.
#[test]
fn bulk_insert_with_unique_constraint_succeeds() {
    let db = db_with_users_and_orders();

    let mut txn = db.begin().unwrap();
    let rows: Vec<_> = (1..=500)
        .map(|i| row(&[("id", json!(i)), ("email", json!(format!("u{i}@x.com")))]))
        .collect();
    let inserted = txn.insert_many("users", rows).unwrap();
    assert_eq!(inserted.len(), 500);
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let q = AggregateQuery {
        table: "users".into(),
        filter: None,
        group_by: vec![],
        aggregates: vec![Agg {
            func: AggFunc::Count,
            column: None,
            alias: "c".into(),
            distinct: false,
        }],
        having: None,
    };
    let rows = txn.aggregate(&q).unwrap();
    let count = rows[0].values.get("c").unwrap().as_i64().unwrap();
    assert_eq!(count, 500);
}

/// A bulk insert with a FK must validate parent existence (the cached probe
/// must still reject a missing parent).
#[test]
fn bulk_insert_fk_rejects_missing_parent() {
    let db = db_with_users_and_orders();

    let mut txn = db.begin().unwrap();
    txn.insert("users", row(&[("id", json!(1)), ("email", json!("a@x.com"))]))
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let err = txn
        .insert_many(
            "orders",
            vec![
                row(&[("id", json!(1)), ("user_id", json!(1))]),  // ok
                row(&[("id", json!(2)), ("user_id", json!(99))]), // missing parent
            ],
        )
        .unwrap_err();
    assert!(matches!(err, KitError::ForeignKey(_)), "{err:?}");
}

/// Repeated parent ids in a bulk batch must all succeed (FK cache returns
/// the cached "exists" for repeated parents).
#[test]
fn bulk_insert_fk_repeated_parent_succeeds() {
    let db = db_with_users_and_orders();

    let mut txn = db.begin().unwrap();
    txn.insert("users", row(&[("id", json!(1)), ("email", json!("a@x.com"))]))
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let rows: Vec<_> = (1..=300)
        .map(|i| row(&[("id", json!(i)), ("user_id", json!(1))]))
        .collect();
    let inserted = txn.insert_many("orders", rows).unwrap();
    assert_eq!(inserted.len(), 300);
    txn.commit().unwrap();
}
