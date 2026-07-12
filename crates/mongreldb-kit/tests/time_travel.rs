//! MVCC time-travel: `rows_at_epoch` reads a table as of a past commit epoch.

use mongreldb_kit::{Column, ColumnType, Database, Schema, Table};
use serde_json::json;
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    tempfile::tempdir().unwrap().keep()
}

fn schema() -> Schema {
    Schema::new(vec![Table {
        id: 1,
        name: "t".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "val", ColumnType::Int64),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap()
}

#[test]
fn rows_at_epoch_reads_history() {
    let db = Database::create(&temp_dir(), schema()).unwrap();

    // v1: insert (id=1, val=10).
    let mut tx = db.begin().unwrap();
    tx.insert(
        "t",
        [("id".into(), json!(1)), ("val".into(), json!(10))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    tx.commit().unwrap();
    let e1 = db.snapshot_epoch();

    // v2: update val -> 20.
    let mut tx = db.begin().unwrap();
    tx.update_where(
        "t",
        Some(mongreldb_kit_core::query::Expr::Eq(
            Box::new(mongreldb_kit_core::query::Expr::Column("id".into())),
            Box::new(mongreldb_kit_core::query::Expr::Literal(
                mongreldb_kit_core::query::Literal::Int(1),
            )),
        )),
        [("val".to_string(), json!(20))].into_iter().collect(),
        vec![],
    )
    .unwrap();
    tx.commit().unwrap();
    let e2 = db.snapshot_epoch();
    assert!(e2 > e1);

    // As-of e1 sees the original value; as-of now sees the update.
    let past = db.rows_at_epoch("t", e1).unwrap();
    assert_eq!(past.len(), 1);
    assert_eq!(past[0].values.get("val"), Some(&json!(10)));

    let now = db.rows_at_epoch("t", e2).unwrap();
    assert_eq!(now[0].values.get("val"), Some(&json!(20)));

    // A future epoch is rejected.
    assert!(db.rows_at_epoch("t", e2 + 100).is_err());
}

#[test]
fn retention_round_trip_and_persistence() {
    let dir = temp_dir();
    let db = Database::create(&dir, schema()).unwrap();

    db.set_history_retention_epochs(512).unwrap();
    assert_eq!(db.history_retention_epochs(), 512);

    // Reopen — the setting persists.
    drop(db);
    let reopened = Database::open(&dir).unwrap();
    assert_eq!(reopened.history_retention_epochs(), 512);
}

#[test]
fn cannot_restore_lost_history() {
    let dir = temp_dir();
    let db = Database::create(&dir, schema()).unwrap();

    // Narrow window so old epochs are pruned.
    db.set_history_retention_epochs(1).unwrap();

    // Write two epochs.
    let mut tx = db.begin().unwrap();
    tx.insert("t", [("id".into(), json!(1)), ("val".into(), json!(10))].into_iter().collect()).unwrap();
    tx.commit().unwrap();
    let e1 = db.snapshot_epoch();

    let mut tx = db.begin().unwrap();
    tx.insert("t", [("id".into(), json!(2)), ("val".into(), json!(20))].into_iter().collect()).unwrap();
    tx.commit().unwrap();

    // e1 may have been pruned. Expanding the window must not restore it.
    db.set_history_retention_epochs(10_000).unwrap();
    let earliest = db.earliest_retained_epoch();
    assert!(
        earliest >= e1,
        "earliest retained epoch ({earliest}) must not move before the first write ({e1})"
    );
}
