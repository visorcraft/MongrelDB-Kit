//! Incrementally-maintained aggregates: exact values, with a delta-merge fast
//! path once data has spilled to immutable runs.

use mongreldb_kit::{Column, ColumnType, Database, IncrementalAggKind, Schema, Table};
use mongreldb_kit_core::query::{Expr, Literal};
use serde_json::{json, Map, Value};
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
            Column::new(2, "amount", ColumnType::Int64),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap()
}

fn insert_range(db: &Database, ids: std::ops::RangeInclusive<i64>) {
    let rows: Vec<Map<String, Value>> = ids
        .map(|i| {
            [
                ("id".to_string(), json!(i)),
                ("amount".to_string(), json!(i)),
            ]
            .into_iter()
            .collect()
        })
        .collect();
    db.transaction(1, |tx| {
        tx.insert_many("t", rows.clone())?;
        Ok(())
    })
    .unwrap();
}

/// Force every `flush` to spill the mutable-run tier to an immutable run, so the
/// incremental fast path (which requires an empty in-memory tier) is eligible on
/// small test data. In production this happens automatically once the tier
/// crosses its byte threshold.
fn force_spill_on_flush(db: &Database) {
    db.raw()
        .table("t")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);
}

#[test]
fn incremental_aggregates_are_exact_and_use_the_delta_path() {
    let db = Database::create(&temp_dir(), schema()).unwrap();
    force_spill_on_flush(&db);

    insert_range(&db, 1..=100);
    db.flush().unwrap();

    // Cold cache ⇒ full recompute, exact.
    let c0 = db
        .incremental_aggregate("t", None, IncrementalAggKind::Count, None)
        .unwrap();
    assert_eq!(c0.value, json!(100));
    assert!(!c0.incremental);
    assert_eq!(
        db.incremental_aggregate("t", Some("amount"), IncrementalAggKind::Sum, None)
            .unwrap()
            .value,
        json!(5050) // 1+..+100
    );

    // Append 50 more, flush, re-read: the count folds in only the delta.
    insert_range(&db, 101..=150);
    db.flush().unwrap();

    let c1 = db
        .incremental_aggregate("t", None, IncrementalAggKind::Count, None)
        .unwrap();
    assert_eq!(c1.value, json!(150));
    assert!(c1.incremental, "expected the delta-merge fast path");
    assert!(c1.delta_rows >= 50, "delta covered the new rows");

    // A same-epoch repeat is served from the cached state with no delta.
    let c1b = db
        .incremental_aggregate("t", None, IncrementalAggKind::Count, None)
        .unwrap();
    assert!(c1b.incremental);
    assert_eq!(c1b.delta_rows, 0);

    // Sum stays integer; avg is a float; max stays integer — all exact.
    assert_eq!(
        db.incremental_aggregate("t", Some("amount"), IncrementalAggKind::Sum, None)
            .unwrap()
            .value,
        json!(150 * 151 / 2) // 11325
    );
    assert_eq!(
        db.incremental_aggregate("t", Some("amount"), IncrementalAggKind::Avg, None)
            .unwrap()
            .value,
        json!(75.5)
    );
    assert_eq!(
        db.incremental_aggregate("t", Some("amount"), IncrementalAggKind::Max, None)
            .unwrap()
            .value,
        json!(150)
    );
}

#[test]
fn incremental_aggregate_matches_a_full_recompute_after_a_delete() {
    let db = Database::create(&temp_dir(), schema()).unwrap();
    force_spill_on_flush(&db);
    insert_range(&db, 1..=100);
    db.flush().unwrap();
    // Warm the cache.
    let _ = db.incremental_aggregate("t", None, IncrementalAggKind::Count, None);

    // Delete a row; the next read must fall back to a full (exact) recompute.
    db.transaction(1, |tx| {
        tx.delete("t", &json!(1))?;
        Ok(())
    })
    .unwrap();
    db.flush().unwrap();

    let c = db
        .incremental_aggregate("t", None, IncrementalAggKind::Count, None)
        .unwrap();
    assert_eq!(c.value, json!(99));
    assert!(!c.incremental, "deletes disable the incremental fast path");
}

#[test]
fn incremental_aggregate_filter_and_errors() {
    let db = Database::create(&temp_dir(), schema()).unwrap();
    insert_range(&db, 1..=10);
    db.flush().unwrap();

    // Exact range filter: amount > 7 ⇒ {8,9,10}.
    let filter = Expr::Gt(
        Box::new(Expr::Column("amount".into())),
        Box::new(Expr::Literal(Literal::Int(7))),
    );
    assert_eq!(
        db.incremental_aggregate("t", None, IncrementalAggKind::Count, Some(&filter))
            .unwrap()
            .value,
        json!(3)
    );

    // Sum without a column errors; unknown table errors.
    assert!(db
        .incremental_aggregate("t", None, IncrementalAggKind::Sum, None)
        .is_err());
    assert!(db
        .incremental_aggregate("missing", None, IncrementalAggKind::Count, None)
        .is_err());

    // A non-exact (residual) filter is rejected rather than silently wrong.
    let contains = Expr::Contains(Box::new(Expr::Column("id".into())), "5".into());
    assert!(db
        .incremental_aggregate("t", None, IncrementalAggKind::Count, Some(&contains))
        .is_err());
}
