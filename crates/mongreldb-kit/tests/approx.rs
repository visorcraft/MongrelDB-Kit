//! Reservoir-sampled approximate aggregates with confidence intervals.

use mongreldb_kit::{ApproxAggKind, Column, ColumnType, Database, Schema, Table};
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
fn approx_aggregate_estimates_with_ci() {
    let db = Database::create(&temp_dir(), schema()).unwrap();
    let n = 1000i64;
    let rows: Vec<_> = (1..=n)
        .map(|i| {
            [("id".to_string(), json!(i)), ("val".to_string(), json!(i))]
                .into_iter()
                .collect()
        })
        .collect();
    db.transaction(1, |tx| {
        tx.insert_many("t", rows.clone())?;
        Ok(())
    })
    .unwrap();

    // Approx COUNT over the whole table (no predicate ⇒ every sampled row
    // passes) estimates the population exactly, with a zero-or-narrow interval.
    let c = db
        .approx_aggregate("t", None, ApproxAggKind::Count, 1.96)
        .unwrap()
        .expect("reservoir populated");
    assert_eq!(c.n_population, n as u64);
    assert!(c.n_sample_live > 0);
    assert!(
        (c.point - n as f64).abs() < 1e-6,
        "point {} vs {n}",
        c.point
    );
    assert!(c.ci_low <= c.point && c.point <= c.ci_high);

    // Approx AVG of val (values 1..=1000, true mean 500.5) lands inside a wide
    // enough interval; just assert the estimate is a sane magnitude.
    let a = db
        .approx_aggregate("t", Some("val"), ApproxAggKind::Avg, 1.96)
        .unwrap()
        .expect("reservoir populated");
    assert!(
        a.point > 100.0 && a.point < 900.0,
        "avg estimate {}",
        a.point
    );

    // Sum/Avg without a column is an error.
    assert!(db
        .approx_aggregate("t", None, ApproxAggKind::Sum, 1.96)
        .is_err());
}
