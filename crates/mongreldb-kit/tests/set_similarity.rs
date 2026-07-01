//! Jaccard set-similarity search (the MinHash-style dedup/join primitive).

use mongreldb_kit::{Column, ColumnType, Database, Schema, Table};
use serde_json::json;
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    tempfile::tempdir().unwrap().keep()
}

fn schema() -> Schema {
    let mut tags = Column::new(2, "tags", ColumnType::Json);
    tags.nullable = true;
    Schema::new(vec![Table {
        id: 1,
        name: "docs".into(),
        columns: vec![Column::new(1, "id", ColumnType::Int64), tags],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap()
}

fn set_col(items: &[&str]) -> serde_json::Value {
    json!(serde_json::to_string(items).unwrap())
}

#[test]
fn set_similarity_ranks_by_jaccard() {
    let db = Database::create(&temp_dir(), schema()).unwrap();
    let mut tx = db.begin().unwrap();
    // Row 1 shares all of {a,b,c}; row 2 shares 2/4; row 3 shares nothing.
    tx.insert(
        "docs",
        [
            ("id".into(), json!(1)),
            ("tags".into(), set_col(&["a", "b", "c"])),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    tx.insert(
        "docs",
        [
            ("id".into(), json!(2)),
            ("tags".into(), set_col(&["a", "b", "x", "y"])),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    tx.insert(
        "docs",
        [("id".into(), json!(3)), ("tags".into(), set_col(&["z"]))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    tx.commit().unwrap();

    let query = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let hits = db.set_similarity("docs", "tags", &query, 10).unwrap();

    // Row 3 (no overlap) is excluded; rows are ranked by Jaccard descending.
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].row.values.get("id"), Some(&json!(1)));
    assert!((hits[0].similarity - 1.0).abs() < 1e-9); // {a,b,c} == {a,b,c}
    assert_eq!(hits[1].row.values.get("id"), Some(&json!(2)));
    // |{a,b}| / |{a,b,c,x,y}| = 2/5.
    assert!((hits[1].similarity - 0.4).abs() < 1e-9);

    // top-k truncation.
    let top1 = db.set_similarity("docs", "tags", &query, 1).unwrap();
    assert_eq!(top1.len(), 1);
    assert_eq!(top1[0].row.values.get("id"), Some(&json!(1)));

    assert!(db.set_similarity("docs", "missing", &query, 1).is_err());
}
