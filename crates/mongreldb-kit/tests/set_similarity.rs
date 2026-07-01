//! Jaccard set-similarity search (the MinHash-style dedup/join primitive).

use mongreldb_kit::{Column, ColumnType, Database, Index, IndexKind, Schema, Table};
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

fn indexed_schema() -> Schema {
    let mut tags = Column::new(2, "tags", ColumnType::Json);
    tags.nullable = true;
    Schema::new(vec![Table {
        id: 1,
        name: "docs".into(),
        columns: vec![Column::new(1, "id", ColumnType::Int64), tags],
        primary_key: vec!["id".into()],
        indexes: vec![Index {
            name: "tags_mh".into(),
            columns: vec!["tags".into()],
            unique: false,
            kind: IndexKind::MinHash,
        }],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap()
}

#[test]
fn set_similarity_uses_the_minhash_index() {
    // Larger sets so LSH banding is stable; the exact re-verification then
    // yields exact scores for the recalled candidates.
    let identical = &["a", "b", "c", "d", "e", "f", "g", "h"];
    let near = &["a", "b", "c", "d", "e", "f", "g", "x"]; // Jaccard 7/9
    let disjoint = &["p", "q", "r", "s", "t", "u", "v", "w"];

    let db = Database::create(&temp_dir(), indexed_schema()).unwrap();
    let mut tx = db.begin().unwrap();
    for (id, set) in [(1, identical), (2, near), (3, disjoint)] {
        tx.insert(
            "docs",
            [("id".into(), json!(id)), ("tags".into(), set_col(set))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let query: Vec<String> = identical.iter().map(|s| s.to_string()).collect();
    let hits = db.set_similarity("docs", "tags", &query, 10).unwrap();
    let ids: Vec<i64> = hits
        .iter()
        .filter_map(|h| h.row.values.get("id").and_then(|v| v.as_i64()))
        .collect();

    // The identical and high-Jaccard sets are recalled; the disjoint one is not.
    assert!(ids.contains(&1), "identical set found: {ids:?}");
    assert!(ids.contains(&2), "high-Jaccard set found: {ids:?}");
    assert!(!ids.contains(&3), "disjoint set excluded: {ids:?}");
    // Re-verification gives the exact score: identical ranks first at 1.0.
    assert_eq!(hits[0].row.values.get("id"), Some(&json!(1)));
    assert!((hits[0].similarity - 1.0).abs() < 1e-9);
    // near: |{a..g}| = 7 shared, union {a..h, x} = 9 ⇒ 7/9.
    let near_hit = hits
        .iter()
        .find(|h| h.row.values.get("id") == Some(&json!(2)));
    assert!((near_hit.unwrap().similarity - 7.0 / 9.0).abs() < 1e-9);
}
