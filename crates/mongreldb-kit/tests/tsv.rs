//! TSV import/export round-trips through defaults/validation and preserves
//! nulls, numbers, escaped text, and JSON-encoded complex values.

use mongreldb_kit::{Column, ColumnType, Database, Schema, Table};
use serde_json::{json, Map, Value};
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    tempfile::tempdir().unwrap().keep()
}

fn col(id: u32, name: &str, ty: ColumnType) -> Column {
    Column::new(id, name, ty)
}

fn nullable(mut c: Column) -> Column {
    c.nullable = true;
    c
}

fn row(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn schema() -> Schema {
    Schema::new(vec![Table {
        id: 1,
        name: "t".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            col(2, "name", ColumnType::Text),
            nullable(col(3, "score", ColumnType::Float64)),
            nullable(col(4, "tags", ColumnType::Json)),
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
fn tsv_export_import_round_trip() {
    let src = temp_dir();
    let db = Database::create(&src, schema()).unwrap();
    let mut tx = db.begin().unwrap();
    tx.insert(
        "t",
        row(&[
            ("id", json!(1)),
            ("name", json!("a\tb\nc")),
            ("score", json!(1.5)),
            ("tags", json!(["x", "y"])),
        ]),
    )
    .unwrap();
    tx.insert(
        "t",
        row(&[
            ("id", json!(2)),
            ("name", json!("plain")),
            ("score", Value::Null),
            ("tags", Value::Null),
        ]),
    )
    .unwrap();
    tx.commit().unwrap();

    let tsv = db.export_tsv("t").unwrap();
    // Header + 2 data rows.
    assert_eq!(tsv.lines().count(), 3);
    assert!(tsv
        .lines()
        .next()
        .unwrap()
        .starts_with("id\tname\tscore\ttags"));
    // The tab/newline inside "a\tb\nc" must be escaped, not real separators.
    assert!(tsv.contains("a\\tb\\nc"));

    // Import into a fresh db; every column must round-trip to the source db's
    // stored representation (nulls, escaped text, numbers, JSON alike).
    let dst = temp_dir();
    let db2 = Database::create(&dst, schema()).unwrap();
    let n = db2.import_tsv("t", &tsv).unwrap();
    assert_eq!(n, 2);

    let src_tx = db.begin().unwrap();
    let dst_tx = db2.begin().unwrap();
    for id in [1, 2] {
        let a = src_tx.get_by_pk("t", &json!(id)).unwrap().unwrap();
        let b = dst_tx.get_by_pk("t", &json!(id)).unwrap().unwrap();
        assert_eq!(a.values, b.values, "row {id} mismatch after TSV round-trip");
    }
    // Sanity: null really is null, escaped text really decoded.
    let r1 = dst_tx.get_by_pk("t", &json!(1)).unwrap().unwrap();
    assert_eq!(r1.values.get("name"), Some(&json!("a\tb\nc")));
    let r2 = dst_tx.get_by_pk("t", &json!(2)).unwrap().unwrap();
    assert_eq!(r2.values.get("score"), Some(&Value::Null));
}
