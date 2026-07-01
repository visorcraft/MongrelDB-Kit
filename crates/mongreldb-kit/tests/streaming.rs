//! `scan_batched` streams a table in bounded row batches.

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
            Column::new(2, "name", ColumnType::Text),
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
fn scan_batched_streams_every_row_in_bounded_chunks() {
    let db = Database::create(&temp_dir(), schema()).unwrap();
    let n = 2500i64;
    let rows: Vec<_> = (1..=n)
        .map(|i| {
            [
                ("id".to_string(), json!(i)),
                ("name".to_string(), json!(format!("r{i}"))),
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

    let batch_size = 1000usize;
    let mut seen = 0usize;
    let mut ids = Vec::new();
    let mut max_batch = 0usize;
    db.scan_batched("t", batch_size, |batch| {
        max_batch = max_batch.max(batch.len());
        for m in batch {
            ids.push(m.get("id").and_then(|v| v.as_i64()).unwrap());
            // The paired text column round-trips.
            let id = m.get("id").and_then(|v| v.as_i64()).unwrap();
            assert_eq!(
                m.get("name").and_then(|v| v.as_str()),
                Some(format!("r{id}").as_str())
            );
        }
        seen += batch.len();
        Ok(())
    })
    .unwrap();

    assert_eq!(seen, n as usize, "streamed row count");
    assert!(
        max_batch <= batch_size,
        "batch {max_batch} exceeded {batch_size}"
    );
    ids.sort_unstable();
    assert_eq!(
        ids,
        (1..=n).collect::<Vec<_>>(),
        "every id streamed exactly once"
    );
}
