use mongreldb_kit::{
    CancelOutcome, Column, ColumnType, Database, KitError, QueryId, Schema, SqlOptions, Table,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

fn database() -> Arc<Database> {
    let directory = tempfile::tempdir().unwrap().keep();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "items".into(),
        columns: vec![Column::new(1, "id", ColumnType::Int64)],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    Arc::new(Database::create(&directory, schema).unwrap())
}

#[test]
fn query_handle_supports_queue_cancel_deadline_conflict_and_reuse() {
    let db = database();
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let entered_once = Arc::new(AtomicBool::new(false));
    let hook_entered_once = Arc::clone(&entered_once);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    db.set_sql_test_hook(Some(Arc::new(move |point| {
        if point == mongreldb_query::SqlTestHookPoint::Planning
            && !hook_entered_once.swap(true, Ordering::AcqRel)
        {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })))
    .unwrap();

    let first_id: QueryId = "11112222333344445555666677778888".parse().unwrap();
    let first = db
        .start_sql(
            "SELECT 1",
            SqlOptions {
                query_id: Some(first_id),
                timeout: Some(Duration::from_secs(5)),
            },
        )
        .unwrap();
    assert_eq!(first.id(), first_id);
    entered_rx.recv().unwrap();

    let duplicate = db.start_sql(
        "SELECT 2",
        SqlOptions {
            query_id: Some(first_id),
            timeout: None,
        },
    );
    assert!(matches!(duplicate, Err(KitError::QueryConflict(_))));

    let cancel_id: QueryId = "aaaabbbbccccddddeeeeffff00001111".parse().unwrap();
    let cancelled = db
        .start_sql(
            "SELECT 2",
            SqlOptions {
                query_id: Some(cancel_id),
                timeout: Some(Duration::from_secs(5)),
            },
        )
        .unwrap();
    assert_eq!(cancelled.cancel(), CancelOutcome::Accepted);
    assert!(matches!(cancelled.wait(), Err(KitError::Cancelled { .. })));

    let timed_out = db
        .start_sql(
            "SELECT 3",
            SqlOptions {
                query_id: None,
                timeout: Some(Duration::from_millis(10)),
            },
        )
        .unwrap();
    assert!(matches!(
        timed_out.wait(),
        Err(KitError::DeadlineExceeded { .. })
    ));

    barrier.wait();
    assert_eq!(first.wait().unwrap()[0].num_rows(), 1);
    db.set_sql_test_hook(None).unwrap();
    assert_eq!(db.sql_rows("SELECT 4 AS value").unwrap()[0]["value"], 4);
}

#[test]
fn row_serialization_remains_cancellable() {
    let db = database();
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    db.set_sql_test_hook(Some(Arc::new(move |point| {
        if point == mongreldb_query::SqlTestHookPoint::BeforeSerializationBatch {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })))
    .unwrap();
    let query_id: QueryId = "99990000111122223333444455556666".parse().unwrap();
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || {
        worker_db.sql_rows_with_options(
            "SELECT 1 AS value",
            SqlOptions {
                query_id: Some(query_id),
                timeout: Some(Duration::from_secs(5)),
            },
        )
    });
    entered_rx.recv().unwrap();
    assert_eq!(db.cancel_sql(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.join().unwrap(),
        Err(KitError::Cancelled { .. })
    ));
    db.set_sql_test_hook(None).unwrap();
    assert_eq!(db.sql_rows("SELECT 2 AS value").unwrap()[0]["value"], 2);
}
