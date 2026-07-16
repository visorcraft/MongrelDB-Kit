#![cfg(unix)]

use mongreldb_kit::{Column, ColumnType, Database, Schema, Table};
use serde_json::json;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn setup_large_database() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "items".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "payload", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(dir.path(), schema).unwrap();
    let payload = "x".repeat(2_048);
    let rows = (0..1_000)
        .map(|id| {
            [
                ("id".to_string(), json!(id)),
                ("payload".to_string(), json!(payload)),
            ]
            .into_iter()
            .collect()
        })
        .collect::<Vec<_>>();
    db.transaction(1, |transaction| {
        transaction.insert_many("items", rows.clone())?;
        Ok(())
    })
    .unwrap();
    dir
}

#[test]
fn ctrl_c_during_stdout_write_exits_130() {
    let dir = setup_large_database();
    let mut child = Command::new(env!("CARGO_BIN_EXE_mongreldb-kit"))
        .args(["sql", dir.path().to_str().unwrap(), "SELECT * FROM items"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // The unread pipe fills after conversion, leaving the process inside the
    // final stdout write where the first SIGINT must restore normal CLI exit.
    thread::sleep(Duration::from_secs(1));
    assert!(child.try_wait().unwrap().is_none());
    let signal = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(signal.success());

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(130));
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("CLI did not exit after SIGINT during output");
        }
        thread::sleep(Duration::from_millis(20));
    }
}
