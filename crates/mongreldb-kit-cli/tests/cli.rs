use assert_cmd::Command;
use predicates::str::contains;
use std::fs;

fn bin() -> Command {
    Command::cargo_bin("mongreldb-kit").unwrap()
}

fn temp_db_path() -> std::path::PathBuf {
    tempfile::tempdir().unwrap().keep()
}

#[test]
fn init_creates_database_directory() {
    let path = temp_db_path();
    bin()
        .arg("init")
        .arg(&path)
        .assert()
        .success();
    assert!(path.join("kit_schema.json").exists());
}

#[test]
fn check_returns_ok_after_init() {
    let path = temp_db_path();
    bin()
        .arg("init")
        .arg(&path)
        .assert()
        .success();
    bin()
        .arg("check")
        .arg(&path)
        .assert()
        .success()
        .stdout(contains("OK"));
}

#[test]
fn schema_validate_accepts_valid_schema() {
    let dir = tempfile::tempdir().unwrap();
    let schema_path = dir.path().join("schema.json");
    fs::write(
        &schema_path,
        r#"{
            "tables": [
                {
                    "id": 1,
                    "name": "users",
                    "columns": [
                        {
                            "id": 1,
                            "name": "id",
                            "storage_type": "int64",
                            "application_type": "int64",
                            "nullable": false,
                            "primary_key": true,
                            "generated": false
                        }
                    ],
                    "primary_key": ["id"]
                }
            ]
        }"#,
    )
    .unwrap();

    bin()
        .arg("schema")
        .arg("validate")
        .arg(&schema_path)
        .assert()
        .success()
        .stdout(contains("OK"));
}

#[test]
fn migrate_status_shows_no_pending_after_init() {
    let path = temp_db_path();
    bin()
        .arg("init")
        .arg(&path)
        .assert()
        .success();
    bin()
        .arg("migrate")
        .arg("status")
        .arg(&path)
        .assert()
        .success()
        .stdout(contains("no migrations applied"));
}
