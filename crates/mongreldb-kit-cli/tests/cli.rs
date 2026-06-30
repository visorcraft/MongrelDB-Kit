use assert_cmd::Command;
use mongreldb_kit::{Database, Query, Schema, Select};
use predicates::str::contains;
use serde_json::{json, Map};
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
    bin().arg("init").arg(&path).assert().success();
    assert!(path.join("kit_schema.json").exists());
}

#[test]
fn check_returns_ok_after_init() {
    let path = temp_db_path();
    bin().arg("init").arg(&path).assert().success();
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
    bin().arg("init").arg(&path).assert().success();
    bin()
        .arg("migrate")
        .arg("status")
        .arg(&path)
        .assert()
        .success()
        .stdout(contains("no migrations applied"));
}

#[test]
fn truncate_removes_table_rows() {
    let path = temp_db_path();
    let schema: Schema = serde_json::from_str(USERS_SCHEMA).unwrap();
    let db = Database::create(&path, schema).unwrap();

    let mut row = Map::new();
    row.insert("id".into(), json!(1));
    row.insert("email".into(), json!("alice@example.com"));
    let mut txn = db.begin().unwrap();
    txn.insert("user_accounts", row).unwrap();
    txn.commit().unwrap();

    bin()
        .arg("truncate")
        .arg(&path)
        .arg("user_accounts")
        .assert()
        .success()
        .stdout(contains("table user_accounts truncated"));

    let db = Database::open(&path).unwrap();
    let txn = db.begin().unwrap();
    let rows = txn
        .select(&Query::Select(Select {
            table: "user_accounts".into(),
            columns: vec![],
            filter: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }))
        .unwrap();
    assert!(rows.is_empty());
}

const USERS_SCHEMA: &str = r#"{
    "tables": [
        {
            "id": 1,
            "name": "user_accounts",
            "columns": [
                {"id":1,"name":"id","storage_type":"int64","application_type":"int64","nullable":false,"primary_key":true,"generated":false},
                {"id":2,"name":"email","storage_type":"text","application_type":"text","nullable":true,"primary_key":false,"generated":false},
                {"id":3,"name":"created_at","storage_type":"text","application_type":"text","nullable":false,"primary_key":false,"generated":false,"default":"now"}
            ],
            "primary_key": ["id"]
        }
    ]
}"#;

fn write_schema(contents: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("schema.json");
    fs::write(&path, contents).unwrap();
    (dir, path)
}

#[test]
fn generate_types_ts_emits_interfaces() {
    let (_dir, schema) = write_schema(USERS_SCHEMA);
    bin()
        .arg("generate")
        .arg("types")
        .arg(&schema)
        .arg("--lang")
        .arg("ts")
        .assert()
        .success()
        .stdout(contains("export interface UserAccountsRow"))
        .stdout(contains("id: bigint;"))
        .stdout(contains("email: string | null;"))
        .stdout(contains("export interface UserAccountsInsert"))
        .stdout(contains("email?: string | null;"))
        .stdout(contains("export interface UserAccountsUpdate"));
}

#[test]
fn generate_types_rust_emits_structs() {
    let (_dir, schema) = write_schema(USERS_SCHEMA);
    bin()
        .arg("generate")
        .arg("types")
        .arg(&schema)
        .arg("--lang")
        .arg("rust")
        .assert()
        .success()
        .stdout(contains("pub struct UserAccountsRow"))
        .stdout(contains("pub id: i64,"))
        .stdout(contains("pub email: Option<String>,"))
        .stdout(contains("pub struct UserAccountsUpdate"));
}

#[test]
fn generate_types_python_emits_dataclasses() {
    let (_dir, schema) = write_schema(USERS_SCHEMA);
    bin()
        .arg("generate")
        .arg("types")
        .arg(&schema)
        .arg("--lang")
        .arg("python")
        .assert()
        .success()
        .stdout(contains("@dataclass"))
        .stdout(contains("class UserAccountsRow:"))
        .stdout(contains("id: int"))
        .stdout(contains("email: Optional[str]"));
}

#[test]
fn generate_types_rejects_unknown_lang() {
    let (_dir, schema) = write_schema(USERS_SCHEMA);
    bin()
        .arg("generate")
        .arg("types")
        .arg(&schema)
        .arg("--lang")
        .arg("java")
        .assert()
        .failure()
        .stderr(contains("unsupported lang"));
}

#[test]
fn schema_validate_rejects_duplicate_column_ids() {
    let (_dir, schema) = write_schema(
        r#"{
            "tables": [
                {
                    "id": 1,
                    "name": "users",
                    "columns": [
                        {"id":1,"name":"id","storage_type":"int64","application_type":"int64","nullable":false,"primary_key":true,"generated":false},
                        {"id":1,"name":"email","storage_type":"text","application_type":"text","nullable":false,"primary_key":false,"generated":false}
                    ],
                    "primary_key": ["id"]
                }
            ]
        }"#,
    );
    bin()
        .arg("schema")
        .arg("validate")
        .arg(&schema)
        .assert()
        .failure()
        .stderr(contains("duplicate/reused column id 1"));
}

/// Seed a database's stored schema by overwriting the sidecar after `init`, so
/// `diff` has a non-empty catalog to compare against.
fn db_with_stored_schema(stored: &str) -> std::path::PathBuf {
    let path = temp_db_path();
    bin().arg("init").arg(&path).assert().success();
    fs::write(path.join("kit_schema.json"), stored).unwrap();
    path
}

#[test]
fn diff_reports_type_and_constraint_changes() {
    let path = db_with_stored_schema(
        r#"{"tables":[{"id":1,"name":"users","columns":[
            {"id":1,"name":"id","storage_type":"int64","application_type":"int64","nullable":false,"primary_key":true,"generated":false},
            {"id":2,"name":"email","storage_type":"text","application_type":"text","nullable":false,"primary_key":false,"generated":false}
        ],"primary_key":["id"]}]}"#,
    );
    let (_dir, code) = write_schema(
        r#"{"tables":[{"id":1,"name":"users","columns":[
            {"id":1,"name":"id","storage_type":"int64","application_type":"int64","nullable":false,"primary_key":true,"generated":false},
            {"id":2,"name":"email","storage_type":"int64","application_type":"int64","nullable":true,"primary_key":false,"generated":false}
        ],"primary_key":["id"],"unique_constraints":[{"name":"uq_users_email","columns":["email"]}]}]}"#,
    );

    bin()
        .arg("diff")
        .arg(&code)
        .arg(&path)
        .assert()
        .success()
        .stdout(contains("~ column users.email type:"))
        .stdout(contains("~ column users.email nullable:"))
        .stdout(contains("+ unique users.uq_users_email"));
}

#[test]
fn diff_detects_column_id_reuse() {
    let path = db_with_stored_schema(
        r#"{"tables":[{"id":1,"name":"users","columns":[
            {"id":1,"name":"id","storage_type":"int64","application_type":"int64","nullable":false,"primary_key":true,"generated":false},
            {"id":2,"name":"email","storage_type":"text","application_type":"text","nullable":false,"primary_key":false,"generated":false}
        ],"primary_key":["id"]}]}"#,
    );
    let (_dir, code) = write_schema(
        r#"{"tables":[{"id":1,"name":"users","columns":[
            {"id":1,"name":"id","storage_type":"int64","application_type":"int64","nullable":false,"primary_key":true,"generated":false},
            {"id":2,"name":"handle","storage_type":"text","application_type":"text","nullable":false,"primary_key":false,"generated":false}
        ],"primary_key":["id"]}]}"#,
    );

    bin()
        .arg("diff")
        .arg(&code)
        .arg(&path)
        .assert()
        .success()
        .stdout(contains("! column id 2 on users reused"))
        .stdout(contains("\"email\" -> \"handle\""));
}
