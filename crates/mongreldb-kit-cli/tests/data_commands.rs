//! End-to-end tests for the CLI data commands (get/query/insert/update/delete/
//! upsert/count) driven through the actual `mongreldb-kit` binary.

use assert_cmd::Command;
use mongreldb_kit::{Database, Schema};
use predicates::prelude::*;

const SCHEMA: &str = r#"{
  "tables": [
    {
      "id": 1,
      "name": "items",
      "columns": [
        {"id":1,"name":"id","storage_type":"int64","application_type":"int64","nullable":false,"primary_key":true,"generated":false},
        {"id":2,"name":"name","storage_type":"text","application_type":"text","nullable":false,"primary_key":false,"generated":false},
        {"id":3,"name":"qty","storage_type":"int64","application_type":"int64","nullable":true,"primary_key":false,"generated":false}
      ],
      "primary_key": ["id"],
      "indexes": [],
      "unique_constraints": [],
      "foreign_keys": [],
      "check_constraints": []
    }
  ]
}"#;

/// Create a fresh database with the `items` schema; the CLI (a separate process)
/// opens it by path afterwards.
fn setup_db() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let schema: Schema = serde_json::from_str(SCHEMA).unwrap();
    Database::create(dir.path(), schema).unwrap();
    dir
}

fn cli() -> Command {
    Command::cargo_bin("mongreldb-kit").unwrap()
}

#[test]
fn data_commands_roundtrip() {
    let dir = setup_db();
    let p = dir.path().to_str().unwrap();

    // insert (prints the row with defaults applied)
    cli()
        .args(["insert", p, "items", r#"{"id":1,"name":"widget","qty":5}"#])
        .assert()
        .success()
        .stdout(predicate::str::contains("widget"));
    cli()
        .args(["insert", p, "items", r#"{"id":2,"name":"gadget","qty":10}"#])
        .assert()
        .success();

    // get by pk (unquoted scalar), and a miss prints null
    cli()
        .args(["get", p, "items", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("widget"));
    cli()
        .args(["get", p, "items", "99"])
        .assert()
        .success()
        .stdout(predicate::str::contains("null"));

    // count, and a filtered count
    cli()
        .args(["count", p, "items"])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("2"));
    cli()
        .args(["count", p, "items", "--filter", r#"{"qty":{"gte":10}}"#])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("1"));

    // filtered query (bare value = eq)
    cli()
        .args(["query", p, "items", "--filter", r#"{"name":"gadget"}"#])
        .assert()
        .success()
        .stdout(predicate::str::contains("gadget").and(predicate::str::contains("widget").not()));

    // update by pk, then confirm via get
    cli()
        .args(["update", p, "items", "1", r#"{"qty":42}"#])
        .assert()
        .success()
        .stdout(predicate::str::contains("42"));
    cli()
        .args(["get", p, "items", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("42"));

    // upsert with --update takes the conflict path
    cli()
        .args([
            "upsert",
            p,
            "items",
            r#"{"id":2,"name":"gadget2","qty":11}"#,
            "--update",
        ])
        .assert()
        .success();
    cli()
        .args(["get", p, "items", "2"])
        .assert()
        .success()
        .stdout(predicate::str::contains("gadget2"));

    // delete by pk, count drops to 1
    cli()
        .args(["delete", p, "items", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("deleted"));
    cli()
        .args(["count", p, "items"])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("1"));
}
