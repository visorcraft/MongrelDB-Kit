use mongreldb_kit::{Column, ColumnType, Database, Schema, Table};

fn database() -> (tempfile::TempDir, Database) {
    let directory = tempfile::tempdir().unwrap();
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
    let db = Database::create(directory.path(), schema).unwrap();
    (directory, db)
}

fn ids(db: &Database) -> Vec<i64> {
    db.sql_rows("SELECT id FROM items ORDER BY id")
        .unwrap()
        .into_iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect()
}

#[test]
fn sql_savepoint_rolls_back_only_later_writes() {
    let (_directory, db) = database();

    db.sql("BEGIN").unwrap();
    db.sql("INSERT INTO items VALUES (1)").unwrap();
    db.sql("SAVEPOINT sp1").unwrap();
    db.sql("INSERT INTO items VALUES (2)").unwrap();
    db.sql("ROLLBACK TO sp1").unwrap();
    db.sql("COMMIT").unwrap();

    assert_eq!(ids(&db), vec![1]);
}

#[test]
fn sql_nested_savepoint_retains_target_and_removes_later_savepoints() {
    let (_directory, db) = database();

    db.sql("BEGIN").unwrap();
    db.sql("INSERT INTO items VALUES (1)").unwrap();
    db.sql("SAVEPOINT sp1").unwrap();
    db.sql("INSERT INTO items VALUES (2)").unwrap();
    db.sql("SAVEPOINT sp2").unwrap();
    db.sql("INSERT INTO items VALUES (3)").unwrap();
    db.sql("ROLLBACK TO sp1").unwrap();
    db.sql("INSERT INTO items VALUES (4)").unwrap();
    db.sql("ROLLBACK TO sp1").unwrap();
    db.sql("INSERT INTO items VALUES (4)").unwrap();
    db.sql("COMMIT").unwrap();

    assert_eq!(ids(&db), vec![1, 4]);

    db.sql("BEGIN").unwrap();
    db.sql("SAVEPOINT outer").unwrap();
    db.sql("SAVEPOINT inner").unwrap();
    db.sql("ROLLBACK TO outer").unwrap();
    let error = db.sql("ROLLBACK TO inner").unwrap_err();
    assert!(error.to_string().contains("no savepoint named 'inner'"));
    db.sql("ROLLBACK").unwrap();
}

#[test]
fn sql_release_removes_target_and_nested_savepoints() {
    let (_directory, db) = database();

    for name in ["outer", "inner"] {
        db.sql("BEGIN").unwrap();
        db.sql("SAVEPOINT outer").unwrap();
        db.sql("SAVEPOINT inner").unwrap();
        db.sql("RELEASE SAVEPOINT outer").unwrap();
        let error = db.sql(&format!("ROLLBACK TO {name}")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("no savepoint named '{name}'")),
            "{error}"
        );
        db.sql("ROLLBACK").unwrap();
    }
}

#[test]
fn sql_rollback_to_recovers_an_aborted_transaction() {
    let (_directory, db) = database();

    db.sql("BEGIN").unwrap();
    db.sql("INSERT INTO items VALUES (1)").unwrap();
    db.sql("SAVEPOINT stable").unwrap();
    assert!(db
        .sql("INSERT INTO items VALUES (2); SELECT * FROM missing_table")
        .is_err());
    assert!(db
        .sql("COMMIT")
        .unwrap_err()
        .to_string()
        .contains("aborted"));

    db.sql("ROLLBACK TO stable").unwrap();
    db.sql("INSERT INTO items VALUES (3)").unwrap();
    db.sql("COMMIT").unwrap();

    assert_eq!(ids(&db), vec![1, 3]);
}
