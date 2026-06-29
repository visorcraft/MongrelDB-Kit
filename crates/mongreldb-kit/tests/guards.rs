//! Tests for the guard-table architecture: sequences + conflict retry,
//! parent-delete vs child-insert row-guard conflicts, custom/sequence defaults,
//! and check-constraint enforcement.

use mongreldb_kit::{
    CheckConstraint, Column, ColumnType, Database, DefaultKind, ForeignKey, ForeignKeyAction,
    KitError, Schema, Table,
};
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::sync::Arc;

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

// ── sequences ────────────────────────────────────────────────────────────────

#[test]
fn sequence_allocation_is_monotonic() {
    let dir = temp_dir();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "t".into(),
        columns: vec![col(1, "id", ColumnType::Int64)],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    // 1-based (AUTO_INCREMENT): 1, then 2, then reserve 5 from 3, then 8.
    assert_eq!(db.allocate_sequence("seq", 1).unwrap(), 1);
    assert_eq!(db.allocate_sequence("seq", 1).unwrap(), 2);
    assert_eq!(db.allocate_sequence("seq", 5).unwrap(), 3);
    assert_eq!(db.allocate_sequence("seq", 1).unwrap(), 8);
    // A different sequence has its own counter.
    assert_eq!(db.allocate_sequence("other", 1).unwrap(), 1);
}

#[test]
fn sequence_allocation_retries_under_contention() {
    let dir = temp_dir();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "t".into(),
        columns: vec![col(1, "id", ColumnType::Int64)],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Arc::new(Database::create(&dir, schema).unwrap());

    const THREADS: usize = 4;
    const PER_THREAD: usize = 50;
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut out = Vec::with_capacity(PER_THREAD);
            for _ in 0..PER_THREAD {
                out.push(db.allocate_sequence("ids", 1).unwrap());
            }
            out
        }));
    }
    let mut all: Vec<i64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    all.sort_unstable();
    // Every allocation must be unique and the set must be a gap-free range
    // starting at 1 (sequences are 1-based).
    let expected: Vec<i64> = (1..=(THREADS * PER_THREAD) as i64).collect();
    assert_eq!(all, expected, "sequence values collided or were lost");
}

// ── defaults ─────────────────────────────────────────────────────────────────

#[test]
fn sequence_and_custom_defaults_apply_on_insert() {
    let dir = temp_dir();
    let id = Column {
        default: Some(DefaultKind::Sequence("user_seq".into())),
        ..col(1, "id", ColumnType::Int64)
    };
    let token = Column {
        default: Some(DefaultKind::CustomName("token".into())),
        ..col(2, "token", ColumnType::Text)
    };
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "users".into(),
        columns: vec![id, token],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let mut db = Database::create(&dir, schema).unwrap();
    db.register_default("token", || json!("fixed-token"));

    let mut txn = db.begin().unwrap();
    let first = txn.insert("users", row(&[])).unwrap();
    assert_eq!(first.values.get("id"), Some(&json!(1)));
    assert_eq!(first.values.get("token"), Some(&json!("fixed-token")));
    let second = txn.insert("users", row(&[])).unwrap();
    assert_eq!(second.values.get("id"), Some(&json!(2)));
    txn.commit().unwrap();
}

#[test]
fn unregistered_custom_default_is_rejected() {
    let dir = temp_dir();
    let token = Column {
        default: Some(DefaultKind::CustomName("missing".into())),
        ..col(2, "token", ColumnType::Text)
    };
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "users".into(),
        columns: vec![col(1, "id", ColumnType::Int64), token],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();
    let mut txn = db.begin().unwrap();
    let err = txn.insert("users", row(&[("id", json!(1))])).unwrap_err();
    assert!(matches!(err, KitError::Validation(_)));
}

// ── check constraints ────────────────────────────────────────────────────────

#[test]
fn table_check_constraint_is_enforced() {
    let dir = temp_dir();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "orders".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            col(2, "quantity", ColumnType::Int64),
            col(3, "price", ColumnType::Int64),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![CheckConstraint {
            name: "positive".into(),
            expr: "quantity > 0 AND price >= 0".into(),
        }],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    let mut txn = db.begin().unwrap();
    // Passing row.
    txn.insert(
        "orders",
        row(&[
            ("id", json!(1)),
            ("quantity", json!(2)),
            ("price", json!(5)),
        ]),
    )
    .unwrap();
    // Violating row (quantity = 0).
    let err = txn
        .insert(
            "orders",
            row(&[
                ("id", json!(2)),
                ("quantity", json!(0)),
                ("price", json!(5)),
            ]),
        )
        .unwrap_err();
    assert!(matches!(err, KitError::Validation(_)));
    txn.rollback();
}

// ── parent-delete vs child-insert row-guard conflict ────────────────────────

fn parent_child_schema() -> Schema {
    let parent = Table {
        id: 1,
        name: "parent".into(),
        columns: vec![col(1, "id", ColumnType::Int64)],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    };
    let child = Table {
        id: 2,
        name: "child".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            nullable(col(2, "parent_id", ColumnType::Int64)),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![ForeignKey {
            name: "fk_child_parent".into(),
            columns: vec!["parent_id".into()],
            references_table: "parent".into(),
            references_columns: vec!["id".into()],
            on_delete: ForeignKeyAction::Cascade,
        }],
        unique_constraints: vec![],
        check_constraints: vec![],
    };
    Schema::new(vec![parent, child]).unwrap()
}

#[test]
fn parent_delete_conflicts_with_concurrent_child_insert() {
    let dir = temp_dir();
    let db = Database::create(&dir, parent_child_schema()).unwrap();

    // Seed a parent.
    let mut seed = db.begin().unwrap();
    seed.insert("parent", row(&[("id", json!(1))])).unwrap();
    seed.commit().unwrap();

    // Two transactions opened at the same snapshot.
    let mut child_txn = db.begin().unwrap();
    let mut delete_txn = db.begin().unwrap();

    // The child insert touches the parent's row guard.
    child_txn
        .insert("child", row(&[("id", json!(1)), ("parent_id", json!(1))]))
        .unwrap();
    // The delete (at its own snapshot, which does not see the child) also
    // touches the same parent row guard.
    delete_txn.delete("parent", &json!(1)).unwrap();

    // First committer wins.
    child_txn.commit().unwrap();
    let err = delete_txn.commit().unwrap_err();
    assert!(
        matches!(err, KitError::Conflict(_)),
        "expected a write-write conflict on the parent row guard, got {err:?}"
    );
}

#[test]
fn unique_guard_is_reclaimed_after_delete() {
    // Deleting a row frees its unique value so it can be re-inserted.
    let dir = temp_dir();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "users".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            col(2, "email", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![mongreldb_kit::UniqueConstraint {
            name: "uq_email".into(),
            columns: vec!["email".into()],
        }],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    let mut txn = db.begin().unwrap();
    txn.insert(
        "users",
        row(&[("id", json!(1)), ("email", json!("a@x.com"))]),
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    txn.delete("users", &json!(1)).unwrap();
    txn.commit().unwrap();

    // Re-insert the same email with a new id: must succeed now the guard is gone.
    let mut txn = db.begin().unwrap();
    txn.insert(
        "users",
        row(&[("id", json!(2)), ("email", json!("a@x.com"))]),
    )
    .unwrap();
    txn.commit().unwrap();
}
