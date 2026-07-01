//! Cross-language correctness fixes mirrored from the TypeScript kit:
//!
//! 1. Nullable columns store real `NULL` (not a zero/empty sentinel); an omitted
//!    nullable column reads back as null and the insert return normalizes it.
//! 2. A plain `default: now` column is insert-only; only a `generated` now column
//!    refreshes on update.
//! 3. Single-column primary-key collisions throw a duplicate error (not upsert).
//! 4. A unique index enforces uniqueness like a `unique()` constraint.

use mongreldb_kit::{
    Column, ColumnType, Database, DefaultKind, Expr, ForeignKey, ForeignKeyAction, Index, KitError,
    Query, Schema, Select, Table, UniqueConstraint,
};
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

fn select_filter(table: &str, filter: Expr) -> Query {
    Query::Select(Select {
        table: table.into(),
        columns: vec![],
        filter: Some(filter),
        order_by: vec![],
        limit: None,
        offset: None,
    })
}

// ── 1. nullable columns store real NULL + insert-return normalization ─────────

#[test]
fn nullable_columns_store_real_null_and_round_trip() {
    let dir = temp_dir();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "t".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            nullable(col(2, "val", ColumnType::Int64)),
            nullable(col(3, "txt", ColumnType::Text)),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    let mut txn = db.begin().unwrap();
    // Row 1 omits both nullable columns entirely.
    let inserted = txn.insert("t", row(&[("id", json!(1))])).unwrap();
    // The insert return normalizes omitted columns to explicit null.
    assert_eq!(inserted.values.get("val"), Some(&Value::Null));
    assert_eq!(inserted.values.get("txt"), Some(&Value::Null));
    // Row 2 has a real zero and a real empty string, distinct from null.
    txn.insert(
        "t",
        row(&[("id", json!(2)), ("val", json!(0)), ("txt", json!(""))]),
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let r1 = txn.get_by_pk("t", &json!(1)).unwrap().unwrap();
    assert_eq!(r1.values.get("val"), Some(&Value::Null));
    assert_eq!(r1.values.get("txt"), Some(&Value::Null));
    let r2 = txn.get_by_pk("t", &json!(2)).unwrap().unwrap();
    assert_eq!(r2.values.get("val"), Some(&json!(0)));
    assert_eq!(r2.values.get("txt"), Some(&json!("")));

    // is_null matches the real null only; is_not_null matches the real zero.
    let nulls = txn
        .select(&select_filter(
            "t",
            Expr::IsNull(Box::new(Expr::Column("val".into()))),
        ))
        .unwrap();
    assert_eq!(nulls.len(), 1);
    assert_eq!(nulls[0].values.get("id"), Some(&json!(1)));

    let not_nulls = txn
        .select(&select_filter(
            "t",
            Expr::IsNotNull(Box::new(Expr::Column("val".into()))),
        ))
        .unwrap();
    assert_eq!(not_nulls.len(), 1);
    assert_eq!(not_nulls[0].values.get("id"), Some(&json!(2)));
}

// ── 2. `now` default is insert-only; `generated` now refreshes on update ──────

fn timestamps_schema() -> Schema {
    let created = Column {
        default: Some(DefaultKind::Now),
        generated: false,
        ..col(3, "created_at", ColumnType::Text)
    };
    let updated = Column {
        default: Some(DefaultKind::Now),
        generated: true,
        ..col(4, "updated_at", ColumnType::Text)
    };
    Schema::new(vec![Table {
        id: 1,
        name: "docs".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            col(2, "body", ColumnType::Text),
            created,
            updated,
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
fn now_default_not_refreshed_on_update_but_generated_now_is() {
    let dir = temp_dir();
    let db = Database::create(&dir, timestamps_schema()).unwrap();

    // Seed with explicit, far-past timestamps so the test is robust to the
    // one-second resolution of `iso_now`.
    let past = "2000-01-01T00:00:00Z";
    let mut txn = db.begin().unwrap();
    txn.insert(
        "docs",
        row(&[
            ("id", json!(1)),
            ("body", json!("a")),
            ("created_at", json!(past)),
            ("updated_at", json!(past)),
        ]),
    )
    .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let updated = txn
        .update("docs", &json!(1), row(&[("body", json!("b"))]))
        .unwrap();
    txn.commit().unwrap();

    // created_at (plain `now` default) must NOT change on update.
    assert_eq!(updated.values.get("created_at"), Some(&json!(past)));
    // updated_at (`generated` now) refreshes to the current time.
    assert_ne!(updated.values.get("updated_at"), Some(&json!(past)));

    let txn = db.begin().unwrap();
    let r = txn.get_by_pk("docs", &json!(1)).unwrap().unwrap();
    assert_eq!(r.values.get("created_at"), Some(&json!(past)));
    assert_ne!(r.values.get("updated_at"), Some(&json!(past)));
    assert_eq!(r.values.get("body"), Some(&json!("b")));
}

// ── 3. single-column primary-key collisions throw + re-insert after delete ────

#[test]
fn duplicate_single_pk_throws_and_reinsert_after_delete_works() {
    let dir = temp_dir();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "k".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            col(2, "v", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    let mut txn = db.begin().unwrap();
    txn.insert("k", row(&[("id", json!(1)), ("v", json!("a"))]))
        .unwrap();
    txn.commit().unwrap();

    // A duplicate single-column PK is rejected, not upserted.
    let mut txn = db.begin().unwrap();
    let err = txn
        .insert("k", row(&[("id", json!(1)), ("v", json!("b"))]))
        .unwrap_err();
    assert!(matches!(err, KitError::Duplicate(_)), "got {err:?}");
    txn.rollback();
    // The original row is intact (no last-writer-wins upsert happened).
    let txn = db.begin().unwrap();
    assert_eq!(
        txn.get_by_pk("k", &json!(1))
            .unwrap()
            .unwrap()
            .values
            .get("v"),
        Some(&json!("a"))
    );

    // After deleting the row its PK guard is reclaimed and re-insert succeeds.
    let mut txn = db.begin().unwrap();
    txn.delete("k", &json!(1)).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    txn.insert("k", row(&[("id", json!(1)), ("v", json!("c"))]))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    assert_eq!(
        txn.get_by_pk("k", &json!(1))
            .unwrap()
            .unwrap()
            .values
            .get("v"),
        Some(&json!("c"))
    );
}

// ── 4. a unique index enforces uniqueness like a unique() constraint ──────────

#[test]
fn unique_index_enforces_uniqueness() {
    let dir = temp_dir();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "u".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            col(2, "email", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![Index {
            name: "idx_email".into(),
            columns: vec!["email".into()],
            unique: true,
            kind: Default::default(),
        }],
        foreign_keys: vec![],
        // No explicit unique constraint: the unique INDEX alone must enforce it.
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    let mut txn = db.begin().unwrap();
    txn.insert("u", row(&[("id", json!(1)), ("email", json!("a@x.com"))]))
        .unwrap();
    // A second row with the same indexed email is rejected as a duplicate.
    let err = txn
        .insert("u", row(&[("id", json!(2)), ("email", json!("a@x.com"))]))
        .unwrap_err();
    assert!(matches!(err, KitError::Duplicate(_)), "got {err:?}");
    // A distinct email is still accepted.
    txn.insert("u", row(&[("id", json!(3)), ("email", json!("b@x.com"))]))
        .unwrap();
    txn.commit().unwrap();
}

#[test]
fn non_unique_index_does_not_enforce_uniqueness() {
    let dir = temp_dir();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "u".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            col(2, "email", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![Index {
            name: "idx_email".into(),
            columns: vec!["email".into()],
            unique: false,
            kind: Default::default(),
        }],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    let mut txn = db.begin().unwrap();
    txn.insert("u", row(&[("id", json!(1)), ("email", json!("a@x.com"))]))
        .unwrap();
    // A non-unique index does not constrain duplicates.
    txn.insert("u", row(&[("id", json!(2)), ("email", json!("a@x.com"))]))
        .unwrap();
    txn.commit().unwrap();
}

// ── auto-assigned primary keys never collide and skip the duplicate check ──────

#[test]
fn auto_increment_pks_do_not_collide() {
    let dir = temp_dir();
    let id = Column {
        default: Some(DefaultKind::Sequence("k_seq".into())),
        ..col(1, "id", ColumnType::Int64)
    };
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "k".into(),
        columns: vec![id, col(2, "v", ColumnType::Text)],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    // Inserts that omit the PK get distinct, auto-assigned ids with no
    // duplicate-PK error — an auto PK is guaranteed unique, so it is never
    // checked or guarded.
    let mut txn = db.begin().unwrap();
    let a = txn.insert("k", row(&[("v", json!("a"))])).unwrap();
    let b = txn.insert("k", row(&[("v", json!("b"))])).unwrap();
    let c = txn.insert("k", row(&[("v", json!("c"))])).unwrap();
    txn.commit().unwrap();

    assert_eq!(a.values.get("id"), Some(&json!(1)));
    assert_eq!(b.values.get("id"), Some(&json!(2)));
    assert_eq!(c.values.get("id"), Some(&json!(3)));

    let txn = db.begin().unwrap();
    let rows = txn
        .select(&select_filter(
            "k",
            Expr::IsNotNull(Box::new(Expr::Column("id".into()))),
        ))
        .unwrap();
    assert_eq!(rows.len(), 3);
}

// ── batch insert (insert_many) inserts a batch in one transaction ─────────────

#[test]
fn insert_many_inserts_a_batch_in_one_transaction() {
    let dir = temp_dir();
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "k".into(),
        columns: vec![
            col(1, "id", ColumnType::Int64),
            col(2, "code", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![UniqueConstraint {
            name: "uq_code".into(),
            columns: vec!["code".into()],
        }],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    // A batch of explicit-PK rows is staged and committed in a single transaction.
    let mut txn = db.begin().unwrap();
    let inserted = txn
        .insert_many(
            "k",
            vec![
                row(&[("id", json!(1)), ("code", json!("a"))]),
                row(&[("id", json!(2)), ("code", json!("b"))]),
                row(&[("id", json!(3)), ("code", json!("c"))]),
            ],
        )
        .unwrap();
    assert_eq!(inserted.len(), 3);
    assert_eq!(inserted[2].values.get("code"), Some(&json!("c")));
    txn.commit().unwrap();

    let count = |db: &Database| {
        let txn = db.begin().unwrap();
        txn.select(&select_filter(
            "k",
            Expr::IsNotNull(Box::new(Expr::Column("id".into()))),
        ))
        .unwrap()
        .len()
    };
    assert_eq!(count(&db), 3);

    // A duplicate PK colliding with a committed row rolls the whole batch back.
    let mut txn = db.begin().unwrap();
    let err = txn
        .insert_many(
            "k",
            vec![
                row(&[("id", json!(4)), ("code", json!("d"))]),
                row(&[("id", json!(1)), ("code", json!("e"))]),
            ],
        )
        .unwrap_err();
    assert!(matches!(err, KitError::Duplicate(_)), "got {err:?}");
    txn.rollback();
    assert_eq!(count(&db), 3);

    // A duplicate PK appearing only within the batch is rejected via the
    // in-memory pk-seen set.
    let mut txn = db.begin().unwrap();
    let err = txn
        .insert_many(
            "k",
            vec![
                row(&[("id", json!(5)), ("code", json!("f"))]),
                row(&[("id", json!(5)), ("code", json!("g"))]),
            ],
        )
        .unwrap_err();
    assert!(matches!(err, KitError::Duplicate(_)), "got {err:?}");
    txn.rollback();
    assert_eq!(count(&db), 3);

    // A duplicate unique value inside the batch is rejected too.
    let mut txn = db.begin().unwrap();
    let err = txn
        .insert_many(
            "k",
            vec![
                row(&[("id", json!(6)), ("code", json!("h"))]),
                row(&[("id", json!(7)), ("code", json!("a"))]),
            ],
        )
        .unwrap_err();
    assert!(matches!(err, KitError::Duplicate(_)), "got {err:?}");
    txn.rollback();
    assert_eq!(count(&db), 3);
}

#[test]
fn insert_many_assigns_sequence_pks() {
    let dir = temp_dir();
    let id = Column {
        default: Some(DefaultKind::Sequence("k_seq".into())),
        ..col(1, "id", ColumnType::Int64)
    };
    let schema = Schema::new(vec![Table {
        id: 1,
        name: "k".into(),
        columns: vec![id, col(2, "v", ColumnType::Text)],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap();
    let db = Database::create(&dir, schema).unwrap();

    let mut txn = db.begin().unwrap();
    let inserted = txn
        .insert_many(
            "k",
            vec![
                row(&[("v", json!("a"))]),
                row(&[("v", json!("b"))]),
                row(&[("v", json!("c"))]),
            ],
        )
        .unwrap();
    txn.commit().unwrap();
    let ids: Vec<_> = inserted
        .iter()
        .map(|r| r.values.get("id").cloned())
        .collect();
    assert_eq!(ids, vec![Some(json!(1)), Some(json!(2)), Some(json!(3))]);
}

// ── cascade delete reuses rows scanned during planning (no per-row re-read) ────

#[test]
fn cascade_delete_removes_all_children() {
    let dir = temp_dir();
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
            col(2, "parent_id", ColumnType::Int64),
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
    let db = Database::create(&dir, Schema::new(vec![parent, child]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    txn.insert("parent", row(&[("id", json!(1))])).unwrap();
    for i in 1..=5 {
        txn.insert("child", row(&[("id", json!(i)), ("parent_id", json!(1))]))
            .unwrap();
    }
    txn.commit().unwrap();

    // Deleting the parent cascades to every child, reusing the child rows scanned
    // while planning rather than re-reading each child by PK.
    let mut txn = db.begin().unwrap();
    txn.delete("parent", &json!(1)).unwrap();
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let children = txn
        .select(&select_filter(
            "child",
            Expr::IsNotNull(Box::new(Expr::Column("id".into()))),
        ))
        .unwrap();
    assert!(
        children.is_empty(),
        "expected all children cascaded, got {children:?}"
    );
    assert!(txn.get_by_pk("parent", &json!(1)).unwrap().is_none());
}
