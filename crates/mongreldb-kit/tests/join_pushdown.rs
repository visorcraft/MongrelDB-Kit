//! P5: join/grouping base-side filter pushdown. A join filter that references
//! only one alias is pushed to that side's engine scan (and re-applied in
//! Rust for partial translation) before join materialization, so rows the
//! filter would drop never enter the join. Multi-alias conjuncts stay as
//! post-join residuals.

use mongreldb_kit::{
    Column, ColumnType, Database, Expr, ForeignKey, ForeignKeyAction, Index, Join, JoinKind,
    JoinQuery, Schema, Table, UniqueConstraint,
};
use serde_json::{json, Map, Value};
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    tempfile::tempdir().unwrap().keep()
}

fn users_table() -> Table {
    Table {
        id: 1,
        name: "users".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "email", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![Index {
            name: "idx_email".into(),
            columns: vec!["email".into()],
            unique: true,
            kind: Default::default(),
            ann_quantization: Default::default(),
        }],
        foreign_keys: vec![],
        unique_constraints: vec![UniqueConstraint {
            name: "uq_email".into(),
            columns: vec!["email".into()],
        }],
        check_constraints: vec![],
    }
}

fn orders_table() -> Table {
    let mut t = Table {
        id: 2,
        name: "orders".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "user_id", ColumnType::Int64),
            Column::new(3, "total", ColumnType::Float64),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![ForeignKey {
            name: "fk_orders_user".into(),
            columns: vec!["user_id".into()],
            references_table: "users".into(),
            references_columns: vec!["id".into()],
            on_delete: ForeignKeyAction::Restrict,
        }],
        unique_constraints: vec![],
        check_constraints: vec![],
    };
    t.columns[2].nullable = true;
    t
}

fn col(n: &str) -> Expr {
    Expr::Column(n.into())
}

fn insert(db: &Database, table: &str, pairs: &[(&str, Value)]) {
    let mut txn = db.begin().unwrap();
    let row: Map<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    txn.insert(table, row).unwrap();
    txn.commit().unwrap();
}

fn setup() -> Database {
    let dir = temp_dir();
    let schema = Schema::new(vec![users_table(), orders_table()]).unwrap();
    let db = Database::create(&dir, schema).unwrap();
    insert(
        &db,
        "users",
        &[("id", json!(1)), ("email", json!("a@x.com"))],
    );
    insert(
        &db,
        "users",
        &[("id", json!(2)), ("email", json!("b@x.com"))],
    );
    insert(
        &db,
        "users",
        &[("id", json!(3)), ("email", json!("c@x.com"))],
    );
    insert(
        &db,
        "orders",
        &[
            ("id", json!(1)),
            ("user_id", json!(1)),
            ("total", json!(10.0)),
        ],
    );
    insert(
        &db,
        "orders",
        &[
            ("id", json!(2)),
            ("user_id", json!(1)),
            ("total", json!(30.0)),
        ],
    );
    insert(
        &db,
        "orders",
        &[
            ("id", json!(3)),
            ("user_id", json!(2)),
            ("total", json!(50.0)),
        ],
    );
    db
}

fn join_query(filter: Option<Expr>) -> JoinQuery {
    JoinQuery {
        table: "users".into(),
        alias: Some("u".into()),
        joins: vec![Join {
            kind: JoinKind::Inner,
            table: "orders".into(),
            alias: Some("o".into()),
            on: Some(Expr::Eq(Box::new(col("u.id")), Box::new(col("o.user_id")))),
        }],
        filter,
        order_by: vec![],
        limit: None,
        offset: None,
    }
}

/// A base-side filter (`u.email = ...`) must be pushed to the users scan so
/// only the matching user(s) enter the join.
#[test]
fn join_base_side_filter_pushdown() {
    let db = setup();
    let txn = db.begin().unwrap();
    let q = join_query(Some(Expr::Eq(
        Box::new(col("u.email")),
        Box::new(Expr::Literal(mongreldb_kit::Literal::Text(
            "b@x.com".into(),
        ))),
    )));
    let rows = txn.join(&q).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["u"]["email"], json!("b@x.com"));
}

/// A right-side filter (`o.total > ...`) must be pushed to the orders scan.
#[test]
fn join_right_side_filter_pushdown() {
    let db = setup();
    let txn = db.begin().unwrap();
    let q = join_query(Some(Expr::Gt(
        Box::new(col("o.total")),
        Box::new(Expr::Literal(mongreldb_kit::Literal::Float(20.0))),
    )));
    let rows = txn.join(&q).unwrap();
    // orders with total > 20: order 2 (30.0) and order 3 (50.0).
    assert_eq!(rows.len(), 2);
}

/// A multi-alias filter (`u.id = o.user_id AND o.total > 40`) must split:
/// the `o.total > 40` conjunct pushes to the right side, the cross-table
/// conjunct is handled by the ON clause.
#[test]
fn join_mixed_filter_splits_correctly() {
    let db = setup();
    let txn = db.begin().unwrap();
    let q = join_query(Some(Expr::And(vec![
        Expr::Gt(
            Box::new(col("o.total")),
            Box::new(Expr::Literal(mongreldb_kit::Literal::Float(20.0))),
        ),
        Expr::Eq(
            Box::new(col("u.email")),
            Box::new(Expr::Literal(mongreldb_kit::Literal::Text(
                "a@x.com".into(),
            ))),
        ),
    ])));
    let rows = txn.join(&q).unwrap();
    // user a@x.com (id=1), orders with total > 20: order 2 (30.0).
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["o"]["total"], json!(30.0));
}

/// A bare-column filter (no alias prefix) must NOT be pushed (ambiguous in a
/// join) — it stays as a post-join residual and still produces correct results.
#[test]
fn join_bare_column_filter_stays_residual() {
    let db = setup();
    let txn = db.begin().unwrap();
    let q = join_query(Some(Expr::Eq(
        Box::new(col("email")), // bare — no alias
        Box::new(Expr::Literal(mongreldb_kit::Literal::Text(
            "c@x.com".into(),
        ))),
    )));
    let rows = txn.join(&q).unwrap();
    // user c@x.com has no orders → inner join returns 0 rows.
    assert_eq!(rows.len(), 0);
}
