//! `Database::explain` reports index push-down without running the query.

use mongreldb_kit::{Column, ColumnType, Database, Schema, Table};
use mongreldb_kit_core::query::{Expr, Literal};
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    tempfile::tempdir().unwrap().keep()
}

fn schema() -> Schema {
    Schema::new(vec![Table {
        id: 1,
        name: "orders".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "amount", ColumnType::Int64),
            Column::new(3, "note", ColumnType::Text),
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
fn explain_reports_pushdown() {
    let db = Database::create(&temp_dir(), schema()).unwrap();

    // A range on an int column pushes an exact RangeInt condition.
    let gt = Expr::Gt(
        Box::new(Expr::Column("amount".into())),
        Box::new(Expr::Literal(Literal::Int(6))),
    );
    let plan = db.explain("orders", &gt).unwrap();
    assert!(plan.index_accelerated);
    assert!(plan.exact);
    assert_eq!(plan.pushed_conditions, vec!["Range".to_string()]);

    // A substring match on a column with no FM index cannot push down.
    let contains = Expr::Contains(Box::new(Expr::Column("note".into())), "x".into());
    let plan = db.explain("orders", &contains).unwrap();
    assert!(!plan.index_accelerated);
    assert!(plan.pushed_conditions.is_empty());

    assert!(db.explain("missing", &gt).is_err());
}
