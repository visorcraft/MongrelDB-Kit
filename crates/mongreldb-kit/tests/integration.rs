use mongreldb_core::memtable::Value as CoreValue;
use mongreldb_core::schema::{ColumnFlags, TypeId};
use mongreldb_core::MongrelError as CoreError;
use mongreldb_kit::{
    AggFunc, Aggregate, AggregateQuery, Column, ColumnType, Cte, Database, Delete, Direction, Expr,
    ForeignKey, ForeignKeyAction, Index, Join, JoinKind, JoinQuery, KitError, Literal, Migration,
    MigrationOp, OnConflict, OrderBy, Query, Row, Schema, Select, Table, Transaction,
    UniqueConstraint, Update,
};
use mongreldb_kit_core::ProcedureSpec;
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
            Column {
                nullable: true,
                ..Column::new(3, "name", ColumnType::Text)
            },
        ],
        primary_key: vec!["id".into()],
        indexes: vec![Index {
            name: "idx_email".into(),
            columns: vec!["email".into()],
            unique: true,
            kind: Default::default(),
            ann_quantization: Default::default(),
            ..Default::default()
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

fn items_table() -> Table {
    Table {
        id: 3,
        name: "items".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "order_id", ColumnType::Int64),
            Column::new(3, "sku", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![ForeignKey {
            name: "fk_items_order".into(),
            columns: vec!["order_id".into()],
            references_table: "orders".into(),
            references_columns: vec!["id".into()],
            on_delete: ForeignKeyAction::Cascade,
        }],
        unique_constraints: vec![],
        check_constraints: vec![],
    }
}

fn make_schema() -> Schema {
    Schema::new(vec![users_table(), orders_table(), items_table()]).unwrap()
}

fn insert_user(txn: &mut Transaction, id: i64, email: &str) -> Row {
    let mut row = Map::new();
    row.insert("id".into(), json!(id));
    row.insert("email".into(), json!(email));
    txn.insert("users", row).unwrap()
}

fn insert_named_user(txn: &mut Transaction, id: i64, email: &str, name: &str) -> Row {
    let mut row = Map::new();
    row.insert("id".into(), json!(id));
    row.insert("email".into(), json!(email));
    row.insert("name".into(), json!(name));
    txn.insert("users", row).unwrap()
}

#[test]
fn stored_procedure_installs_and_calls_through_rust_kit() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();
    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    txn.commit().unwrap();

    let procedure = ProcedureSpec::new(json!({
        "name": "read_users",
        "version": 1,
        "mode": "read_only",
        "params": [],
        "body": {
            "steps": [{
                "kind": "native_query",
                "id": "read",
                "table": "users",
                "conditions": [],
                "projection": [1, 2],
                "limit": 10
            }],
            "return_value": { "kind": "step_rows", "value": "read" }
        },
        "checksum": "",
        "created_epoch": 0,
        "updated_epoch": 0
    }));

    db.create_procedure(&procedure).unwrap();
    let result = db.call_procedure("read_users", Map::new()).unwrap();

    let mongreldb_core::ProcedureCallOutput::Rows(rows) = result.output else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.get(&1), Some(&CoreValue::Int64(1)));
}

fn insert_order(txn: &mut Transaction, id: i64, user_id: i64, total: f64) {
    let mut row = Map::new();
    row.insert("id".into(), json!(id));
    row.insert("user_id".into(), json!(user_id));
    row.insert("total".into(), json!(total));
    txn.insert("orders", row).unwrap();
}

fn col(name: &str) -> Expr {
    Expr::Column(name.into())
}

fn select_all(table: &str, filter: Option<Expr>) -> Select {
    Select {
        table: table.into(),
        columns: vec![],
        filter,
        order_by: vec![],
        limit: None,
        offset: None,
    }
}

/// Kit Priority 7: a bare `COUNT(*)` is served from the engine (survivor
/// cardinality / live_count) and must match the row-scan result exactly across
/// unfiltered, PK-filtered, post-delete, and staged-write (fall-back) cases.
#[test]
fn count_star_native_delegation_matches_scan() {
    fn count_star(txn: &Transaction, table: &str, filter: Option<Expr>) -> i64 {
        let q = AggregateQuery {
            table: table.into(),
            filter,
            group_by: vec![],
            aggregates: vec![Aggregate {
                func: AggFunc::Count,
                column: None,
                alias: "n".into(),
                distinct: false,
            }],
            having: None,
        };
        let rows = txn.aggregate(&q).unwrap();
        assert_eq!(rows.len(), 1);
        rows[0].values.get("n").unwrap().as_i64().unwrap()
    }

    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();
    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "a@example.com");
    insert_user(&mut txn, 2, "b@example.com");
    insert_order(&mut txn, 1, 1, 10.0);
    insert_order(&mut txn, 2, 1, 30.0);
    insert_order(&mut txn, 3, 2, 5.0);
    txn.commit().unwrap();

    // Unfiltered: delegated (snapshot == latest, no staged) ⇒ 3.
    {
        let txn = db.begin().unwrap();
        assert_eq!(count_star(&txn, "orders", None), 3);
    }
    // PK-filtered (exact, pushable) ⇒ 1 (delegated or scanned — must agree).
    {
        let txn = db.begin().unwrap();
        let f = Expr::Eq(
            Box::new(col("id")),
            Box::new(Expr::Literal(Literal::Int(2))),
        );
        assert_eq!(count_star(&txn, "orders", Some(f)), 1);
    }
    // After deleting one order, the delegated COUNT(*) stays exact (validates the
    // engine count under deletes, not just inserts).
    {
        let mut txn = db.begin().unwrap();
        txn.execute(&Query::Delete(Delete {
            table: "orders".into(),
            filter: Some(Expr::Eq(
                Box::new(col("id")),
                Box::new(Expr::Literal(Literal::Int(3))),
            )),
            returning: vec![],
            pk: None,
        }))
        .unwrap();
        txn.commit().unwrap();
        let txn = db.begin().unwrap();
        assert_eq!(count_star(&txn, "orders", None), 2);
    }
    // Staged (uncommitted) write in the SAME txn ⇒ delegation must fall back to
    // the row scan, which replays staged rows ⇒ 3 (2 committed + 1 staged).
    {
        let mut txn = db.begin().unwrap();
        insert_order(&mut txn, 4, 1, 7.0);
        assert_eq!(count_star(&txn, "orders", None), 3);
        txn.rollback();
    }
}

/// Kit Priority 7: SUM/MIN/MAX/AVG/COUNT(col) over a column must match the
/// in-Rust result whether served natively (single sorted run) or by fall-back.
#[test]
fn column_aggregate_native_delegation_matches_scan() {
    fn agg1(txn: &Transaction, func: AggFunc, col: &str) -> Value {
        let q = AggregateQuery {
            table: "orders".into(),
            filter: None,
            group_by: vec![],
            aggregates: vec![Aggregate {
                func,
                column: Some(col.into()),
                alias: "v".into(),
                distinct: false,
            }],
            having: None,
        };
        let rows = txn.aggregate(&q).unwrap();
        assert_eq!(rows.len(), 1);
        rows[0].values.get("v").cloned().unwrap()
    }

    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();
    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "a@example.com");
    insert_order(&mut txn, 1, 1, 10.0);
    insert_order(&mut txn, 2, 1, 30.0);
    insert_order(&mut txn, 3, 1, 5.0);
    // A null-total order (total is nullable) exercises COUNT(col) NULL exclusion.
    {
        let mut row = Map::new();
        row.insert("id".into(), json!(4));
        row.insert("user_id".into(), json!(1));
        txn.insert("orders", row).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    assert_eq!(agg1(&txn, AggFunc::Sum, "total"), json!(45.0));
    assert_eq!(agg1(&txn, AggFunc::Min, "total"), json!(5.0));
    assert_eq!(agg1(&txn, AggFunc::Max, "total"), json!(30.0));
    assert_eq!(agg1(&txn, AggFunc::Avg, "total"), json!(15.0));
    assert_eq!(agg1(&txn, AggFunc::Count, "total"), json!(3)); // null excluded
}

/// Kit Priority 7: COUNT(DISTINCT col) — the unique non-null count — matches the
/// scan result, unfiltered and filtered. (Delegates to the engine's bitmap
/// cardinality for a single-sorted-run, bitmap-indexed column; otherwise served
/// in-Rust, which this exercises.)
#[test]
fn count_distinct_matches_scan() {
    fn cdistinct(txn: &Transaction, col: &str, filter: Option<Expr>) -> i64 {
        let q = AggregateQuery {
            table: "orders".into(),
            filter,
            group_by: vec![],
            aggregates: vec![Aggregate {
                func: AggFunc::Count,
                column: Some(col.into()),
                alias: "d".into(),
                distinct: true,
            }],
            having: None,
        };
        txn.aggregate(&q).unwrap()[0]
            .values
            .get("d")
            .unwrap()
            .as_i64()
            .unwrap()
    }

    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();
    let mut txn = db.begin().unwrap();
    for id in 1..=3i64 {
        insert_user(&mut txn, id, &format!("u{id}@x.com"));
    }
    insert_order(&mut txn, 1, 1, 10.0);
    insert_order(&mut txn, 2, 1, 20.0); // user 1 again
    insert_order(&mut txn, 3, 2, 30.0);
    insert_order(&mut txn, 4, 2, 30.0); // user 2, total 30 again
    insert_order(&mut txn, 5, 3, 40.0);
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    // distinct user_id {1,2,3} = 3; distinct total {10,20,30,40} = 4.
    assert_eq!(cdistinct(&txn, "user_id", None), 3);
    assert_eq!(cdistinct(&txn, "total", None), 4);
    // Filtered: user_id >= 2 ⇒ orders 3,4,5 ⇒ distinct user_id {2,3} = 2.
    let f = Expr::Gte(
        Box::new(col("user_id")),
        Box::new(Expr::Literal(Literal::Int(2))),
    );
    assert_eq!(cdistinct(&txn, "user_id", Some(f)), 2);
}

#[test]
fn aggregates_group_by_and_having() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "a@example.com");
    insert_user(&mut txn, 2, "b@example.com");
    insert_order(&mut txn, 1, 1, 10.0);
    insert_order(&mut txn, 2, 1, 30.0);
    insert_order(&mut txn, 3, 2, 5.0);
    txn.commit().unwrap();

    let txn = db.begin().unwrap();

    // No group_by: whole table is one group.
    let q = AggregateQuery {
        table: "orders".into(),
        filter: None,
        group_by: vec![],
        aggregates: vec![
            Aggregate {
                func: AggFunc::Count,
                column: None,
                alias: "n".into(),
                distinct: false,
            },
            Aggregate {
                func: AggFunc::Sum,
                column: Some("total".into()),
                alias: "s".into(),
                distinct: false,
            },
            Aggregate {
                func: AggFunc::Min,
                column: Some("total".into()),
                alias: "mn".into(),
                distinct: false,
            },
            Aggregate {
                func: AggFunc::Max,
                column: Some("total".into()),
                alias: "mx".into(),
                distinct: false,
            },
            Aggregate {
                func: AggFunc::Avg,
                column: Some("total".into()),
                alias: "av".into(),
                distinct: false,
            },
        ],
        having: None,
    };
    let rows = txn.aggregate(&q).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.get("n"), Some(&json!(3)));
    assert_eq!(rows[0].values.get("s"), Some(&json!(45.0)));
    assert_eq!(rows[0].values.get("mn"), Some(&json!(5.0)));
    assert_eq!(rows[0].values.get("mx"), Some(&json!(30.0)));
    assert_eq!(rows[0].values.get("av"), Some(&json!(15.0)));

    // group_by user_id with HAVING n > 1 keeps only user 1.
    let q = AggregateQuery {
        table: "orders".into(),
        filter: None,
        group_by: vec!["user_id".into()],
        aggregates: vec![
            Aggregate {
                func: AggFunc::Count,
                column: None,
                alias: "n".into(),
                distinct: false,
            },
            Aggregate {
                func: AggFunc::Sum,
                column: Some("total".into()),
                alias: "s".into(),
                distinct: false,
            },
        ],
        having: Some(Expr::Gt(
            Box::new(col("n")),
            Box::new(Expr::Literal(Literal::Int(1))),
        )),
    };
    let rows = txn.aggregate(&q).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.get("user_id"), Some(&json!(1)));
    assert_eq!(rows[0].values.get("n"), Some(&json!(2)));
    assert_eq!(rows[0].values.get("s"), Some(&json!(40.0)));
}

#[test]
fn joins_inner_left_cross() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "a@example.com");
    insert_user(&mut txn, 2, "b@example.com");
    insert_user(&mut txn, 3, "c@example.com");
    insert_order(&mut txn, 1, 1, 10.0);
    insert_order(&mut txn, 2, 1, 30.0);
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let on = Some(Expr::Eq(Box::new(col("u.id")), Box::new(col("o.user_id"))));

    let inner = JoinQuery {
        table: "users".into(),
        alias: Some("u".into()),
        joins: vec![Join {
            kind: JoinKind::Inner,
            table: "orders".into(),
            alias: Some("o".into()),
            on: on.clone(),
        }],
        filter: None,
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let rows = txn.join(&inner).unwrap();
    assert_eq!(rows.len(), 2);
    for r in &rows {
        assert_eq!(r["o"]["user_id"], json!(1));
    }

    let left = JoinQuery {
        joins: vec![Join {
            kind: JoinKind::Left,
            table: "orders".into(),
            alias: Some("o".into()),
            on: on.clone(),
        }],
        ..inner.clone()
    };
    let rows = txn.join(&left).unwrap();
    assert_eq!(rows.len(), 4);
    let nulls = rows.iter().filter(|r| r["o"].is_null()).count();
    assert_eq!(nulls, 2);

    let cross = JoinQuery {
        joins: vec![Join {
            kind: JoinKind::Cross,
            table: "orders".into(),
            alias: Some("o".into()),
            on: None,
        }],
        ..inner
    };
    let rows = txn.join(&cross).unwrap();
    assert_eq!(rows.len(), 6); // 3 users x 2 orders
}

#[test]
fn select_distinct_dedupes_projection() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_named_user(&mut txn, 1, "a@example.com", "Dup");
    insert_named_user(&mut txn, 2, "b@example.com", "Dup");
    insert_named_user(&mut txn, 3, "c@example.com", "Other");
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let q = Query::Select(Select {
        table: "users".into(),
        columns: vec![col("name")],
        filter: None,
        order_by: vec![],
        limit: None,
        offset: None,
    });
    let rows = txn.select_distinct(&q).unwrap();
    let names: Vec<&Value> = rows.iter().map(|r| r.values.get("name").unwrap()).collect();
    assert_eq!(names, vec![&json!("Dup"), &json!("Other")]);
    assert!(rows.iter().all(|r| r.values.len() == 1));
}

#[test]
fn like_contains_and_not_in() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    insert_user(&mut txn, 2, "bob@test.com");
    insert_user(&mut txn, 3, "carol@example.com");
    txn.commit().unwrap();

    let txn = db.begin().unwrap();

    let like = txn
        .select(&Query::Select(select_all(
            "users",
            Some(Expr::Like(Box::new(col("email")), "%@example.com".into())),
        )))
        .unwrap();
    assert_eq!(like.len(), 2);

    let contains = txn
        .select(&Query::Select(select_all(
            "users",
            Some(Expr::Contains(Box::new(col("email")), "bob".into())),
        )))
        .unwrap();
    assert_eq!(contains.len(), 1);
    assert_eq!(contains[0].values.get("id"), Some(&json!(2)));

    let not_in = txn
        .select(&Query::Select(select_all(
            "users",
            Some(Expr::NotIn(
                Box::new(col("id")),
                vec![Literal::Int(1), Literal::Int(2)],
            )),
        )))
        .unwrap();
    assert_eq!(not_in.len(), 1);
    assert_eq!(not_in[0].values.get("id"), Some(&json!(3)));
}

#[test]
fn exists_and_in_subquery() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "a@example.com");
    insert_user(&mut txn, 2, "b@example.com");
    insert_user(&mut txn, 3, "c@example.com");
    insert_order(&mut txn, 1, 1, 150.0);
    insert_order(&mut txn, 2, 2, 50.0);
    txn.commit().unwrap();

    let txn = db.begin().unwrap();

    // id IN (SELECT user_id FROM orders WHERE total > 100) -> only user 1.
    let sub = Select {
        table: "orders".into(),
        columns: vec![col("user_id")],
        filter: Some(Expr::Gt(
            Box::new(col("total")),
            Box::new(Expr::Literal(Literal::Float(100.0))),
        )),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let rows = txn
        .select(&Query::Select(select_all(
            "users",
            Some(Expr::InSubquery(Box::new(col("id")), Box::new(sub.clone()))),
        )))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.get("id"), Some(&json!(1)));

    // EXISTS (orders with total > 100) -> all users (uncorrelated).
    let rows = txn
        .select(&Query::Select(select_all(
            "users",
            Some(Expr::Exists(Box::new(sub))),
        )))
        .unwrap();
    assert_eq!(rows.len(), 3);

    // NOT EXISTS (orders with total > 1000) -> all users.
    let none = Select {
        table: "orders".into(),
        columns: vec![],
        filter: Some(Expr::Gt(
            Box::new(col("total")),
            Box::new(Expr::Literal(Literal::Float(1000.0))),
        )),
        order_by: vec![],
        limit: None,
        offset: None,
    };
    let rows = txn
        .select(&Query::Select(select_all(
            "users",
            Some(Expr::NotExists(Box::new(none))),
        )))
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn cte_materializes_and_reads() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "a@example.com");
    insert_user(&mut txn, 2, "b@example.com");
    insert_order(&mut txn, 1, 1, 150.0);
    insert_order(&mut txn, 2, 2, 50.0);
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let cte = Cte {
        name: "big_orders".into(),
        query: Box::new(Select {
            table: "orders".into(),
            columns: vec![],
            filter: Some(Expr::Gt(
                Box::new(col("total")),
                Box::new(Expr::Literal(Literal::Float(100.0))),
            )),
            order_by: vec![OrderBy {
                expr: col("id"),
                direction: Direction::Asc,
            }],
            limit: None,
            offset: None,
        }),
    };
    let body = select_all("big_orders", None);
    let rows = txn.select_with(&[cte], &body).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.get("user_id"), Some(&json!(1)));
    assert_eq!(rows[0].values.get("total"), Some(&json!(150.0)));
}

#[test]
fn create_open_and_crud() {
    let dir = temp_dir();
    let schema = make_schema();
    {
        let db = Database::create(&dir, schema.clone()).unwrap();
        assert!(db.table("users").is_some());

        let mut txn = db.begin().unwrap();
        insert_user(&mut txn, 1, "alice@example.com");
        txn.commit().unwrap();

        let mut txn = db.begin().unwrap();
        let row = txn.get_by_pk("users", &json!(1)).unwrap().unwrap();
        assert_eq!(row.values.get("email"), Some(&json!("alice@example.com")));

        let mut patch = Map::new();
        patch.insert("name".into(), json!("Alice"));
        txn.update("users", &json!(1), patch).unwrap();
        txn.commit().unwrap();

        let txn = db.begin().unwrap();
        let row = txn.get_by_pk("users", &json!(1)).unwrap().unwrap();
        assert_eq!(row.values.get("name"), Some(&json!("Alice")));
    }

    // Re-open and verify persistence.
    let db = Database::open(&dir).unwrap();
    let txn = db.begin().unwrap();
    let row = txn.get_by_pk("users", &json!(1)).unwrap().unwrap();
    assert_eq!(row.values.get("name"), Some(&json!("Alice")));
}

#[test]
fn insert_returning_projects_requested_columns() {
    let dir = temp_dir();
    let db = Database::create(&dir, Schema::new(vec![users_table()]).unwrap()).unwrap();

    let mut row = Map::new();
    row.insert("id".into(), json!(1));
    row.insert("email".into(), json!("alice@example.com"));
    row.insert("name".into(), json!("Alice"));

    let mut txn = db.begin().unwrap();
    let returned = txn
        .insert_returning("users", row, vec!["id".into(), "name".into()])
        .unwrap();
    assert_eq!(returned, json!({"id": 1, "name": "Alice"}));
    txn.commit().unwrap();
}

#[test]
fn truncate_clears_rows_and_unique_guards() {
    let dir = temp_dir();
    let db = Database::create(&dir, Schema::new(vec![users_table()]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    insert_user(&mut txn, 2, "bob@example.com");
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    txn.truncate("users").unwrap();
    let rows = txn
        .select(&Query::Select(select_all("users", None)))
        .unwrap();
    assert!(rows.is_empty());
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let rows = txn
        .select(&Query::Select(select_all("users", None)))
        .unwrap();
    assert!(rows.is_empty());
    insert_user(&mut txn, 3, "alice@example.com");
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let rows = txn
        .select(&Query::Select(select_all("users", None)))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values.get("email"),
        Some(&json!("alice@example.com"))
    );
}

#[test]
fn truncate_rejects_referenced_table() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    let err = txn.truncate("users").unwrap_err();
    assert!(matches!(err, KitError::Restrict(_)));
}

#[test]
fn truncate_allows_repeat_in_same_transaction() {
    let dir = temp_dir();
    let db = Database::create(&dir, Schema::new(vec![users_table()]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    txn.truncate("users").unwrap();
    txn.truncate("users").unwrap(); // prior truncate is allowed
    txn.commit().unwrap();
}

#[test]
fn truncate_rejects_prior_data_write_in_same_transaction() {
    let dir = temp_dir();
    let db = Database::create(&dir, Schema::new(vec![users_table()]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "a@example.com");
    let err = txn.truncate("users").unwrap_err();
    assert!(matches!(err, KitError::Validation(_)));
}

#[test]
fn upsert_insert_do_nothing_and_do_update() {
    let dir = temp_dir();
    let db = Database::create(&dir, Schema::new(vec![users_table()]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    let mut row = Map::new();
    row.insert("id".into(), json!(1));
    row.insert("email".into(), json!("alice@example.com"));
    row.insert("name".into(), json!("Alice"));
    let inserted = txn
        .upsert(
            "users",
            row,
            OnConflict::DoNothing,
            vec!["id".into(), "name".into()],
        )
        .unwrap();
    assert_eq!(inserted, json!({"id": 1, "name": "Alice"}));
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let mut duplicate = Map::new();
    duplicate.insert("id".into(), json!(1));
    duplicate.insert("email".into(), json!("changed@example.com"));
    duplicate.insert("name".into(), json!("Ignored"));
    let unchanged = txn
        .upsert(
            "users",
            duplicate,
            OnConflict::DoNothing,
            vec!["email".into(), "name".into()],
        )
        .unwrap();
    assert_eq!(
        unchanged,
        json!({"email": "alice@example.com", "name": "Alice"})
    );
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let mut incoming = Map::new();
    incoming.insert("id".into(), json!(1));
    incoming.insert("email".into(), json!("alice@example.com"));
    incoming.insert("name".into(), json!("Incoming"));
    let mut patch = Map::new();
    patch.insert("name".into(), json!("Updated"));
    let updated = txn
        .upsert(
            "users",
            incoming,
            OnConflict::DoUpdate(patch),
            vec!["id".into(), "name".into()],
        )
        .unwrap();
    assert_eq!(updated, json!({"id": 1, "name": "Updated"}));
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let row = txn.get_by_pk("users", &json!(1)).unwrap().unwrap();
    assert_eq!(row.values.get("email"), Some(&json!("alice@example.com")));
    assert_eq!(row.values.get("name"), Some(&json!("Updated")));
}

#[test]
fn update_where_returns_post_images() {
    let dir = temp_dir();
    let db = Database::create(&dir, Schema::new(vec![users_table()]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    insert_user(&mut txn, 2, "bob@example.com");
    insert_user(&mut txn, 3, "carol@example.com");
    txn.commit().unwrap();

    let mut patch = Map::new();
    patch.insert("name".into(), json!("Updated"));
    let mut txn = db.begin().unwrap();
    let mut returned = txn
        .update_where(
            "users",
            Some(Expr::Gt(
                Box::new(col("id")),
                Box::new(Expr::Literal(Literal::Int(1))),
            )),
            patch,
            vec!["id".into(), "name".into()],
        )
        .unwrap();
    returned.sort_by_key(|v| v["id"].as_i64().unwrap());
    assert_eq!(
        returned,
        vec![
            json!({"id": 2, "name": "Updated"}),
            json!({"id": 3, "name": "Updated"})
        ]
    );
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    assert_eq!(
        txn.get_by_pk("users", &json!(1))
            .unwrap()
            .unwrap()
            .values
            .get("name"),
        Some(&Value::Null)
    );
    assert_eq!(
        txn.get_by_pk("users", &json!(2))
            .unwrap()
            .unwrap()
            .values
            .get("name"),
        Some(&json!("Updated"))
    );
}

#[test]
fn delete_where_returns_preimages_and_cascades() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    insert_order(&mut txn, 10, 1, 100.0);
    insert_order(&mut txn, 11, 1, 25.0);
    let mut item = Map::new();
    item.insert("id".into(), json!(1));
    item.insert("order_id".into(), json!(10));
    item.insert("sku".into(), json!("ABC"));
    txn.insert("items", item).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let returned = txn
        .delete_where(
            "orders",
            Some(Expr::Eq(
                Box::new(col("id")),
                Box::new(Expr::Literal(Literal::Int(10))),
            )),
            vec!["id".into(), "total".into()],
        )
        .unwrap();
    assert_eq!(returned, vec![json!({"id": 10, "total": 100.0})]);
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    assert!(txn.get_by_pk("orders", &json!(10)).unwrap().is_none());
    assert!(txn.get_by_pk("items", &json!(1)).unwrap().is_none());
    assert!(txn.get_by_pk("orders", &json!(11)).unwrap().is_some());
}

#[test]
fn select_filters_and_orders() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    insert_user(&mut txn, 2, "bob@example.com");
    insert_user(&mut txn, 3, "carol@example.com");
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let q = mongreldb_kit::Query::Select(mongreldb_kit::Select {
        table: "users".into(),
        columns: vec![mongreldb_kit::Expr::Column("id".into())],
        filter: Some(mongreldb_kit::Expr::Gt(
            Box::new(mongreldb_kit::Expr::Column("id".into())),
            Box::new(mongreldb_kit::Expr::Literal(mongreldb_kit::Literal::Int(1))),
        )),
        order_by: vec![mongreldb_kit::OrderBy {
            expr: mongreldb_kit::Expr::Column("id".into()),
            direction: mongreldb_kit::Direction::Desc,
        }],
        limit: Some(2),
        offset: None,
    });
    let rows = txn.select(&q).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values.get("id"), Some(&json!(3)));
    assert_eq!(rows[1].values.get("id"), Some(&json!(2)));
}

#[test]
fn unique_constraint_violation() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    let mut row = Map::new();
    row.insert("id".into(), json!(2));
    row.insert("email".into(), json!("alice@example.com"));
    let err = txn.insert("users", row).unwrap_err();
    assert!(matches!(err, KitError::Duplicate(_)));
}

#[test]
fn foreign_key_violation() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    let mut order = Map::new();
    order.insert("id".into(), json!(1));
    order.insert("user_id".into(), json!(99));
    order.insert("total".into(), json!(10.0));
    let err = txn.insert("orders", order).unwrap_err();
    assert!(matches!(err, KitError::ForeignKey(_)));
}

#[test]
fn restrict_delete_blocks() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    let mut order = Map::new();
    order.insert("id".into(), json!(1));
    order.insert("user_id".into(), json!(1));
    order.insert("total".into(), json!(10.0));
    txn.insert("orders", order).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let err = txn.delete("users", &json!(1)).unwrap_err();
    assert!(matches!(err, KitError::Restrict(_)));
}

#[test]
fn cascade_delete_removes_children() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    let mut order = Map::new();
    order.insert("id".into(), json!(1));
    order.insert("user_id".into(), json!(1));
    order.insert("total".into(), json!(10.0));
    txn.insert("orders", order).unwrap();
    let mut item = Map::new();
    item.insert("id".into(), json!(1));
    item.insert("order_id".into(), json!(1));
    item.insert("sku".into(), json!("ABC"));
    txn.insert("items", item).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    txn.delete("orders", &json!(1)).unwrap();
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    assert!(txn.get_by_pk("orders", &json!(1)).unwrap().is_none());
    assert!(txn.get_by_pk("items", &json!(1)).unwrap().is_none());
}

#[test]
fn migrate_records_versions() {
    let dir = temp_dir();
    let schema = Schema::new(vec![users_table()]).unwrap();
    let mut db = Database::create(&dir, schema).unwrap();

    let migrations = vec![
        Migration {
            version: 1,
            name: "init".into(),
            ops: vec![MigrationOp::CreateTable {
                name: "users".into(),
            }],
        },
        Migration {
            version: 2,
            name: "add_orders".into(),
            ops: vec![MigrationOp::CreateTable {
                name: "orders".into(),
            }],
        },
    ];

    // Re-create schema with orders so the migration runner can create it.
    let schema2 = make_schema();
    db.set_schema(schema2);
    mongreldb_kit::migrate(&mut db, &migrations).unwrap();

    let txn = db.begin().unwrap();
    assert!(txn.get_by_pk("users", &json!(1)).unwrap().is_none());
}

// ── migration ops: unique / foreign-key backfill, drop table ────────────────

fn people_table(with_unique: bool) -> Table {
    Table {
        id: 20,
        name: "people".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "email", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: if with_unique {
            vec![UniqueConstraint {
                name: "uq_people_email".into(),
                columns: vec!["email".into()],
            }]
        } else {
            vec![]
        },
        check_constraints: vec![],
    }
}

fn people_table_with_index(index: Option<Index>) -> Table {
    Table {
        indexes: index.into_iter().collect(),
        ..people_table(false)
    }
}

fn people_table_without_email() -> Table {
    Table {
        columns: vec![Column::new(1, "id", ColumnType::Int64)],
        primary_key: vec!["id".into()],
        ..people_table(false)
    }
}

fn insert_person(txn: &mut Transaction, id: i64, email: &str) -> Result<Row, KitError> {
    let mut row = Map::new();
    row.insert("id".into(), json!(id));
    row.insert("email".into(), json!(email));
    txn.insert("people", row)
}

fn widgets_table(value_name: &str, value_type: ColumnType, nullable: bool) -> Table {
    Table {
        id: 30,
        name: "widgets".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column {
                nullable,
                ..Column::new(2, value_name, value_type)
            },
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }
}

fn alter_widget_value_migration(column: &str) -> Vec<Migration> {
    vec![Migration {
        version: 1,
        name: "alter_widget_value".into(),
        ops: vec![MigrationOp::AlterColumn {
            table: "widgets".into(),
            column: column.into(),
        }],
    }]
}

fn core_widget_column(db: &Database, name: &str) -> mongreldb_core::schema::ColumnDef {
    let handle = db.raw().table("widgets").unwrap();
    let guard = handle.lock();
    guard.schema().column(name).unwrap().clone()
}

#[test]
fn migrate_add_unique_backfills_and_enforces() {
    let dir = temp_dir();
    let mut db = Database::create(&dir, Schema::new(vec![people_table(false)]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_person(&mut txn, 1, "a@example.com").unwrap();
    insert_person(&mut txn, 2, "b@example.com").unwrap();
    txn.commit().unwrap();

    // Add the constraint; the runner backfills guards for the existing rows.
    db.set_schema(Schema::new(vec![people_table(true)]).unwrap());
    let migrations = vec![Migration {
        version: 1,
        name: "add_people_unique".into(),
        ops: vec![MigrationOp::AddUnique {
            table: "people".into(),
            constraint: "uq_people_email".into(),
        }],
    }];
    mongreldb_kit::migrate(&mut db, &migrations).unwrap();

    // A duplicate email is now rejected.
    let mut txn = db.begin().unwrap();
    let err = insert_person(&mut txn, 3, "a@example.com").unwrap_err();
    assert!(matches!(err, KitError::Duplicate(_)));
    txn.rollback();

    // A fresh email is still accepted.
    let mut txn = db.begin().unwrap();
    insert_person(&mut txn, 4, "c@example.com").unwrap();
    txn.commit().unwrap();
}

#[test]
fn migrate_add_unique_rejects_existing_duplicates() {
    let dir = temp_dir();
    let mut db = Database::create(&dir, Schema::new(vec![people_table(false)]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_person(&mut txn, 1, "dup@example.com").unwrap();
    insert_person(&mut txn, 2, "dup@example.com").unwrap();
    txn.commit().unwrap();

    db.set_schema(Schema::new(vec![people_table(true)]).unwrap());
    let migrations = vec![Migration {
        version: 1,
        name: "add_people_unique".into(),
        ops: vec![MigrationOp::AddUnique {
            table: "people".into(),
            constraint: "uq_people_email".into(),
        }],
    }];
    let err = mongreldb_kit::migrate(&mut db, &migrations).unwrap_err();
    assert!(matches!(err, KitError::Migration(_)));
}

#[test]
fn migrate_alter_column_renames_native_column() {
    let dir = temp_dir();
    let v1 = widgets_table("label", ColumnType::Text, false);
    let v2 = widgets_table("name", ColumnType::Text, false);
    let mut db = Database::create(&dir, Schema::new(vec![v1]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    let mut row = Map::new();
    row.insert("id".into(), json!(1));
    row.insert("label".into(), json!("one"));
    txn.insert("widgets", row).unwrap();
    txn.commit().unwrap();

    db.set_schema(Schema::new(vec![v2]).unwrap());
    mongreldb_kit::migrate(&mut db, &alter_widget_value_migration("label")).unwrap();

    assert!(db
        .raw()
        .table("widgets")
        .unwrap()
        .lock()
        .schema()
        .column("label")
        .is_none());
    assert_eq!(core_widget_column(&db, "name").id, 2);

    let txn = db.begin().unwrap();
    let row = txn.get_by_pk("widgets", &json!(1)).unwrap().unwrap();
    assert_eq!(row.values.get("name"), Some(&json!("one")));
}

#[test]
fn migrate_alter_column_changes_native_type_on_empty_table() {
    let dir = temp_dir();
    let v1 = widgets_table("qty", ColumnType::Int64, false);
    let v2 = widgets_table("qty", ColumnType::Float64, false);
    let mut db = Database::create(&dir, Schema::new(vec![v1]).unwrap()).unwrap();

    db.set_schema(Schema::new(vec![v2]).unwrap());
    mongreldb_kit::migrate(&mut db, &alter_widget_value_migration("qty")).unwrap();

    assert_eq!(core_widget_column(&db, "qty").ty, TypeId::Float64);

    let mut txn = db.begin().unwrap();
    let mut row = Map::new();
    row.insert("id".into(), json!(1));
    row.insert("qty".into(), json!(1.5));
    txn.insert("widgets", row).unwrap();
    txn.commit().unwrap();
}

#[test]
fn migrate_alter_column_drops_not_null() {
    let dir = temp_dir();
    let v1 = widgets_table("name", ColumnType::Text, false);
    let v2 = widgets_table("name", ColumnType::Text, true);
    let mut db = Database::create(&dir, Schema::new(vec![v1]).unwrap()).unwrap();

    db.set_schema(Schema::new(vec![v2]).unwrap());
    mongreldb_kit::migrate(&mut db, &alter_widget_value_migration("name")).unwrap();

    assert!(core_widget_column(&db, "name")
        .flags
        .contains(ColumnFlags::NULLABLE));

    let mut txn = db.begin().unwrap();
    let mut row = Map::new();
    row.insert("id".into(), json!(1));
    row.insert("name".into(), Value::Null);
    txn.insert("widgets", row).unwrap();
    txn.commit().unwrap();
}

#[test]
fn migrate_alter_column_rejects_set_not_null_with_existing_nulls() {
    let dir = temp_dir();
    let v1 = widgets_table("name", ColumnType::Text, true);
    let v2 = widgets_table("name", ColumnType::Text, false);
    let mut db = Database::create(&dir, Schema::new(vec![v1]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    let mut row = Map::new();
    row.insert("id".into(), json!(1));
    row.insert("name".into(), Value::Null);
    txn.insert("widgets", row).unwrap();
    txn.commit().unwrap();

    db.set_schema(Schema::new(vec![v2]).unwrap());
    let err = mongreldb_kit::migrate(&mut db, &alter_widget_value_migration("name")).unwrap_err();
    assert!(matches!(err, KitError::Validation(_)));
}

fn owners_table() -> Table {
    Table {
        id: 21,
        name: "owners".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "name", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }
}

fn pets_table(with_fk: bool) -> Table {
    Table {
        id: 22,
        name: "pets".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "owner_id", ColumnType::Int64),
            Column::new(3, "name", ColumnType::Text),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: if with_fk {
            vec![ForeignKey {
                name: "fk_pets_owner".into(),
                columns: vec!["owner_id".into()],
                references_table: "owners".into(),
                references_columns: vec!["id".into()],
                on_delete: ForeignKeyAction::Restrict,
            }]
        } else {
            vec![]
        },
        unique_constraints: vec![],
        check_constraints: vec![],
    }
}

fn insert_pet(txn: &mut Transaction, id: i64, owner_id: i64, name: &str) -> Result<Row, KitError> {
    let mut row = Map::new();
    row.insert("id".into(), json!(id));
    row.insert("owner_id".into(), json!(owner_id));
    row.insert("name".into(), json!(name));
    txn.insert("pets", row)
}

#[test]
fn migrate_add_foreign_key_backfills_and_enforces() {
    let dir = temp_dir();
    let mut db = Database::create(
        &dir,
        Schema::new(vec![owners_table(), pets_table(false)]).unwrap(),
    )
    .unwrap();

    let mut txn = db.begin().unwrap();
    let mut owner = Map::new();
    owner.insert("id".into(), json!(1));
    owner.insert("name".into(), json!("Ada"));
    txn.insert("owners", owner).unwrap();
    insert_pet(&mut txn, 1, 1, "Rex").unwrap();
    txn.commit().unwrap();

    db.set_schema(Schema::new(vec![owners_table(), pets_table(true)]).unwrap());
    let migrations = vec![Migration {
        version: 1,
        name: "add_pets_fk".into(),
        ops: vec![MigrationOp::AddForeignKey {
            table: "pets".into(),
            constraint: "fk_pets_owner".into(),
        }],
    }];
    mongreldb_kit::migrate(&mut db, &migrations).unwrap();

    // FK now enforced: a child referencing a missing parent is rejected.
    let mut txn = db.begin().unwrap();
    let err = insert_pet(&mut txn, 2, 99, "Lost").unwrap_err();
    assert!(matches!(err, KitError::ForeignKey(_)));
    txn.rollback();

    // A valid child is still accepted.
    let mut txn = db.begin().unwrap();
    insert_pet(&mut txn, 3, 1, "Spot").unwrap();
    txn.commit().unwrap();
}

#[test]
fn migrate_add_foreign_key_rejects_orphans() {
    let dir = temp_dir();
    let mut db = Database::create(
        &dir,
        Schema::new(vec![owners_table(), pets_table(false)]).unwrap(),
    )
    .unwrap();

    let mut txn = db.begin().unwrap();
    let mut owner = Map::new();
    owner.insert("id".into(), json!(1));
    owner.insert("name".into(), json!("Ada"));
    txn.insert("owners", owner).unwrap();
    // Orphan child: no owner 42 exists.
    insert_pet(&mut txn, 1, 42, "Orphan").unwrap();
    txn.commit().unwrap();

    db.set_schema(Schema::new(vec![owners_table(), pets_table(true)]).unwrap());
    let migrations = vec![Migration {
        version: 1,
        name: "add_pets_fk".into(),
        ops: vec![MigrationOp::AddForeignKey {
            table: "pets".into(),
            constraint: "fk_pets_owner".into(),
        }],
    }];
    let err = mongreldb_kit::migrate(&mut db, &migrations).unwrap_err();
    assert!(matches!(err, KitError::ForeignKey(_)));
}

#[test]
fn migrate_drop_table_removes_table() {
    let dir = temp_dir();
    let mut db = Database::create(&dir, Schema::new(vec![people_table(false)]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_person(&mut txn, 1, "a@example.com").unwrap();
    txn.commit().unwrap();

    assert!(db.raw().table_id("people").is_ok());

    // Desired schema no longer contains the table.
    db.set_schema(Schema::new(vec![]).unwrap());
    let migrations = vec![Migration {
        version: 1,
        name: "drop_people".into(),
        ops: vec![MigrationOp::DropTable {
            name: "people".into(),
        }],
    }];
    mongreldb_kit::migrate(&mut db, &migrations).unwrap();

    assert!(db.raw().table_id("people").is_err());
    assert!(db.table("people").is_none());
}

#[test]
fn migrate_add_index_rebuilds_table_and_preserves_rows() {
    let dir = temp_dir();
    let mut db = Database::create(&dir, Schema::new(vec![people_table(false)]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_person(&mut txn, 1, "a@example.com").unwrap();
    insert_person(&mut txn, 2, "b@example.com").unwrap();
    txn.commit().unwrap();

    db.set_schema(
        Schema::new(vec![people_table_with_index(Some(Index {
            name: "idx_people_email".into(),
            columns: vec!["email".into()],
            unique: false,
            kind: Default::default(),
            ann_quantization: Default::default(),
            ..Default::default()
        }))])
        .unwrap(),
    );
    let migrations = vec![Migration {
        version: 1,
        name: "add_email_index".into(),
        ops: vec![MigrationOp::AddIndex {
            table: "people".into(),
            index: "idx_people_email".into(),
        }],
    }];
    mongreldb_kit::migrate(&mut db, &migrations).unwrap();

    let schema = db.raw().table("people").unwrap().lock().schema().clone();
    assert_eq!(schema.indexes.len(), 1);
    assert_eq!(schema.indexes[0].name, "idx_people_email_email");

    let txn = db.begin().unwrap();
    let row = txn.get_by_pk("people", &json!(1)).unwrap().unwrap();
    assert_eq!(row.values.get("email"), Some(&json!("a@example.com")));
}

#[test]
fn migrate_drop_index_rebuilds_table_and_preserves_rows() {
    let dir = temp_dir();
    let indexed = people_table_with_index(Some(Index {
        name: "idx_people_email".into(),
        columns: vec!["email".into()],
        unique: false,
        kind: Default::default(),
        ann_quantization: Default::default(),
        ..Default::default()
    }));
    let mut db = Database::create(&dir, Schema::new(vec![indexed]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_person(&mut txn, 1, "a@example.com").unwrap();
    txn.commit().unwrap();

    db.set_schema(Schema::new(vec![people_table(false)]).unwrap());
    let migrations = vec![Migration {
        version: 1,
        name: "drop_email_index".into(),
        ops: vec![MigrationOp::DropIndex {
            table: "people".into(),
            index: "idx_people_email".into(),
        }],
    }];
    mongreldb_kit::migrate(&mut db, &migrations).unwrap();

    let schema = db.raw().table("people").unwrap().lock().schema().clone();
    assert!(schema.indexes.is_empty());

    let txn = db.begin().unwrap();
    let row = txn.get_by_pk("people", &json!(1)).unwrap().unwrap();
    assert_eq!(row.values.get("email"), Some(&json!("a@example.com")));
}

#[test]
fn migrate_drop_column_rebuilds_table_and_removes_stale_values() {
    let dir = temp_dir();
    let mut db = Database::create(&dir, Schema::new(vec![people_table(false)]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_person(&mut txn, 1, "a@example.com").unwrap();
    txn.commit().unwrap();

    db.set_schema(Schema::new(vec![people_table_without_email()]).unwrap());
    let migrations = vec![Migration {
        version: 1,
        name: "drop_email".into(),
        ops: vec![MigrationOp::DropColumn {
            table: "people".into(),
            column: "email".into(),
        }],
    }];
    mongreldb_kit::migrate(&mut db, &migrations).unwrap();

    let schema = db.raw().table("people").unwrap().lock().schema().clone();
    assert!(schema.column("email").is_none());

    let txn = db.begin().unwrap();
    let row = txn.get_by_pk("people", &json!(1)).unwrap().unwrap();
    assert_eq!(row.values.get("id"), Some(&json!(1)));
    assert!(row.values.get("email").is_none());
}

#[test]
fn migrate_raw_sql_runs_through_the_sql_surface() {
    // RawSql previously errored (the kit had no SQL surface); it now runs
    // through the embedded MongrelSession. A VACUUM is a no-op DDL statement
    // that should succeed and leave the migration recorded as applied.
    let dir = temp_dir();
    let mut db = Database::create(&dir, Schema::new(vec![people_table(false)]).unwrap()).unwrap();

    let migrations = vec![Migration {
        version: 1,
        name: "raw".into(),
        ops: vec![MigrationOp::RawSql("VACUUM".into())],
    }];
    mongreldb_kit::migrate(&mut db, &migrations).expect("RawSql migration should succeed");

    // The migration is recorded as applied (re-migrating is a no-op).
    let applied = db.applied_migrations().unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version, 1);
}

#[test]
fn migrate_create_view_and_drop_view_round_trip() {
    use mongreldb_kit_core::ViewSpec;

    let dir = temp_dir();
    let mut db = Database::create(&dir, make_schema()).unwrap();
    {
        let mut txn = db.begin().unwrap();
        insert_user(&mut txn, 1, "a@x.com");
        insert_user(&mut txn, 2, "b@x.com");
        txn.commit().unwrap();
    }

    // CreateView via migration: the view lands in the kit's long-lived SQL
    // session and is queryable afterward.
    let migrations = vec![Migration {
        version: 1,
        name: "add_view".into(),
        ops: vec![MigrationOp::CreateView {
            name: "v".into(),
            view: ViewSpec::new("v", "SELECT id, email FROM users WHERE id >= 2"),
        }],
    }];
    mongreldb_kit::migrate(&mut db, &migrations).expect("view migration should succeed");

    let rows = db.sql_rows("SELECT * FROM v ORDER BY id").unwrap();
    assert_eq!(
        rows,
        vec![json!({"id": 2, "email": "b@x.com"})
            .as_object()
            .unwrap()
            .clone()]
    );

    // DropView via migration: the view is gone; subsequent SELECTs error.
    let drop_migrations = vec![Migration {
        version: 2,
        name: "drop_view".into(),
        ops: vec![MigrationOp::DropView { name: "v".into() }],
    }];
    mongreldb_kit::migrate(&mut db, &drop_migrations).expect("drop view should succeed");
    assert!(db.sql_rows("SELECT * FROM v").is_err());
}

#[test]
fn truncate_then_reuse_pk() {
    let dir = temp_dir();
    let db = Database::create(&dir, Schema::new(vec![users_table()]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    txn.truncate("users").unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "alice@example.com");
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let rows = txn
        .select(&Query::Select(select_all("users", None)))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.get("id"), Some(&json!(1)));
}

#[test]
fn truncate_conflicts_with_concurrent_insert() {
    let dir = temp_dir();
    let db = Database::create(&dir, Schema::new(vec![users_table()]).unwrap()).unwrap();
    let core_db = db.raw();

    let mut seed = core_db.begin();
    seed.put(
        "users",
        vec![
            (1, CoreValue::Int64(1)),
            (2, CoreValue::Bytes(b"alice@example.com".to_vec())),
        ],
    )
    .unwrap();
    seed.commit().unwrap();

    // The insert transaction opens before the truncate commits, so its read
    // snapshot predates the truncate. Committing the insert afterwards must
    // fail with a write-write conflict against the table-scope truncate key.
    let mut insert = core_db.begin();
    insert
        .put(
            "users",
            vec![
                (1, CoreValue::Int64(1)),
                (2, CoreValue::Bytes(b"bob@example.com".to_vec())),
            ],
        )
        .unwrap();

    let mut truncate = core_db.begin();
    truncate.truncate("users").unwrap();
    truncate.commit().unwrap();

    let err = insert.commit().unwrap_err();
    assert!(
        matches!(err, CoreError::Conflict(_)),
        "expected conflict, got {err:?}"
    );
}

#[test]
fn update_where_multi_row_returning() {
    let dir = temp_dir();
    let db = Database::create(&dir, Schema::new(vec![users_table()]).unwrap()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_named_user(&mut txn, 1, "a@example.com", "A");
    insert_named_user(&mut txn, 2, "b@example.com", "B");
    insert_named_user(&mut txn, 3, "c@example.com", "C");
    txn.commit().unwrap();

    let mut patch = Map::new();
    patch.insert("name".into(), json!("Updated"));

    let mut txn = db.begin().unwrap();
    let returned = txn
        .execute(&Query::Update(Update {
            table: "users".into(),
            set: patch,
            filter: Some(Expr::Gt(
                Box::new(col("id")),
                Box::new(Expr::Literal(Literal::Int(1))),
            )),
            returning: vec!["id".into(), "name".into()],
            pk: None,
        }))
        .unwrap();
    assert_eq!(returned.len(), 2);

    let mut returned = returned;
    returned.sort_by_key(|v| v["id"].as_i64().unwrap());
    assert_eq!(
        returned,
        vec![
            json!({"id": 2, "name": "Updated"}),
            json!({"id": 3, "name": "Updated"})
        ]
    );
    txn.commit().unwrap();
}

#[test]
fn delete_where_multi_row_cascade() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();

    let mut txn = db.begin().unwrap();
    insert_user(&mut txn, 1, "user@example.com");
    insert_order(&mut txn, 10, 1, 100.0);
    insert_order(&mut txn, 11, 1, 50.0);

    let mut item = Map::new();
    item.insert("id".into(), json!(1));
    item.insert("order_id".into(), json!(10));
    item.insert("sku".into(), json!("ABC"));
    txn.insert("items", item).unwrap();

    let mut item = Map::new();
    item.insert("id".into(), json!(2));
    item.insert("order_id".into(), json!(11));
    item.insert("sku".into(), json!("DEF"));
    txn.insert("items", item).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin().unwrap();
    let returned = txn
        .execute(&Query::Delete(Delete {
            table: "orders".into(),
            filter: Some(Expr::Gte(
                Box::new(col("id")),
                Box::new(Expr::Literal(Literal::Int(10))),
            )),
            returning: vec!["id".into()],
            pk: None,
        }))
        .unwrap();
    assert_eq!(returned.len(), 2);

    let mut returned = returned;
    returned.sort_by_key(|v| v["id"].as_i64().unwrap());
    assert_eq!(returned, vec![json!({"id": 10}), json!({"id": 11})]);
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    assert!(txn.get_by_pk("orders", &json!(10)).unwrap().is_none());
    assert!(txn.get_by_pk("orders", &json!(11)).unwrap().is_none());
    assert!(txn.get_by_pk("items", &json!(1)).unwrap().is_none());
    assert!(txn.get_by_pk("items", &json!(2)).unwrap().is_none());
}

#[test]
fn sql_surface_runs_selects_and_returns_rows() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();
    {
        let mut txn = db.begin().unwrap();
        insert_user(&mut txn, 1, "a@x.com");
        insert_user(&mut txn, 2, "b@x.com");
        txn.commit().unwrap();
    }

    // sql_rows: read path returns JSON-style rows.
    let rows = db
        .sql_rows("SELECT id, email FROM users ORDER BY id")
        .unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "email": "a@x.com"})
                .as_object()
                .unwrap()
                .clone(),
            json!({"id": 2, "email": "b@x.com"})
                .as_object()
                .unwrap()
                .clone(),
        ]
    );

    // sql: returns raw Arrow record batches with the same content.
    let batches = db.sql("SELECT id FROM users ORDER BY id").unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    // sql_arrow: returns non-empty IPC bytes that round-trip through the
    // shared decoder.
    let ipc = db.sql_arrow("SELECT id FROM users ORDER BY id").unwrap();
    assert!(!ipc.is_empty());
    let decoded = mongreldb_kit::arrow_util::read_arrow_ipc(&ipc).unwrap();
    assert_eq!(decoded.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[test]
fn sql_surface_runs_ddl_and_views() {
    let dir = temp_dir();
    let db = Database::create(&dir, make_schema()).unwrap();
    {
        let mut txn = db.begin().unwrap();
        insert_user(&mut txn, 1, "a@x.com");
        insert_user(&mut txn, 2, "b@x.com");
        txn.commit().unwrap();
    }

    // CREATE VIEW returns no rows (DDL).
    assert!(db
        .sql_rows("CREATE VIEW active_users AS SELECT id, email FROM users WHERE id >= 2")
        .unwrap()
        .is_empty());

    // SELECT * FROM <view> resolves via the view's defining SQL.
    let rows = db
        .sql_rows("SELECT * FROM active_users ORDER BY id")
        .unwrap();
    assert_eq!(
        rows,
        vec![json!({"id": 2, "email": "b@x.com"})
            .as_object()
            .unwrap()
            .clone()]
    );

    // DROP VIEW removes it; the subsequent SELECT errors cleanly.
    assert!(db.sql_rows("DROP VIEW active_users").is_ok());
    assert!(db.sql_rows("SELECT * FROM active_users").is_err());
}
