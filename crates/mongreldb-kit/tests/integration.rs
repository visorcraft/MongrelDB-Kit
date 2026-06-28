use mongreldb_kit::{
    AggFunc, Aggregate, AggregateQuery, Column, ColumnType, Cte, Database, Direction, Expr,
    ForeignKey, ForeignKeyAction, Index, Join, JoinKind, JoinQuery, KitError, Literal, Migration,
    MigrationOp, OrderBy, Query, Row, Schema, Select, Table, Transaction, UniqueConstraint,
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
            Aggregate { func: AggFunc::Count, column: None, alias: "n".into() },
            Aggregate { func: AggFunc::Sum, column: Some("total".into()), alias: "s".into() },
            Aggregate { func: AggFunc::Min, column: Some("total".into()), alias: "mn".into() },
            Aggregate { func: AggFunc::Max, column: Some("total".into()), alias: "mx".into() },
            Aggregate { func: AggFunc::Avg, column: Some("total".into()), alias: "av".into() },
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
            Aggregate { func: AggFunc::Count, column: None, alias: "n".into() },
            Aggregate { func: AggFunc::Sum, column: Some("total".into()), alias: "s".into() },
        ],
        having: Some(Expr::Gt(Box::new(col("n")), Box::new(Expr::Literal(Literal::Int(1))))),
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
            Some(Expr::NotIn(Box::new(col("id")), vec![Literal::Int(1), Literal::Int(2)])),
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
        filter: Some(Expr::Gt(Box::new(col("total")), Box::new(Expr::Literal(Literal::Float(100.0))))),
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
        filter: Some(Expr::Gt(Box::new(col("total")), Box::new(Expr::Literal(Literal::Float(1000.0))))),
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
            order_by: vec![OrderBy { expr: col("id"), direction: Direction::Asc }],
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
            ops: vec![MigrationOp::CreateTable { name: "users".into() }],
        },
        Migration {
            version: 2,
            name: "add_orders".into(),
            ops: vec![MigrationOp::CreateTable { name: "orders".into() }],
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

fn insert_person(txn: &mut Transaction, id: i64, email: &str) -> Result<Row, KitError> {
    let mut row = Map::new();
    row.insert("id".into(), json!(id));
    row.insert("email".into(), json!(email));
    txn.insert("people", row)
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
    let mut db =
        Database::create(&dir, Schema::new(vec![owners_table(), pets_table(false)]).unwrap())
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
    let mut db =
        Database::create(&dir, Schema::new(vec![owners_table(), pets_table(false)]).unwrap())
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
fn migrate_unsupported_op_errors_clearly() {
    let dir = temp_dir();
    let mut db = Database::create(&dir, Schema::new(vec![people_table(false)]).unwrap()).unwrap();

    let migrations = vec![Migration {
        version: 1,
        name: "raw".into(),
        ops: vec![MigrationOp::AddIndex {
            table: "people".into(),
            index: "idx_email".into(),
        }],
    }];
    let err = mongreldb_kit::migrate(&mut db, &migrations).unwrap_err();
    match err {
        KitError::Migration(msg) => assert!(msg.contains("add_index")),
        other => panic!("expected migration error, got {other:?}"),
    }
}
