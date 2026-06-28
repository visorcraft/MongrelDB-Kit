use mongreldb_kit::{
    Column, ColumnType, Database, ForeignKey, ForeignKeyAction, Index, KitError, Migration,
    MigrationOp, Row, Schema, Table, Transaction, UniqueConstraint,
};
use serde_json::{json, Map};
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
