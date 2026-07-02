use mongreldb_kit::{Column, ColumnType, Schema, Table};
use serde_json::{json, Map, Value};

pub fn users_schema() -> Schema {
    Schema::new(vec![Table {
        id: 1,
        name: "users".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "name", ColumnType::Text),
            Column::new(3, "cost", ColumnType::Float64),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap()
}

pub fn row(id: i64, name: &str, cost: f64) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("id".into(), json!(id));
    m.insert("name".into(), json!(name));
    m.insert("cost".into(), json!(cost));
    m
}
