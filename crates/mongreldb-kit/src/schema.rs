//! Schema/value conversion between the kit model and MongrelDB core.

use crate::error::{KitError, Result};
use mongreldb_core::memtable::Value as CoreValue;
use mongreldb_core::schema::{
    ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema as CoreSchema, TypeId,
};
use mongreldb_kit_core::schema::{ColumnType, Table as KitTable};
use serde_json::{Map, Value};

/// Convert a kit table to a core schema.
pub fn to_core_schema(table: &KitTable) -> CoreSchema {
    let columns: Vec<ColumnDef> = table
        .columns
        .iter()
        .map(|c| {
            let mut flags = ColumnFlags::empty();
            if c.nullable {
                flags = flags.with(ColumnFlags::NULLABLE);
            }
            if table.primary_key.contains(&c.name) || c.primary_key {
                flags = flags.with(ColumnFlags::PRIMARY_KEY);
            }
            ColumnDef {
                id: c.id as u16,
                name: c.name.clone(),
                ty: to_core_type(c.storage_type),
                flags,
            }
        })
        .collect();

    let mut indexes: Vec<IndexDef> = Vec::new();
    for idx in &table.indexes {
        for col_name in &idx.columns {
            if let Some(col) = table.column(col_name) {
                indexes.push(IndexDef {
                    name: format!("{}_{}", idx.name, col_name),
                    column_id: col.id as u16,
                    kind: IndexKind::Bitmap,
                });
            }
        }
    }
    for uq in &table.unique_constraints {
        for col_name in &uq.columns {
            if let Some(col) = table.column(col_name) {
                indexes.push(IndexDef {
                    name: format!("uq_{}_{}", uq.name, col_name),
                    column_id: col.id as u16,
                    kind: IndexKind::Bitmap,
                });
            }
        }
    }

    CoreSchema {
        schema_id: table.id as u64,
        columns,
        indexes,
        colocation: Vec::new(),
    }
}

pub(crate) fn to_core_type(ty: ColumnType) -> TypeId {
    match ty {
        ColumnType::Bool => TypeId::Bool,
        ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64 => {
            TypeId::Int64
        }
        ColumnType::Float32 | ColumnType::Float64 => TypeId::Float64,
        ColumnType::Text
        | ColumnType::Bytes
        | ColumnType::Json
        | ColumnType::Date
        | ColumnType::DateTime => TypeId::Bytes,
        ColumnType::TimestampNanos => TypeId::Int64,
    }
}

/// Convert a JSON value to a core cell value using the column type for guidance.
pub fn json_to_core(value: &Value, ty: ColumnType) -> Result<CoreValue> {
    Ok(match value {
        Value::Null => CoreValue::Null,
        Value::Bool(b) => CoreValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                CoreValue::Int64(i)
            } else {
                CoreValue::Float64(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::String(s) => CoreValue::Bytes(s.as_bytes().to_vec()),
        Value::Array(arr) => {
            if ty == ColumnType::Bytes {
                let mut bytes = Vec::with_capacity(arr.len());
                for v in arr {
                    match v {
                        Value::Number(n) => bytes.push(n.as_i64().unwrap_or(0) as u8),
                        _ => return Err(KitError::Validation("bytes array expected".into())),
                    }
                }
                CoreValue::Bytes(bytes)
            } else {
                CoreValue::Bytes(serde_json::to_vec(value)?)
            }
        }
        Value::Object(_) => CoreValue::Bytes(serde_json::to_vec(value)?),
    })
}

/// Convert a core cell value back to JSON, guided by the column type.
pub fn core_to_json(value: &CoreValue, ty: ColumnType) -> Result<Value> {
    Ok(match (value, ty) {
        (CoreValue::Null, _) => Value::Null,
        (CoreValue::Bool(b), _) => Value::Bool(*b),
        (CoreValue::Int64(i), ColumnType::Int8) => Value::Number((*i as i8).into()),
        (CoreValue::Int64(i), ColumnType::Int16) => Value::Number((*i as i16).into()),
        (CoreValue::Int64(i), ColumnType::Int32) => Value::Number((*i as i32).into()),
        (CoreValue::Int64(i), ColumnType::Int64) => Value::Number((*i).into()),
        (CoreValue::Int64(i), ColumnType::TimestampNanos) => Value::Number((*i).into()),
        (CoreValue::Int64(i), _) => Value::Number((*i).into()),
        (CoreValue::Float64(f), ColumnType::Float32) => serde_json::to_value(*f as f32)?,
        (CoreValue::Float64(f), _) => serde_json::to_value(*f)?,
        (CoreValue::Bytes(b), ColumnType::Bytes) => {
            Value::Array(b.iter().map(|x| Value::Number((*x).into())).collect())
        }
        (CoreValue::Bytes(b), _) => match std::str::from_utf8(b) {
            Ok(s) => Value::String(s.to_string()),
            Err(_) => Value::Array(b.iter().map(|x| Value::Number((*x).into())).collect()),
        },
        (CoreValue::Embedding(v), _) => serde_json::to_value(v)?,
    })
}

/// Build a JSON row from a core row and a kit table definition.
pub fn core_row_to_json(row: &mongreldb_core::memtable::Row, table: &KitTable) -> Result<Row> {
    let mut values = Map::new();
    for col in &table.columns {
        let v = row
            .columns
            .get(&(col.id as u16))
            .cloned()
            .unwrap_or(CoreValue::Null);
        values.insert(col.name.clone(), core_to_json(&v, col.storage_type)?);
    }
    Ok(Row {
        row_id: row.row_id.0,
        values,
    })
}

/// A kit row, identified by its internal storage row id and column values.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub row_id: u64,
    pub values: Map<String, Value>,
}

impl Row {
    /// Extract the primary-key value(s) as a JSON value.
    ///
    /// Single-column primary keys return the scalar value; composite keys return
    /// an object.
    pub fn pk(&self, table: &KitTable) -> Option<Value> {
        if table.primary_key.len() == 1 {
            self.values.get(&table.primary_key[0]).cloned()
        } else {
            let mut obj = Map::new();
            for name in &table.primary_key {
                obj.insert(
                    name.clone(),
                    self.values.get(name).cloned().unwrap_or(Value::Null),
                );
            }
            Some(Value::Object(obj))
        }
    }
}

/// Extract the primary-key value(s) from a JSON value map.
pub fn pk_value(values: &Map<String, Value>, table: &KitTable) -> Option<Value> {
    if table.primary_key.len() == 1 {
        values.get(&table.primary_key[0]).cloned()
    } else {
        let mut obj = Map::new();
        for name in &table.primary_key {
            obj.insert(
                name.clone(),
                values.get(name).cloned().unwrap_or(Value::Null),
            );
        }
        Some(Value::Object(obj))
    }
}

/// Convert a primary-key value into the column values for lookup.
pub fn pk_to_map(pk: &Value, table: &KitTable) -> Result<Map<String, Value>> {
    let mut map = Map::new();
    match pk {
        Value::Object(obj) => {
            for name in &table.primary_key {
                let v = obj
                    .get(name)
                    .cloned()
                    .ok_or_else(|| KitError::Validation(format!("missing pk column {name}")))?;
                map.insert(name.clone(), v);
            }
        }
        scalar if table.primary_key.len() == 1 => {
            map.insert(table.primary_key[0].clone(), scalar.clone());
        }
        _ => {
            return Err(KitError::Validation(
                "primary key value shape mismatch".into(),
            ))
        }
    }
    Ok(map)
}

/// Build a core cell vector from a JSON row and kit table definition.
pub fn row_to_core_cells(
    values: &Map<String, Value>,
    table: &KitTable,
) -> Result<Vec<(u16, CoreValue)>> {
    let mut cells = Vec::with_capacity(table.columns.len());
    for col in &table.columns {
        let v = values.get(&col.name).cloned().unwrap_or(Value::Null);
        cells.push((col.id as u16, json_to_core(&v, col.storage_type)?));
    }
    Ok(cells)
}
