//! TSV import/export for kit tables.
//!
//! Format (matches the engine's TSV convention): a header row of column names,
//! tab-separated cells, `NULL` encoded as an empty field, and `\t \n \r \\`
//! backslash-escaped. Numbers/bools render as their literal text; arrays and
//! objects (json/embedding/sparse columns) render as escaped JSON.
//!
//! Because `NULL` is the empty field, an empty *string* value round-trips as
//! `null` — the documented limitation of this format.

use crate::error::Result;
use crate::schema::Row;
use mongreldb_kit_core::schema::{ColumnType, Table as KitTable};
use serde_json::{Map, Value};

fn escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => o.push_str("\\\\"),
            '\t' => o.push_str("\\t"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            _ => o.push(c),
        }
    }
    o
}

fn unescape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c == '\\' {
            match it.next() {
                Some('t') => o.push('\t'),
                Some('n') => o.push('\n'),
                Some('r') => o.push('\r'),
                Some('\\') => o.push('\\'),
                Some(other) => {
                    o.push('\\');
                    o.push(other);
                }
                None => o.push('\\'),
            }
        } else {
            o.push(c);
        }
    }
    o
}

fn cell_to_tsv(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => escape(s),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => escape(&serde_json::to_string(other).unwrap_or_default()),
    }
}

/// Serialize `rows` (in schema column order) to a TSV document.
pub fn rows_to_tsv(table: &KitTable, rows: &[Row]) -> String {
    let cols: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
    let mut out = String::new();
    out.push_str(&cols.join("\t"));
    out.push('\n');
    for row in rows {
        let line: Vec<String> = cols
            .iter()
            .map(|name| cell_to_tsv(row.values.get(*name).unwrap_or(&Value::Null)))
            .collect();
        out.push_str(&line.join("\t"));
        out.push('\n');
    }
    out
}

fn parse_cell(raw: &str, ty: ColumnType) -> Result<Value> {
    if raw.is_empty() {
        return Ok(Value::Null);
    }
    let text = unescape(raw);
    let v = match ty {
        ColumnType::Bool => Value::Bool(text == "true"),
        ColumnType::Int8
        | ColumnType::Int16
        | ColumnType::Int32
        | ColumnType::Int64
        | ColumnType::TimestampNanos => match text.parse::<i64>() {
            Ok(n) => Value::Number(n.into()),
            Err(_) => Value::String(text),
        },
        ColumnType::Float32 | ColumnType::Float64 => match text.parse::<f64>() {
            Ok(f) => serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Err(_) => Value::String(text),
        },
        ColumnType::Text | ColumnType::Date | ColumnType::DateTime
        | ColumnType::Date64 | ColumnType::Time64 | ColumnType::Interval
        | ColumnType::Decimal128 => Value::String(text),
        ColumnType::Bytes | ColumnType::Json | ColumnType::Embedding | ColumnType::Sparse => {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        }
    };
    Ok(v)
}

/// Parse a TSV document into rows (maps keyed by the header column names). Only
/// columns named in the header are set; unknown header columns are ignored.
pub fn tsv_to_rows(table: &KitTable, text: &str) -> Result<Vec<Map<String, Value>>> {
    let mut lines = text.split('\n');
    let header = match lines.next() {
        Some(h) if !h.is_empty() => h,
        _ => return Ok(Vec::new()),
    };
    let names: Vec<String> = header.split('\t').map(|s| s.to_string()).collect();
    let types: Vec<Option<ColumnType>> = names
        .iter()
        .map(|n| {
            table
                .columns
                .iter()
                .find(|c| &c.name == n)
                .map(|c| c.application_type)
        })
        .collect();

    let mut rows = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let mut map = Map::new();
        for (i, field) in line.split('\t').enumerate() {
            let Some(name) = names.get(i) else { continue };
            let Some(Some(ty)) = types.get(i) else {
                continue; // header column not in the schema
            };
            map.insert(name.clone(), parse_cell(field, *ty)?);
        }
        rows.push(map);
    }
    Ok(rows)
}
