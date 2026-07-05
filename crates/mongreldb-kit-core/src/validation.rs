//! Row validation against a [`Table`] schema.

use crate::schema::{Column, ColumnType, Table};
use serde_json::{Map, Number, Value};

/// A validation failure returned by [`validate_row`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("validation error in table \"{table}\", column \"{column}\": {message}")]
pub struct ValidationError {
    pub table: String,
    pub column: String,
    pub message: String,
}

impl ValidationError {
    pub fn new(
        table: impl Into<String>,
        column: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
            message: message.into(),
        }
    }
}

/// Validate a JSON row against a table definition.
///
/// Checks performed:
/// * not-null constraints
/// * type compatibility with the column's [`ColumnType`]
/// * `enum_values` membership
/// * numeric `min` / `max`
/// * string/bytes `min_length` / `max_length`
/// * `regex` pattern match
/// * JSON parseability for `Json` columns
/// * table-level `check_constraints` names (validation of the expression itself
///   is left to the runtime that registered the named check)
pub fn validate_row(table: &Table, row: &Map<String, Value>) -> Result<(), ValidationError> {
    for col in &table.columns {
        let value = row.get(&col.name);
        validate_column(table, col, value, row)?;
    }

    for check in &table.check_constraints {
        if check.expr.trim().is_empty() {
            return Err(ValidationError::new(
                &table.name,
                "",
                format!(
                    "check constraint \"{}\" has an empty expression",
                    check.name
                ),
            ));
        }
        match crate::check::eval_check(&check.expr, row) {
            Ok(true) => {}
            Ok(false) => {
                return Err(ValidationError::new(
                    &table.name,
                    "",
                    format!("check constraint \"{}\" failed", check.name),
                ));
            }
            Err(e) => {
                return Err(ValidationError::new(
                    &table.name,
                    "",
                    format!("check constraint \"{}\" is invalid: {}", check.name, e.0),
                ));
            }
        }
    }

    Ok(())
}

fn validate_column(
    table: &Table,
    col: &Column,
    value: Option<&Value>,
    row: &Map<String, Value>,
) -> Result<(), ValidationError> {
    let value = match value {
        Some(Value::Null) | None => {
            if !col.nullable {
                return Err(ValidationError::new(
                    &table.name,
                    &col.name,
                    "cannot be null",
                ));
            }
            return Ok(());
        }
        Some(v) => v,
    };

    type_check(table, col, value)?;

    if let Some(enum_values) = &col.enum_values {
        if let Value::String(s) = value {
            if !enum_values.contains(s) {
                return Err(ValidationError::new(
                    &table.name,
                    &col.name,
                    format!("value \"{s}\" must be one of {}", enum_values.join(", ")),
                ));
            }
        }
    }

    match value {
        Value::Number(n) => {
            let f = number_to_f64(n);
            if let Some(min) = col.min {
                if f < min {
                    return Err(ValidationError::new(
                        &table.name,
                        &col.name,
                        format!("must be at least {min}"),
                    ));
                }
            }
            if let Some(max) = col.max {
                if f > max {
                    return Err(ValidationError::new(
                        &table.name,
                        &col.name,
                        format!("must be at most {max}"),
                    ));
                }
            }
        }
        Value::String(s) => {
            if let Some(min_len) = col.min_length {
                if s.chars().count() < min_len {
                    return Err(ValidationError::new(
                        &table.name,
                        &col.name,
                        format!("must have length at least {min_len}"),
                    ));
                }
            }
            if let Some(max_len) = col.max_length {
                if s.chars().count() > max_len {
                    return Err(ValidationError::new(
                        &table.name,
                        &col.name,
                        format!("must have length at most {max_len}"),
                    ));
                }
            }
            if let Some(pattern) = &col.regex {
                let re = regex::Regex::new(pattern).map_err(|e| {
                    ValidationError::new(
                        &table.name,
                        &col.name,
                        format!("invalid regex pattern: {e}"),
                    )
                })?;
                if !re.is_match(s) {
                    return Err(ValidationError::new(
                        &table.name,
                        &col.name,
                        "does not match required pattern",
                    ));
                }
            }
        }
        Value::Array(arr) => {
            if let Some(max_len) = col.max_length {
                if arr.len() > max_len {
                    return Err(ValidationError::new(
                        &table.name,
                        &col.name,
                        format!("must have length at most {max_len}"),
                    ));
                }
            }
            if let Some(min_len) = col.min_length {
                if arr.len() < min_len {
                    return Err(ValidationError::new(
                        &table.name,
                        &col.name,
                        format!("must have length at least {min_len}"),
                    ));
                }
            }
        }
        Value::Object(obj) => {
            if let Some(max_len) = col.max_length {
                if obj.len() > max_len {
                    return Err(ValidationError::new(
                        &table.name,
                        &col.name,
                        format!("must have length at most {max_len}"),
                    ));
                }
            }
            if let Some(min_len) = col.min_length {
                if obj.len() < min_len {
                    return Err(ValidationError::new(
                        &table.name,
                        &col.name,
                        format!("must have length at least {min_len}"),
                    ));
                }
            }
        }
        _ => {}
    }

    if let Some(expr) = &col.check_expr {
        if expr.trim().is_empty() {
            return Err(ValidationError::new(
                &table.name,
                &col.name,
                "column check expression is empty",
            ));
        }
        // Column checks are evaluated against the full row so they may
        // reference the column by name (and any sibling column).
        match crate::check::eval_check(expr, row) {
            Ok(true) => {}
            Ok(false) => {
                return Err(ValidationError::new(
                    &table.name,
                    &col.name,
                    "column check constraint failed",
                ));
            }
            Err(e) => {
                return Err(ValidationError::new(
                    &table.name,
                    &col.name,
                    format!("column check constraint is invalid: {}", e.0),
                ));
            }
        }
    }

    Ok(())
}

fn type_check(table: &Table, col: &Column, value: &Value) -> Result<(), ValidationError> {
    let ok = match col.storage_type {
        ColumnType::Bool => value.is_boolean(),
        ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64 => {
            value.as_i64().is_some()
        }
        ColumnType::Float32 | ColumnType::Float64 => value.is_number(),
        ColumnType::Text => value.is_string(),
        ColumnType::Bytes => value.is_string() || value.is_array(),
        ColumnType::Json => {
            // Any serde_json::Value is JSON-compatible, but circular references
            // are impossible here and serialization always succeeds for owned
            // values. We accept objects/arrays/scalars.
            true
        }
        ColumnType::Date | ColumnType::DateTime | ColumnType::TimestampNanos
        | ColumnType::Date64 | ColumnType::Time64 | ColumnType::Interval
        | ColumnType::Decimal128 => value.is_string() || value.is_number(),
        ColumnType::Embedding => value
            .as_array()
            .is_some_and(|a| a.iter().all(|v| v.is_number())),
        ColumnType::Sparse => value.as_array().is_some_and(|a| {
            a.iter().all(|pair| {
                pair.as_array()
                    .is_some_and(|p| p.len() == 2 && p[0].is_u64() && p[1].is_number())
            })
        }),
    };

    if !ok {
        return Err(ValidationError::new(
            &table.name,
            &col.name,
            format!("must be {:?}", col.storage_type),
        ));
    }

    Ok(())
}

fn number_to_f64(n: &Number) -> f64 {
    n.as_f64().unwrap_or(f64::NAN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{CheckConstraint, ColumnType};
    use serde_json::json;

    fn users_table() -> Table {
        Table {
            id: 1,
            name: "users".into(),
            columns: vec![
                Column::new(1, "id", ColumnType::Int64),
                Column::new(2, "email", ColumnType::Text),
                Column {
                    nullable: true,
                    ..Column::new(3, "age", ColumnType::Int64)
                },
                Column {
                    min_length: Some(2),
                    max_length: Some(10),
                    ..Column::new(4, "handle", ColumnType::Text)
                },
                Column {
                    enum_values: Some(vec!["user".into(), "admin".into()]),
                    ..Column::new(5, "role", ColumnType::Text)
                },
                Column {
                    regex: Some(r"^\d{3}-\d{4}$".into()),
                    ..Column::new(6, "zip", ColumnType::Text)
                },
            ],
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }
    }

    fn row(value: serde_json::Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn valid_row_passes() {
        let table = users_table();
        let r = row(json!({
            "id": 1,
            "email": "a@b.com",
            "age": 30,
            "handle": "ab",
            "role": "user",
            "zip": "123-4567"
        }));
        validate_row(&table, &r).unwrap();
    }

    #[test]
    fn rejects_null_in_non_nullable_column() {
        let table = users_table();
        let r = row(json!({ "id": null, "email": "a@b.com", "role": "user", "zip": "123-4567" }));
        let err = validate_row(&table, &r).unwrap_err();
        assert_eq!(err.column, "id");
        assert!(err.message.contains("cannot be null"));
    }

    #[test]
    fn rejects_missing_non_nullable_column() {
        let table = users_table();
        let r = row(json!({ "email": "a@b.com", "role": "user", "zip": "123-4567" }));
        let err = validate_row(&table, &r).unwrap_err();
        assert_eq!(err.column, "id");
    }

    #[test]
    fn rejects_type_mismatch() {
        let table = users_table();
        let r = row(
            json!({ "id": "not-a-number", "email": "a@b.com", "handle": "ab", "role": "user", "zip": "123-4567" }),
        );
        let err = validate_row(&table, &r).unwrap_err();
        assert_eq!(err.column, "id");
    }

    #[test]
    fn rejects_enum_violation() {
        let table = users_table();
        let r = row(
            json!({ "id": 1, "email": "a@b.com", "handle": "ab", "role": "superuser", "zip": "123-4567" }),
        );
        let err = validate_row(&table, &r).unwrap_err();
        assert_eq!(err.column, "role");
    }

    #[test]
    fn rejects_min_max() {
        let table = users_table();
        let mut col = Column::new(3, "score", ColumnType::Int64);
        col.min = Some(0.0);
        col.max = Some(100.0);
        let table = Table {
            columns: vec![col],
            ..table
        };
        let r = row(
            json!({ "id": 1, "email": "a@b.com", "handle": "ab", "role": "user", "zip": "123-4567", "score": 101 }),
        );
        let err = validate_row(&table, &r).unwrap_err();
        assert_eq!(err.column, "score");
    }

    #[test]
    fn rejects_length() {
        let table = users_table();
        let r = row(
            json!({ "id": 1, "email": "a@b.com", "handle": "x", "role": "user", "zip": "123-4567" }),
        );
        let err = validate_row(&table, &r).unwrap_err();
        assert_eq!(err.column, "handle");
    }

    #[test]
    fn rejects_regex() {
        let table = users_table();
        let r = row(
            json!({ "id": 1, "email": "a@b.com", "handle": "ab", "role": "user", "zip": "bad" }),
        );
        let err = validate_row(&table, &r).unwrap_err();
        assert_eq!(err.column, "zip");
    }

    #[test]
    fn rejects_empty_table_check_expr() {
        let table = Table {
            check_constraints: vec![CheckConstraint {
                name: "empty".into(),
                expr: "   ".into(),
            }],
            ..users_table()
        };
        let r = row(
            json!({ "id": 1, "email": "a@b.com", "handle": "ab", "role": "user", "zip": "123-4567" }),
        );
        let err = validate_row(&table, &r).unwrap_err();
        assert!(err.message.contains("empty"));
    }
}
