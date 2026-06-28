//! Query execution for kit `Select` statements.
//!
//! The implementation is intentionally simple: it materializes every visible row
//! from the target table, evaluates the filter expression in Rust, sorts, and
//! applies limit/offset. This keeps the crate independent of MongrelDB core's
//! native query primitives while remaining correct for the supported subset.

use crate::error::{KitError, Result};
use crate::schema::{core_row_to_json, Row};
use mongreldb_core::memtable::Row as CoreRow;
use mongreldb_kit_core::query::{Direction, Expr, Literal, OrderBy, Query, Select};
use mongreldb_kit_core::schema::Table as KitTable;
use serde_json::{Map, Value};

/// Execute a kit [`Query::Select`] against the supplied visible rows.
///
/// `rows` must be the newest visible version of every non-deleted row in the
/// target table at the transaction's read snapshot.
pub fn execute_select(table: &KitTable, visible_rows: Vec<CoreRow>, select: &Select) -> Result<Vec<Row>> {
    let mut rows: Vec<Row> = visible_rows
        .into_iter()
        .map(|r| core_row_to_json(&r, table))
        .collect::<Result<Vec<_>>>()?;

    if let Some(filter) = &select.filter {
        rows.retain(|r| eval_expr(filter, &r.values, table).unwrap_or(false));
    }

    for order in &select.order_by {
        sort_rows(&mut rows, order, table)?;
    }

    let offset = select.offset.unwrap_or(0);
    let limit = select.limit;

    if offset > 0 || limit.is_some() {
        let start = offset.min(rows.len());
        let end = limit.map(|l| start + l).unwrap_or(rows.len()).min(rows.len());
        rows = rows.drain(start..end).collect();
    }

    Ok(rows)
}

fn eval_expr(expr: &Expr, row: &Map<String, Value>, table: &KitTable) -> Result<bool> {
    Ok(match expr {
        Expr::Column(name) => truthy(row.get(name).unwrap_or(&Value::Null)),
        Expr::Literal(lit) => truthy(&literal_to_value(lit)),
        Expr::And(parts) => parts.iter().all(|p| eval_expr(p, row, table).unwrap_or(false)),
        Expr::Or(parts) => parts.iter().any(|p| eval_expr(p, row, table).unwrap_or(false)),
        Expr::Not(inner) => !eval_expr(inner, row, table)?,
        Expr::Eq(a, b) => compare(a, b, row, table)? == Some(std::cmp::Ordering::Equal),
        Expr::Ne(a, b) => compare(a, b, row, table)? != Some(std::cmp::Ordering::Equal),
        Expr::Gt(a, b) => compare(a, b, row, table)? == Some(std::cmp::Ordering::Greater),
        Expr::Gte(a, b) => compare(a, b, row, table)?
            .is_some_and(|o| o == std::cmp::Ordering::Greater || o == std::cmp::Ordering::Equal),
        Expr::Lt(a, b) => compare(a, b, row, table)? == Some(std::cmp::Ordering::Less),
        Expr::Lte(a, b) => compare(a, b, row, table)?
            .is_some_and(|o| o == std::cmp::Ordering::Less || o == std::cmp::Ordering::Equal),
        Expr::In(col, list) => {
            let v = eval_value(col, row, table)?;
            list.iter().any(|lit| v == literal_to_value(lit))
        }
        Expr::NotIn(col, list) => {
            let v = eval_value(col, row, table)?;
            list.iter().all(|lit| v != literal_to_value(lit))
        }
        Expr::IsNull(inner) => eval_value(inner, row, table)?.is_null(),
        Expr::IsNotNull(inner) => !eval_value(inner, row, table)?.is_null(),
        Expr::Like(col, pattern) => {
            let v = eval_value(col, row, table)?;
            if let Value::String(s) = v {
                like(&s, pattern)
            } else {
                false
            }
        }
    })
}

fn eval_value(expr: &Expr, row: &Map<String, Value>, _table: &KitTable) -> Result<Value> {
    Ok(match expr {
        Expr::Column(name) => row.get(name).cloned().unwrap_or(Value::Null),
        Expr::Literal(lit) => literal_to_value(lit),
        other => {
            return Err(KitError::Validation(format!(
                "expression {other:?} cannot be used as a scalar value"
            )))
        }
    })
}

fn literal_to_value(lit: &Literal) -> Value {
    match lit {
        Literal::Null => Value::Null,
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Int(i) => Value::Number((*i).into()),
        Literal::Float(f) => serde_json::to_value(*f).unwrap_or(Value::Null),
        Literal::Text(s) => Value::String(s.clone()),
        Literal::Json(v) => v.clone(),
    }
}

fn compare(a: &Expr, b: &Expr, row: &Map<String, Value>, table: &KitTable) -> Result<Option<std::cmp::Ordering>> {
    let av = eval_value(a, row, table)?;
    let bv = eval_value(b, row, table)?;
    Ok(json_cmp(&av, &bv))
}

fn json_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                Some(ai.cmp(&bi))
            } else {
                let af = a.as_f64().unwrap_or(f64::NAN);
                let bf = b.as_f64().unwrap_or(f64::NAN);
                af.partial_cmp(&bf)
            }
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Array(a), Value::Array(b)) => compare_arrays(a, b),
        (Value::Object(a), Value::Object(b)) => compare_objects(a, b),
        _ => None,
    }
}

fn compare_arrays(a: &[Value], b: &[Value]) -> Option<std::cmp::Ordering> {
    let len_cmp = a.len().partial_cmp(&b.len())?;
    if len_cmp != std::cmp::Ordering::Equal {
        return Some(len_cmp);
    }
    for (x, y) in a.iter().zip(b.iter()) {
        match json_cmp(x, y) {
            Some(std::cmp::Ordering::Equal) => {}
            other => return other,
        }
    }
    Some(std::cmp::Ordering::Equal)
}

fn compare_objects(
    a: &serde_json::Map<String, Value>,
    b: &serde_json::Map<String, Value>,
) -> Option<std::cmp::Ordering> {
    let len_cmp = a.len().partial_cmp(&b.len())?;
    if len_cmp != std::cmp::Ordering::Equal {
        return Some(len_cmp);
    }
    let mut a_keys: Vec<&String> = a.keys().collect();
    let mut b_keys: Vec<&String> = b.keys().collect();
    a_keys.sort();
    b_keys.sort();
    for (ak, bk) in a_keys.iter().zip(b_keys.iter()) {
        match ak.cmp(bk) {
            std::cmp::Ordering::Equal => {}
            other => return Some(other),
        }
        let av = a.get(*ak).unwrap();
        let bv = b.get(*bk).unwrap();
        match json_cmp(av, bv) {
            Some(std::cmp::Ordering::Equal) => {}
            other => return other,
        }
    }
    Some(std::cmp::Ordering::Equal)
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn sort_rows(rows: &mut [Row], order: &OrderBy, table: &KitTable) -> Result<()> {
    let col_name = match &order.expr {
        Expr::Column(name) => name.clone(),
        other => return Err(KitError::Validation(format!("unsupported order by: {other:?}"))),
    };
    if table.column(&col_name).is_none() {
        return Err(KitError::Validation(format!("unknown order column {col_name}")));
    }

    rows.sort_by(|a, b| {
        let av = a.values.get(&col_name).cloned().unwrap_or(Value::Null);
        let bv = b.values.get(&col_name).cloned().unwrap_or(Value::Null);
        let ord = json_cmp(&av, &bv).unwrap_or(std::cmp::Ordering::Equal);
        match order.direction {
            Direction::Asc => ord,
            Direction::Desc => ord.reverse(),
        }
    });
    Ok(())
}

fn like(text: &str, pattern: &str) -> bool {
    let regex = match regex_like(pattern) {
        Ok(re) => re,
        Err(_) => return false,
    };
    regex.is_match(text)
}

fn regex_like(pattern: &str) -> Result<regex::Regex> {
    let mut out = String::with_capacity(pattern.len() * 2);
    out.push('^');
    for ch in pattern.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            c => {
                if regex::escape(&c.to_string()).len() > 1 {
                    out.push_str(&regex::escape(&c.to_string()));
                } else {
                    out.push(c);
                }
            }
        }
    }
    out.push('$');
    regex::Regex::new(&out).map_err(|e| KitError::Validation(format!("invalid LIKE pattern: {e}")))
}

/// Execute any supported kit query statement against visible rows.
pub fn execute_query(table: &KitTable, visible_rows: Vec<CoreRow>, query: &Query) -> Result<Vec<Row>> {
    match query {
        Query::Select(select) => execute_select(table, visible_rows, select),
        _ => Err(KitError::Validation(
            "only SELECT queries are supported by execute_query".into(),
        )),
    }
}
