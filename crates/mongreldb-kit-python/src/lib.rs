//! PyO3 bindings for MongrelDB Kit.
//!
//! Exposes a small Python API over `mongreldb-kit`: database open/create,
//! transactions with CRUD, migrations, and stable error categories.

use mongreldb_kit::{Database, KitError, Transaction};
use mongreldb_kit_core::keys::{
    encode_pk as core_encode_pk, encode_row_guard_key as core_encode_row_guard_key,
    encode_unique_key as core_encode_unique_key, KeyComponent,
};
use mongreldb_kit_core::query::{
    Aggregate, AggregateQuery, Cte, Direction, Expr, JoinQuery, Literal, OrderBy, Query, Select,
};
use mongreldb_kit_core::schema::Schema as KitSchema;
use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde_json::{Map, Value};
use std::path::Path;

// ---------------------------------------------------------------------------
// Python-visible exception hierarchy. Each class gets a stable `code` attribute
// in `__init__` so callers can distinguish error categories without parsing
// messages.
// ---------------------------------------------------------------------------

create_exception!(
    mongreldb_kit_py,
    ValidationError,
    pyo3::exceptions::PyException
);
create_exception!(
    mongreldb_kit_py,
    DuplicateError,
    pyo3::exceptions::PyException
);
create_exception!(
    mongreldb_kit_py,
    ForeignKeyError,
    pyo3::exceptions::PyException
);
create_exception!(
    mongreldb_kit_py,
    RestrictError,
    pyo3::exceptions::PyException
);
create_exception!(
    mongreldb_kit_py,
    MigrationError,
    pyo3::exceptions::PyException
);
create_exception!(
    mongreldb_kit_py,
    ConflictError,
    pyo3::exceptions::PyException
);
create_exception!(
    mongreldb_kit_py,
    StorageError,
    pyo3::exceptions::PyException
);
create_exception!(
    mongreldb_kit_py,
    IntegrityError,
    pyo3::exceptions::PyException
);

fn map_err(e: KitError) -> PyErr {
    let msg = e.to_string();
    match e {
        KitError::Validation(_) => ValidationError::new_err(msg),
        KitError::Duplicate(_) => DuplicateError::new_err(msg),
        KitError::ForeignKey(_) => ForeignKeyError::new_err(msg),
        KitError::Restrict(_) => RestrictError::new_err(msg),
        KitError::Migration(_) => MigrationError::new_err(msg),
        KitError::Conflict(_) => ConflictError::new_err(msg),
        KitError::Storage(_) => StorageError::new_err(msg),
        KitError::Integrity(_) => IntegrityError::new_err(msg),
    }
}

fn py_json_err(e: serde_json::Error) -> PyErr {
    map_err(KitError::from(e))
}

fn set_code(m: &Bound<'_, PyModule>, name: &str, code: &str) -> PyResult<()> {
    let cls = m.getattr(name)?;
    cls.setattr("code", code)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

#[pyclass(name = "Database", unsendable)]
pub struct PyDatabase {
    db: Database,
}

#[pymethods]
impl PyDatabase {
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let db = Database::open(Path::new(path)).map_err(map_err)?;
        Ok(Self { db })
    }

    #[staticmethod]
    fn create(path: &str, schema_json: &str) -> PyResult<Self> {
        let schema: KitSchema = serde_json::from_str(schema_json).map_err(py_json_err)?;
        let db = Database::create(Path::new(path), schema).map_err(map_err)?;
        Ok(Self { db })
    }

    fn begin<'py>(
        slf: &Bound<'py, PyDatabase>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyTransaction>> {
        let this = slf.borrow();
        let txn = this.db.begin().map_err(map_err)?;
        // Safety: the transaction borrows the Database. We keep the Python
        // Database object alive by storing a cloned `Py<PyDatabase>` handle,
        // so the reference remains valid for the transaction's lifetime.
        let txn: Transaction<'static> = unsafe { std::mem::transmute(txn) };
        let py_txn = PyTransaction {
            _db: slf.clone().unbind(),
            txn: Some(txn),
        };
        Bound::new(py, py_txn)
    }

    fn migrate(&mut self, migrations_json: &str) -> PyResult<()> {
        let migrations: Vec<mongreldb_kit_core::migrations::Migration> =
            serde_json::from_str(migrations_json).map_err(py_json_err)?;
        mongreldb_kit::migrate(&mut self.db, &migrations).map_err(map_err)
    }

    fn set_schema(&mut self, schema_json: &str) -> PyResult<()> {
        let schema: KitSchema = serde_json::from_str(schema_json).map_err(py_json_err)?;
        self.db.set_schema(schema);
        Ok(())
    }

    /// Allocate `count` values from a named sequence, returning the first value.
    /// Retries internally on write-write conflicts.
    #[pyo3(signature = (name, count = 1))]
    fn allocate_sequence(&self, name: &str, count: i64) -> PyResult<i64> {
        self.db.allocate_sequence(name, count).map_err(map_err)
    }

    /// Application table names, excluding the reserved `__kit_*` tables. This is
    /// the Python analogue of the raw database accessor.
    fn table_names(&self) -> Vec<String> {
        self.db.table_names()
    }
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

#[pyclass(name = "Transaction", unsendable)]
pub struct PyTransaction {
    // Keep the owning Database alive while the transaction exists.
    _db: Py<PyDatabase>,
    txn: Option<Transaction<'static>>,
}

impl Drop for PyTransaction {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            txn.rollback();
        }
    }
}

fn require_txn<'a>(
    txn: &'a mut Option<Transaction<'static>>,
) -> PyResult<&'a mut Transaction<'static>> {
    txn.as_mut()
        .ok_or_else(|| PyRuntimeError::new_err("transaction already closed"))
}

fn row_to_json(row: &mongreldb_kit::Row) -> PyResult<String> {
    serde_json::to_string(&row.values).map_err(|e| StorageError::new_err(e.to_string()))
}

#[pymethods]
impl PyTransaction {
    fn insert(&mut self, table: &str, row_json: &str) -> PyResult<String> {
        let row: Map<String, Value> = serde_json::from_str(row_json).map_err(py_json_err)?;
        let result = require_txn(&mut self.txn)?
            .insert(table, row)
            .map_err(map_err)?;
        row_to_json(&result)
    }

    /// Insert many rows in this single transaction. `rows_json` is a JSON array of
    /// row objects; returns a list of the inserted rows (with defaults applied).
    fn insert_many(&mut self, table: &str, rows_json: &str) -> PyResult<Vec<String>> {
        let rows: Vec<Map<String, Value>> = serde_json::from_str(rows_json).map_err(py_json_err)?;
        let results = require_txn(&mut self.txn)?
            .insert_many(table, rows)
            .map_err(map_err)?;
        results.iter().map(row_to_json).collect()
    }

    fn update(&mut self, table: &str, pk_json: &str, patch_json: &str) -> PyResult<String> {
        let pk: Value = serde_json::from_str(pk_json).map_err(py_json_err)?;
        let patch: Map<String, Value> = serde_json::from_str(patch_json).map_err(py_json_err)?;
        let result = require_txn(&mut self.txn)?
            .update(table, &pk, patch)
            .map_err(map_err)?;
        row_to_json(&result)
    }

    fn delete(&mut self, table: &str, pk_json: &str) -> PyResult<()> {
        let pk: Value = serde_json::from_str(pk_json).map_err(py_json_err)?;
        require_txn(&mut self.txn)?
            .delete(table, &pk)
            .map_err(map_err)
    }

    fn get_by_pk(&self, table: &str, pk_json: &str) -> PyResult<Option<String>> {
        let pk: Value = serde_json::from_str(pk_json).map_err(py_json_err)?;
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("transaction already closed"))?;
        match txn.get_by_pk(table, &pk).map_err(map_err)? {
            Some(row) => Ok(Some(row_to_json(&row)?)),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (table, filter_json=None, order=None, limit=None, offset=None, columns=None, distinct=false, ctes_json=None))]
    #[allow(clippy::too_many_arguments)]
    fn select(
        &self,
        table: &str,
        filter_json: Option<&str>,
        order: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
        columns: Option<Vec<String>>,
        distinct: bool,
        ctes_json: Option<&str>,
    ) -> PyResult<Vec<String>> {
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("transaction already closed"))?;

        let filter = match filter_json {
            Some(s) => Some(serde_json::from_str::<Value>(s).map_err(py_json_err)?),
            None => None,
        };
        let select =
            build_select_stmt(table, filter, order, limit, offset, columns).map_err(map_err)?;
        let rows = if let Some(cj) = ctes_json {
            let ctes = parse_ctes(cj).map_err(map_err)?;
            txn.select_with(&ctes, &select).map_err(map_err)?
        } else if distinct {
            txn.select_distinct(&Query::Select(select))
                .map_err(map_err)?
        } else {
            txn.select(&Query::Select(select)).map_err(map_err)?
        };
        rows.iter().map(row_to_json).collect()
    }

    /// Run an aggregate / group-by / having query. `aggregates_json` is a JSON
    /// array of `{func, column?, alias}`; `filter_json`/`having_json` use the same
    /// friendly filter shape as `select`. Returns one JSON row per group.
    #[pyo3(signature = (table, aggregates_json, filter_json=None, group_by=None, having_json=None))]
    fn aggregate(
        &self,
        table: &str,
        aggregates_json: &str,
        filter_json: Option<&str>,
        group_by: Option<Vec<String>>,
        having_json: Option<&str>,
    ) -> PyResult<Vec<String>> {
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("transaction already closed"))?;

        let aggregates: Vec<Aggregate> =
            serde_json::from_str(aggregates_json).map_err(py_json_err)?;
        let query = AggregateQuery {
            table: table.into(),
            filter: parse_optional_filter(filter_json)?,
            group_by: group_by.unwrap_or_default(),
            aggregates,
            having: parse_optional_filter(having_json)?,
        };
        let rows = txn.aggregate(&query).map_err(map_err)?;
        rows.iter().map(row_to_json).collect()
    }

    /// Run a nested-loop join described by a serialized `JoinQuery`. Returns one
    /// JSON object per combined row, keyed by table alias (see `JoinQuery`).
    fn join(&self, query_json: &str) -> PyResult<Vec<String>> {
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("transaction already closed"))?;
        let query: JoinQuery = serde_json::from_str(query_json).map_err(py_json_err)?;
        let rows = txn.join(&query).map_err(map_err)?;
        rows.iter()
            .map(|m| serde_json::to_string(m).map_err(|e| StorageError::new_err(e.to_string())))
            .collect()
    }

    fn commit(&mut self) -> PyResult<()> {
        if let Some(txn) = self.txn.take() {
            txn.commit().map_err(map_err)
        } else {
            Err(PyRuntimeError::new_err("transaction already closed"))
        }
    }

    fn rollback(&mut self) -> PyResult<()> {
        if let Some(txn) = self.txn.take() {
            txn.rollback();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Query construction
// ---------------------------------------------------------------------------

fn parse_order(order: Option<&str>) -> Vec<OrderBy> {
    let mut order_by = Vec::new();
    if let Some(order_str) = order {
        for part in order_str.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (direction, col) = if let Some(rest) = part.strip_prefix('+') {
                (Direction::Asc, rest)
            } else if let Some(rest) = part.strip_prefix('-') {
                (Direction::Desc, rest)
            } else {
                (Direction::Asc, part)
            };
            order_by.push(OrderBy {
                expr: Expr::Column(col.into()),
                direction,
            });
        }
    }
    order_by
}

fn build_select_stmt(
    table: &str,
    filter: Option<Value>,
    order: Option<&str>,
    limit: Option<usize>,
    offset: Option<usize>,
    columns: Option<Vec<String>>,
) -> Result<Select, KitError> {
    let parsed_filter = match filter {
        Some(Value::Object(map)) => Some(parse_filter(&map)?),
        Some(Value::Null) | None => None,
        Some(_) => return Err(KitError::Validation("filter must be a JSON object".into())),
    };
    let columns = columns
        .unwrap_or_default()
        .into_iter()
        .map(Expr::Column)
        .collect();

    Ok(Select {
        table: table.into(),
        columns,
        filter: parsed_filter,
        order_by: parse_order(order),
        limit,
        offset,
    })
}

fn parse_optional_filter(filter_json: Option<&str>) -> PyResult<Option<Expr>> {
    match filter_json {
        Some(s) => {
            let map: Map<String, Value> = serde_json::from_str(s).map_err(py_json_err)?;
            Ok(Some(parse_filter(&map).map_err(map_err)?))
        }
        None => Ok(None),
    }
}

/// Convert a friendly object filter into a kit `Expr`.
///
/// Per-column shapes: `{ "col": { "op": value } }` where `op` is one of `eq`,
/// `ne`, `gt`, `gte`, `lt`, `lte`, `like`, `contains`, `in`, `not_in`,
/// `is_null`, `is_not_null`, `in_subquery`. `{ "col": value }` is shorthand for
/// `eq`. Top-level logical keys: `and`/`or` (array of filters), `not` (a filter),
/// `exists`/`not_exists` (a subselect). Multiple keys are AND-ed.
fn parse_filter(map: &Map<String, Value>) -> Result<Expr, KitError> {
    let mut parts = Vec::new();
    for (key, val) in map {
        let expr = match key.as_str() {
            "and" => Expr::And(parse_filter_array(val)?),
            "or" => Expr::Or(parse_filter_array(val)?),
            "not" => Expr::Not(Box::new(parse_filter_node(val)?)),
            "exists" => Expr::Exists(Box::new(parse_subselect(val)?)),
            "not_exists" => Expr::NotExists(Box::new(parse_subselect(val)?)),
            column => column_predicate(column, val)?,
        };
        parts.push(expr);
    }

    Ok(match parts.len() {
        0 => Expr::Literal(Literal::Bool(true)),
        1 => parts.into_iter().next().unwrap(),
        _ => Expr::And(parts),
    })
}

fn parse_filter_node(val: &Value) -> Result<Expr, KitError> {
    match val {
        Value::Object(map) => parse_filter(map),
        _ => Err(KitError::Validation("filter must be a JSON object".into())),
    }
}

fn parse_filter_array(val: &Value) -> Result<Vec<Expr>, KitError> {
    match val {
        Value::Array(items) => items.iter().map(parse_filter_node).collect(),
        _ => Err(KitError::Validation(
            "and/or expects an array of filters".into(),
        )),
    }
}

fn column_predicate(column: &str, val: &Value) -> Result<Expr, KitError> {
    let col_expr = || Expr::Column(column.to_string());
    match val {
        Value::Object(op_map) if op_map.len() == 1 => {
            let (op, operand) = op_map.iter().next().unwrap();
            let operand_lit = || Expr::Literal(value_to_literal(operand));
            Ok(match op.as_str() {
                "eq" => Expr::Eq(Box::new(col_expr()), Box::new(operand_lit())),
                "ne" => Expr::Ne(Box::new(col_expr()), Box::new(operand_lit())),
                "gt" => Expr::Gt(Box::new(col_expr()), Box::new(operand_lit())),
                "gte" => Expr::Gte(Box::new(col_expr()), Box::new(operand_lit())),
                "lt" => Expr::Lt(Box::new(col_expr()), Box::new(operand_lit())),
                "lte" => Expr::Lte(Box::new(col_expr()), Box::new(operand_lit())),
                "like" => Expr::Like(Box::new(col_expr()), as_str(operand, "like")?),
                "contains" => Expr::Contains(Box::new(col_expr()), as_str(operand, "contains")?),
                "in" => Expr::In(Box::new(col_expr()), as_literal_list(operand)?),
                "not_in" => Expr::NotIn(Box::new(col_expr()), as_literal_list(operand)?),
                "is_null" => Expr::IsNull(Box::new(col_expr())),
                "is_not_null" => Expr::IsNotNull(Box::new(col_expr())),
                "in_subquery" => {
                    Expr::InSubquery(Box::new(col_expr()), Box::new(parse_subselect(operand)?))
                }
                other => return Err(KitError::Validation(format!("unknown operator {other}"))),
            })
        }
        _ => Ok(Expr::Eq(
            Box::new(col_expr()),
            Box::new(Expr::Literal(value_to_literal(val))),
        )),
    }
}

fn as_str(value: &Value, op: &str) -> Result<String, KitError> {
    value
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| KitError::Validation(format!("{op} expects a string")))
}

fn as_literal_list(value: &Value) -> Result<Vec<Literal>, KitError> {
    match value {
        Value::Array(items) => Ok(items.iter().map(value_to_literal).collect()),
        _ => Err(KitError::Validation("in/not_in expects an array".into())),
    }
}

/// Parse a JSON array of friendly CTE definitions. Each item is a subselect
/// object (`{ "table", "filter"?, ... }`) plus a `"name"` key.
fn parse_ctes(json: &str) -> Result<Vec<Cte>, KitError> {
    let items: Vec<Value> = serde_json::from_str(json).map_err(KitError::from)?;
    items
        .iter()
        .map(|item| {
            let name = item
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| KitError::Validation("cte requires a name".into()))?
                .to_string();
            Ok(Cte {
                name,
                query: Box::new(parse_subselect(item)?),
            })
        })
        .collect()
}

/// Parse a `{ "table", "filter"?, "columns"?, "limit"?, "offset"? }` object into
/// a kit `Select` for use as a subquery / CTE / `exists` body.
fn parse_subselect(value: &Value) -> Result<Select, KitError> {
    let obj = value
        .as_object()
        .ok_or_else(|| KitError::Validation("subquery must be a JSON object".into()))?;
    let table = obj
        .get("table")
        .and_then(|v| v.as_str())
        .ok_or_else(|| KitError::Validation("subquery requires a table".into()))?
        .to_string();
    let filter = match obj.get("filter") {
        Some(Value::Object(map)) => Some(parse_filter(map)?),
        Some(Value::Null) | None => None,
        Some(_) => {
            return Err(KitError::Validation(
                "subquery filter must be an object".into(),
            ))
        }
    };
    let columns = match obj.get("columns") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| Expr::Column(s.to_string()))
            .collect(),
        _ => Vec::new(),
    };
    Ok(Select {
        table,
        columns,
        filter,
        order_by: Vec::new(),
        limit: obj
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize),
        offset: obj
            .get("offset")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize),
    })
}

fn value_to_literal(value: &Value) -> Literal {
    match value {
        Value::Null => Literal::Null,
        Value::Bool(b) => Literal::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Literal::Int(i)
            } else {
                Literal::Float(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::String(s) => Literal::Text(s.clone()),
        Value::Array(_) | Value::Object(_) => Literal::Json(value.clone()),
    }
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

#[pyfunction]
fn migrate(db: &Bound<'_, PyDatabase>, migrations_json: &str) -> PyResult<()> {
    let migrations: Vec<mongreldb_kit_core::migrations::Migration> =
        serde_json::from_str(migrations_json).map_err(py_json_err)?;
    let mut db = db.borrow_mut();
    mongreldb_kit::migrate(&mut db.db, &migrations).map_err(map_err)
}

// ---------------------------------------------------------------------------
// Key encoding. Components are passed as a JSON array of typed values so the
// canonical, byte-identical encoding can be shared with the TypeScript and Rust
// kits. Each component is `{"int": <i64>}`, `{"text": <string>}`, or
// `{"null": true}`.
// ---------------------------------------------------------------------------

fn parse_key_components(components_json: &str) -> PyResult<Vec<KeyComponent>> {
    let value: Value = serde_json::from_str(components_json).map_err(py_json_err)?;
    let arr = value
        .as_array()
        .ok_or_else(|| ValidationError::new_err("key components must be a JSON array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        if let Some(i) = item.get("int") {
            let n = i
                .as_i64()
                .ok_or_else(|| ValidationError::new_err("int component must be an integer"))?;
            out.push(KeyComponent::Int(n));
        } else if let Some(t) = item.get("text") {
            let s = t
                .as_str()
                .ok_or_else(|| ValidationError::new_err("text component must be a string"))?;
            out.push(KeyComponent::Text(s.to_string()));
        } else if item.get("null").is_some() {
            out.push(KeyComponent::Null);
        } else {
            return Err(ValidationError::new_err(format!(
                "invalid key component: {item}"
            )));
        }
    }
    Ok(out)
}

#[pyfunction]
fn encode_pk(components_json: &str) -> PyResult<String> {
    Ok(core_encode_pk(&parse_key_components(components_json)?))
}

#[pyfunction]
fn encode_unique_key(version: u32, constraint: &str, components_json: &str) -> PyResult<String> {
    Ok(core_encode_unique_key(
        version,
        constraint,
        &parse_key_components(components_json)?,
    ))
}

#[pyfunction]
fn encode_row_guard_key(table: &str, components_json: &str) -> PyResult<String> {
    let comps = parse_key_components(components_json)?;
    Ok(core_encode_row_guard_key(table, &core_encode_pk(&comps)))
}

#[pymodule]
fn mongreldb_kit_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyTransaction>()?;
    m.add_wrapped(wrap_pyfunction!(migrate))?;
    m.add_wrapped(wrap_pyfunction!(encode_pk))?;
    m.add_wrapped(wrap_pyfunction!(encode_unique_key))?;
    m.add_wrapped(wrap_pyfunction!(encode_row_guard_key))?;

    let py = m.py();
    m.add("ValidationError", py.get_type::<ValidationError>())?;
    m.add("DuplicateError", py.get_type::<DuplicateError>())?;
    m.add("ForeignKeyError", py.get_type::<ForeignKeyError>())?;
    m.add("RestrictError", py.get_type::<RestrictError>())?;
    m.add("MigrationError", py.get_type::<MigrationError>())?;
    m.add("ConflictError", py.get_type::<ConflictError>())?;
    m.add("StorageError", py.get_type::<StorageError>())?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;

    set_code(m, "ValidationError", "VALIDATION")?;
    set_code(m, "DuplicateError", "DUPLICATE")?;
    set_code(m, "ForeignKeyError", "FOREIGN_KEY")?;
    set_code(m, "RestrictError", "RESTRICT")?;
    set_code(m, "MigrationError", "MIGRATION")?;
    set_code(m, "ConflictError", "CONFLICT")?;
    set_code(m, "StorageError", "STORAGE")?;
    set_code(m, "IntegrityError", "INTEGRITY")?;

    Ok(())
}
