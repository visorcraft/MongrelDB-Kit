//! PyO3 bindings for MongrelDB Kit.
//!
//! Exposes a small Python API over `mongreldb-kit`: database open/create,
//! transactions with CRUD, migrations, and stable error categories.

use mongreldb_kit::{Database, KitError, Transaction};
use mongreldb_kit_core::query::{Expr, OrderBy, Query, Select};
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

create_exception!(mongreldb_kit_py, ValidationError, pyo3::exceptions::PyException);
create_exception!(mongreldb_kit_py, DuplicateError, pyo3::exceptions::PyException);
create_exception!(mongreldb_kit_py, ForeignKeyError, pyo3::exceptions::PyException);
create_exception!(mongreldb_kit_py, RestrictError, pyo3::exceptions::PyException);
create_exception!(mongreldb_kit_py, MigrationError, pyo3::exceptions::PyException);
create_exception!(mongreldb_kit_py, ConflictError, pyo3::exceptions::PyException);
create_exception!(mongreldb_kit_py, StorageError, pyo3::exceptions::PyException);
create_exception!(mongreldb_kit_py, IntegrityError, pyo3::exceptions::PyException);

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

    fn begin<'py>(slf: &Bound<'py, PyDatabase>, py: Python<'py>) -> PyResult<Bound<'py, PyTransaction>> {
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

fn require_txn<'a>(txn: &'a mut Option<Transaction<'static>>) -> PyResult<&'a mut Transaction<'static>> {
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
        let result = require_txn(&mut self.txn)?.insert(table, row).map_err(map_err)?;
        row_to_json(&result)
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
        require_txn(&mut self.txn)?.delete(table, &pk).map_err(map_err)
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

    fn select(
        &self,
        table: &str,
        filter_json: Option<&str>,
        order: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> PyResult<Vec<String>> {
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("transaction already closed"))?;

        let filter = match filter_json {
            Some(s) => Some(serde_json::from_str::<Value>(s).map_err(py_json_err)?),
            None => None,
        };
        let query = build_select_query(table, filter, order, limit, offset).map_err(map_err)?;
        let rows = txn.select(&query).map_err(map_err)?;
        rows.iter().map(row_to_json).collect()
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

fn build_select_query(
    table: &str,
    filter: Option<Value>,
    order: Option<&str>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Query, KitError> {
    use mongreldb_kit_core::query::Direction;

    let parsed_filter = match filter {
        Some(Value::Object(map)) => {
            let expr = object_filter_to_expr(&map)?;
            Some(expr)
        }
        Some(_) => return Err(KitError::Validation("filter must be a JSON object".into())),
        None => None,
    };

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

    Ok(Query::Select(Select {
        table: table.into(),
        columns: Vec::new(),
        filter: parsed_filter,
        order_by,
        limit,
        offset,
    }))
}

/// Convert a simple object filter into a kit `Expr`.
///
/// Supported shapes:
/// - `{ "column": { "op": value } }` where op is one of `eq`, `ne`, `gt`, `gte`, `lt`, `lte`.
/// - `{ "column": value }` is shorthand for `eq`.
fn object_filter_to_expr(map: &Map<String, Value>) -> Result<Expr, KitError> {
    use mongreldb_kit_core::query::{Expr, Literal};

    let mut parts = Vec::new();
    for (col, val) in map {
        match val {
            Value::Object(op_map) if op_map.len() == 1 => {
                let (op, operand) = op_map.iter().next().unwrap();
                let operand = Expr::Literal(value_to_literal(operand));
                let col_expr = Expr::Column(col.clone());
                let expr = match op.as_str() {
                    "eq" => Expr::Eq(Box::new(col_expr), Box::new(operand)),
                    "ne" => Expr::Ne(Box::new(col_expr), Box::new(operand)),
                    "gt" => Expr::Gt(Box::new(col_expr), Box::new(operand)),
                    "gte" => Expr::Gte(Box::new(col_expr), Box::new(operand)),
                    "lt" => Expr::Lt(Box::new(col_expr), Box::new(operand)),
                    "lte" => Expr::Lte(Box::new(col_expr), Box::new(operand)),
                    _ => return Err(KitError::Validation(format!("unknown operator {op}"))),
                };
                parts.push(expr);
            }
            _ => {
                parts.push(Expr::Eq(
                    Box::new(Expr::Column(col.clone())),
                    Box::new(Expr::Literal(value_to_literal(val))),
                ));
            }
        }
    }

    if parts.is_empty() {
        Ok(Expr::Literal(Literal::Bool(true)))
    } else if parts.len() == 1 {
        Ok(parts.into_iter().next().unwrap())
    } else {
        Ok(Expr::And(parts))
    }
}

fn value_to_literal(value: &Value) -> mongreldb_kit_core::query::Literal {
    use mongreldb_kit_core::query::Literal;
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

#[pymodule]
fn mongreldb_kit_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyTransaction>()?;
    m.add_wrapped(wrap_pyfunction!(migrate))?;

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
