//! PyO3 bindings for MongrelDB Kit.
//!
//! Exposes a small Python API over `mongreldb-kit`: database open/create,
//! transactions with CRUD, migrations, and stable error categories.

use mongreldb_kit::{ApproxAggKind, Database, IncrementalAggKind, KitError, Transaction};
use mongreldb_kit_core::keys::{
    encode_pk as core_encode_pk, encode_row_guard_key as core_encode_row_guard_key,
    encode_unique_key as core_encode_unique_key, KeyComponent,
};
use mongreldb_kit_core::query::{
    Aggregate, AggregateQuery, Cte, Direction, Expr, JoinQuery, Literal, OnConflict, OrderBy,
    Query, Select,
};
use mongreldb_kit_core::schema::Schema as KitSchema;
use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::IntoPyObjectExt;
use serde_json::{Map, Number, Value};
use std::path::Path;
use std::rc::Rc;

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
    TriggerValidationError,
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
        KitError::TriggerValidation(_) => TriggerValidationError::new_err(msg),
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
    // Held behind `Rc` so an open transaction can pin the engine alive: a
    // `PyTransaction` keeps a clone of this `Rc`, so closing the handle (or
    // finalizing it during interpreter shutdown) never frees the `Database`
    // out from under a live transaction that still borrows it.
    db: Option<Rc<Database>>,
}

impl PyDatabase {
    fn require_db(&self) -> PyResult<&Database> {
        self.db
            .as_deref()
            .ok_or_else(|| PyRuntimeError::new_err("database already closed"))
    }

    fn require_db_mut(&mut self) -> PyResult<&mut Database> {
        let db = self
            .db
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("database already closed"))?;
        Rc::get_mut(db).ok_or_else(|| {
            PyRuntimeError::new_err("cannot mutate the database while a transaction is open")
        })
    }
}

#[pymethods]
impl PyDatabase {
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let db = Database::open(Path::new(path)).map_err(map_err)?;
        Ok(Self {
            db: Some(Rc::new(db)),
        })
    }

    #[staticmethod]
    fn open_encrypted(path: &str, passphrase: &str) -> PyResult<Self> {
        let db = Database::open_encrypted(Path::new(path), passphrase).map_err(map_err)?;
        Ok(Self {
            db: Some(Rc::new(db)),
        })
    }

    #[staticmethod]
    fn create_encrypted(path: &str, schema_json: &str, passphrase: &str) -> PyResult<Self> {
        let schema: KitSchema = serde_json::from_str(schema_json).map_err(py_json_err)?;
        let db =
            Database::create_encrypted(Path::new(path), schema, passphrase).map_err(map_err)?;
        Ok(Self {
            db: Some(Rc::new(db)),
        })
    }

    #[staticmethod]
    fn create(path: &str, schema_json: &str) -> PyResult<Self> {
        let schema: KitSchema = serde_json::from_str(schema_json).map_err(py_json_err)?;
        let db = Database::create(Path::new(path), schema).map_err(map_err)?;
        Ok(Self {
            db: Some(Rc::new(db)),
        })
    }

    fn begin<'py>(
        slf: &Bound<'py, PyDatabase>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyTransaction>> {
        let db = {
            let this = slf.borrow();
            this.db
                .clone()
                .ok_or_else(|| PyRuntimeError::new_err("database already closed"))?
        };
        let txn = db.begin().map_err(map_err)?;
        // Safety: `txn` borrows the `Database` inside `db`. `db` (an `Rc` clone)
        // is moved into the `PyTransaction` as `_db_owner`, which is declared
        // *after* `txn` and so drops *after* it — the borrow can never outlive
        // the allocation, even if the owning handle is closed first.
        let txn: Transaction<'static> = unsafe { std::mem::transmute(txn) };
        let py_txn = PyTransaction {
            txn: Some(txn),
            _db_owner: Some(db),
            _db: slf.clone().unbind(),
        };
        Bound::new(py, py_txn)
    }

    fn migrate(&mut self, migrations_json: &str) -> PyResult<()> {
        let migrations: Vec<mongreldb_kit_core::migrations::Migration> =
            serde_json::from_str(migrations_json).map_err(py_json_err)?;
        mongreldb_kit::migrate(self.require_db_mut()?, &migrations).map_err(map_err)
    }

    fn set_schema(&mut self, schema_json: &str) -> PyResult<()> {
        let schema: KitSchema = serde_json::from_str(schema_json).map_err(py_json_err)?;
        self.require_db_mut()?.set_schema(schema);
        Ok(())
    }

    /// Allocate `count` values from a named sequence, returning the first value.
    /// Retries internally on write-write conflicts.
    #[pyo3(signature = (name, count = 1))]
    fn allocate_sequence(&self, name: &str, count: i64) -> PyResult<i64> {
        self.require_db()?
            .allocate_sequence(name, count)
            .map_err(map_err)
    }

    /// Application table names, excluding the reserved `__kit_*` tables. This is
    /// the Python analogue of the raw database accessor.
    fn table_names(&self) -> PyResult<Vec<String>> {
        Ok(self.require_db()?.table_names())
    }

    /// Reclaim orphaned runs and stale WAL/shadow files; returns the count.
    fn gc(&self) -> PyResult<usize> {
        self.require_db()?.gc().map_err(map_err)
    }

    /// Verify run footer checksums; returns any integrity issues (JSON strings).
    fn check(&self) -> PyResult<Vec<String>> {
        self.require_db()?
            .check()
            .iter()
            .map(|v| serde_json::to_string(v).map_err(|e| StorageError::new_err(e.to_string())))
            .collect()
    }

    /// Drop corrupt runs; returns the ids of the runs that were dropped.
    fn doctor(&self) -> PyResult<Vec<u64>> {
        self.require_db()?.doctor().map_err(map_err)
    }

    /// The current visible commit epoch (monotonically increasing version).
    fn snapshot_epoch(&self) -> PyResult<u64> {
        Ok(self.require_db()?.snapshot_epoch())
    }

    fn create_procedure(&self, procedure_json: &str) -> PyResult<String> {
        let value: Value = serde_json::from_str(procedure_json).map_err(py_json_err)?;
        let spec = mongreldb_kit_core::ProcedureSpec::new(value);
        let procedure = self
            .require_db()?
            .create_procedure(&spec)
            .map_err(map_err)?;
        serde_json::to_string(&procedure).map_err(py_json_err)
    }

    fn replace_procedure(&self, procedure_json: &str) -> PyResult<String> {
        let value: Value = serde_json::from_str(procedure_json).map_err(py_json_err)?;
        let spec = mongreldb_kit_core::ProcedureSpec::new(value);
        let procedure = self
            .require_db()?
            .replace_procedure(&spec)
            .map_err(map_err)?;
        serde_json::to_string(&procedure).map_err(py_json_err)
    }

    fn drop_procedure(&self, name: &str) -> PyResult<()> {
        self.require_db()?.drop_procedure(name).map_err(map_err)
    }

    fn call_procedure(&self, name: &str, args_json: &str) -> PyResult<String> {
        let args: Map<String, Value> = serde_json::from_str(args_json).map_err(py_json_err)?;
        let result = self
            .require_db()?
            .call_procedure(name, args)
            .map_err(map_err)?;
        serde_json::to_string(&result).map_err(py_json_err)
    }

    fn create_trigger(&self, trigger_json: &str) -> PyResult<String> {
        let value: Value = serde_json::from_str(trigger_json).map_err(py_json_err)?;
        let spec = mongreldb_kit_core::TriggerSpec::new(value);
        let trigger = self.require_db()?.create_trigger(&spec).map_err(map_err)?;
        serde_json::to_string(&trigger).map_err(py_json_err)
    }

    fn replace_trigger(&self, trigger_json: &str) -> PyResult<String> {
        let value: Value = serde_json::from_str(trigger_json).map_err(py_json_err)?;
        let spec = mongreldb_kit_core::TriggerSpec::new(value);
        let trigger = self.require_db()?.replace_trigger(&spec).map_err(map_err)?;
        serde_json::to_string(&trigger).map_err(py_json_err)
    }

    fn drop_trigger(&self, name: &str) -> PyResult<()> {
        self.require_db()?.drop_trigger(name).map_err(map_err)
    }

    fn triggers(&self) -> PyResult<Vec<String>> {
        self.require_db()?
            .triggers()
            .iter()
            .map(|trigger| serde_json::to_string(trigger).map_err(py_json_err))
            .collect()
    }

    fn trigger(&self, name: &str) -> PyResult<Option<String>> {
        self.require_db()?
            .trigger(name)
            .map(|trigger| serde_json::to_string(&trigger).map_err(py_json_err))
            .transpose()
    }

    /// Export every visible row of `table` as a TSV document.
    fn export_tsv(&self, table: &str) -> PyResult<String> {
        self.require_db()?.export_tsv(table).map_err(map_err)
    }

    /// Import a TSV document into `table`; returns the number of rows inserted.
    fn import_tsv(&self, table: &str, text: &str) -> PyResult<usize> {
        self.require_db()?.import_tsv(table, text).map_err(map_err)
    }

    /// Read every row of `table` visible at commit `epoch` (MVCC time-travel).
    fn rows_at_epoch(&self, py: Python<'_>, table: &str, epoch: u64) -> PyResult<Vec<Py<PyAny>>> {
        let rows = self
            .require_db()?
            .rows_at_epoch(table, epoch)
            .map_err(map_err)?;
        rows.iter().map(|row| row_to_py(py, row)).collect()
    }

    /// Approximate aggregate (`count`/`sum`/`avg`) from the reservoir sample with
    /// a `z`-score confidence interval. Returns a dict, or `None` when the
    /// reservoir is empty. (Native conversion — no JSON round-trip.)
    fn approx_aggregate(
        &self,
        py: Python<'_>,
        table: &str,
        agg: &str,
        column: Option<&str>,
        z: f64,
    ) -> PyResult<Option<Py<PyAny>>> {
        let kind = match agg {
            "count" => ApproxAggKind::Count,
            "sum" => ApproxAggKind::Sum,
            "avg" => ApproxAggKind::Avg,
            other => {
                return Err(ValidationError::new_err(format!(
                    "unknown approx aggregate '{other}'"
                )))
            }
        };
        let res = self
            .require_db()?
            .approx_aggregate(table, column, kind, z)
            .map_err(map_err)?;
        Ok(res.map(|r| {
            let dict = PyDict::new(py);
            let _ = dict.set_item("point", r.point);
            let _ = dict.set_item("ci_low", r.ci_low);
            let _ = dict.set_item("ci_high", r.ci_high);
            let _ = dict.set_item("n_population", r.n_population);
            let _ = dict.set_item("n_sample_live", r.n_sample_live);
            let _ = dict.set_item("n_passing", r.n_passing);
            dict.into_py_any(py).unwrap()
        }))
    }

    /// Stream `table` in batches of at most `batch_size` rows; `callback` is
    /// invoked once per batch with a list of dict rows.
    fn scan_batched(
        &self,
        py: Python<'_>,
        table: &str,
        batch_size: usize,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        let db = self.require_db()?;
        let mut cb_err: Option<PyErr> = None;
        let res = db.scan_batched(table, batch_size, |batch| {
            let list = PyList::empty(py);
            for m in batch {
                let d =
                    json_map_to_pydict(py, m).map_err(|e| KitError::Validation(e.to_string()))?;
                list.append(d)
                    .map_err(|e| KitError::Validation(e.to_string()))?;
            }
            match callback.call1(py, (list,)) {
                Ok(_) => Ok(()),
                Err(e) => {
                    cb_err = Some(e);
                    Err(KitError::Validation("scan_batched callback raised".into()))
                }
            }
        });
        if let Some(e) = cb_err {
            return Err(e);
        }
        res.map_err(map_err)
    }

    /// Rank rows by Jaccard set-similarity between `query` and the string set in
    /// `column`, returning the top `k` as `{row, similarity}` dicts.
    fn set_similarity(
        &self,
        py: Python<'_>,
        table: &str,
        column: &str,
        query: Vec<String>,
        k: usize,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let hits = self
            .require_db()?
            .set_similarity(table, column, &query, k)
            .map_err(map_err)?;
        hits.iter()
            .map(|h| {
                let dict = PyDict::new(py);
                dict.set_item("row", json_map_to_pydict(py, &h.row.values)?)?;
                dict.set_item("similarity", h.similarity)?;
                dict.into_py_any(py)
            })
            .collect()
    }

    /// Flush all tables' in-memory writes to durable runs (also enables the
    /// incremental-aggregate fast path).
    fn flush(&self) -> PyResult<()> {
        self.require_db()?.flush().map_err(map_err)
    }

    /// Compact every table's sorted runs into one clean run so query latency
    /// stays flat. Returns `(compacted, skipped)`. Safe at any time.
    fn compact_all(&self) -> PyResult<(usize, usize)> {
        self.require_db()?.compact_all().map_err(map_err)
    }

    /// Compact a single table by name. Returns `True` if compacted, `False`
    /// if skipped (fewer than 2 runs).
    fn compact_table(&self, name: &str) -> PyResult<bool> {
        self.require_db()?.compact_table(name).map_err(map_err)
    }

    /// Rename a live table. Fails if `from` does not exist or `to` is already
    /// in use; a no-op when `from == to`. Names beginning with `__kit_` are
    /// reserved for internal tables. The kit schema catalog is also updated
    /// (in memory and persisted) so the new name works end-to-end.
    fn rename_table(&mut self, from: &str, to: &str) -> PyResult<()> {
        self.require_db_mut()?
            .rename_table(from, to)
            .map_err(map_err)
    }

    /// Rebuild statistics/metadata for every table's indexes (the engine's
    /// `ANALYZE` equivalent). Safe to run at any time; useful after bulk loads.
    fn analyze(&self) -> PyResult<()> {
        self.require_db()?.analyze().map_err(map_err)
    }

    /// Reclaim space across all tables: compact every sorted run, then gc.
    /// Returns the count of reclaimed orphaned runs/files. (Engine `VACUUM`.)
    fn vacuum(&self) -> PyResult<usize> {
        self.require_db()?.vacuum().map_err(map_err)
    }

    /// Create a SQL view from a JSON spec `{"name": ..., "sql": "SELECT ..."}`.
    fn create_view(&self, view_json: &str) -> PyResult<()> {
        let spec: mongreldb_kit_core::ViewSpec =
            serde_json::from_str(view_json).map_err(py_json_err)?;
        self.require_db()?.create_view(&spec).map_err(map_err)
    }

    /// Drop a SQL view by name (idempotent).
    fn drop_view(&self, name: &str) -> PyResult<()> {
        self.require_db()?.drop_view(name).map_err(map_err)
    }

    /// Reserve (without inserting) the next engine-native AUTO_INCREMENT value
    /// for `table`. Returns `None` when the table has no auto-increment column.
    fn reserve_auto_inc(&self, table: &str) -> PyResult<Option<i64>> {
        self.require_db()?.reserve_auto_inc(table).map_err(map_err)
    }

    /// Run a SQL read/DDL/DML statement and return the result rows as a list
    /// of dicts (column name → value). Empty for DDL/DML. Writes through SQL
    /// bypass kit-level constraints — use the transactional API for those.
    fn sql_rows(&self, py: Python<'_>, sql: &str) -> PyResult<Vec<Py<PyAny>>> {
        let rows = self.require_db()?.sql_rows(sql).map_err(map_err)?;
        rows.into_iter()
            .map(|row| json_map_to_pydict(py, &row))
            .collect()
    }

    /// Run a SQL statement and return the result as raw Arrow IPC *file* bytes
    /// (decode with `pyarrow.ipc.open_file`). Empty for DDL/DML.
    fn sql_arrow(&self, sql: &str) -> PyResult<Vec<u8>> {
        self.require_db()?.sql_arrow(sql).map_err(map_err)
    }

    /// Incrementally-maintained aggregate (`count`/`sum`/`min`/`max`/`avg`) over
    /// `table`, optionally filtered by the friendly `filter` object (which must
    /// translate exactly to index conditions). Returns a dict
    /// `{value, incremental, delta_rows}`; the value is always exact. (Native
    /// conversion — no JSON round-trip.)
    fn incremental_aggregate(
        &self,
        py: Python<'_>,
        table: &str,
        agg: &str,
        column: Option<&str>,
        filter: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let kind = match agg {
            "count" => IncrementalAggKind::Count,
            "sum" => IncrementalAggKind::Sum,
            "min" => IncrementalAggKind::Min,
            "max" => IncrementalAggKind::Max,
            "avg" => IncrementalAggKind::Avg,
            other => {
                return Err(ValidationError::new_err(format!(
                    "unknown aggregate '{other}'"
                )))
            }
        };
        let expr = match filter {
            Some(obj) if !obj.is_none(py) => {
                let value = py_to_value(obj.bind(py))?;
                let map = value
                    .as_object()
                    .ok_or_else(|| ValidationError::new_err("filter must be an object"))?;
                Some(parse_filter(map).map_err(map_err)?)
            }
            _ => None,
        };
        let res = self
            .require_db()?
            .incremental_aggregate(table, column, kind, expr.as_ref())
            .map_err(map_err)?;
        let dict = PyDict::new(py);
        let _ = dict.set_item("value", value_to_py(py, &res.value)?);
        let _ = dict.set_item("incremental", res.incremental);
        let _ = dict.set_item("delta_rows", res.delta_rows);
        dict.into_py_any(py)
    }

    /// Explain how `filter` (the friendly filter object) would push down against
    /// `table`. Returns a dict; does not run the query. (Native conversion.)
    fn explain(&self, py: Python<'_>, table: &str, filter: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let value = py_to_value(filter.bind(py))?;
        let map = value
            .as_object()
            .ok_or_else(|| ValidationError::new_err("filter must be an object"))?;
        let expr = parse_filter(map).map_err(map_err)?;
        let plan = self.require_db()?.explain(table, &expr).map_err(map_err)?;
        let dict = PyDict::new(py);
        let _ = dict.set_item("index_accelerated", plan.index_accelerated);
        let _ = dict.set_item("exact", plan.exact);
        let conditions: Vec<Py<PyAny>> = plan
            .pushed_conditions
            .iter()
            .map(|s| s.into_py_any(py))
            .collect::<PyResult<_>>()?;
        let _ = dict.set_item("pushed_conditions", conditions);
        dict.into_py_any(py)
    }

    fn close(&mut self) {
        self.db = None;
    }
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

#[pyclass(name = "Transaction", unsendable)]
pub struct PyTransaction {
    // Field order is load-bearing: `txn` borrows the `Database` and MUST drop
    // before `_db_owner` (the `Rc` that keeps that `Database` alive). Rust drops
    // fields top-to-bottom, so this ordering guarantees the borrow is released
    // before the allocation can be freed. `_db_owner` is cleared eagerly once the
    // transaction commits/rolls back, so a finished (but not-yet-collected) txn
    // object no longer pins the engine.
    txn: Option<Transaction<'static>>,
    _db_owner: Option<Rc<Database>>,
    // Keep the owning Python Database object alive while the transaction exists.
    _db: Py<PyDatabase>,
}

impl PyTransaction {
    /// Release the read side of the transaction: the borrow is already gone (the
    /// caller has taken and finished `txn`), so the engine pin can drop too.
    fn release_pin(&mut self) {
        self._db_owner = None;
    }
}

impl Drop for PyTransaction {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            txn.rollback();
        }
        self.release_pin();
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

fn value_to_json(value: &Value) -> PyResult<String> {
    serde_json::to_string(value).map_err(|e| StorageError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// Direct serde_json::Value <-> PyObject conversion. These helpers bypass the
// json.dumps / json.loads round-trip used by the JSON-string methods: rows are
// built straight into Python dicts, and Python dicts are read straight into the
// kit `Map<String, Value>` shape.
// ---------------------------------------------------------------------------

fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => b.into_py_any(py),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_py_any(py)
            } else {
                n.as_f64().unwrap_or(f64::NAN).into_py_any(py)
            }
        }
        Value::String(s) => s.as_str().into_py_any(py),
        Value::Array(arr) => {
            let items = arr
                .iter()
                .map(|v| value_to_py(py, v))
                .collect::<PyResult<Vec<Py<PyAny>>>>()?;
            items.into_py_any(py)
        }
        Value::Object(map) => json_map_to_pydict(py, map),
    }
}

fn json_map_to_pydict(py: Python<'_>, map: &Map<String, Value>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (k, v) in map {
        dict.set_item(k.as_str(), value_to_py(py, v)?)?;
    }
    dict.into_py_any(py)
}

fn row_to_py(py: Python<'_>, row: &mongreldb_kit::Row) -> PyResult<Py<PyAny>> {
    json_map_to_pydict(py, &row.values)
}

/// Convert a Python object into the kit JSON model. Booleans are matched before
/// integers because `bool` is a subclass of `int` in Python.
fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_to_value(&item)?);
        }
        return Ok(Value::Array(arr));
    }
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        let mut arr = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            arr.push(py_to_value(&item)?);
        }
        return Ok(Value::Array(arr));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = Map::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_value(&v)?);
        }
        return Ok(Value::Object(map));
    }
    let type_name = obj
        .get_type()
        .name()
        .and_then(|n| n.extract::<String>())
        .unwrap_or_default();
    Err(ValidationError::new_err(format!(
        "cannot convert {type_name} to a JSON value"
    )))
}

/// Convert a Python dict row into the kit `Map<String, Value>` shape.
fn py_row_to_map(obj: &Bound<'_, PyAny>) -> PyResult<Map<String, Value>> {
    let dict = obj
        .cast::<PyDict>()
        .map_err(|_| ValidationError::new_err("row must be a dict"))?;
    let mut map = Map::with_capacity(dict.len());
    for (k, v) in dict.iter() {
        let key: String = k.extract()?;
        map.insert(key, py_to_value(&v)?);
    }
    Ok(map)
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

    fn insert_returning(
        &mut self,
        table: &str,
        row_json: &str,
        returning_json: &str,
    ) -> PyResult<String> {
        let row: Map<String, Value> = serde_json::from_str(row_json).map_err(py_json_err)?;
        let returning: Vec<String> = serde_json::from_str(returning_json).map_err(py_json_err)?;
        let result = require_txn(&mut self.txn)?
            .insert_returning(table, row, returning)
            .map_err(map_err)?;
        value_to_json(&result)
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

    /// Insert many rows from a Python list of dicts, returning the inserted rows
    /// as Python dicts. This is the direct-conversion path: each dict is read
    /// straight into the kit row shape and each result row is built straight into
    /// a Python dict, with no JSON-string intermediary.
    fn insert_many_py(
        &mut self,
        py: Python<'_>,
        table: &str,
        rows: Vec<Py<PyAny>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let mut maps = Vec::with_capacity(rows.len());
        for row in &rows {
            maps.push(py_row_to_map(row.bind(py))?);
        }
        let results = require_txn(&mut self.txn)?
            .insert_many(table, maps)
            .map_err(map_err)?;
        results.iter().map(|row| row_to_py(py, row)).collect()
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

    fn truncate(&mut self, table: &str) -> PyResult<()> {
        require_txn(&mut self.txn)?.truncate(table).map_err(map_err)
    }

    fn upsert(
        &mut self,
        table: &str,
        row_json: &str,
        on_conflict_json: &str,
        returning_json: &str,
    ) -> PyResult<String> {
        let row: Map<String, Value> = serde_json::from_str(row_json).map_err(py_json_err)?;
        let on_conflict = parse_on_conflict(on_conflict_json)?;
        let returning: Vec<String> = serde_json::from_str(returning_json).map_err(py_json_err)?;
        let result = require_txn(&mut self.txn)?
            .upsert(table, row, on_conflict, returning)
            .map_err(map_err)?;
        value_to_json(&result)
    }

    fn update_where(
        &mut self,
        table: &str,
        filter_json: Option<&str>,
        patch_json: &str,
        returning_json: &str,
    ) -> PyResult<Vec<String>> {
        let filter = parse_optional_filter(filter_json)?;
        let patch: Map<String, Value> = serde_json::from_str(patch_json).map_err(py_json_err)?;
        let returning: Vec<String> = serde_json::from_str(returning_json).map_err(py_json_err)?;
        let rows = require_txn(&mut self.txn)?
            .update_where(table, filter, patch, returning)
            .map_err(map_err)?;
        rows.iter().map(value_to_json).collect()
    }

    fn delete_where(
        &mut self,
        table: &str,
        filter_json: Option<&str>,
        returning_json: &str,
    ) -> PyResult<Vec<String>> {
        let filter = parse_optional_filter(filter_json)?;
        let returning: Vec<String> = serde_json::from_str(returning_json).map_err(py_json_err)?;
        let rows = require_txn(&mut self.txn)?
            .delete_where(table, filter, returning)
            .map_err(map_err)?;
        rows.iter().map(value_to_json).collect()
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
        let ctes = match ctes_json {
            Some(s) => Some(parse_ctes(s).map_err(map_err)?),
            None => None,
        };
        let rows = select_core(
            txn, table, filter, order, limit, offset, columns, distinct, ctes,
        )?;
        rows.iter().map(row_to_json).collect()
    }

    /// Run a SELECT and return the rows as Python dicts directly. `filter` and
    /// `ctes` are Python objects (dict / list, or `None`); the result rows are
    /// built straight into Python dicts with no JSON-string intermediary.
    #[pyo3(signature = (table, filter=None, order=None, limit=None, offset=None, columns=None, distinct=false, ctes=None))]
    #[allow(clippy::too_many_arguments)]
    fn select_py(
        &self,
        py: Python<'_>,
        table: &str,
        filter: Option<Py<PyAny>>,
        order: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
        columns: Option<Vec<String>>,
        distinct: bool,
        ctes: Option<Py<PyAny>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("transaction already closed"))?;

        let filter_value = match filter {
            Some(obj) if !obj.is_none(py) => Some(py_to_value(obj.bind(py))?),
            _ => None,
        };
        let ctes_value = match ctes {
            Some(obj) if !obj.is_none(py) => {
                let value = py_to_value(obj.bind(py))?;
                let items = value.as_array().ok_or_else(|| {
                    ValidationError::new_err("ctes must be a list of CTE definitions")
                })?;
                Some(parse_ctes_items(items).map_err(map_err)?)
            }
            _ => None,
        };
        let rows = select_core(
            txn,
            table,
            filter_value,
            order,
            limit,
            offset,
            columns,
            distinct,
            ctes_value,
        )?;
        rows.iter().map(|row| row_to_py(py, row)).collect()
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

    /// Approximate nearest-neighbour search over an `Embedding` column's ANN
    /// index; returns the top-`k` rows (as JSON strings).
    fn ann_search(
        &self,
        table: &str,
        column: &str,
        query: Vec<f32>,
        k: usize,
    ) -> PyResult<Vec<String>> {
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("transaction already closed"))?;
        let rows = txn.ann_search(table, column, query, k).map_err(map_err)?;
        rows.iter().map(row_to_json).collect()
    }

    /// Learned-sparse (SPLADE) retrieval over a `Sparse` column's index; returns
    /// the top-`k` rows for the weighted query tokens (as JSON strings).
    fn sparse_match(
        &self,
        table: &str,
        column: &str,
        query: Vec<(u32, f32)>,
        k: usize,
    ) -> PyResult<Vec<String>> {
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("transaction already closed"))?;
        let rows = txn.sparse_match(table, column, query, k).map_err(map_err)?;
        rows.iter().map(row_to_json).collect()
    }

    fn commit(&mut self) -> PyResult<()> {
        if let Some(txn) = self.txn.take() {
            let result = txn.commit().map_err(map_err);
            self.release_pin();
            result
        } else {
            Err(PyRuntimeError::new_err("transaction already closed"))
        }
    }

    fn rollback(&mut self) -> PyResult<()> {
        if let Some(txn) = self.txn.take() {
            txn.rollback();
        }
        self.release_pin();
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

/// Run a SELECT, returning the raw kit rows. Shared by the JSON-string
/// [`PyTransaction::select`] and the direct-conversion
/// [`PyTransaction::select_py`]; each then serializes the rows in its own shape.
#[allow(clippy::too_many_arguments)]
fn select_core(
    txn: &Transaction<'static>,
    table: &str,
    filter: Option<Value>,
    order: Option<&str>,
    limit: Option<usize>,
    offset: Option<usize>,
    columns: Option<Vec<String>>,
    distinct: bool,
    ctes: Option<Vec<Cte>>,
) -> PyResult<Vec<mongreldb_kit::Row>> {
    let select =
        build_select_stmt(table, filter, order, limit, offset, columns).map_err(map_err)?;
    let rows = match ctes {
        Some(ctes) => txn.select_with(&ctes, &select).map_err(map_err)?,
        None if distinct => txn
            .select_distinct(&Query::Select(select))
            .map_err(map_err)?,
        None => txn.select(&Query::Select(select)).map_err(map_err)?,
    };
    Ok(rows)
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

fn parse_on_conflict(json: &str) -> PyResult<OnConflict> {
    let value: Value = serde_json::from_str(json).map_err(py_json_err)?;
    match value {
        Value::Null => Ok(OnConflict::DoNothing),
        Value::String(action) if action == "do_nothing" => Ok(OnConflict::DoNothing),
        Value::String(action) => Err(ValidationError::new_err(format!(
            "unknown on_conflict action {action}"
        ))),
        Value::Object(mut map) => {
            if map.contains_key("do_nothing") {
                return Ok(OnConflict::DoNothing);
            }
            if let Some(patch) = map.remove("do_update").or_else(|| map.remove("set")) {
                // Accept both {"do_update": {"set": {...}}} and the older
                // {"do_update": {...}} shorthand, plus legacy top-level "set".
                if let Some(Value::Object(inner)) = patch.get("set") {
                    return Ok(OnConflict::DoUpdate(inner.clone()));
                }
                return patch
                    .as_object()
                    .cloned()
                    .map(OnConflict::DoUpdate)
                    .ok_or_else(|| ValidationError::new_err("do_update expects an object"));
            }
            if let Some(Value::String(action)) = map.remove("action") {
                return match action.as_str() {
                    "do_nothing" => Ok(OnConflict::DoNothing),
                    "do_update" => map
                        .remove("set")
                        .and_then(|v| v.as_object().cloned())
                        .map(OnConflict::DoUpdate)
                        .ok_or_else(|| ValidationError::new_err("do_update expects set object")),
                    other => Err(ValidationError::new_err(format!(
                        "unknown on_conflict action {other}"
                    ))),
                };
            }
            Err(ValidationError::new_err(
                "on_conflict must be do_nothing or do_update",
            ))
        }
        _ => Err(ValidationError::new_err(
            "on_conflict must be a string, object, or null",
        )),
    }
}

/// Convert a friendly object filter into a kit `Expr`.
///
/// Per-column shapes: `{ "col": { "op": value } }` where `op` is one of `eq`,
/// `ne`, `gt`, `gte`, `lt`, `lte`, `like`, `contains`, `bytes_prefix`, `in`,
/// `not_in`, `is_null`, `is_not_null`, `in_subquery`. `{ "col": value }` is
/// shorthand for `eq`. Top-level logical keys: `and`/`or` (array of filters),
/// `not` (a filter), `exists`/`not_exists` (a subselect). Multiple keys are
/// AND-ed.
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
                "bytes_prefix" => {
                    Expr::BytesPrefix(Box::new(col_expr()), as_str(operand, "bytes_prefix")?)
                }
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
    parse_ctes_items(&items)
}

/// Parse already-decoded CTE items. Shared by the JSON-string path
/// ([`parse_ctes`]) and the direct-conversion `select_py` path.
fn parse_ctes_items(items: &[Value]) -> Result<Vec<Cte>, KitError> {
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
    mongreldb_kit::migrate(db.require_db_mut()?, &migrations).map_err(map_err)
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
    m.add(
        "TriggerValidationError",
        py.get_type::<TriggerValidationError>(),
    )?;
    m.add("StorageError", py.get_type::<StorageError>())?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;

    set_code(m, "ValidationError", "VALIDATION")?;
    set_code(m, "DuplicateError", "DUPLICATE")?;
    set_code(m, "ForeignKeyError", "FOREIGN_KEY")?;
    set_code(m, "RestrictError", "RESTRICT")?;
    set_code(m, "MigrationError", "MIGRATION")?;
    set_code(m, "ConflictError", "CONFLICT")?;
    set_code(m, "TriggerValidationError", "TRIGGER_VALIDATION")?;
    set_code(m, "StorageError", "STORAGE")?;
    set_code(m, "IntegrityError", "INTEGRITY")?;

    Ok(())
}
