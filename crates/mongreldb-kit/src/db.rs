//! Database handle for `mongreldb-kit`.

use crate::error::{KitError, Result};
use crate::internal::{ensure_internal_tables, internal_tables_core};
use crate::schema::to_core_schema;
use mongreldb_core::epoch::Snapshot;
use mongreldb_core::memtable::Row as CoreRow;
use mongreldb_core::memtable::Value as CoreValue;
use mongreldb_core::schema::Schema as CoreSchema;
use mongreldb_core::Database as CoreDatabase;
use mongreldb_core::{AggState, ApproxAgg, NativeAgg, NativeAggResult, RowId};
use mongreldb_kit_core::schema::IndexKind as KitIndexKind;
use mongreldb_kit_core::schema::Schema as KitSchema;
use mongreldb_kit_core::schema::Table as KitTable;
use mongreldb_kit_core::{ProcedureSpec, TriggerSpec};
use serde_json::Value;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

const SCHEMA_FILE: &str = "kit_schema.json";

/// A named default-value provider registered by the application.
pub type DefaultProvider = Box<dyn Fn() -> Value + Send + Sync>;

/// The result of [`Database::explain`]: a static description of a predicate's
/// index push-down, without running the query.
#[derive(Debug, Clone)]
pub struct ExplainPlan {
    /// Whether at least one native index condition pushes down (vs. a full scan).
    pub index_accelerated: bool,
    /// Whether the push-down is exact — the whole predicate translated, so no
    /// Rust-side residual re-filtering is needed.
    pub exact: bool,
    /// The kind of each pushed condition (e.g. `BitmapEq`, `RangeInt`, `Ann`).
    pub pushed_conditions: Vec<String>,
}

/// A row paired with its Jaccard set-similarity to a query set (`0.0..=1.0`).
#[derive(Debug, Clone)]
pub struct SimilarRow {
    pub row: crate::schema::Row,
    pub similarity: f64,
}

/// Collect the string members of a set-valued column cell. Accepts either a
/// JSON array value or a JSON string holding an array (how the Kit stores
/// `json`/`text` set columns); anything else yields the empty set.
fn parse_string_set(value: Option<&Value>) -> std::collections::HashSet<String> {
    let arr = match value {
        Some(Value::Array(a)) => Some(a.clone()),
        Some(Value::String(s)) => serde_json::from_str::<Value>(s)
            .ok()
            .and_then(|v| v.as_array().cloned()),
        _ => None,
    };
    arr.into_iter()
        .flatten()
        .filter_map(|v| match v {
            Value::String(s) => Some(s),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            _ => None,
        })
        .collect()
}

/// Which aggregate to maintain incrementally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalAggKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// The result of [`Database::incremental_aggregate`].
#[derive(Debug, Clone)]
pub struct IncrementalAggregate {
    /// The exact aggregate value at the current epoch: a JSON number, or `null`
    /// when no rows matched (`COUNT` returns `0`, not null).
    pub value: Value,
    /// `true` when produced by merging only newly-committed rows (the fast
    /// path); `false` when a full recompute was required (cold cache, a delete,
    /// pending writes, or the same epoch as the cached state).
    pub incremental: bool,
    /// Rows processed in the delta pass (`0` for a full recompute).
    pub delta_rows: u64,
}

/// Stable per-`(table, column, agg, filter)` cache key for the engine's
/// incremental-aggregate cache. Deterministic within a process (fixed-seed
/// hasher); the cache itself is per-`Db`, so cross-process stability is moot.
fn incremental_cache_key(
    table_id: u32,
    column: Option<u16>,
    agg: IncrementalAggKind,
    conditions: &[mongreldb_core::query::Condition],
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    table_id.hash(&mut h);
    column.hash(&mut h);
    (agg as u8).hash(&mut h);
    // `Condition` has no `Hash`; its `Debug` form is stable and unique enough.
    format!("{conditions:?}").hash(&mut h);
    h.finish()
}

/// Finalize a mergeable [`AggState`] to a JSON scalar, preserving integer-ness
/// for `COUNT`/`MIN`/`MAX`/int `SUM` and using a float for averages / float
/// columns. `null` when there were no matching inputs.
fn agg_state_value(s: &AggState) -> Value {
    let num_f64 = |x: f64| {
        serde_json::Number::from_f64(x)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    };
    match s {
        AggState::Count(n) => Value::from(*n),
        AggState::SumI { sum, .. } => i64::try_from(*sum)
            .map(Value::from)
            .unwrap_or_else(|_| num_f64(*sum as f64)),
        AggState::SumF { sum, .. } => num_f64(*sum),
        AggState::AvgI { sum, count } if *count > 0 => num_f64(*sum as f64 / *count as f64),
        AggState::AvgF { sum, count } if *count > 0 => num_f64(*sum / *count as f64),
        AggState::AvgI { .. } | AggState::AvgF { .. } => Value::Null,
        AggState::MinI(n) | AggState::MaxI(n) => Value::from(*n),
        AggState::MinF(f) | AggState::MaxF(f) => num_f64(*f),
        AggState::Empty => Value::Null,
    }
}

/// Which approximate aggregate to estimate from the reservoir sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApproxAggKind {
    Count,
    Sum,
    Avg,
}

/// A reservoir-sampled approximate aggregate with a normal-theory confidence
/// interval. `ci_low`/`ci_high` bracket `point` at the requested z-score; the
/// interval collapses to zero width when the sample covers the whole table.
#[derive(Debug, Clone)]
pub struct ApproxAggregate {
    pub point: f64,
    pub ci_low: f64,
    pub ci_high: f64,
    pub n_population: u64,
    pub n_sample_live: usize,
    pub n_passing: usize,
}

/// Short kind label for a core `Condition` (the variant name), decoupled from
/// the enum shape via its `Debug` form.
fn condition_label(c: &mongreldb_core::query::Condition) -> String {
    let dbg = format!("{c:?}");
    dbg.split(['(', '{', ' ']).next().unwrap_or("").to_string()
}

/// A kit database handle.
///
/// Wraps a MongrelDB core database and a kit schema, providing table metadata
/// and transaction creation.
pub struct Database {
    pub(crate) inner: CoreDatabase,
    pub(crate) schema: KitSchema,
    pub(crate) root: PathBuf,
    /// Application-registered named default providers (`DefaultKind::CustomName`).
    pub(crate) default_providers: HashMap<String, DefaultProvider>,
}

impl Database {
    /// Open an existing kit database.
    pub fn open(path: &Path) -> Result<Self> {
        let inner = CoreDatabase::open(path)?;
        let schema = load_schema(path)?;
        // Ensure reserved tables exist for databases created by older versions.
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
        })
    }

    /// Open an existing page-encrypted kit database with its passphrase.
    pub fn open_encrypted(path: &Path, passphrase: &str) -> Result<Self> {
        let inner = CoreDatabase::open_encrypted(path, passphrase)?;
        let schema = load_schema(path)?;
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
        })
    }

    /// Create a fresh page-encrypted kit database (AES-256-GCM; the passphrase
    /// derives the key hierarchy). Columns flagged `encrypted` /
    /// `encrypted_indexable` are encrypted at rest.
    pub fn create_encrypted(path: &Path, schema: KitSchema, passphrase: &str) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let inner = CoreDatabase::create_encrypted(path, passphrase)?;
        ensure_internal_tables(&inner)?;
        store_schema(path, &schema)?;
        for table in &schema.tables {
            create_core_table(&inner, &table.name, to_core_schema(table))?;
        }
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
        })
    }

    /// Create a fresh kit database with the given schema.
    pub fn create(path: &Path, schema: KitSchema) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let inner = CoreDatabase::create(path)?;

        // Create the reserved kit tables first so we can record migrations,
        // reserve unique keys, and touch row guards.
        ensure_internal_tables(&inner)?;

        // Persist the kit schema to a sidecar file (core tables cannot update
        // a specific row by id, so a file is the pragmatic stable store).
        store_schema(path, &schema)?;

        // Create core tables for every user table.
        for table in &schema.tables {
            create_core_table(&inner, &table.name, to_core_schema(table))?;
        }

        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
        })
    }

    /// Register a named default provider used by `DefaultKind::CustomName`
    /// columns. Returns the database for chaining.
    pub fn register_default(
        &mut self,
        name: impl Into<String>,
        provider: impl Fn() -> Value + Send + Sync + 'static,
    ) {
        self.default_providers
            .insert(name.into(), Box::new(provider));
    }

    /// The raw, unguarded MongrelDB core database. This is the Rust analogue of
    /// the TypeScript kit's `nativeDb` escape hatch: writes made directly
    /// against it bypass all kit constraints.
    pub fn raw(&self) -> &CoreDatabase {
        &self.inner
    }

    /// Application table names, excluding the reserved `__kit_*` tables.
    pub fn table_names(&self) -> Vec<String> {
        self.schema
            .tables
            .iter()
            .map(|t| t.name.clone())
            .filter(|n| !n.starts_with("__kit_"))
            .collect()
    }

    pub fn create_procedure(
        &self,
        spec: &ProcedureSpec,
    ) -> Result<mongreldb_core::StoredProcedure> {
        let procedure = core_procedure(spec)?;
        self.inner
            .create_procedure(procedure)
            .map_err(KitError::from)
    }

    pub fn replace_procedure(
        &self,
        spec: &ProcedureSpec,
    ) -> Result<mongreldb_core::StoredProcedure> {
        let procedure = core_procedure(spec)?;
        self.inner
            .create_or_replace_procedure(procedure)
            .map_err(KitError::from)
    }

    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        self.inner.drop_procedure(name).map_err(KitError::from)
    }

    pub fn call_procedure(
        &self,
        name: &str,
        args: serde_json::Map<String, Value>,
    ) -> Result<mongreldb_core::ProcedureCallResult> {
        let args = args
            .iter()
            .map(|(key, value)| Ok((key.clone(), json_to_core_value(value)?)))
            .collect::<Result<HashMap<_, _>>>()?;
        self.inner
            .call_procedure(name, args)
            .map_err(KitError::from)
    }

    pub fn create_trigger(&self, spec: &TriggerSpec) -> Result<mongreldb_core::StoredTrigger> {
        let trigger = core_trigger(spec)?;
        self.inner.create_trigger(trigger).map_err(KitError::from)
    }

    pub fn replace_trigger(&self, spec: &TriggerSpec) -> Result<mongreldb_core::StoredTrigger> {
        let trigger = core_trigger(spec)?;
        self.inner
            .create_or_replace_trigger(trigger)
            .map_err(KitError::from)
    }

    pub fn drop_trigger(&self, name: &str) -> Result<()> {
        self.inner.drop_trigger(name).map_err(KitError::from)
    }

    pub fn triggers(&self) -> Vec<mongreldb_core::StoredTrigger> {
        self.inner.triggers()
    }

    pub fn trigger(&self, name: &str) -> Option<mongreldb_core::StoredTrigger> {
        self.inner.trigger(name)
    }

    /// Allocate `count` values from the named sequence, returning the first
    /// allocated value. A fresh sequence starts at `1` (SQL AUTO_INCREMENT
    /// semantics). The allocation
    /// runs in its own committed transaction and retries on write-write
    /// conflicts.
    pub fn allocate_sequence(&self, name: &str, count: i64) -> Result<i64> {
        use crate::internal::cols;
        let mut attempt = 0;
        loop {
            let mut txn = self.inner.begin();
            let snapshot = txn.read_snapshot();
            let existing = self
                .visible_core_rows_at(crate::internal::SEQUENCES, snapshot)?
                .into_iter()
                .find(|r| internal_bytes(r, cols::SEQ_NAME) == Some(name.to_string()));

            let now = crate::internal::iso_now();
            // Sequences are 1-based, matching SQL AUTO_INCREMENT / SERIAL. A
            // starting value of 0 is unsafe: applications use 0 as the "unset"
            // sentinel for nullable foreign keys.
            let (start, next, old_row_id) = match &existing {
                Some(row) => {
                    let current = match row.columns.get(&cols::SEQ_NEXT) {
                        Some(CoreValue::Int64(i)) => *i,
                        _ => 1,
                    };
                    (current, current + count, Some(row.row_id))
                }
                None => (1, 1 + count, None),
            };

            if let Some(rid) = old_row_id {
                txn.delete(crate::internal::SEQUENCES, rid)
                    .map_err(KitError::from)?;
            }
            txn.put(
                crate::internal::SEQUENCES,
                vec![
                    (cols::SEQ_NAME, CoreValue::Bytes(name.as_bytes().to_vec())),
                    (cols::SEQ_NEXT, CoreValue::Int64(next)),
                    (cols::SEQ_UPDATED, CoreValue::Bytes(now.into_bytes())),
                ],
            )
            .map_err(KitError::from)?;
            match txn.commit() {
                Ok(_) => return Ok(start),
                Err(mongreldb_core::MongrelError::Conflict(_)) if attempt < 10_000 => {
                    attempt += 1;
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => return Err(KitError::from(e)),
            }
        }
    }

    /// Run `f` inside a kit transaction, committing on success and retrying on
    /// retryable write-write conflicts up to `max_retries` times.
    pub fn transaction<T, F>(&self, max_retries: usize, mut f: F) -> Result<T>
    where
        F: FnMut(&mut crate::txn::Transaction<'_>) -> Result<T>,
    {
        let mut attempt = 0;
        loop {
            let mut txn = self.begin()?;
            match f(&mut txn) {
                Ok(value) => match txn.commit() {
                    Ok(()) => return Ok(value),
                    Err(KitError::Conflict(_)) if attempt < max_retries => {
                        attempt += 1;
                        continue;
                    }
                    Err(e) => return Err(e),
                },
                Err(KitError::Conflict(_)) if attempt < max_retries => {
                    txn.rollback();
                    attempt += 1;
                    continue;
                }
                Err(e) => {
                    txn.rollback();
                    return Err(e);
                }
            }
        }
    }

    /// Look up a table definition by name.
    pub fn table(&self, name: &str) -> Option<&KitTable> {
        self.schema.table(name)
    }

    /// Return the currently loaded schema.
    pub fn schema(&self) -> &KitSchema {
        &self.schema
    }

    /// Begin a new kit transaction.
    pub fn begin(&self) -> Result<crate::txn::Transaction<'_>> {
        let core_txn = self.inner.begin();
        Ok(crate::txn::Transaction::new(self, core_txn))
    }

    /// Replace the in-memory schema, usually after a migration.
    pub fn set_schema(&mut self, schema: KitSchema) {
        self.schema = schema;
    }

    /// Verify that the sidecar schema file and all reserved `__kit_*` tables
    /// are present.
    pub fn check_internal_tables(&self) -> Result<()> {
        let schema_file = self.root.join(SCHEMA_FILE);
        if !schema_file.exists() {
            return Err(KitError::Integrity(format!(
                "schema file {} is missing",
                schema_file.display()
            )));
        }
        for (name, _) in internal_tables_core() {
            if self.inner.table_id(name).is_err() {
                return Err(KitError::Integrity(format!(
                    "internal table {name} is missing"
                )));
            }
        }
        Ok(())
    }

    /// Reclaim orphaned runs and stale WAL/shadow files; returns the count
    /// removed. Safe to run on a live database.
    pub fn gc(&self) -> Result<usize> {
        self.inner.gc().map_err(KitError::from)
    }

    /// Verify run footer checksums; returns any integrity issues as JSON objects
    /// (`table_id`, `table_name`, `severity`, `description`). Empty ⇒ healthy.
    pub fn check(&self) -> Vec<serde_json::Value> {
        self.inner
            .check()
            .into_iter()
            .map(|i| {
                serde_json::json!({
                    "table_id": i.table_id,
                    "table_name": i.table_name,
                    "severity": i.severity,
                    "description": i.description,
                })
            })
            .collect()
    }

    /// Drop corrupt runs; returns the ids of the runs that were dropped.
    pub fn doctor(&self) -> Result<Vec<u64>> {
        self.inner.doctor().map_err(KitError::from)
    }

    /// The current visible commit epoch — a monotonically increasing version
    /// stamp. A committed write bumps it; a snapshot at this epoch sees all
    /// currently-committed data.
    pub fn snapshot_epoch(&self) -> u64 {
        self.inner.snapshot().0.epoch.0
    }

    /// Export every visible row of `table` as a TSV document (header row of
    /// column names, tab-separated cells, `NULL` = empty field). See
    /// [`crate::tsv`] for the escaping rules.
    pub fn export_tsv(&self, table: &str) -> Result<String> {
        let t = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| KitError::Validation(format!("unknown table '{table}'")))?
            .clone();
        let tx = self.begin()?;
        let rows = tx.all_rows(table)?;
        Ok(crate::tsv::rows_to_tsv(&t, &rows))
    }

    /// Import a TSV document into `table` (one committed transaction). Each row
    /// passes through defaults, validation, and constraint checks like a normal
    /// insert. Returns the number of rows inserted.
    pub fn import_tsv(&self, table: &str, text: &str) -> Result<usize> {
        let t = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| KitError::Validation(format!("unknown table '{table}'")))?
            .clone();
        let rows = crate::tsv::tsv_to_rows(&t, text)?;
        let n = rows.len();
        self.transaction(1, |tx| {
            tx.insert_many(table, rows.clone())?;
            Ok(())
        })?;
        Ok(n)
    }

    /// Describe how `predicate` would be executed against `table`: which native
    /// index conditions push down, whether the push-down is exact (no residual
    /// re-filtering), and whether any index acceleration applies at all. A pure
    /// diagnostic — it plans but does not run the query.
    pub fn explain(
        &self,
        table: &str,
        predicate: &mongreldb_kit_core::query::Expr,
    ) -> Result<ExplainPlan> {
        let t = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| KitError::Validation(format!("unknown table '{table}'")))?;
        Ok(match crate::pushdown::translate_predicate(t, predicate) {
            Some(p) => ExplainPlan {
                index_accelerated: p.can_push(),
                exact: p.fully_translated,
                pushed_conditions: p.conditions.iter().map(condition_label).collect(),
            },
            None => ExplainPlan {
                index_accelerated: false,
                exact: false,
                pushed_conditions: Vec::new(),
            },
        })
    }

    /// Read every row of `table` visible at commit `epoch` — a point-in-time
    /// (MVCC time-travel) read. `epoch` must not exceed the current snapshot
    /// epoch. Rows reclaimed by GC/compaction for retired snapshots may no
    /// longer be reconstructable; this reads whatever the engine still retains
    /// at that epoch.
    pub fn rows_at_epoch(&self, table: &str, epoch: u64) -> Result<Vec<crate::schema::Row>> {
        let t = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| KitError::Validation(format!("unknown table '{table}'")))?;
        let current = self.snapshot_epoch();
        if epoch > current {
            return Err(KitError::Validation(format!(
                "epoch {epoch} is in the future (current committed epoch is {current})"
            )));
        }
        let snap = Snapshot::at(mongreldb_core::epoch::Epoch(epoch));
        let rows = self.visible_core_rows_at(table, snap)?;
        rows.iter()
            .map(|r| crate::schema::core_row_to_json(r, t))
            .collect()
    }

    /// Estimate an aggregate over `table` from the engine's reservoir sample,
    /// returning a point estimate and a `z`-score confidence interval (e.g.
    /// `z = 1.96` for ~95%). `column` is required for `Sum`/`Avg` and ignored
    /// for `Count`. Returns `None` when the reservoir is empty (no sampled rows
    /// yet). Fast and O(sample) — trades exactness for speed on large tables.
    pub fn approx_aggregate(
        &self,
        table: &str,
        column: Option<&str>,
        agg: ApproxAggKind,
        z: f64,
    ) -> Result<Option<ApproxAggregate>> {
        let t = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| KitError::Validation(format!("unknown table '{table}'")))?;
        if matches!(agg, ApproxAggKind::Sum | ApproxAggKind::Avg) && column.is_none() {
            return Err(KitError::Validation(
                "approx sum/avg requires a column".into(),
            ));
        }
        let cid = match column {
            Some(name) => Some(
                t.columns
                    .iter()
                    .find(|c| c.name == name)
                    .ok_or_else(|| KitError::Validation(format!("unknown column '{name}'")))?
                    .id as u16,
            ),
            None => None,
        };
        let core_agg = match agg {
            ApproxAggKind::Count => ApproxAgg::Count,
            ApproxAggKind::Sum => ApproxAgg::Sum,
            ApproxAggKind::Avg => ApproxAgg::Avg,
        };
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let mut guard = handle.lock();
        let res = guard
            .approx_aggregate(&[], cid, core_agg, z)
            .map_err(KitError::from)?;
        Ok(res.map(|r| ApproxAggregate {
            point: r.point,
            ci_low: r.ci_low,
            ci_high: r.ci_high,
            n_population: r.n_population,
            n_sample_live: r.n_sample_live,
            n_passing: r.n_passing,
        }))
    }

    /// Stream `table` in row batches without materializing the whole table at
    /// once. `f` receives successive chunks of at most `batch_size` value-maps,
    /// all from one snapshot. Backed by the engine's native scan cursor when the
    /// table has a sorted run; for an overlay-only table (no run yet) it falls
    /// back to a single in-memory pass, still chunked to `batch_size`.
    pub fn scan_batched<F>(&self, table: &str, batch_size: usize, mut f: F) -> Result<()>
    where
        F: FnMut(&[serde_json::Map<String, Value>]) -> Result<()>,
    {
        let kit_t = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| KitError::Validation(format!("unknown table '{table}'")))?;
        let batch_size = batch_size.max(1);
        // Keep the pin guard alive for the whole scan so GC can't reclaim the
        // snapshot's versions mid-stream.
        let (snapshot, _pin) = self.inner.snapshot();
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let guard = handle.lock();

        // Projection + per-column (name, kit type), index-aligned, in core order.
        let mut projection: Vec<(u16, mongreldb_core::schema::TypeId)> = Vec::new();
        let mut meta: Vec<(String, mongreldb_kit_core::schema::ColumnType)> = Vec::new();
        for c in &guard.schema().columns {
            if let Some(kc) = kit_t.columns.iter().find(|kc| kc.id as u16 == c.id) {
                projection.push((c.id, c.ty));
                meta.push((kc.name.clone(), kc.storage_type));
            }
        }

        match guard
            .scan_cursor(snapshot, projection, &[])
            .map_err(KitError::from)?
        {
            Some(mut cursor) => {
                let mut buf: Vec<serde_json::Map<String, Value>> = Vec::with_capacity(batch_size);
                while let Some(batch) = cursor.next_batch().map_err(KitError::from)? {
                    let nrows = batch.first().map(|c| c.len()).unwrap_or(0);
                    for j in 0..nrows {
                        let mut m = serde_json::Map::new();
                        for (ci, (name, ty)) in meta.iter().enumerate() {
                            let cv = batch
                                .get(ci)
                                .and_then(|col| col.value_at(j))
                                .unwrap_or(CoreValue::Null);
                            m.insert(name.clone(), crate::schema::core_to_json(&cv, *ty)?);
                        }
                        buf.push(m);
                        if buf.len() >= batch_size {
                            f(&buf)?;
                            buf.clear();
                        }
                    }
                }
                if !buf.is_empty() {
                    f(&buf)?;
                }
                Ok(())
            }
            None => {
                drop(guard);
                let rows = self.visible_core_rows_at(table, snapshot)?;
                let maps: Vec<serde_json::Map<String, Value>> = rows
                    .iter()
                    .map(|r| crate::schema::core_row_to_json(r, kit_t).map(|row| row.values))
                    .collect::<Result<Vec<_>>>()?;
                for chunk in maps.chunks(batch_size) {
                    f(chunk)?;
                }
                Ok(())
            }
        }
    }

    /// Rank rows of `table` by Jaccard set-similarity between `query` and the
    /// string set stored (as a JSON array) in `column`, returning the top `k`
    /// with similarity `> 0`, highest first — the dedup/join primitive.
    ///
    /// When `column` has a `MinHash` index, candidate rows come from the engine's
    /// LSH index (sub-linear) and are then re-verified with exact Jaccard, so the
    /// top-k is exact for the recalled candidates (LSH recall is high but < 100%).
    /// Without an index it is an exact linear scan.
    pub fn set_similarity(
        &self,
        table: &str,
        column: &str,
        query: &[String],
        k: usize,
    ) -> Result<Vec<SimilarRow>> {
        let t = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| KitError::Validation(format!("unknown table '{table}'")))?;
        let col = t.columns.iter().find(|c| c.name == column).ok_or_else(|| {
            KitError::Validation(format!("unknown column '{column}' on table '{table}'"))
        })?;
        let query_set: std::collections::HashSet<String> = query.iter().cloned().collect();

        let has_minhash = t.indexes.iter().any(|idx| {
            idx.kind == KitIndexKind::MinHash && idx.columns.iter().any(|c| c == column)
        });
        let rows = if has_minhash {
            // Sub-linear candidate generation via the engine MinHash/LSH index.
            let query_hashes: Vec<u64> = query
                .iter()
                .map(|s| mongreldb_core::index::minhash_token_hash(s))
                .collect();
            // Generous candidate budget so exact top-k keeps high recall.
            let cand_k = k.saturating_mul(8).max(k + 64);
            let cond = mongreldb_core::query::Condition::MinHashSimilar {
                column_id: col.id as u16,
                query: query_hashes,
                k: cand_k,
            };
            let (snapshot, _pin) = self.inner.snapshot();
            let core_rows = self.query_core_rows_at(table, &[cond], snapshot)?;
            core_rows
                .iter()
                .map(|r| crate::schema::core_row_to_json(r, t))
                .collect::<Result<Vec<_>>>()?
        } else {
            let tx = self.begin()?;
            tx.all_rows(table)?
        };

        let mut scored: Vec<SimilarRow> = Vec::new();
        for row in rows {
            let set = parse_string_set(row.values.get(column));
            let inter = set.iter().filter(|x| query_set.contains(*x)).count();
            let union = set.len() + query_set.len() - inter;
            let sim = if union == 0 {
                0.0
            } else {
                inter as f64 / union as f64
            };
            if sim > 0.0 {
                scored.push(SimilarRow {
                    row,
                    similarity: sim,
                });
            }
        }
        scored.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(k);
        Ok(scored)
    }

    /// Flush every table's in-memory writes to durable sorted runs. Besides
    /// durability, this empties the memtable, which is what enables the engine's
    /// incremental-aggregate fast path (see [`Self::incremental_aggregate`]).
    pub fn flush(&self) -> Result<()> {
        for name in self.inner.table_names() {
            let handle = self.inner.table(&name).map_err(KitError::from)?;
            let mut guard = handle.lock();
            guard.flush().map_err(KitError::from)?;
        }
        Ok(())
    }

    /// Maintain and read an aggregate over `table` that updates by merging only
    /// newly-committed rows instead of rescanning. `column` is required for
    /// `Sum`/`Min`/`Max`/`Avg` and ignored for `Count`. An optional `filter`
    /// restricts the aggregate; it must translate **exactly** to index
    /// conditions (no residual), otherwise this errors — an inexact filter would
    /// silently aggregate the wrong rows.
    ///
    /// The engine keeps a per-`(table, column, agg, filter)` cached state and,
    /// on a warm cache with an advanced epoch and no deletes/pending writes,
    /// folds in just the delta. The returned `value` is always exact; the
    /// `incremental` flag reports whether the fast path was taken.
    pub fn incremental_aggregate(
        &self,
        table: &str,
        column: Option<&str>,
        agg: IncrementalAggKind,
        filter: Option<&mongreldb_kit_core::query::Expr>,
    ) -> Result<IncrementalAggregate> {
        let t = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == table)
            .ok_or_else(|| KitError::Validation(format!("unknown table '{table}'")))?;
        if !matches!(agg, IncrementalAggKind::Count) && column.is_none() {
            return Err(KitError::Validation(
                "sum/min/max/avg incremental aggregate requires a column".into(),
            ));
        }
        let cid = match column {
            Some(name) => Some(
                t.columns
                    .iter()
                    .find(|c| c.name == name)
                    .ok_or_else(|| KitError::Validation(format!("unknown column '{name}'")))?
                    .id as u16,
            ),
            None => None,
        };
        let conditions = match filter {
            Some(expr) => {
                let plan = crate::pushdown::translate_predicate(t, expr).ok_or_else(|| {
                    KitError::Validation(
                        "filter is not index-translatable for an incremental aggregate".into(),
                    )
                })?;
                if !plan.fully_translated {
                    return Err(KitError::Validation(
                        "filter has a residual that an incremental aggregate cannot apply exactly"
                            .into(),
                    ));
                }
                plan.conditions
            }
            None => Vec::new(),
        };
        let core_agg = match agg {
            IncrementalAggKind::Count => NativeAgg::Count,
            IncrementalAggKind::Sum => NativeAgg::Sum,
            IncrementalAggKind::Min => NativeAgg::Min,
            IncrementalAggKind::Max => NativeAgg::Max,
            IncrementalAggKind::Avg => NativeAgg::Avg,
        };
        let cache_key = incremental_cache_key(t.id, cid, agg, &conditions);
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let mut guard = handle.lock();
        let res = guard
            .aggregate_incremental(cache_key, &conditions, cid, core_agg)
            .map_err(KitError::from)?;
        Ok(IncrementalAggregate {
            value: agg_state_value(&res.state),
            incremental: res.incremental,
            delta_rows: res.delta_rows,
        })
    }

    /// Return the migrations already recorded in `__kit_schema_migrations`.
    pub fn applied_migrations(&self) -> Result<Vec<mongreldb_kit_core::migrations::Migration>> {
        crate::migrate::load_applied_migrations(&self.inner)
    }

    pub(crate) fn core_db(&self) -> &CoreDatabase {
        &self.inner
    }

    /// Best-effort flush-on-close (§4.4): force-flush pending writes on every
    /// table to `.sr` sorted runs so WAL segments stay bounded across repeated
    /// short-lived process invocations (e.g. the CLI). Call as the last action
    /// before exit. The daemon does not need this (auto-compactor handles it).
    pub fn close(&self) -> Result<()> {
        self.inner.close().map_err(KitError::from)
    }

    /// Compact all tables: merge sorted runs into one clean run each so query
    /// latency stays flat. Returns `(compacted, skipped)`. Safe to run at any
    /// time — honors snapshot retention. The daemon's background auto-compactor
    /// already does this periodically; this is for manual/cron use.
    pub fn compact_all(&self) -> Result<(usize, usize)> {
        self.inner.compact().map_err(KitError::from)
    }

    /// Compact a single table by name. Returns `true` if compacted, `false` if
    /// skipped (< 2 runs).
    pub fn compact_table(&self, name: &str) -> Result<bool> {
        self.inner.compact_table(name).map_err(KitError::from)
    }

    /// Direct HOT (PK → RowId) lookup via the core engine — no full-row
    /// materialization. Used by the §4.3 delete fast path when the table
    /// has no Kit-level constraints requiring guard cleanup.
    pub(crate) fn lookup_row_id(&self, table: &str, key: &[u8]) -> Result<Option<RowId>> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let mut guard = handle.lock();
        guard.ensure_indexes_complete()?;
        Ok(guard.lookup_pk(key))
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// All visible core rows for a table at a specific read snapshot. Used so
    /// kit transactions read at their own snapshot (repeatable reads) rather
    /// than the latest committed state.
    pub(crate) fn visible_core_rows_at(
        &self,
        table_name: &str,
        snapshot: Snapshot,
    ) -> Result<Vec<CoreRow>> {
        let handle = self.inner.table(table_name).map_err(KitError::from)?;
        let guard = handle.lock();
        guard.visible_rows(snapshot).map_err(KitError::from)
    }

    /// Query visible core rows with native `Condition`s at a specific read
    /// snapshot (Kit Priority 1 pushdown). Resolves `conditions` via core's
    /// indexes (HOT / bitmap / range) and returns only the matching rows —
    /// avoiding the full scan that `visible_core_rows_at` does. Returns the
    /// empty vec when no conditions match, and falls back to
    /// `visible_core_rows_at` when `conditions` is empty (unfiltered).
    pub(crate) fn query_core_rows_at(
        &self,
        table_name: &str,
        conditions: &[mongreldb_core::query::Condition],
        snapshot: Snapshot,
    ) -> Result<Vec<CoreRow>> {
        if conditions.is_empty() {
            return self.visible_core_rows_at(table_name, snapshot);
        }
        let handle = self.inner.table(table_name).map_err(KitError::from)?;
        let mut guard = handle.lock();
        let q = mongreldb_core::query::Query {
            conditions: conditions.to_vec(),
        };
        guard.query(&q).map_err(KitError::from)
    }

    /// Drain `table`'s memtable into the mutable-run tier, spilling to a
    /// durable, checkpointed `.sr` run once the tier crosses its watermark.
    /// Called after a large batch commit (see `Transaction::commit`) so a
    /// short-lived process (the CLI, or any fresh `Database::open`) isn't
    /// stuck replaying the whole batch from the WAL on its next invocation —
    /// without a flush, committed-but-unflushed writes only exist as WAL
    /// records and must be fully replayed to rebuild the in-memory indexes.
    pub(crate) fn flush_table(&self, table_name: &str) -> Result<()> {
        let handle = self.inner.table(table_name).map_err(KitError::from)?;
        handle.lock().flush().map_err(KitError::from)?;
        Ok(())
    }

    /// Count visible rows matching `conditions` without materializing them
    /// (Kit Priority 7 pushdown). Returns `None` when the conditions cannot be
    /// served by indexes, or when `snapshot` is not the latest committed epoch
    /// (caller falls back to a snapshot-correct row scan).
    ///
    /// `count_conditions` counts the engine's latest committed index state, not
    /// a snapshot-filtered scan, so it only matches a repeatable-read row count
    /// when the read snapshot IS the latest epoch. We hold the table lock while
    /// comparing, so no commit can interleave between the check and the count.
    pub(crate) fn count_core_rows_at(
        &self,
        table_name: &str,
        conditions: &[mongreldb_core::query::Condition],
        snapshot: Snapshot,
    ) -> Result<Option<u64>> {
        let handle = self.inner.table(table_name).map_err(KitError::from)?;
        let mut guard = handle.lock();
        if guard.snapshot().epoch != snapshot.epoch {
            return Ok(None); // stale read snapshot ⇒ caller scans
        }
        guard
            .count_conditions(conditions, snapshot)
            .map_err(KitError::from)
    }

    /// Compute `SUM`/`MIN`/`MAX`/`AVG`/`COUNT(col)` over a column without
    /// materializing rows (Kit Priority 7), via the engine's native aggregate.
    /// `column` is the engine column id. Returns `None` when the engine fast
    /// path does not apply (multi-run / non-empty overlay / non-numeric column),
    /// or when `snapshot` is not the latest committed epoch — the same
    /// guarantee as [`count_core_rows_at`](Self::count_core_rows_at): the engine
    /// aggregate matches a snapshot-consistent row scan only at the latest epoch,
    /// and we compare under the table lock so no commit can interleave.
    pub(crate) fn aggregate_core_at(
        &self,
        table_name: &str,
        column: Option<u16>,
        conditions: &[mongreldb_core::query::Condition],
        agg: NativeAgg,
        snapshot: Snapshot,
    ) -> Result<Option<NativeAggResult>> {
        let handle = self.inner.table(table_name).map_err(KitError::from)?;
        let guard = handle.lock();
        if guard.snapshot().epoch != snapshot.epoch {
            return Ok(None); // stale read snapshot ⇒ caller scans
        }
        guard
            .aggregate_native(snapshot, column, conditions, agg)
            .map_err(KitError::from)
    }

    /// `COUNT(DISTINCT col)` from the bitmap index's partition cardinality (Kit
    /// Priority 7) — the number of distinct indexed values, no scan. Returns
    /// `None` without a bitmap index on the column, when the table is not
    /// insert-only, or when `snapshot` is not the latest committed epoch. The
    /// engine method reads the latest committed index state (no snapshot
    /// parameter), so — as with [`count_core_rows_at`](Self::count_core_rows_at)
    /// — it only matches a repeatable-read scan at the latest epoch; we compare
    /// under the table lock so no commit can interleave.
    pub(crate) fn count_distinct_core_at(
        &self,
        table_name: &str,
        column_id: u16,
        snapshot: Snapshot,
    ) -> Result<Option<u64>> {
        let handle = self.inner.table(table_name).map_err(KitError::from)?;
        let mut guard = handle.lock();
        if guard.snapshot().epoch != snapshot.epoch {
            return Ok(None); // stale read snapshot ⇒ caller scans
        }
        guard
            .count_distinct_from_bitmap(column_id)
            .map_err(KitError::from)
    }

    /// Materialize a single row by storage row id.
    #[allow(dead_code)]
    pub(crate) fn get_core_row(&self, table_name: &str, row_id: u64) -> Result<Option<CoreRow>> {
        let handle = self.inner.table(table_name).map_err(KitError::from)?;
        let guard = handle.lock();
        let snapshot = guard.snapshot();
        Ok(guard.get(mongreldb_core::RowId(row_id), snapshot))
    }
}

pub(crate) fn create_core_table(db: &CoreDatabase, name: &str, schema: CoreSchema) -> Result<()> {
    if db.table_id(name).is_ok() {
        return Ok(());
    }
    db.create_table(name, schema).map_err(KitError::from)?;
    Ok(())
}

fn core_procedure(spec: &ProcedureSpec) -> Result<mongreldb_core::StoredProcedure> {
    let parsed: mongreldb_core::StoredProcedure =
        serde_json::from_value(spec.json.clone()).map_err(KitError::from)?;
    mongreldb_core::StoredProcedure::new(parsed.name, parsed.mode, parsed.params, parsed.body, 0)
        .map_err(KitError::from)
}

fn core_trigger(spec: &TriggerSpec) -> Result<mongreldb_core::StoredTrigger> {
    let parsed: mongreldb_core::StoredTrigger =
        serde_json::from_value(spec.json.clone()).map_err(KitError::from)?;
    mongreldb_core::StoredTrigger::new(
        parsed.name,
        mongreldb_core::TriggerDefinition {
            target: parsed.target,
            timing: parsed.timing,
            event: parsed.event,
            update_of: parsed.update_of,
            target_columns: parsed.target_columns,
            when: parsed.when,
            program: parsed.program,
        },
        0,
    )
    .map_err(KitError::from)
}

fn json_to_core_value(value: &Value) -> Result<CoreValue> {
    match value {
        Value::Null => Ok(CoreValue::Null),
        Value::Bool(value) => Ok(CoreValue::Bool(*value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(CoreValue::Int64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(CoreValue::Float64(value))
            } else {
                Err(KitError::Validation("unsupported JSON number".into()))
            }
        }
        Value::String(value) => Ok(CoreValue::Bytes(value.as_bytes().to_vec())),
        Value::Array(_) | Value::Object(_) => Err(KitError::Validation(
            "procedure args only support scalar JSON values".into(),
        )),
    }
}

/// Read a `Bytes` column from an internal-table core row as a UTF-8 string.
pub(crate) fn internal_bytes(row: &CoreRow, col_id: u16) -> Option<String> {
    match row.columns.get(&col_id) {
        Some(CoreValue::Bytes(b)) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
}

/// Best-effort: reap any WAL segments a previous session left rotated but
/// unreaped, now that this `open()` has minted a fresh active segment
/// (`SharedWal::open` never truncates prior segments on its own —
/// [`CoreDatabase::gc`] does, but only once every mounted table's data is
/// durable in runs). Called before any write in *this* session, so that
/// check reflects exactly what the previous session left behind: if that
/// session ended with everything flushed (e.g. a bulk `insert_many`
/// followed by `Transaction::commit`'s large-batch auto-flush), this is the
/// one moment the now-inactive segment holding that batch is actually
/// eligible for cleanup. Without it, a short-lived process (the CLI has no
/// daemon mode; every invocation opens cold) keeps paying to read and
/// deserialize that segment's records on every subsequent open, even though
/// none of them still need replaying. Errors are ignored — this is a
/// disk-usage/reopen-latency optimization, never a correctness requirement.
fn reap_rotated_wal_segments(db: &CoreDatabase) {
    let _ = db.gc();
}

pub(crate) fn load_schema(path: &Path) -> Result<KitSchema> {
    let file = path.join(SCHEMA_FILE);
    let json = std::fs::read_to_string(&file)
        .map_err(|e| KitError::Migration(format!("cannot read schema file: {e}")))?;
    let schema: KitSchema = serde_json::from_str(&json)?;
    Ok(schema)
}

pub(crate) fn store_schema(path: &Path, schema: &KitSchema) -> Result<()> {
    let file = path.join(SCHEMA_FILE);
    let json = serde_json::to_string_pretty(schema)?;
    std::fs::write(&file, json)?;
    Ok(())
}

/// Persist a kit schema into the database. Used after migrations.
pub(crate) fn persist_schema(db: &Database, schema: &KitSchema) -> Result<()> {
    store_schema(&db.root, schema)
}
