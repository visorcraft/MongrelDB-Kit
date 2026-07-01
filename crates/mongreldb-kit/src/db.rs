//! Database handle for `mongreldb-kit`.

use crate::error::{KitError, Result};
use crate::internal::{ensure_internal_tables, internal_tables_core};
use crate::schema::to_core_schema;
use mongreldb_core::epoch::Snapshot;
use mongreldb_core::memtable::Row as CoreRow;
use mongreldb_core::memtable::Value as CoreValue;
use mongreldb_core::schema::Schema as CoreSchema;
use mongreldb_core::Database as CoreDatabase;
use mongreldb_core::{NativeAgg, NativeAggResult};
use mongreldb_kit_core::schema::Schema as KitSchema;
use mongreldb_kit_core::schema::Table as KitTable;
use serde_json::Value;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

const SCHEMA_FILE: &str = "kit_schema.json";

/// A named default-value provider registered by the application.
pub type DefaultProvider = Box<dyn Fn() -> Value + Send + Sync>;

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

    /// Return the migrations already recorded in `__kit_schema_migrations`.
    pub fn applied_migrations(&self) -> Result<Vec<mongreldb_kit_core::migrations::Migration>> {
        crate::migrate::load_applied_migrations(&self.inner)
    }

    pub(crate) fn core_db(&self) -> &CoreDatabase {
        &self.inner
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
        let guard = handle.lock();
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

/// Read a `Bytes` column from an internal-table core row as a UTF-8 string.
pub(crate) fn internal_bytes(row: &CoreRow, col_id: u16) -> Option<String> {
    match row.columns.get(&col_id) {
        Some(CoreValue::Bytes(b)) => String::from_utf8(b.clone()).ok(),
        _ => None,
    }
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
