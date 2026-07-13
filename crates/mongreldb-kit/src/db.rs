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
use mongreldb_kit_core::{ProcedureSpec, TriggerSpec, ViewSpec};
use serde_json::Value;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const SCHEMA_FILE: &str = "kit_schema.json";

/// Knobs for kit-level database opens.
///
/// Default is `lock_timeout_ms = 0` (fail-fast), matching the historical
/// `Database::open` behavior and preserving backwards compatibility.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenOptions {
    /// How long to wait for the cross-process exclusive lock on
    /// `<dir>/_meta/.lock` to become available, in milliseconds. `0`
    /// preserves the fail-fast behavior. Non-zero enables
    /// SQLite-style `busy_timeout` semantics: 1ms → 10ms → 50ms
    /// backoff with a hard deadline at `lock_timeout_ms`.
    pub lock_timeout_ms: u32,
}

impl OpenOptions {
    /// Create a new `OpenOptions` with all defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set [`OpenOptions::lock_timeout_ms`]. `0` keeps the fail-fast default;
    /// SQLite-style applications typically pick 1_000 – 5_000ms.
    pub fn with_lock_timeout_ms(mut self, ms: u32) -> Self {
        self.lock_timeout_ms = ms;
        self
    }
}

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

fn open_core_with_retry<T>(
    timeout_ms: u32,
    mut open: impl FnMut() -> mongreldb_core::Result<T>,
) -> mongreldb_core::Result<T> {
    if timeout_ms == 0 {
        return open();
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms as u64);
    let mut next_sleep = std::time::Duration::from_millis(1);
    loop {
        match open() {
            Ok(db) => return Ok(db),
            Err(err) if is_lock_contention(&err) => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    return Err(mongreldb_core::MongrelError::Io(std::io::Error::other(
                        format!("database lock timeout after {timeout_ms}ms: {err}"),
                    )));
                }
                let sleep = next_sleep.min(deadline - now);
                std::thread::sleep(sleep);
                next_sleep = next_sleep
                    .saturating_mul(10)
                    .min(std::time::Duration::from_millis(50));
            }
            Err(err) => return Err(err),
        }
    }
}

fn is_lock_contention(err: &mongreldb_core::MongrelError) -> bool {
    matches!(
        err,
        mongreldb_core::MongrelError::Io(io)
            if io.to_string().contains("locked by another process")
    )
}

/// A kit database handle.
///
/// Wraps a MongrelDB core database and a kit schema, providing table metadata
/// and transaction creation.
pub struct Database {
    pub(crate) inner: Arc<CoreDatabase>,
    pub(crate) schema: KitSchema,
    pub(crate) root: PathBuf,
    /// Application-registered named default providers (`DefaultKind::CustomName`).
    pub(crate) default_providers: HashMap<String, DefaultProvider>,
    /// Lazily-initialized long-lived SQL session. Views, prepared statements,
    /// and the result cache are session-scoped (the engine does not persist
    /// them), so the kit holds one session for the database's lifetime rather
    /// than opening one per `sql()` call — mirroring how the daemon and any
    /// long-lived application use MongrelDB. Built on first use so tables
    /// created in `Database::create` are visible to it.
    pub(crate) session: parking_lot::Mutex<Option<mongreldb_query::MongrelSession>>,
}

impl Database {
    /// Open an existing kit database.
    pub fn open(path: &Path) -> Result<Self> {
        let inner = Arc::new(CoreDatabase::open(path)?);
        let schema = load_schema(path)?;
        // Ensure reserved tables exist for databases created by older versions.
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Open an existing kit database with kit-level [`OpenOptions`]. Use this
    /// when another process may already be holding the cross-process lock
    /// and you want SQLite-style `busy_timeout` semantics instead of an
    /// immediate failure.
    ///
    /// Existing callers of [`open`](Self::open) keep the fail-fast behavior;
    /// this method is opt-in.
    pub fn open_with_options(path: &Path, opts: OpenOptions) -> Result<Self> {
        let inner = Arc::new(open_core_with_retry(opts.lock_timeout_ms, || {
            CoreDatabase::open(path)
        })?);
        let schema = load_schema(path)?;
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Open an existing page-encrypted kit database with its passphrase.
    pub fn open_encrypted(path: &Path, passphrase: &str) -> Result<Self> {
        let inner = Arc::new(CoreDatabase::open_encrypted(path, passphrase)?);
        let schema = load_schema(path)?;
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Open an existing page-encrypted kit database with its passphrase and
    /// kit-level [`OpenOptions`]. Opt-in lock-timeout semantics, mirroring
    /// [`open_with_options`](Self::open_with_options).
    pub fn open_encrypted_with_options(
        path: &Path,
        passphrase: &str,
        opts: OpenOptions,
    ) -> Result<Self> {
        let inner = Arc::new(open_core_with_retry(opts.lock_timeout_ms, || {
            CoreDatabase::open_encrypted(path, passphrase)
        })?);
        let schema = load_schema(path)?;
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Create a fresh page-encrypted kit database (AES-256-GCM; the passphrase
    /// derives the key hierarchy). Columns flagged `encrypted` /
    /// `encrypted_indexable` are encrypted at rest.
    pub fn create_encrypted(path: &Path, schema: KitSchema, passphrase: &str) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let inner = Arc::new(CoreDatabase::create_encrypted(path, passphrase)?);
        ensure_internal_tables(&inner)?;
        store_schema(path, &schema)?;
        for table in &schema.tables {
            create_core_table(&inner, &table.name, to_core_schema(table)?)?;
        }
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Create a fresh kit database with the given schema.
    pub fn create(path: &Path, schema: KitSchema) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let inner = Arc::new(CoreDatabase::create(path)?);

        // Create the reserved kit tables first so we can record migrations,
        // reserve unique keys, and touch row guards.
        ensure_internal_tables(&inner)?;

        // Persist the kit schema to a sidecar file (core tables cannot update
        // a specific row by id, so a file is the pragmatic stable store).
        store_schema(path, &schema)?;

        // Create core tables for every user table.
        for table in &schema.tables {
            create_core_table(&inner, &table.name, to_core_schema(table)?)?;
        }

        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Open an existing kit database that has `require_auth = true`,
    /// verifying credentials up front. Every subsequent operation is checked
    /// against the authenticated principal's permissions.
    ///
    /// Returns an error if the database does not have `require_auth` enabled
    /// (use [`open`](Self::open) for credentialless databases) or if the
    /// credentials are invalid.
    ///
    /// See `docs/15-credential-enforcement.md`.
    pub fn open_with_credentials(path: &Path, username: &str, password: &str) -> Result<Self> {
        let inner = Arc::new(CoreDatabase::open_with_credentials(
            path, username, password,
        )?);
        let schema = load_schema(path)?;
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Open a credentialed kit database with kit-level [`OpenOptions`]. Use
    /// this when another process may already hold the cross-process lock
    /// and you want SQLite-style `busy_timeout` semantics.
    pub fn open_with_credentials_and_options(
        path: &Path,
        username: &str,
        password: &str,
        opts: OpenOptions,
    ) -> Result<Self> {
        let inner = Arc::new(open_core_with_retry(opts.lock_timeout_ms, || {
            CoreDatabase::open_with_credentials(path, username, password)
        })?);
        let schema = load_schema(path)?;
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Create a fresh kit database with `require_auth = true`, a single admin
    /// user, and the given schema. The returned handle is already authenticated
    /// as the admin.
    ///
    /// See `docs/15-credential-enforcement.md`.
    pub fn create_with_credentials(
        path: &Path,
        schema: KitSchema,
        admin_username: &str,
        admin_password: &str,
    ) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let inner = Arc::new(CoreDatabase::create_with_credentials(
            path,
            admin_username,
            admin_password,
        )?);
        ensure_internal_tables(&inner)?;
        store_schema(path, &schema)?;
        for table in &schema.tables {
            create_core_table(&inner, &table.name, to_core_schema(table)?)?;
        }
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Open an existing page-encrypted kit database that has `require_auth =
    /// true`, combining the encryption passphrase with credential verification.
    pub fn open_encrypted_with_credentials(
        path: &Path,
        passphrase: &str,
        username: &str,
        password: &str,
    ) -> Result<Self> {
        let inner = Arc::new(CoreDatabase::open_encrypted_with_credentials(
            path, passphrase, username, password,
        )?);
        let schema = load_schema(path)?;
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Open an encrypted + credentialed kit database with kit-level
    /// [`OpenOptions`]. Opt-in lock-timeout semantics, mirroring
    /// [`open_with_credentials_and_options`](Self::open_with_credentials_and_options).
    pub fn open_encrypted_with_credentials_and_options(
        path: &Path,
        passphrase: &str,
        username: &str,
        password: &str,
        opts: OpenOptions,
    ) -> Result<Self> {
        let inner = Arc::new(open_core_with_retry(opts.lock_timeout_ms, || {
            CoreDatabase::open_encrypted_with_credentials(path, passphrase, username, password)
        })?);
        let schema = load_schema(path)?;
        ensure_internal_tables(&inner)?;
        reap_rotated_wal_segments(&inner);
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Create a fresh page-encrypted kit database with `require_auth = true`
    /// and a single admin user. Composes encryption-at-rest with credential
    /// enforcement.
    pub fn create_encrypted_with_credentials(
        path: &Path,
        schema: KitSchema,
        passphrase: &str,
        admin_username: &str,
        admin_password: &str,
    ) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let inner = Arc::new(CoreDatabase::create_encrypted_with_credentials(
            path,
            passphrase,
            admin_username,
            admin_password,
        )?);
        ensure_internal_tables(&inner)?;
        store_schema(path, &schema)?;
        for table in &schema.tables {
            create_core_table(&inner, &table.name, to_core_schema(table)?)?;
        }
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
            default_providers: HashMap::new(),
            session: parking_lot::Mutex::new(None),
        })
    }

    /// Convert a credentialless kit database to a credentialed one in place.
    /// Creates the first admin user, sets `require_auth = true`, and caches
    /// the admin principal on this handle.
    pub fn enable_auth(&self, admin_username: &str, admin_password: &str) -> Result<()> {
        self.inner
            .enable_auth(admin_username, admin_password)
            .map_err(KitError::from)
    }

    /// Disable `require_auth`, reverting to credentialless mode. The recovery
    /// path — requires an open (already-authenticated) handle. Users and roles
    /// are preserved but no longer enforced.
    pub fn disable_auth(&self) -> Result<()> {
        self.inner.disable_auth().map_err(KitError::from)
    }

    /// Returns `true` if this database has `require_auth = true`.
    pub fn require_auth_enabled(&self) -> bool {
        self.inner.require_auth_enabled()
    }

    /// Re-resolve the cached principal from the on-disk catalog, picking up
    /// role/permission changes made by other handles. No-op on credentialless
    /// databases.
    pub fn refresh_principal(&self) -> Result<()> {
        self.inner.refresh_principal().map_err(KitError::from)?;
        // Clear the SQL session so cached query results (which bypass the
        // permission check) don't serve stale data after a permission change.
        *self.session.lock() = None;
        Ok(())
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

    pub fn set_history_retention_epochs(&self, epochs: u64) -> Result<()> {
        self.inner
            .set_history_retention_epochs(epochs)
            .map_err(KitError::from)
    }

    pub fn history_retention_epochs(&self) -> u64 {
        self.inner.history_retention_epochs()
    }

    pub fn earliest_retained_epoch(&self) -> u64 {
        self.inner.earliest_retained_epoch().0
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
                projection.push((c.id, c.ty.clone()));
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

    /// The underlying engine handle wrapped in an `Arc`, for callers that need
    /// a shared/owned reference (e.g. building a `MongrelSession`).
    pub(crate) fn core_arc(&self) -> Arc<CoreDatabase> {
        Arc::clone(&self.inner)
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

    /// Rename a live table. Fails if `from` does not exist or `to` is already
    /// in use; a no-op when `from == to`. Names beginning with `__kit_` are
    /// reserved for internal tables and rejected here (parity with the
    /// TypeScript kit).
    ///
    /// Updates both the engine table and the kit schema catalog (in memory and
    /// persisted to `kit_schema.json`), so subsequent `table_names()`,
    /// `table(name)`, and transactional reads by the new name all work. Foreign
    /// keys in other tables that reference `from` are retargeted to `to`.
    pub fn rename_table(&mut self, from: &str, to: &str) -> Result<()> {
        if from.starts_with("__kit_") || to.starts_with("__kit_") {
            return Err(KitError::Validation(
                "rename_table: names beginning with '__kit_' are reserved for internal tables"
                    .into(),
            ));
        }
        self.inner.rename_table(from, to).map_err(KitError::from)?;
        // Keep the kit schema catalog in sync: rename the table (updating the
        // by_name index), retarget any FKs that pointed at it, then persist.
        if !self.schema.rename_table(from, to) {
            // The engine renamed it but the kit schema didn't have it / had a
            // clash — surface the divergence rather than silently desyncing.
            return Err(KitError::Integrity(format!(
                "rename_table: kit schema has no table '{from}' (or '{to}' already exists)"
            )));
        }
        for table in &mut self.schema.tables {
            for fk in &mut table.foreign_keys {
                if fk.references_table == from {
                    fk.references_table = to.to_string();
                }
            }
        }
        store_schema(&self.root, &self.schema)?;
        Ok(())
    }

    /// Rebuild statistics/metadata for every table's indexes (the engine's
    /// `ANALYZE` equivalent: `ensure_indexes_complete` on each table). Safe to
    /// run at any time; useful after bulk loads so the query planner and
    /// learned indexes have fresh data.
    pub fn analyze(&self) -> Result<()> {
        for name in self.inner.table_names() {
            let handle = self.inner.table(&name).map_err(KitError::from)?;
            handle.lock().ensure_indexes_complete()?;
        }
        Ok(())
    }

    /// Reclaim space across all tables: compacts every table's sorted runs,
    /// then runs `gc`. Returns the count of reclaimed orphaned runs/files.
    /// Equivalent to the engine's `VACUUM`. Safe to run at any time.
    pub fn vacuum(&self) -> Result<usize> {
        self.inner.compact().map_err(KitError::from)?;
        self.inner.gc().map_err(KitError::from)
    }

    /// Create a SQL view (`CREATE VIEW <name> AS <select>`). The engine
    /// overwrites any existing view with the same name, so this also serves as
    /// replace. The view lives in the kit's long-lived SQL session — it is not
    /// persisted to the catalog, so reopening the database loses it (re-apply
    /// a `CreateView` migration to restore).
    pub fn create_view(&self, spec: &ViewSpec) -> Result<()> {
        self.sql(&spec.create_sql())?;
        Ok(())
    }

    /// Drop a SQL view by name (idempotent — `DROP VIEW IF EXISTS`).
    pub fn drop_view(&self, name: &str) -> Result<()> {
        self.sql(&format!("DROP VIEW IF EXISTS {name}"))?;
        Ok(())
    }

    /// Reserve (without inserting) the next engine-native `AUTO_INCREMENT` value
    /// for `table`, advancing the per-table counter. Returns `None` when the
    /// table has no auto-increment column. This is the escape hatch for callers
    /// that stage a row with an explicit id inside a transaction; the
    /// reservation becomes durable when a row carrying the id commits, and an
    /// unused reservation just leaves a gap. Parity with the TypeScript kit's
    /// `reserveAutoIncSync`.
    pub fn reserve_auto_inc(&self, table: &str) -> Result<Option<i64>> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let mut guard = handle.lock();
        guard.reserve_auto_inc().map_err(KitError::from)
    }

    // ── user/role/credentials management ─────────────────────────────────

    /// Create a catalog user with an Argon2id-hashed password.
    pub fn create_user(&self, username: &str, password: &str) -> Result<()> {
        self.inner
            .create_user(username, password)
            .map_err(KitError::from)?;
        Ok(())
    }

    /// Drop a user by username.
    pub fn drop_user(&self, username: &str) -> Result<()> {
        self.inner.drop_user(username).map_err(KitError::from)
    }

    /// Change a user's password.
    pub fn alter_user_password(&self, username: &str, new_password: &str) -> Result<()> {
        self.inner
            .alter_user_password(username, new_password)
            .map_err(KitError::from)
    }

    /// Verify credentials. Returns `Some(entry)` on success.
    pub fn verify_user(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<mongreldb_core::auth::UserEntry>> {
        self.inner
            .verify_user(username, password)
            .map_err(KitError::from)
    }

    /// Grant or revoke admin privileges on a user.
    pub fn set_user_admin(&self, username: &str, is_admin: bool) -> Result<()> {
        self.inner
            .set_user_admin(username, is_admin)
            .map_err(KitError::from)
    }

    /// List all usernames.
    pub fn users(&self) -> Vec<String> {
        self.inner.users().into_iter().map(|u| u.username).collect()
    }

    /// Create a role.
    pub fn create_role(&self, name: &str) -> Result<()> {
        self.inner.create_role(name).map_err(KitError::from)?;
        Ok(())
    }

    /// Drop a role.
    pub fn drop_role(&self, name: &str) -> Result<()> {
        self.inner.drop_role(name).map_err(KitError::from)
    }

    /// List all role names.
    pub fn roles(&self) -> Vec<String> {
        self.inner.roles().into_iter().map(|r| r.name).collect()
    }

    /// Grant a role to a user.
    pub fn grant_role(&self, username: &str, role_name: &str) -> Result<()> {
        self.inner
            .grant_role(username, role_name)
            .map_err(KitError::from)
    }

    /// Revoke a role from a user.
    pub fn revoke_role(&self, username: &str, role_name: &str) -> Result<()> {
        self.inner
            .revoke_role(username, role_name)
            .map_err(KitError::from)
    }

    /// Grant a permission to a role.
    pub fn grant_permission(
        &self,
        role_name: &str,
        permission: mongreldb_core::auth::Permission,
    ) -> Result<()> {
        self.inner
            .grant_permission(role_name, permission)
            .map_err(KitError::from)
    }

    /// Revoke a permission from a role.
    pub fn revoke_permission(
        &self,
        role_name: &str,
        permission: mongreldb_core::auth::Permission,
    ) -> Result<()> {
        self.inner
            .revoke_permission(role_name, permission)
            .map_err(KitError::from)
    }

    // ── storage tuning & introspection (Tier 3) ─────────────────────────────

    /// Set the per-table spill threshold (bytes). When a transaction's staged
    /// bytes for a single table exceed this, rows are written as a uniform-epoch
    /// pending run instead of streamed Put records.
    pub fn set_spill_threshold(&self, bytes: u64) {
        self.inner.set_spill_threshold(bytes);
    }

    /// Enable or disable recursive trigger execution (database-wide).
    pub fn set_recursive_triggers(&self, enabled: bool) {
        self.inner.set_recursive_triggers(enabled);
    }

    /// Read the current trigger execution policy.
    pub fn trigger_config(&self) -> mongreldb_core::TriggerConfig {
        self.inner.trigger_config()
    }

    /// Set the trigger execution policy. `max_depth` must be > 0.
    pub fn set_trigger_config(&self, config: mongreldb_core::TriggerConfig) -> Result<()> {
        self.inner
            .set_trigger_config(config)
            .map_err(KitError::from)
    }

    /// Set a table's compaction zstd level (-1 = default, 0 = none, 1..22).
    pub fn set_table_compaction_zstd_level(&self, table: &str, level: i32) -> Result<()> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        handle.lock().set_compaction_zstd_level(level);
        Ok(())
    }

    /// Set a table's result-cache max bytes.
    pub fn set_table_result_cache_max_bytes(&self, table: &str, max_bytes: u64) -> Result<()> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        handle.lock().set_result_cache_max_bytes(max_bytes);
        Ok(())
    }

    /// Set a table's mutable-run spill threshold (bytes).
    pub fn set_table_mutable_run_spill_bytes(&self, table: &str, bytes: u64) -> Result<()> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        handle.lock().set_mutable_run_spill_bytes(bytes);
        Ok(())
    }

    /// Set a table's WAL sync byte threshold (bytes between group-syncs).
    pub fn set_table_sync_byte_threshold(&self, table: &str, threshold: u64) -> Result<()> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        handle.lock().set_sync_byte_threshold(threshold);
        Ok(())
    }

    /// Set a table's index build policy (`Deferred` for fast ingest, `Eager`
    /// for fast first query).
    pub fn set_table_index_build_policy(
        &self,
        table: &str,
        policy: mongreldb_core::IndexBuildPolicy,
    ) -> Result<()> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        handle.lock().set_index_build_policy(policy);
        Ok(())
    }

    /// Page-cache statistics for a table.
    pub fn table_page_cache_stats(&self, table: &str) -> Result<mongreldb_core::cache::CacheStats> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let stats = handle.lock().page_cache_stats();
        Ok(stats)
    }

    /// Number of sorted runs a table currently has (compaction target: 1).
    pub fn table_run_count(&self, table: &str) -> Result<usize> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let n = handle.lock().run_count();
        Ok(n)
    }

    /// Memtable length (uncommitted staged rows) for a table.
    pub fn table_memtable_len(&self, table: &str) -> Result<usize> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let n = handle.lock().memtable_len();
        Ok(n)
    }

    /// Mutable-run length for a table.
    pub fn table_mutable_run_len(&self, table: &str) -> Result<usize> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let n = handle.lock().mutable_run_len();
        Ok(n)
    }

    /// Page-cache entry count for a table.
    pub fn table_page_cache_len(&self, table: &str) -> Result<usize> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let n = handle.lock().page_cache_len();
        Ok(n)
    }

    /// Decoded-page-cache entry count for a table.
    pub fn table_decoded_cache_len(&self, table: &str) -> Result<usize> {
        let handle = self.inner.table(table).map_err(KitError::from)?;
        let n = handle.lock().decoded_cache_len();
        Ok(n)
    }

    /// Run a SQL statement through the embedded `MongrelSession` (DataFusion
    /// frontend) and return the result as Arrow [`RecordBatch`]es. This is the
    /// Rust analogue of the TypeScript kit's `db.sql(sql)` (which returns Arrow
    /// IPC bytes) and the NAPI `Database.sql`.
    ///
    /// Read statements return their rows; DDL/DML (e.g. `CREATE VIEW`,
    /// `ANALYZE`, `VACUUM`, `CREATE VIRTUAL TABLE`) return an empty vec. Writes
    /// made directly through SQL bypass Kit-level constraints (defaults,
    /// enums, min/max, length, regex, triggers) — use the transactional
    /// [`Transaction`](crate::Transaction) API for constrained writes. The
    /// engine's own declarative constraints (unique, FK, check) still apply.
    ///
    /// The session is held for the database's lifetime, so session-scoped
    /// objects (views, prepared statements, the result cache) persist across
    /// calls — mirroring a long-lived database connection. After a migration
    /// that creates/drops tables, call [`Database::refresh_sql_session`] so the
    /// session sees the new table set.
    pub fn sql(&self, statement: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let session = match self.session.lock().take() {
            Some(s) => s,
            None => {
                mongreldb_query::MongrelSession::open(self.core_arc()).map_err(KitError::from)?
            }
        };
        let runtime = sql_runtime();
        let result = runtime
            .block_on(session.run(statement))
            .map_err(KitError::from);
        // Preserve the session (and any views/state created during the call).
        *self.session.lock() = Some(session);
        result
    }

    /// (Re)build the cached SQL session so it sees the current table set. The
    /// engine's `MongrelSession` snapshots the table list at construction; this
    /// rebuilds it after a migration creates or drops tables. Views and other
    /// session-scoped state are reset.
    pub fn refresh_sql_session(&self) -> Result<()> {
        let session =
            mongreldb_query::MongrelSession::open(self.core_arc()).map_err(KitError::from)?;
        *self.session.lock() = Some(session);
        Ok(())
    }

    /// Like [`Database::sql`], but returns the result serialized as Arrow IPC
    /// *file* bytes — the same wire format the NAPI addon and the daemon emit.
    /// Decode with `pyarrow.ipc.open_file`, the JS `apache-arrow`
    /// `tableFromIPC`, or [`crate::arrow_util::read_arrow_ipc`]. Empty for
    /// DDL/DML.
    pub fn sql_arrow(&self, statement: &str) -> Result<Vec<u8>> {
        let batches = self.sql(statement)?;
        crate::arrow_util::batches_to_ipc(&batches)
    }

    /// Like [`Database::sql`], but materializes the result rows into JSON-style
    /// maps (column name → value) for callers that don't want to take a direct
    /// Arrow dependency. Empty for DDL/DML.
    pub fn sql_rows(&self, statement: &str) -> Result<Vec<serde_json::Map<String, Value>>> {
        let batches = self.sql(statement)?;
        crate::arrow_util::batches_to_rows(&batches)
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
            limit: None,
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

/// A cached single-threaded tokio runtime for driving `MongrelSession::run`
/// (which is async) from the kit's otherwise-blocking SQL surface. Built once
/// per process and reused; `CurrentThread` is sufficient since the kit never
/// runs concurrent SQL statements on the same database from one thread.
fn sql_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build kit SQL tokio runtime")
    })
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

#[cfg(test)]
mod tests {
    use super::open_core_with_retry;

    fn lock_error() -> mongreldb_core::MongrelError {
        mongreldb_core::MongrelError::Io(std::io::Error::other(
            "database at /tmp/db is locked by another process: would block",
        ))
    }

    #[test]
    fn open_retry_waits_for_lock_contention_only() {
        let mut calls = 0;
        let value = open_core_with_retry(50, || {
            calls += 1;
            if calls < 3 {
                Err(lock_error())
            } else {
                Ok(7)
            }
        })
        .unwrap();
        assert_eq!(value, 7);
        assert_eq!(calls, 3);

        let mut non_lock_calls = 0;
        let err: mongreldb_core::Result<()> = open_core_with_retry(50, || {
            non_lock_calls += 1;
            Err(mongreldb_core::MongrelError::Other("nope".into()))
        });
        let err = err.unwrap_err();
        assert_eq!(non_lock_calls, 1);
        assert!(matches!(err, mongreldb_core::MongrelError::Other(_)));
    }
}
