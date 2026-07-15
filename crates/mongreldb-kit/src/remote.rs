//! Typed remote client for a running `mongreldb-server` daemon (PLAN.md #3).
//!
//! [`RemoteDatabase`] speaks the daemon's typed endpoints over HTTP:
//! - `GET  /kit/schema` — schema + constraint metadata,
//! - `POST /kit/txn` — a typed atomic write batch (put / put_returning / upsert
//!   / delete / delete_by_pk) with an idempotency key,
//! - `POST /kit/query` — a native typed query returning physical row ids +
//!   typed cells,
//! - `POST /sql` — SQL reads decoded to JSON rows.
//!
//! **Authority is server-side**: writes run inside one core transaction on the
//! daemon, which enforces the engine's declarative constraints (unique /
//! FK-RESTRICT/CASCADE/SET-NULL / check) atomically, and the idempotency store
//! is persisted to `<root>/_idem/` so retries survive a daemon restart.
//!
//! **Architectural boundary (by design):** the daemon stores engine-level
//! constraints only — not the Kit's richer per-column metadata (defaults,
//! enums, min/max, length, regex). Those Kit-specific field validations are
//! therefore **the caller's responsibility in remote mode**; only the engine
//! constraints are enforced authoritatively server-side.
//!
//! Enable with the `remote` cargo feature.

#![cfg(feature = "remote")]

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::arrow_util::{batch_to_rows, read_arrow_ipc};
use crate::error::{KitError, Result};
use mongreldb_kit_core::{ProcedureSpec, TriggerSpec, VirtualTableSpec};

const EC_UNIQUE: &str = "UNIQUE_VIOLATION";
const EC_FK: &str = "FK_VIOLATION";
const EC_CHECK: &str = "CHECK_VIOLATION";
const EC_CONFLICT: &str = "CONFLICT";
const EC_BAD: &str = "BAD_REQUEST";
const EC_TRIGGER_VALIDATION: &str = "TRIGGER_VALIDATION";

/// A typed remote client bound to a `mongreldb-server` URL.
#[derive(Clone)]
pub struct RemoteDatabase {
    base_url: String,
    client: reqwest::blocking::Client,
    schemas: HashMap<String, RemoteTable>,
    sql_cancellation: Option<SqlCancellationCapabilities>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteSqlFormat {
    Arrow,
    Json,
}

impl RemoteSqlFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Arrow => "arrow",
            Self::Json => "json",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteSqlOptions {
    pub query_id: Option<mongreldb_query::QueryId>,
    pub timeout: Option<Duration>,
    pub transport_timeout: Option<Duration>,
    pub format: RemoteSqlFormat,
}

impl Default for RemoteSqlOptions {
    fn default() -> Self {
        Self {
            query_id: None,
            timeout: None,
            transport_timeout: None,
            format: RemoteSqlFormat::Arrow,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SqlCancellationCapabilities {
    pub version: u8,
    pub client_query_ids: bool,
    pub cancel_endpoint: bool,
    pub query_status: bool,
    pub stream_disconnect_cancels: bool,
}

#[derive(Debug, Deserialize)]
struct CapabilitiesResponse {
    sql_cancellation: SqlCancellationCapabilities,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RemoteQueryStatus {
    pub query_id: String,
    pub state: String,
    pub operation: String,
    pub committed: bool,
    pub completed_statements: usize,
    pub statement_index: usize,
}

pub struct RemoteSqlQueryHandle {
    query_id: mongreldb_query::QueryId,
    database: RemoteDatabase,
    worker: Option<RemoteSqlWorker>,
}

type RemoteSqlWorker = std::thread::JoinHandle<Result<Vec<Map<String, Value>>>>;

impl RemoteSqlQueryHandle {
    pub fn id(&self) -> mongreldb_query::QueryId {
        self.query_id
    }

    pub fn cancel(&self) -> Result<mongreldb_query::CancelOutcome> {
        self.database.cancel_sql(self.query_id)
    }

    pub fn wait(mut self) -> Result<Vec<Map<String, Value>>> {
        let worker = self
            .worker
            .take()
            .ok_or_else(|| KitError::Storage("remote SQL query already awaited".into()))?;
        worker
            .join()
            .map_err(|_| KitError::Storage("remote SQL query worker panicked".into()))?
    }
}

impl Drop for RemoteSqlQueryHandle {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _ = self.database.cancel_sql(self.query_id);
        }
    }
}

#[derive(Debug, Clone)]
struct RemoteTable {
    id_to_name: HashMap<u16, String>,
    name_to_id: HashMap<String, u16>,
    primary_key: Option<u16>,
}

/// The server-side schema descriptor subset we rely on.
#[derive(Debug, Deserialize)]
struct SchemaInfo {
    columns: Vec<ColumnMeta>,
}

/// `POST /compact` response: `{ "compacted": N, "skipped": M }`.
#[derive(Debug, Deserialize)]
struct CompactResp {
    compacted: usize,
    skipped: usize,
}

/// `POST /tables/{name}/compact` response: `{ "status": "compacted"|"skipped" }`.
#[derive(Debug, Deserialize)]
struct CompactTableResp {
    status: String,
}

/// `GET/PUT /history/retention` response.
#[derive(Debug, Clone, Deserialize)]
pub struct HistoryRetention {
    pub history_retention_epochs: u64,
    pub earliest_retained_epoch: u64,
}
#[derive(Debug, Deserialize)]
struct ColumnMeta {
    id: u16,
    name: String,
    primary_key: bool,
    #[allow(dead_code)]
    #[serde(default)]
    nullable: bool,
    #[allow(dead_code)]
    #[serde(default)]
    auto_increment: bool,
}
#[derive(Debug, Deserialize)]
struct AllSchemas {
    tables: Map<String, serde_json::Value>,
}

impl RemoteDatabase {
    /// Connect to a daemon and load every table's schema metadata.
    pub fn connect(url: &str) -> Result<Self> {
        let mut db = Self {
            base_url: url.trim_end_matches('/').to_string(),
            client: reqwest::blocking::Client::new(),
            schemas: HashMap::new(),
            sql_cancellation: None,
        };
        db.sql_cancellation = db.fetch_sql_cancellation_capabilities()?;
        db.refresh()?;
        Ok(db)
    }

    fn fetch_sql_cancellation_capabilities(&self) -> Result<Option<SqlCancellationCapabilities>> {
        let response = self
            .client
            .get(self.url("/capabilities"))
            .send()
            .map_err(ioe)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let capabilities: CapabilitiesResponse = decode(response)?;
        Ok(Some(capabilities.sql_cancellation))
    }

    pub fn sql_cancellation_capabilities(&self) -> Option<&SqlCancellationCapabilities> {
        self.sql_cancellation.as_ref()
    }

    fn require_sql_cancellation(&self) -> Result<&SqlCancellationCapabilities> {
        let capabilities = self.sql_cancellation.as_ref().ok_or_else(|| {
            KitError::Unsupported(
                "server does not advertise SQL cancellation capability version 1".into(),
            )
        })?;
        if capabilities.version != 1
            || !capabilities.client_query_ids
            || !capabilities.cancel_endpoint
        {
            return Err(KitError::Unsupported(
                "server SQL cancellation capability is incompatible".into(),
            ));
        }
        Ok(capabilities)
    }

    /// Re-fetch schema metadata (call after DDL on the server).
    pub fn refresh(&mut self) -> Result<()> {
        let resp = self
            .client
            .get(format!("{}/kit/schema", self.base_url))
            .send()
            .map_err(ioe)?;
        let all: AllSchemas = decode(resp)?;
        self.schemas.clear();
        for (name, body) in &all.tables {
            let info: SchemaInfo = serde_json::from_value(body.clone())
                .map_err(|e| KitError::Storage(e.to_string()))?;
            let mut id_to_name = HashMap::new();
            let mut name_to_id = HashMap::new();
            let mut primary_key = None;
            for c in &info.columns {
                id_to_name.insert(c.id, c.name.clone());
                name_to_id.insert(c.name.clone(), c.id);
                if c.primary_key {
                    primary_key = Some(c.id);
                }
            }
            self.schemas.insert(
                name.clone(),
                RemoteTable {
                    id_to_name,
                    name_to_id,
                    primary_key,
                },
            );
        }
        Ok(())
    }

    /// Create a constraint-bearing table over HTTP (`POST /kit/create_table`)
    /// and refresh the local schema cache. `body` is the full request JSON —
    /// `{name, columns:[{id,name,ty,primary_key,nullable,auto_increment,…}],
    /// constraints:{uniques,…,foreign_keys,…,checks:[{id,name,expr}]}}`.
    /// Returns the assigned table id.
    pub fn create_table(&mut self, body: &Value) -> Result<u64> {
        let resp = self
            .client
            .post(self.url("/kit/create_table"))
            .json(body)
            .send()
            .map_err(ioe)?;
        let v: Value = decode(resp)?;
        let table_id = v.get("table_id").and_then(|t| t.as_u64()).unwrap_or(0);
        self.refresh()?;
        Ok(table_id)
    }

    pub fn table_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    pub fn create_procedure(&self, spec: &ProcedureSpec) -> Result<Value> {
        let resp = self
            .client
            .post(self.url("/procedures"))
            .json(&json!({ "procedure": spec.json }))
            .send()
            .map_err(ioe)?;
        decode(resp)
    }

    pub fn replace_procedure(&self, name: &str, spec: &ProcedureSpec) -> Result<Value> {
        let resp = self
            .client
            .put(self.url(&format!("/procedures/{name}")))
            .json(&json!({ "procedure": spec.json }))
            .send()
            .map_err(ioe)?;
        decode(resp)
    }

    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/procedures/{name}")))
            .send()
            .map_err(ioe)?;
        let _: Value = decode(resp)?;
        Ok(())
    }

    pub fn call_procedure(&self, name: &str, args: Map<String, Value>) -> Result<Value> {
        let resp = self
            .client
            .post(self.url(&format!("/kit/procedures/{name}/call")))
            .json(&json!({ "args": args }))
            .send()
            .map_err(ioe)?;
        decode(resp)
    }

    pub fn triggers(&self) -> Result<Vec<Value>> {
        let resp = self.client.get(self.url("/triggers")).send().map_err(ioe)?;
        let v: Value = decode(resp)?;
        Ok(v.get("triggers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default())
    }

    pub fn trigger(&self, name: &str) -> Result<Value> {
        let resp = self
            .client
            .get(self.url(&format!("/triggers/{name}")))
            .send()
            .map_err(ioe)?;
        let v: Value = decode(resp)?;
        Ok(v.get("trigger").cloned().unwrap_or(Value::Null))
    }

    pub fn create_trigger(&self, spec: &TriggerSpec) -> Result<Value> {
        let resp = self
            .client
            .post(self.url("/triggers"))
            .json(&json!({ "trigger": spec.json }))
            .send()
            .map_err(ioe)?;
        decode(resp)
    }

    pub fn replace_trigger(&self, name: &str, spec: &TriggerSpec) -> Result<Value> {
        let resp = self
            .client
            .put(self.url(&format!("/triggers/{name}")))
            .json(&json!({ "trigger": spec.json }))
            .send()
            .map_err(ioe)?;
        decode(resp)
    }

    pub fn drop_trigger(&self, name: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/triggers/{name}")))
            .send()
            .map_err(ioe)?;
        let _: Value = decode(resp)?;
        Ok(())
    }

    pub fn create_virtual_table(
        &mut self,
        table: &VirtualTableSpec,
    ) -> Result<Vec<Map<String, Value>>> {
        let rows = self.sql_rows(&table.create_sql())?;
        self.refresh()?;
        Ok(rows)
    }

    pub fn drop_virtual_table(&mut self, name: &str) -> Result<Vec<Map<String, Value>>> {
        let rows = self.sql_rows(&format!(
            "DROP TABLE {}",
            mongreldb_kit_core::quote_ident(name)
        ))?;
        self.refresh()?;
        Ok(rows)
    }

    /// Compact every table on the daemon (`POST /compact`). Returns
    /// `(compacted, skipped)`.
    pub fn compact(&self) -> Result<(usize, usize)> {
        let resp: CompactResp =
            decode(self.client.post(self.url("/compact")).send().map_err(ioe)?)?;
        Ok((resp.compacted, resp.skipped))
    }

    /// Compact a single table on the daemon (`POST /tables/{name}/compact`).
    /// Returns `true` if compacted, `false` if skipped (fewer than 2 runs).
    pub fn compact_table(&self, name: &str) -> Result<bool> {
        let resp: CompactTableResp = decode(
            self.client
                .post(self.url(&format!("/tables/{name}/compact")))
                .send()
                .map_err(ioe)?,
        )?;
        Ok(resp.status == "compacted")
    }

    /// Set the durable MVCC history-retention window in epochs
    /// (`PUT /history/retention`). Returns the post-update window size and
    /// earliest retained epoch.
    pub fn set_history_retention_epochs(&self, epochs: u64) -> Result<HistoryRetention> {
        let resp = self
            .client
            .put(self.url("/history/retention"))
            .json(&json!({ "history_retention_epochs": epochs }))
            .send()
            .map_err(ioe)?;
        decode(resp)
    }

    /// Read the current history-retention window size (`GET /history/retention`).
    pub fn history_retention_epochs(&self) -> Result<u64> {
        Ok(self.history_retention()?.history_retention_epochs)
    }

    /// Read the earliest retained epoch (`GET /history/retention`).
    pub fn earliest_retained_epoch(&self) -> Result<u64> {
        Ok(self.history_retention()?.earliest_retained_epoch)
    }

    /// Fetch the full history-retention descriptor (`GET /history/retention`).
    pub fn history_retention(&self) -> Result<HistoryRetention> {
        decode(
            self.client
                .get(self.url("/history/retention"))
                .send()
                .map_err(ioe)?,
        )
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn require_table(&self, table: &str) -> Result<&RemoteTable> {
        self.schemas
            .get(table)
            .ok_or_else(|| KitError::Integrity(format!("unknown table {table:?}")))
    }

    /// Translate a column-name → JSON-value map into the flat
    /// `[col_id, val, col_id, val, …]` cell array the daemon expects.
    fn cells(&self, table: &str, row: &Map<String, Value>) -> Result<Vec<Value>> {
        let t = self.require_table(table)?;
        let mut out = Vec::with_capacity(row.len() * 2);
        for (name, val) in row {
            let id = *t.name_to_id.get(name).ok_or_else(|| {
                KitError::Validation(format!("unknown column {name:?} in table {table:?}"))
            })?;
            out.push(json!(id));
            out.push(val.clone());
        }
        Ok(out)
    }

    /// Begin a typed atomic batch.
    pub fn begin(&self) -> RemoteTransaction<'_> {
        RemoteTransaction {
            db: self,
            ops: Vec::new(),
            idempotency_key: None,
        }
    }

    /// Run a SQL read and decode the Arrow response into JSON row maps keyed by
    /// column name. Covers the common fixed-width types + UTF-8 + null.
    pub fn sql_rows(&self, sql: &str) -> Result<Vec<Map<String, Value>>> {
        self.sql_rows_with_options(sql, RemoteSqlOptions::default())
    }

    pub fn sql_rows_with_options(
        &self,
        sql: &str,
        options: RemoteSqlOptions,
    ) -> Result<Vec<Map<String, Value>>> {
        let format = options.format;
        let bytes = self.sql_bytes_with_options(sql, options)?;
        if format == RemoteSqlFormat::Json {
            return serde_json::from_slice(&bytes).map_err(KitError::from);
        }
        let batches = read_arrow_ipc(&bytes)?;
        let mut rows = Vec::new();
        for batch in &batches {
            rows.extend(batch_to_rows(batch)?);
        }
        Ok(rows)
    }

    pub fn sql_arrow_with_options(
        &self,
        sql: &str,
        mut options: RemoteSqlOptions,
    ) -> Result<Vec<u8>> {
        options.format = RemoteSqlFormat::Arrow;
        self.sql_bytes_with_options(sql, options)
    }

    fn sql_bytes_with_options(&self, sql: &str, options: RemoteSqlOptions) -> Result<Vec<u8>> {
        let controlled = options.query_id.is_some()
            || options.timeout.is_some()
            || options.transport_timeout.is_some();
        if controlled {
            self.require_sql_cancellation()?;
        }
        let query_id = if controlled {
            Some(match options.query_id {
                Some(query_id) => query_id,
                None => mongreldb_query::QueryId::random().map_err(KitError::from)?,
            })
        } else {
            None
        };
        let timeout_ms = options.timeout.map(duration_millis);
        let body = json!({
            "sql": sql,
            "format": options.format.as_str(),
            "query_id": query_id.map(|value| value.to_string()),
            "timeout_ms": timeout_ms,
        });
        let mut request = self.client.post(self.url("/sql")).json(&body);
        if let Some(timeout) = options.transport_timeout {
            request = request.timeout(timeout);
        }
        let response = match request.send() {
            Ok(response) => response,
            Err(error) => {
                if let Some(query_id) = query_id {
                    self.best_effort_cancel(query_id);
                    return Err(KitError::Transport {
                        query_id: query_id.to_string(),
                        message: format!(
                            "{error}; server cancellation was requested but is not confirmed"
                        ),
                    });
                }
                return Err(ioe(error));
            }
        };
        if !response.status().is_success() {
            return Err(map_error(response));
        }
        response.bytes().map(|bytes| bytes.to_vec()).map_err(ioe)
    }

    pub fn start_sql_rows(
        &self,
        sql: String,
        mut options: RemoteSqlOptions,
    ) -> Result<RemoteSqlQueryHandle> {
        self.require_sql_cancellation()?;
        let query_id = match options.query_id {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random().map_err(KitError::from)?,
        };
        options.query_id = Some(query_id);
        let database = self.clone();
        let worker_database = database.clone();
        let worker = std::thread::Builder::new()
            .name(format!("mongreldb-kit-remote-sql-{query_id}"))
            .spawn(move || worker_database.sql_rows_with_options(&sql, options))
            .map_err(|error| KitError::Storage(error.to_string()))?;
        Ok(RemoteSqlQueryHandle {
            query_id,
            database,
            worker: Some(worker),
        })
    }

    pub fn cancel_sql(
        &self,
        query_id: mongreldb_query::QueryId,
    ) -> Result<mongreldb_query::CancelOutcome> {
        self.require_sql_cancellation()?;
        let response = self
            .client
            .post(self.url(&format!("/queries/{query_id}/cancel")))
            .send()
            .map_err(ioe)?;
        match response.status() {
            reqwest::StatusCode::ACCEPTED => Ok(mongreldb_query::CancelOutcome::Accepted),
            reqwest::StatusCode::OK => {
                let body: Value = response.json().map_err(ioe)?;
                match body.get("state").and_then(Value::as_str) {
                    Some("cancelling") => Ok(mongreldb_query::CancelOutcome::AlreadyCancelling),
                    Some("finished") => Ok(mongreldb_query::CancelOutcome::AlreadyFinished),
                    _ => Ok(mongreldb_query::CancelOutcome::Accepted),
                }
            }
            reqwest::StatusCode::CONFLICT => Ok(mongreldb_query::CancelOutcome::TooLate),
            reqwest::StatusCode::NOT_FOUND => Ok(mongreldb_query::CancelOutcome::NotFound),
            _ => Err(map_error(response)),
        }
    }

    pub fn sql_query_status(
        &self,
        query_id: mongreldb_query::QueryId,
    ) -> Result<Option<RemoteQueryStatus>> {
        let capabilities = self.require_sql_cancellation()?;
        if !capabilities.query_status {
            return Err(KitError::Unsupported(
                "server does not advertise SQL query status".into(),
            ));
        }
        let response = self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .send()
            .map_err(ioe)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        decode(response).map(Some)
    }

    fn best_effort_cancel(&self, query_id: mongreldb_query::QueryId) {
        let _ = reqwest::blocking::Client::new()
            .post(self.url(&format!("/queries/{query_id}/cancel")))
            .timeout(Duration::from_secs(5))
            .send();
    }

    /// Run a native typed query (`POST /kit/query`) returning rows with their
    /// physical row ids and name-keyed cells. Conditions are raw JSON objects
    /// mirroring the daemon's condition variants (e.g. `{"pk": {"value": 2}}`).
    pub fn query(
        &self,
        table: &str,
        conditions: Vec<Value>,
        projection: Option<Vec<u16>>,
        limit: Option<usize>,
    ) -> Result<Vec<RemoteQueryRow>> {
        #[derive(serde::Serialize)]
        struct Req<'a> {
            table: &'a str,
            #[serde(skip_serializing_if = "<[_]>::is_empty")]
            conditions: &'a [Value],
            #[serde(skip_serializing_if = "Option::is_none")]
            projection: Option<Vec<u16>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<usize>,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            #[allow(dead_code)]
            truncated: bool,
            rows: Vec<RawRow>,
        }
        #[derive(serde::Deserialize)]
        struct RawRow {
            row_id: String,
            cells: Vec<Value>,
        }
        let req = Req {
            table,
            conditions: &conditions,
            projection,
            limit,
        };
        let resp = self
            .client
            .post(self.url("/kit/query"))
            .json(&req)
            .send()
            .map_err(ioe)?;
        let parsed: Resp = decode(resp)?;
        let info = self.require_table(table)?;
        let mut out = Vec::with_capacity(parsed.rows.len());
        for r in parsed.rows {
            out.push(RemoteQueryRow {
                row_id: r.row_id,
                values: decode_cells(&r.cells, &info.id_to_name),
            });
        }
        Ok(out)
    }
}

/// A row returned by [`RemoteDatabase::query`]: its physical row id plus the
/// projected cells keyed by column name.
#[derive(Debug, Clone)]
pub struct RemoteQueryRow {
    pub row_id: String,
    pub values: Map<String, Value>,
}

fn decode_cells(cells: &[Value], id_to_name: &HashMap<u16, String>) -> Map<String, Value> {
    let mut out = Map::new();
    let mut i = 0;
    while i + 1 < cells.len() {
        if let Some(id) = cells[i].as_u64() {
            if let Some(name) = id_to_name.get(&(id as u16)) {
                out.insert(name.clone(), cells[i + 1].clone());
            }
        }
        i += 2;
    }
    out
}

/// An in-flight typed batch against the daemon. Buffered ops commit atomically
/// on [`RemoteTransaction::commit`].
pub struct RemoteTransaction<'a> {
    db: &'a RemoteDatabase,
    ops: Vec<TxnOp>,
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum TxnOp {
    Put {
        table: String,
        cells: Vec<Value>,
        returning: bool,
    },
    Upsert {
        table: String,
        cells: Vec<Value>,
        update_cells: Option<Vec<Value>>,
        returning: bool,
    },
    DeleteByPk {
        table: String,
        pk: Value,
    },
}

#[derive(Debug, Serialize)]
struct TxnRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<String>,
    ops: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct TxnResponse {
    #[allow(dead_code)]
    status: String,
    #[allow(dead_code)]
    epoch: u64,
    results: Vec<OpResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum OpResult {
    Put {
        #[allow(dead_code)]
        row_id: Option<String>,
        auto_inc: Option<i64>,
        #[serde(default)]
        row: Option<Vec<Value>>,
    },
    Upsert {
        action: String,
        #[allow(dead_code)]
        auto_inc: Option<i64>,
        #[serde(default)]
        row: Option<Vec<Value>>,
    },
    Deleted,
    NotFound,
}

/// The decoded outcome of a committed batch — the committed epoch plus the
/// per-op typed results (post-image rows where `returning` was set).
#[derive(Debug, Clone)]
pub struct RemoteBatch {
    pub epoch: u64,
    pub results: Vec<RemoteOpResult>,
}

#[derive(Debug, Clone)]
pub enum RemoteOpResult {
    /// A put result. `row` is the post-image when `returning` was requested.
    Put {
        auto_inc: Option<i64>,
        row: Option<Map<String, Value>>,
    },
    /// An upsert result with the resolved action (`inserted` / `updated` / `unchanged`).
    Upsert {
        action: String,
        row: Option<Map<String, Value>>,
    },
    Deleted,
    NotFound,
}

impl RemoteOpResult {
    /// The committed post-image row, when `returning` was requested.
    pub fn row_ref(&self) -> Option<&Map<String, Value>> {
        match self {
            RemoteOpResult::Put { row, .. } | RemoteOpResult::Upsert { row, .. } => row.as_ref(),
            _ => None,
        }
    }
}

impl<'a> RemoteTransaction<'a> {
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Stage an insert (a `put`).
    pub fn insert(mut self, table: &str, row: Map<String, Value>) -> Result<Self> {
        let cells = self.db.cells(table, &row)?;
        self.ops.push(TxnOp::Put {
            table: table.to_string(),
            cells,
            returning: false,
        });
        Ok(self)
    }

    /// Stage an insert that returns the committed post-image row.
    pub fn insert_returning(mut self, table: &str, row: Map<String, Value>) -> Result<Self> {
        let cells = self.db.cells(table, &row)?;
        self.ops.push(TxnOp::Put {
            table: table.to_string(),
            cells,
            returning: true,
        });
        Ok(self)
    }

    /// Stage an upsert (DO NOTHING unless `update` cells are supplied).
    pub fn upsert(
        mut self,
        table: &str,
        row: Map<String, Value>,
        update: Option<Map<String, Value>>,
    ) -> Result<Self> {
        let cells = self.db.cells(table, &row)?;
        let update_cells = match update {
            Some(u) => Some(self.db.cells(table, &u)?),
            None => None,
        };
        self.ops.push(TxnOp::Upsert {
            table: table.to_string(),
            cells,
            update_cells,
            returning: true,
        });
        Ok(self)
    }

    /// Stage a delete of the row with the given primary-key scalar value.
    pub fn delete_by_pk(mut self, table: &str, pk: Value) -> Result<Self> {
        let t = self.db.require_table(table)?;
        if t.primary_key.is_none() {
            return Err(KitError::Validation(format!(
                "table {table:?} has no primary key"
            )));
        }
        self.ops.push(TxnOp::DeleteByPk {
            table: table.to_string(),
            pk,
        });
        Ok(self)
    }

    /// Commit the batch atomically. Constraint violations map to [`KitError`].
    pub fn commit(self) -> Result<RemoteBatch> {
        let mut ops_json = Vec::with_capacity(self.ops.len());
        for op in &self.ops {
            ops_json.push(serde_json::to_value(op).map_err(|e| KitError::Storage(e.to_string()))?);
        }
        let req = TxnRequest {
            idempotency_key: self.idempotency_key.clone(),
            ops: ops_json,
        };
        let resp = self
            .db
            .client
            .post(self.db.url("/kit/txn"))
            .json(&req)
            .send()
            .map_err(ioe)?;
        if !resp.status().is_success() {
            return Err(map_error(resp));
        }
        let txn: TxnResponse = decode(resp)?;
        let mut results = Vec::with_capacity(txn.results.len());
        for (i, r) in txn.results.into_iter().enumerate() {
            match r {
                OpResult::Put { auto_inc, row, .. } => {
                    results.push(RemoteOpResult::Put {
                        auto_inc,
                        row: row_to_map(self.db, op_table(&self.ops, i)?, row.as_deref()),
                    });
                }
                OpResult::Upsert { action, row, .. } => {
                    results.push(RemoteOpResult::Upsert {
                        action,
                        row: row_to_map(self.db, op_table(&self.ops, i)?, row.as_deref()),
                    });
                }
                OpResult::Deleted => results.push(RemoteOpResult::Deleted),
                OpResult::NotFound => results.push(RemoteOpResult::NotFound),
            }
        }
        Ok(RemoteBatch {
            epoch: txn.epoch,
            results,
        })
    }
}

fn op_table(ops: &[TxnOp], _i: usize) -> Result<&str> {
    match ops.get(_i) {
        Some(TxnOp::Put { table, .. })
        | Some(TxnOp::Upsert { table, .. })
        | Some(TxnOp::DeleteByPk { table, .. }) => Ok(table.as_str()),
        None => Err(KitError::Integrity("op/result length mismatch".into())),
    }
}

/// Decode a `[col_id, val, col_id, val, …]` post-image into a name-keyed map.
fn row_to_map(
    db: &RemoteDatabase,
    table: &str,
    row: Option<&[Value]>,
) -> Option<Map<String, Value>> {
    let row = row?;
    let t = db.schemas.get(table)?;
    let mut out = Map::new();
    let mut i = 0;
    while i + 1 < row.len() {
        if let Some(id) = row[i].as_u64() {
            if let Some(name) = t.id_to_name.get(&(id as u16)) {
                out.insert(name.clone(), row[i + 1].clone());
            }
        }
        i += 2;
    }
    Some(out)
}

fn map_error(resp: reqwest::blocking::Response) -> KitError {
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    if let Ok(v) = serde_json::from_str::<Value>(&body) {
        let code = v["error"]["code"].as_str().unwrap_or("");
        let msg = v["error"]["message"]
            .as_str()
            .unwrap_or("remote transaction rejected")
            .to_string();
        match code {
            EC_UNIQUE => return KitError::Duplicate(msg),
            EC_FK => return KitError::ForeignKey(msg),
            EC_CHECK | EC_BAD => return KitError::Validation(msg),
            EC_CONFLICT => return KitError::Conflict(msg),
            EC_TRIGGER_VALIDATION => return KitError::TriggerValidation(msg),
            "QUERY_CANCELLED" => {
                return KitError::Cancelled {
                    query_id: v["error"]["query_id"]
                        .as_str()
                        .unwrap_or("unknown")
                        .to_string(),
                    reason: msg,
                }
            }
            "DEADLINE_EXCEEDED" => {
                return KitError::DeadlineExceeded {
                    query_id: v["error"]["query_id"]
                        .as_str()
                        .unwrap_or("unknown")
                        .to_string(),
                    timeout_ms: None,
                }
            }
            "QUERY_ID_CONFLICT" => {
                return KitError::QueryConflict(
                    v["error"]["query_id"].as_str().unwrap_or(&msg).to_string(),
                )
            }
            "TRANSACTION_ABORTED" => return KitError::TransactionAborted(msg),
            _ => {}
        }
    }
    KitError::Storage(format!("http {status}: {body}"))
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn ioe(e: reqwest::Error) -> KitError {
    KitError::Storage(e.to_string())
}

fn decode<T: for<'de> Deserialize<'de>>(resp: reqwest::blocking::Response) -> Result<T> {
    if !resp.status().is_success() {
        return Err(map_error(resp));
    }
    let v: T = resp.json().map_err(|e| KitError::Storage(e.to_string()))?;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    fn read_request(mut stream: &TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
        }
        String::from_utf8(bytes).unwrap()
    }

    fn mock_server(
        responses: Vec<(&'static str, &'static str)>,
    ) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&stream));
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            requests
        });
        (format!("http://{address}"), worker)
    }

    #[test]
    fn cells_flat_encode() {
        let mut schemas = HashMap::new();
        let mut id_to_name = HashMap::new();
        let mut name_to_id = HashMap::new();
        id_to_name.insert(1u16, "id".to_string());
        id_to_name.insert(2u16, "name".to_string());
        name_to_id.insert("id".to_string(), 1u16);
        name_to_id.insert("name".to_string(), 2u16);
        schemas.insert(
            "t".to_string(),
            RemoteTable {
                id_to_name,
                name_to_id,
                primary_key: Some(1),
            },
        );
        let db = RemoteDatabase {
            base_url: "http://x".into(),
            client: reqwest::blocking::Client::new(),
            schemas,
            sql_cancellation: None,
        };
        let mut row = Map::new();
        row.insert("id".into(), json!(5));
        row.insert("name".into(), json!("a"));
        let cells = db.cells("t", &row).unwrap();
        assert_eq!(cells, vec![json!(1), json!(5), json!(2), json!("a")]);
    }

    #[test]
    fn controlled_sql_requires_advertised_capability() {
        let (url, server) =
            mock_server(vec![("404 Not Found", ""), ("200 OK", r#"{"tables":{}}"#)]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let error = database
            .sql_rows_with_options(
                "SELECT 1",
                RemoteSqlOptions {
                    timeout: Some(Duration::from_secs(1)),
                    ..RemoteSqlOptions::default()
                },
            )
            .unwrap_err();
        assert!(matches!(error, KitError::Unsupported(_)));
        assert_eq!(server.join().unwrap().len(), 2);
    }

    #[test]
    fn controlled_sql_sends_client_id_and_server_timeout() {
        let capabilities = r#"{"sql_cancellation":{"version":1,"client_query_ids":true,"cancel_endpoint":true,"query_status":true,"stream_disconnect_cancels":true}}"#;
        let timeout = r#"{"error":{"code":"DEADLINE_EXCEEDED","message":"timed out","query_id":"11112222333344445555666677778888"}}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", capabilities),
            ("200 OK", r#"{"tables":{}}"#),
            ("504 Gateway Timeout", timeout),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let query_id = "11112222333344445555666677778888".parse().unwrap();
        let error = database
            .sql_rows_with_options(
                "SELECT 1",
                RemoteSqlOptions {
                    query_id: Some(query_id),
                    timeout: Some(Duration::from_millis(250)),
                    ..RemoteSqlOptions::default()
                },
            )
            .unwrap_err();
        assert!(matches!(error, KitError::DeadlineExceeded { .. }));
        let requests = server.join().unwrap();
        assert!(requests[2].starts_with("POST /sql "));
        assert!(requests[2].contains(r#""query_id":"11112222333344445555666677778888""#));
        assert!(requests[2].contains(r#""timeout_ms":250"#));
        assert!(requests[2].contains(r#""format":"arrow""#));
    }

    #[test]
    fn cancel_maps_accepted_response() {
        let capabilities = r#"{"sql_cancellation":{"version":1,"client_query_ids":true,"cancel_endpoint":true,"query_status":true,"stream_disconnect_cancels":true}}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", capabilities),
            ("200 OK", r#"{"tables":{}}"#),
            (
                "202 Accepted",
                r#"{"query_id":"aaaabbbbccccddddeeeeffff00001111","state":"cancellation_requested"}"#,
            ),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let query_id = "aaaabbbbccccddddeeeeffff00001111".parse().unwrap();
        assert_eq!(
            database.cancel_sql(query_id).unwrap(),
            mongreldb_query::CancelOutcome::Accepted
        );
        let requests = server.join().unwrap();
        assert!(requests[2].starts_with("POST /queries/aaaabbbbccccddddeeeeffff00001111/cancel "));
    }

    #[test]
    fn transport_timeout_requests_best_effort_cancel() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let capabilities = r#"{"sql_cancellation":{"version":1,"client_query_ids":true,"cancel_endpoint":true,"query_status":true,"stream_disconnect_cancels":true}}"#;
            let mut requests = Vec::new();
            for body in [capabilities, r#"{"tables":{}}"#] {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&stream));
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            let (mut sql_stream, _) = listener.accept().unwrap();
            requests.push(read_request(&sql_stream));
            std::thread::sleep(Duration::from_millis(50));
            let _ = write!(
                sql_stream,
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]"
            );
            let (mut cancel_stream, _) = listener.accept().unwrap();
            requests.push(read_request(&cancel_stream));
            let body = r#"{"state":"cancellation_requested"}"#;
            write!(
                cancel_stream,
                "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            requests
        });
        let database = RemoteDatabase::connect(&format!("http://{address}")).unwrap();
        let query_id = "3333444455556666777788889999aaaa".parse().unwrap();
        let error = database
            .sql_rows_with_options(
                "SELECT 1",
                RemoteSqlOptions {
                    query_id: Some(query_id),
                    transport_timeout: Some(Duration::from_millis(10)),
                    ..RemoteSqlOptions::default()
                },
            )
            .unwrap_err();
        assert!(matches!(error, KitError::Transport { .. }));
        let requests = server.join().unwrap();
        assert!(requests[2].starts_with("POST /sql "));
        assert!(requests[3].starts_with("POST /queries/3333444455556666777788889999aaaa/cancel "));
    }
}
