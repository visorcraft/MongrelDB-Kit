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
pub struct RemoteDatabase {
    base_url: String,
    client: reqwest::blocking::Client,
    schemas: HashMap<String, RemoteTable>,
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
        };
        db.refresh()?;
        Ok(db)
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
        let resp = self
            .client
            .post(self.url("/sql"))
            .json(&serde_json::json!({ "sql": sql }))
            .send()
            .map_err(ioe)?;
        if !resp.status().is_success() {
            return Err(KitError::Storage(format!(
                "sql http {}: {}",
                resp.status(),
                resp.text().unwrap_or_default()
            )));
        }
        let bytes = resp.bytes().map_err(ioe)?;
        let batches = read_arrow_ipc(&bytes)?;
        let mut rows = Vec::new();
        for b in &batches {
            for row in batch_to_rows(b)? {
                rows.push(row);
            }
        }
        Ok(rows)
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
            _ => {}
        }
    }
    KitError::Storage(format!("http {status}: {body}"))
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
        };
        let mut row = Map::new();
        row.insert("id".into(), json!(5));
        row.insert("name".into(), json!("a"));
        let cells = db.cells("t", &row).unwrap();
        assert_eq!(cells, vec![json!(1), json!(5), json!(2), json!("a")]);
    }
}
