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

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use secrecy::ExposeSecret;
pub use secrecy::SecretString;
use serde::de::{DeserializeOwned, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use zeroize::Zeroizing;

use crate::arrow_util::{batch_to_rows, read_arrow_ipc};
use crate::error::{boxed_query_metadata, KitError, QueryExecutionOutcome, Result};
use mongreldb_kit_core::{ProcedureSpec, TriggerSpec, VirtualTableSpec};

const EC_UNIQUE: &str = "UNIQUE_VIOLATION";
const EC_FK: &str = "FK_VIOLATION";
const EC_CHECK: &str = "CHECK_VIOLATION";
const EC_CONFLICT: &str = "CONFLICT";
const EC_BAD: &str = "BAD_REQUEST";
const EC_TRIGGER_VALIDATION: &str = "TRIGGER_VALIDATION";
const SQL_RECOVERY_WINDOW: Duration = Duration::from_secs(2);
const SQL_RECOVERY_REQUEST_TIMEOUT: Duration = Duration::from_millis(250);
const SQL_RECOVERY_POLL_INTERVAL: Duration = Duration::from_millis(25);
const MAX_CONTROL_JSON_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_JSON_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

struct StrictJson(Value);

impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictJsonVisitor)
    }
}

struct StrictJsonVisitor;

impl<'de> Visitor<'de> for StrictJsonVisitor {
    type Value = StrictJson;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("valid JSON without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(StrictJson(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(StrictJson(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(StrictJson(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(StrictJson)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
        Ok(StrictJson(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(StrictJson(Value::String(value)))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictJson(Value::Null))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictJson(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        StrictJson::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(StrictJson(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(StrictJson(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(serde::de::Error::custom(format!(
                    "duplicate JSON object key {key:?}"
                )));
            }
            let StrictJson(value) = object.next_value()?;
            values.insert(key, value);
        }
        Ok(StrictJson(Value::Object(values)))
    }
}

fn strict_json_from_slice<T: DeserializeOwned>(bytes: &[u8]) -> serde_json::Result<T> {
    let StrictJson(value) = serde_json::from_slice(bytes)?;
    serde_json::from_value(value)
}

fn strict_json_from_str<T: DeserializeOwned>(text: &str) -> serde_json::Result<T> {
    strict_json_from_slice(text.as_bytes())
}

fn response_bytes_with_limit(
    mut response: reqwest::blocking::Response,
    limit: usize,
) -> std::result::Result<Vec<u8>, String> {
    use std::io::Read as _;

    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(format!("HTTP response exceeded {limit} bytes"));
    }
    let mut bytes = Vec::with_capacity(
        response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or_default()
            .min(limit),
    );
    response
        .by_ref()
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > limit {
        return Err(format!("HTTP response exceeded {limit} bytes"));
    }
    Ok(bytes)
}

fn sql_response_limit(requested: Option<usize>) -> usize {
    requested
        .unwrap_or(MAX_JSON_RESPONSE_BYTES)
        .min(MAX_JSON_RESPONSE_BYTES)
}

fn strict_response_json_with_limit<T: DeserializeOwned>(
    response: reqwest::blocking::Response,
    limit: usize,
) -> std::result::Result<T, String> {
    let bytes = response_bytes_with_limit(response, limit)?;
    strict_json_from_slice(&bytes).map_err(|error| error.to_string())
}

fn strict_response_json<T: DeserializeOwned>(
    response: reqwest::blocking::Response,
) -> std::result::Result<T, String> {
    strict_response_json_with_limit(response, MAX_JSON_RESPONSE_BYTES)
}

fn strict_control_response_json<T: DeserializeOwned>(
    response: reqwest::blocking::Response,
) -> std::result::Result<T, String> {
    strict_response_json_with_limit(response, MAX_CONTROL_JSON_RESPONSE_BYTES)
}

fn response_text_with_limit(
    response: reqwest::blocking::Response,
    limit: usize,
) -> std::result::Result<String, String> {
    String::from_utf8(response_bytes_with_limit(response, limit)?)
        .map_err(|_| "HTTP response was not valid UTF-8".to_owned())
}

#[derive(Clone)]
pub enum RemoteAuth {
    Bearer(SecretString),
    Basic {
        username: String,
        password: SecretString,
    },
}

impl std::fmt::Debug for RemoteAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bearer(_) => formatter.write_str("Bearer([REDACTED])"),
            Self::Basic { username, .. } => formatter
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RemoteOptions {
    pub auth: Option<RemoteAuth>,
    pub transport_timeout: Option<Duration>,
}

/// A typed remote client bound to a `mongreldb-server` URL.
#[derive(Clone)]
pub struct RemoteDatabase {
    base_url: String,
    client: reqwest::blocking::Client,
    schemas: HashMap<String, RemoteTable>,
    sql_cancellation: Option<SqlCancellationCapabilities>,
    sql_idempotency: Option<SqlIdempotencyCapabilities>,
    sql_pagination: Option<SqlPaginationCapabilities>,
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
    pub max_output_rows: Option<usize>,
    pub max_output_bytes: Option<usize>,
}

impl Default for RemoteSqlOptions {
    fn default() -> Self {
        Self {
            query_id: None,
            timeout: None,
            transport_timeout: None,
            format: RemoteSqlFormat::Arrow,
            max_output_rows: None,
            max_output_bytes: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RemoteSqlControlOptions {
    pub query_id: Option<mongreldb_query::QueryId>,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct RemoteSqlPaginationOptions {
    pub query_id: Option<mongreldb_query::QueryId>,
    pub timeout: Option<Duration>,
    pub page_size_rows: usize,
    pub projection: Vec<String>,
    pub max_page_bytes: Option<usize>,
    pub max_page_tokens: Option<usize>,
    pub max_output_rows: Option<usize>,
    pub max_output_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSqlPageLimits {
    pub rows: usize,
    pub bytes: usize,
    pub tokens: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSqlPageInfo {
    pub offset: usize,
    pub row_count: usize,
    pub total_rows: usize,
    pub byte_count: usize,
    pub estimated_tokens: usize,
    pub limits: RemoteSqlPageLimits,
    pub projection: Vec<String>,
    pub expires_at_ms: u64,
    pub snapshot: String,
    pub token_estimate: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSqlPage {
    pub status: String,
    pub rows: Vec<Map<String, Value>>,
    pub next_cursor: Option<String>,
    pub page: RemoteSqlPageInfo,
}

#[derive(Debug, Clone)]
pub struct RemoteIdempotentSqlOptions {
    pub query_id: Option<mongreldb_query::QueryId>,
    pub timeout: Option<Duration>,
    pub idempotency_key: String,
    pub max_output_rows: Option<usize>,
    pub max_output_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSqlReceiptError {
    pub code: String,
    pub category: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSqlWriteReceipt {
    pub query_id: String,
    pub original_query_id: String,
    pub status: String,
    #[serde(default)]
    pub terminal_state: Option<String>,
    #[serde(default)]
    pub server_state: Option<String>,
    #[serde(default)]
    pub cancel_outcome: Option<RemoteCancelOutcome>,
    #[serde(default)]
    pub cancellation_reason: Option<String>,
    pub committed: bool,
    pub committed_statements: usize,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub last_commit_epoch: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub last_commit_epoch_text: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub first_commit_statement_index: Option<usize>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub last_commit_statement_index: Option<usize>,
    pub completed_statements: usize,
    pub statement_index: usize,
    pub retryable: bool,
    pub idempotency_replayed: bool,
    pub idempotency_persisted: bool,
    pub idempotency_expires_at_ms: u64,
    pub outcome: RemoteQueryOutcome,
    pub terminal_error: Option<RemoteSqlReceiptError>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlCancellationCapabilities {
    pub version: u8,
    pub client_query_ids: bool,
    pub cancel_endpoint: bool,
    pub query_status: bool,
    #[serde(default)]
    pub pre_registration_cancel: bool,
    pub stream_disconnect_cancels: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlIdempotencyCapabilities {
    pub version: u8,
    pub durable_pre_execution_intent: bool,
    pub replay_committed_receipt: bool,
    pub indeterminate_never_reexecutes: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlPaginationCapabilities {
    pub version: u8,
    pub continuation_endpoint: String,
    pub retained_snapshot: bool,
    pub projection_required: bool,
    pub byte_and_token_hints: bool,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CapabilitiesResponse {
    #[serde(default)]
    sql_cancellation: Option<SqlCancellationCapabilities>,
    #[serde(default)]
    sql_idempotency: Option<SqlIdempotencyCapabilities>,
    #[serde(default)]
    sql_pagination: Option<SqlPaginationCapabilities>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteCancelOutcome {
    Accepted,
    AlreadyCancelling,
    TooLate,
    AlreadyFinished,
    NotFound,
    PreCancelled,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteQueryOutcome {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub committed: Option<bool>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub committed_statements: Option<usize>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub last_commit_epoch: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub last_commit_epoch_text: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub first_commit_statement_index: Option<usize>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub last_commit_statement_index: Option<usize>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub completed_statements: Option<usize>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub statement_index: Option<usize>,
    pub serialization: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteTerminalError {
    pub code: String,
    pub category: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteQueryTrace {
    #[serde(default)]
    pub queue_duration_us: Option<u64>,
    #[serde(default)]
    pub planning_duration_us: Option<u64>,
    #[serde(default)]
    pub execution_duration_us: Option<u64>,
    #[serde(default)]
    pub serialization_duration_us: Option<u64>,
    #[serde(default)]
    pub cancel_requested_phase: Option<String>,
    #[serde(default)]
    pub cancel_observed_phase: Option<String>,
    #[serde(default)]
    pub commit_fence_outcome: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteQueryStatus {
    pub query_id: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub server_state: String,
    #[serde(default)]
    pub terminal_state: Option<String>,
    #[serde(default)]
    pub operation: String,
    #[serde(default)]
    pub started_ms_ago: Option<u64>,
    #[serde(default)]
    pub deadline_ms_remaining: Option<u64>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub committed: Option<bool>,
    #[serde(default)]
    pub completed_statements: Option<usize>,
    #[serde(default)]
    pub statement_index: Option<usize>,
    #[serde(default)]
    pub committed_statements: Option<usize>,
    #[serde(default)]
    pub last_commit_epoch: Option<u64>,
    #[serde(default)]
    pub last_commit_epoch_text: Option<String>,
    #[serde(default)]
    pub first_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub last_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub cancel_outcome: Option<RemoteCancelOutcome>,
    #[serde(default)]
    pub cancellation_reason: Option<String>,
    #[serde(default)]
    pub retryable: bool,
    pub outcome: RemoteQueryOutcome,
    #[serde(default)]
    pub terminal_error: Option<RemoteTerminalError>,
    #[serde(default)]
    pub trace: Option<RemoteQueryTrace>,
}

impl RemoteQueryStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.server_state_or_state(),
            "completed" | "failed" | "cancelled" | "pre_cancelled"
        )
    }

    fn is_recovery_decisive(&self) -> bool {
        self.is_terminal() || self.durably_committed()
    }

    fn server_state_or_state(&self) -> &str {
        if self.server_state.is_empty() {
            &self.state
        } else {
            &self.server_state
        }
    }

    pub fn durable_commit_state(&self) -> Option<bool> {
        if self.committed == Some(true) || self.outcome.committed == Some(true) {
            Some(true)
        } else {
            self.committed.or(self.outcome.committed)
        }
    }

    pub fn durably_committed(&self) -> bool {
        self.durable_commit_state() == Some(true)
    }
}

fn validated_status_epoch(
    text: Option<&str>,
    numeric: Option<u64>,
) -> std::result::Result<Option<u64>, String> {
    match text {
        Some(text) => {
            let epoch = text
                .parse::<u64>()
                .map_err(|_| "last_commit_epoch_text is not an unsigned integer".to_owned())?;
            if epoch.to_string() != text {
                return Err("last_commit_epoch_text is not canonical".into());
            }
            Ok(Some(epoch))
        }
        None => Ok(numeric),
    }
}

fn validate_query_status(
    mut status: RemoteQueryStatus,
    expected_query_id: mongreldb_query::QueryId,
) -> std::result::Result<RemoteQueryStatus, String> {
    const STATES: &[&str] = &[
        "queued",
        "planning",
        "executing",
        "streaming",
        "serializing",
        "commit_critical",
        "cancelling",
        "completed",
        "failed",
        "cancelled",
        "pre_cancelled",
        "finished",
    ];
    const STATUSES: &[&str] = &[
        "running",
        "outcome_unknown",
        "completed",
        "failed_before_commit",
        "cancelled_before_commit",
        "deadline_before_commit",
        "cancelled_before_start",
        "committed",
        "committed_with_error",
        "partially_committed",
        "cancelled_after_commit",
        "deadline_after_commit",
        "finished",
    ];
    if status.query_id != expected_query_id.to_string() {
        return Err("query status query_id does not match the request".into());
    }
    if status
        .detail
        .as_deref()
        .is_some_and(|detail| detail != "compact")
    {
        return Err("query status detail is invalid".into());
    }
    if !STATUSES.contains(&status.status.as_str())
        || status.state.is_empty()
        || !STATES.contains(&status.state.as_str())
        || (!status.server_state.is_empty()
            && (!STATES.contains(&status.server_state.as_str())
                || status.server_state != status.state))
        || status
            .terminal_state
            .as_ref()
            .is_some_and(|terminal| terminal != &status.status)
    {
        return Err("query status state or status is invalid".into());
    }
    if status
        .session_id
        .as_ref()
        .is_some_and(|session| session.len() > 256)
    {
        return Err("query status session_id is too long".into());
    }
    if let Some(trace) = status.trace.as_ref() {
        let phase_is_valid =
            |phase: Option<&str>| phase.is_none_or(|phase| STATES.contains(&phase));
        if !phase_is_valid(trace.cancel_requested_phase.as_deref())
            || !phase_is_valid(trace.cancel_observed_phase.as_deref())
            || !matches!(
                trace.commit_fence_outcome.as_deref(),
                None | Some("not_reached" | "cancel_won" | "commit_won")
            )
        {
            return Err("query status trace fields are invalid".into());
        }
    }
    let state_matches_status = match status.status.as_str() {
        "running" => matches!(
            status.state.as_str(),
            "queued"
                | "planning"
                | "executing"
                | "streaming"
                | "serializing"
                | "commit_critical"
                | "cancelling"
        ),
        "committed" => matches!(
            status.state.as_str(),
            "planning"
                | "executing"
                | "streaming"
                | "serializing"
                | "commit_critical"
                | "cancelling"
                | "completed"
        ),
        "completed" => status.state == "completed",
        "failed_before_commit"
        | "committed_with_error"
        | "partially_committed"
        | "outcome_unknown" => status.state == "failed",
        "cancelled_before_commit"
        | "deadline_before_commit"
        | "cancelled_after_commit"
        | "deadline_after_commit" => status.state == "cancelled",
        "cancelled_before_start" => status.state == "pre_cancelled",
        "finished" => status.state == "finished",
        _ => false,
    };
    if !state_matches_status {
        return Err("query status state and status disagree".into());
    }
    let expected_terminal = if status.status == "running"
        || status.status == "finished"
        || (status.status == "committed" && status.state != "completed")
    {
        None
    } else {
        Some(status.status.as_str())
    };
    if status.terminal_state.as_deref() != expected_terminal {
        return Err("query status terminal_state disagrees with status".into());
    }
    let top_epoch = validated_status_epoch(
        status.last_commit_epoch_text.as_deref(),
        status.last_commit_epoch,
    )?;
    let outcome_epoch = validated_status_epoch(
        status.outcome.last_commit_epoch_text.as_deref(),
        status.outcome.last_commit_epoch,
    )?;
    if status
        .last_commit_epoch
        .is_some_and(|numeric| Some(numeric) != top_epoch)
        || status
            .outcome
            .last_commit_epoch
            .is_some_and(|numeric| Some(numeric) != outcome_epoch)
        || top_epoch != outcome_epoch
        || status.committed != status.outcome.committed
        || status.committed_statements != status.outcome.committed_statements
        || status.first_commit_statement_index != status.outcome.first_commit_statement_index
        || status.last_commit_statement_index != status.outcome.last_commit_statement_index
        || status.completed_statements != status.outcome.completed_statements
        || status.statement_index != status.outcome.statement_index
    {
        return Err("query status top-level and outcome fields disagree".into());
    }
    match status.committed {
        Some(true) => {
            if status.committed_statements == Some(0)
                || status.committed_statements.is_none()
                || top_epoch.is_none()
                || status.last_commit_epoch_text.is_none()
                || status.outcome.last_commit_epoch_text.is_none()
                || status.first_commit_statement_index.is_none()
                || status.last_commit_statement_index.is_none()
                || status.completed_statements.is_none()
                || status.statement_index.is_none()
                || !matches!(
                    status.status.as_str(),
                    "committed"
                        | "committed_with_error"
                        | "partially_committed"
                        | "cancelled_after_commit"
                        | "deadline_after_commit"
                )
            {
                return Err("committed query status has invalid durable metadata".into());
            }
        }
        Some(false) => {
            if status.committed_statements != Some(0)
                || top_epoch.is_some()
                || status.first_commit_statement_index.is_some()
                || status.last_commit_statement_index.is_some()
                || status.completed_statements.is_none()
                || status.statement_index.is_none()
                || matches!(
                    status.status.as_str(),
                    "committed"
                        | "committed_with_error"
                        | "partially_committed"
                        | "cancelled_after_commit"
                        | "deadline_after_commit"
                        | "outcome_unknown"
                        | "finished"
                )
            {
                return Err("non-committed query status has invalid durable metadata".into());
            }
        }
        None => {
            if status.committed_statements.is_some()
                || top_epoch.is_some()
                || status.first_commit_statement_index.is_some()
                || status.last_commit_statement_index.is_some()
                || status.completed_statements.is_some()
                || status.statement_index.is_some()
                || !matches!(status.status.as_str(), "outcome_unknown" | "finished")
            {
                return Err("unknown query status contains durable metadata".into());
            }
        }
    }
    if let (Some(first), Some(last), Some(committed), Some(statement)) = (
        status.first_commit_statement_index,
        status.last_commit_statement_index,
        status.committed_statements,
        status.statement_index,
    ) {
        if first > last
            || committed > last.saturating_sub(first).saturating_add(1)
            || last > statement
        {
            return Err("query status commit statement indexes are invalid".into());
        }
    }
    if let (Some(completed), Some(statement)) =
        (status.completed_statements, status.statement_index)
    {
        if statement > completed || completed > statement.saturating_add(1) {
            return Err("query status statement index and completed count disagree".into());
        }
    }
    if status.terminal_error.as_ref().is_some_and(|error| {
        error.code.trim().is_empty()
            || !matches!(
                error.category.as_str(),
                "cancellation" | "deadline" | "result_limit" | "serialization" | "execution"
            )
    }) {
        return Err("query status terminal error fields are invalid".into());
    }
    if !matches!(
        status.outcome.serialization.as_str(),
        "not_started" | "in_progress" | "succeeded" | "failed" | "unknown"
    ) {
        return Err("query status serialization outcome is invalid".into());
    }
    let expected_cancel = match status.state.as_str() {
        "cancelling" => Some(RemoteCancelOutcome::Accepted),
        "commit_critical" => Some(RemoteCancelOutcome::TooLate),
        "completed" | "failed" | "cancelled" | "finished" => {
            Some(RemoteCancelOutcome::AlreadyFinished)
        }
        "pre_cancelled" => Some(RemoteCancelOutcome::PreCancelled),
        _ => None,
    };
    if status.cancel_outcome != expected_cancel {
        return Err("query status cancel outcome disagrees with state".into());
    }
    let terminal_error = status.terminal_error.as_ref();
    let terminal_error_matches = match status.status.as_str() {
        "running" | "completed" | "committed" | "finished" => terminal_error.is_none(),
        "outcome_unknown" => terminal_error.is_some_and(|error| {
            error.code == "QUERY_OUTCOME_UNKNOWN" && error.category == "execution"
        }),
        "cancelled_before_commit" | "cancelled_before_start" => {
            terminal_error.is_some_and(|error| {
                error.code == "QUERY_CANCELLED" && error.category == "cancellation"
            })
        }
        "cancelled_after_commit" => terminal_error.is_some_and(|error| {
            error.code == "QUERY_CANCELLED_AFTER_COMMIT" && error.category == "cancellation"
        }),
        "deadline_before_commit" => terminal_error
            .is_some_and(|error| error.code == "DEADLINE_EXCEEDED" && error.category == "deadline"),
        "deadline_after_commit" => terminal_error.is_some_and(|error| {
            error.code == "DEADLINE_AFTER_COMMIT" && error.category == "deadline"
        }),
        _ => terminal_error.is_some(),
    };
    if !terminal_error_matches {
        return Err("query status terminal error disagrees with status".into());
    }
    if terminal_error.is_some_and(|error| {
        (error.category == "cancellation")
            != matches!(
                error.code.as_str(),
                "QUERY_CANCELLED" | "QUERY_CANCELLED_AFTER_COMMIT"
            )
            || (error.category == "deadline")
                != matches!(
                    error.code.as_str(),
                    "DEADLINE_EXCEEDED" | "DEADLINE_AFTER_COMMIT"
                )
    }) {
        return Err("query status terminal error code and category disagree".into());
    }
    let expected_retryable = terminal_error.is_some_and(|error| {
        matches!(
            error.code.as_str(),
            "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"
        )
    });
    if status.retryable != expected_retryable {
        return Err("query status retryable flag disagrees with terminal error".into());
    }
    const REASONS: &[&str] = &[
        "none",
        "client_request",
        "deadline",
        "client_disconnected",
        "session_closed",
        "server_shutdown",
    ];
    let reason = status.cancellation_reason.as_deref();
    let reason_matches = if !reason.is_some_and(|reason| REASONS.contains(&reason)) {
        false
    } else {
        match status.status.as_str() {
            "deadline_before_commit" | "deadline_after_commit" => reason == Some("deadline"),
            "cancelled_before_commit" | "cancelled_before_start" | "cancelled_after_commit" => {
                !matches!(reason, Some("none" | "deadline"))
            }
            "running" | "committed" if status.state == "cancelling" => reason != Some("none"),
            "running" | "committed" if status.state == "commit_critical" => true,
            _ => reason == Some("none"),
        }
    };
    if !reason_matches {
        return Err("query status cancellation reason disagrees with status".into());
    }
    status.last_commit_epoch = top_epoch;
    status.outcome.last_commit_epoch = outcome_epoch;
    Ok(status)
}

fn invalid_query_status_error(
    query_id: mongreldb_query::QueryId,
    message: impl Into<String>,
) -> KitError {
    KitError::OutcomeUnknown {
        query_id: query_id.to_string(),
        message: format!("invalid query status response: {}", message.into()),
        metadata: boxed_query_metadata(None, None, Some(false), Some("invalid_status")),
    }
}

fn max_known(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

fn remote_cancel_outcome_name(outcome: RemoteCancelOutcome) -> &'static str {
    match outcome {
        RemoteCancelOutcome::Accepted => "accepted",
        RemoteCancelOutcome::AlreadyCancelling => "already_cancelling",
        RemoteCancelOutcome::TooLate => "too_late",
        RemoteCancelOutcome::AlreadyFinished => "already_finished",
        RemoteCancelOutcome::NotFound => "not_found",
        RemoteCancelOutcome::PreCancelled => "pre_cancelled",
    }
}

fn cancel_outcome_from_wire(value: Option<&str>) -> Option<RemoteCancelOutcome> {
    match value {
        Some("accepted" | "cancellation_requested") => Some(RemoteCancelOutcome::Accepted),
        Some("already_cancelling" | "cancelling") => Some(RemoteCancelOutcome::AlreadyCancelling),
        Some("too_late" | "commit_critical") => Some(RemoteCancelOutcome::TooLate),
        Some("already_finished" | "finished") => Some(RemoteCancelOutcome::AlreadyFinished),
        Some("not_found") => Some(RemoteCancelOutcome::NotFound),
        Some("pre_cancelled") => Some(RemoteCancelOutcome::PreCancelled),
        _ => None,
    }
}

fn validate_cancel_metadata(object: &Map<String, Value>) -> std::result::Result<(), String> {
    let outcome = object.get("outcome").and_then(Value::as_object);
    if let Some(outcome) = outcome {
        for field in [
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "serialization",
        ] {
            if !outcome.contains_key(field) {
                return Err(format!("cancellation outcome {field} is missing"));
            }
        }
        value_optional_bool(&outcome["committed"])
            .ok_or_else(|| "cancellation outcome committed is invalid".to_owned())?;
        for field in [
            "committed_statements",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
        ] {
            value_optional_usize(&outcome[field])
                .ok_or_else(|| format!("cancellation outcome {field} is invalid"))?;
        }
        value_exact_epoch(outcome)?;
        if !matches!(
            outcome.get("serialization").and_then(Value::as_str),
            Some("not_started" | "in_progress" | "succeeded" | "failed" | "unknown")
        ) {
            return Err("cancellation outcome serialization is invalid".into());
        }
    }
    if let Some(committed) = object.get("committed") {
        let committed = value_optional_bool(committed)
            .ok_or_else(|| "cancellation committed is invalid".to_owned())?;
        if outcome
            .is_some_and(|outcome| value_optional_bool(&outcome["committed"]) != Some(committed))
        {
            return Err("cancellation committed disagrees with outcome".into());
        }
    }
    for field in [
        "committed_statements",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
    ] {
        if let Some(value) = object.get(field) {
            let value = value_optional_usize(value)
                .ok_or_else(|| format!("cancellation {field} is invalid"))?;
            if outcome.is_some_and(|outcome| value_optional_usize(&outcome[field]) != Some(value)) {
                return Err(format!("cancellation {field} disagrees with outcome"));
            }
        }
    }
    if object.contains_key("last_commit_epoch") || object.contains_key("last_commit_epoch_text") {
        let epoch = value_exact_epoch(object)?;
        if outcome.is_some_and(|outcome| value_exact_epoch(outcome).ok() != Some(epoch)) {
            return Err("cancellation commit epoch disagrees with outcome".into());
        }
    }
    if let Some(status) = object.get("status") {
        let status = status
            .as_str()
            .ok_or_else(|| "cancellation status is invalid".to_owned())?;
        if !matches!(
            status,
            "unknown"
                | "running"
                | "outcome_unknown"
                | "completed"
                | "failed_before_commit"
                | "cancelled_before_commit"
                | "deadline_before_commit"
                | "cancelled_before_start"
                | "committed"
                | "committed_with_error"
                | "partially_committed"
                | "cancelled_after_commit"
                | "deadline_after_commit"
                | "finished"
        ) {
            return Err("cancellation status is invalid".into());
        }
    }
    if let Some(terminal) = object.get("terminal_state") {
        if !terminal.is_null() && terminal.as_str() != object.get("status").and_then(Value::as_str)
        {
            return Err("cancellation terminal_state is invalid".into());
        }
    }
    if object
        .get("retryable")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err("cancellation retryable is invalid".into());
    }
    if let Some(server_state) = object.get("server_state") {
        if !matches!(
            server_state.as_str(),
            Some(
                "queued"
                    | "planning"
                    | "executing"
                    | "streaming"
                    | "serializing"
                    | "commit_critical"
                    | "cancelling"
                    | "completed"
                    | "failed"
                    | "cancelled"
                    | "pre_cancelled"
                    | "finished"
                    | "not_found"
            )
        ) {
            return Err("cancellation server_state is invalid".into());
        }
    }
    if let Some(reason) = object.get("cancellation_reason") {
        if !reason.is_null()
            && !matches!(
                reason.as_str(),
                Some(
                    "none"
                        | "client_request"
                        | "deadline"
                        | "client_disconnected"
                        | "session_closed"
                        | "server_shutdown"
                )
            )
        {
            return Err("cancellation reason is invalid".into());
        }
    }
    if let Some(error) = object.get("error").and_then(Value::as_object) {
        if error
            .get("code")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
            || error
                .get("message")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            || error
                .get("query_id")
                .is_some_and(|value| !value.is_null() && !value.is_string())
            || error
                .get("committed")
                .is_some_and(|value| value_optional_bool(value).is_none())
            || error
                .get("retryable")
                .is_some_and(|value| !value.is_boolean())
        {
            return Err("cancellation error fields are invalid".into());
        }
        if let (Some(top), Some(detail)) = (
            object.get("query_id").and_then(Value::as_str),
            error.get("query_id").and_then(Value::as_str),
        ) {
            if top != detail {
                return Err("cancellation error query_id disagrees".into());
            }
        }
    }
    Ok(())
}

fn validate_cancel_response(
    body: &Value,
    expected_query_id: mongreldb_query::QueryId,
    status: reqwest::StatusCode,
) -> Result<RemoteCancelOutcome> {
    let invalid =
        |message: &str| KitError::Storage(format!("invalid SQL cancellation response: {message}"));
    let object = body
        .as_object()
        .ok_or_else(|| invalid("body is not an object"))?;
    reject_unknown_fields(
        object,
        &[
            "query_id",
            "state",
            "cancel_outcome",
            "status",
            "terminal_state",
            "code",
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "cancellation_reason",
            "retryable",
            "server_state",
            "outcome",
            "error",
            "terminal_error",
        ],
        "cancellation response",
    )
    .map_err(|message| invalid(&message))?;
    if object
        .get("outcome")
        .is_some_and(|value| !value.is_null() && !value.is_object())
    {
        return Err(invalid("cancellation outcome is not an object"));
    }
    if let Some(outcome) = object.get("outcome").and_then(Value::as_object) {
        reject_unknown_fields(
            outcome,
            &[
                "committed",
                "committed_statements",
                "last_commit_epoch",
                "last_commit_epoch_text",
                "first_commit_statement_index",
                "last_commit_statement_index",
                "completed_statements",
                "statement_index",
                "serialization",
            ],
            "cancellation outcome",
        )
        .map_err(|message| invalid(&message))?;
    }
    if object
        .get("error")
        .is_some_and(|value| !value.is_null() && !value.is_object())
    {
        return Err(invalid("cancellation error is not an object"));
    }
    if let Some(error) = object.get("error").and_then(Value::as_object) {
        reject_unknown_fields(
            error,
            &["code", "message", "query_id", "committed", "retryable"],
            "cancellation error",
        )
        .map_err(|message| invalid(&message))?;
    }
    if object
        .get("terminal_error")
        .is_some_and(|value| !value.is_null() && !value.is_object())
    {
        return Err(invalid("terminal_error is not an object"));
    }
    if let Some(error) = object.get("terminal_error").and_then(Value::as_object) {
        reject_unknown_fields(error, &["code", "category"], "terminal_error")
            .map_err(|message| invalid(&message))?;
        if error
            .get("code")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
            || error
                .get("category")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        {
            return Err(invalid("terminal_error fields are invalid"));
        }
    }
    validate_cancel_metadata(object).map_err(|message| invalid(&message))?;
    if body.get("query_id").and_then(Value::as_str) != Some(expected_query_id.to_string().as_str())
    {
        return Err(invalid("query_id does not match the request"));
    }
    let decode_field = |name: &str| -> Result<Option<RemoteCancelOutcome>> {
        match body.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => cancel_outcome_from_wire(value.as_str())
                .map(Some)
                .ok_or_else(|| invalid(&format!("{name} is invalid"))),
        }
    };
    let outcome = decode_field("cancel_outcome")?;
    let mut state = decode_field("state")?;
    if outcome == Some(RemoteCancelOutcome::NotFound)
        && state.is_none()
        && object.get("server_state").and_then(Value::as_str) == Some("not_found")
    {
        state = Some(RemoteCancelOutcome::NotFound);
    }
    let (Some(outcome), Some(state)) = (outcome, state) else {
        return Err(invalid("state and cancel_outcome are required"));
    };
    if outcome != state {
        return Err(invalid("state and cancel_outcome disagree"));
    }
    let compatible = matches!(
        (status.as_u16(), outcome),
        (
            202,
            RemoteCancelOutcome::Accepted | RemoteCancelOutcome::PreCancelled
        ) | (
            200,
            RemoteCancelOutcome::AlreadyCancelling | RemoteCancelOutcome::AlreadyFinished
        ) | (409, RemoteCancelOutcome::TooLate)
            | (404, RemoteCancelOutcome::NotFound)
    );
    if !compatible {
        return Err(invalid("HTTP status and outcome disagree"));
    }
    let terminal_error = object.get("terminal_error").and_then(Value::as_object);
    let terminal_matches = if outcome == RemoteCancelOutcome::PreCancelled {
        terminal_error.is_none_or(|error| {
            error.get("code").and_then(Value::as_str) == Some("QUERY_CANCELLED")
                && error.get("category").and_then(Value::as_str) == Some("cancellation")
        })
    } else {
        terminal_error.is_none()
    };
    if !terminal_matches {
        return Err(invalid(
            "terminal_error disagrees with cancellation outcome",
        ));
    }
    Ok(outcome)
}

fn validate_query_not_found_response(
    body: &Value,
    expected_query_id: mongreldb_query::QueryId,
) -> std::result::Result<(), String> {
    const FIELDS: &[&str] = &[
        "query_id",
        "status",
        "terminal_state",
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "cancel_outcome",
        "cancellation_reason",
        "retryable",
        "server_state",
        "outcome",
        "error",
    ];
    const OUTCOME_FIELDS: &[&str] = &[
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "serialization",
    ];
    const ERROR_FIELDS: &[&str] = &["code", "message", "query_id", "committed", "retryable"];

    exact_object_fields(body, FIELDS, FIELDS, "query-not-found response")?;
    exact_object_fields(
        &body["outcome"],
        OUTCOME_FIELDS,
        OUTCOME_FIELDS,
        "query-not-found outcome",
    )?;
    exact_object_fields(
        &body["error"],
        ERROR_FIELDS,
        ERROR_FIELDS,
        "query-not-found error",
    )?;
    let expected_query_id = expected_query_id.to_string();
    if body["query_id"].as_str() != Some(expected_query_id.as_str())
        || body["status"].as_str() != Some("unknown")
        || !body["terminal_state"].is_null()
        || body["cancel_outcome"].as_str() != Some("not_found")
        || !body["cancellation_reason"].is_null()
        || body["retryable"].as_bool() != Some(false)
        || body["server_state"].as_str() != Some("not_found")
        || body["outcome"]["serialization"].as_str() != Some("unknown")
        || body["error"]["code"].as_str() != Some("QUERY_NOT_FOUND")
        || body["error"]["message"].as_str().is_none_or(str::is_empty)
        || body["error"]["query_id"].as_str() != Some(expected_query_id.as_str())
        || !body["error"]["committed"].is_null()
        || body["error"]["retryable"].as_bool() != Some(false)
    {
        return Err("query-not-found response metadata is invalid".into());
    }
    for field in [
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
    ] {
        if !body[field].is_null() || !body["outcome"][field].is_null() {
            return Err(format!(
                "query-not-found response field {field} must be null"
            ));
        }
    }
    Ok(())
}

fn validate_sql_cursor_error_envelope(body: &Value) -> std::result::Result<(), String> {
    const FIELDS: &[&str] = &[
        "status",
        "terminal_state",
        "server_state",
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "cancel_outcome",
        "cancellation_reason",
        "retryable",
        "outcome",
        "error",
    ];
    const OUTCOME_FIELDS: &[&str] = &[
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "serialization",
    ];
    const ERROR_FIELDS: &[&str] = &["code", "message", "committed", "retryable"];
    exact_object_fields(body, FIELDS, FIELDS, "SQL cursor error response")?;
    exact_object_fields(
        &body["outcome"],
        OUTCOME_FIELDS,
        OUTCOME_FIELDS,
        "SQL cursor error outcome",
    )?;
    exact_object_fields(
        &body["error"],
        ERROR_FIELDS,
        ERROR_FIELDS,
        "SQL cursor error",
    )?;
    if body["status"].as_str() != Some("failed_before_commit")
        || body["terminal_state"].as_str() != Some("failed_before_commit")
        || body["server_state"].as_str() != Some("failed")
        || body["committed"].as_bool() != Some(false)
        || body["committed_statements"].as_u64() != Some(0)
        || !body["last_commit_epoch"].is_null()
        || !body["last_commit_epoch_text"].is_null()
        || !body["first_commit_statement_index"].is_null()
        || !body["last_commit_statement_index"].is_null()
        || body["completed_statements"].as_u64() != Some(0)
        || body["statement_index"].as_u64() != Some(0)
        || !body["cancel_outcome"].is_null()
        || !body["cancellation_reason"].is_null()
        || body["retryable"].as_bool() != Some(false)
        || body["outcome"]["committed"].as_bool() != Some(false)
        || body["outcome"]["committed_statements"].as_u64() != Some(0)
        || !body["outcome"]["last_commit_epoch"].is_null()
        || !body["outcome"]["last_commit_epoch_text"].is_null()
        || !body["outcome"]["first_commit_statement_index"].is_null()
        || !body["outcome"]["last_commit_statement_index"].is_null()
        || body["outcome"]["completed_statements"].as_u64() != Some(0)
        || body["outcome"]["statement_index"].as_u64() != Some(0)
        || body["outcome"]["serialization"].as_str() != Some("not_started")
        || body["error"]["code"].as_str().is_none_or(str::is_empty)
        || body["error"]["message"].as_str().is_none_or(str::is_empty)
        || body["error"]["committed"].as_bool() != Some(false)
        || body["error"]["retryable"].as_bool() != Some(false)
    {
        return Err("SQL cursor error metadata is invalid".into());
    }
    Ok(())
}

fn remote_status_metadata(status: &RemoteQueryStatus) -> Box<crate::error::QueryErrorMetadata> {
    boxed_query_metadata(
        status.cancel_outcome.map(remote_cancel_outcome_name),
        status.cancellation_reason.as_deref(),
        Some(status.retryable),
        (!status.server_state_or_state().is_empty()).then_some(status.server_state_or_state()),
    )
}

fn precise_epoch(text: Option<&str>, numeric: Option<u64>) -> Result<Option<u64>> {
    match text {
        Some(text) => {
            let exact = text.parse::<u64>().map_err(|_| {
                KitError::Storage("invalid last_commit_epoch_text from server".into())
            })?;
            if exact.to_string() != text || numeric.is_some_and(|value| value != exact) {
                return Err(KitError::Storage(
                    "conflicting or non-canonical exact commit epoch from server".into(),
                ));
            }
            Ok(Some(exact))
        }
        None => Ok(numeric),
    }
}

fn validated_receipt_epoch(
    text: Option<&str>,
    numeric: Option<u64>,
    scope: &str,
) -> Result<Option<u64>> {
    let exact = precise_epoch(text, numeric)?;
    if let Some(text) = text {
        if exact.is_some_and(|epoch| epoch.to_string() != text) {
            return Err(KitError::Storage(format!(
                "invalid SQL idempotency receipt: {scope} exact commit epoch is not canonical"
            )));
        }
        if numeric.is_some() && exact != numeric {
            return Err(KitError::Storage(format!(
                "invalid SQL idempotency receipt: {scope} numeric and exact commit epochs disagree"
            )));
        }
    }
    Ok(exact)
}

fn validate_sql_query_id_header(
    headers: &reqwest::header::HeaderMap,
    expected_query_id: mongreldb_query::QueryId,
) -> std::result::Result<(), String> {
    let mut values = headers.get_all("x-mongreldb-query-id").iter();
    let value = values
        .next()
        .ok_or_else(|| "SQL response is missing x-mongreldb-query-id".to_owned())?;
    if values.next().is_some() {
        return Err("SQL response has duplicate x-mongreldb-query-id headers".into());
    }
    let value = value
        .to_str()
        .map_err(|_| "SQL response has a non-UTF-8 x-mongreldb-query-id".to_owned())?;
    if value != expected_query_id.to_string() {
        return Err("SQL response x-mongreldb-query-id does not match the request".into());
    }
    Ok(())
}

fn validate_sql_write_receipt(
    mut receipt: RemoteSqlWriteReceipt,
    expected_query_id: &str,
    expected_original_query_id: Option<&str>,
) -> Result<RemoteSqlWriteReceipt> {
    let invalid =
        |message: &str| KitError::Storage(format!("invalid SQL idempotency receipt: {message}"));
    if receipt.query_id != expected_query_id {
        return Err(invalid("query_id does not match the request"));
    }
    if receipt
        .original_query_id
        .parse::<mongreldb_query::QueryId>()
        .is_err()
    {
        return Err(invalid("original_query_id is not a 32-digit hex ID"));
    }
    let status_committed = match receipt.status.as_str() {
        "completed" => false,
        "committed"
        | "committed_with_error"
        | "partially_committed"
        | "cancelled_after_commit"
        | "deadline_after_commit" => true,
        _ => return Err(invalid("status is invalid")),
    };
    if !matches!(
        receipt.outcome.serialization.as_str(),
        "not_started" | "in_progress" | "succeeded" | "failed" | "unknown"
    ) {
        return Err(invalid("outcome serialization is invalid"));
    }
    if receipt.terminal_error.as_ref().is_some_and(|error| {
        error.code.trim().is_empty()
            || !matches!(
                error.category.as_str(),
                "cancellation" | "deadline" | "result_limit" | "serialization" | "execution"
            )
            || (error.category == "cancellation")
                != matches!(
                    error.code.as_str(),
                    "QUERY_CANCELLED" | "QUERY_CANCELLED_AFTER_COMMIT"
                )
            || (error.category == "deadline")
                != matches!(
                    error.code.as_str(),
                    "DEADLINE_EXCEEDED" | "DEADLINE_AFTER_COMMIT"
                )
            || (error.category == "result_limit") != (error.code == "RESULT_LIMIT_EXCEEDED")
            || (error.category == "serialization")
                != matches!(
                    error.code.as_str(),
                    "SERIALIZATION_FAILED" | "SERIALIZATION_FAILED_AFTER_COMMIT"
                )
    }) {
        return Err(invalid("terminal error code or category is invalid"));
    }
    if receipt
        .terminal_state
        .as_deref()
        .is_some_and(|terminal| terminal != receipt.status)
        || receipt
            .server_state
            .as_deref()
            .is_some_and(|state| !matches!(state, "completed" | "failed" | "cancelled"))
        || receipt
            .cancel_outcome
            .is_some_and(|outcome| outcome != RemoteCancelOutcome::AlreadyFinished)
        || receipt
            .cancellation_reason
            .as_deref()
            .is_some_and(|reason| {
                !matches!(
                    reason,
                    "none"
                        | "client_request"
                        | "deadline"
                        | "client_disconnected"
                        | "session_closed"
                        | "server_shutdown"
                )
            })
    {
        return Err(invalid("terminal control metadata is invalid"));
    }
    if let Some(server_state) = receipt.server_state.as_deref() {
        let expected = match receipt.status.as_str() {
            "completed" | "committed" => "completed",
            "committed_with_error" | "partially_committed" => "failed",
            "cancelled_after_commit" | "deadline_after_commit" => "cancelled",
            _ => return Err(invalid("status is invalid")),
        };
        if server_state != expected {
            return Err(invalid("server_state disagrees with status"));
        }
    }
    if let Some(reason) = receipt.cancellation_reason.as_deref() {
        let reason_matches = match receipt.status.as_str() {
            "cancelled_after_commit" => !matches!(reason, "none" | "deadline"),
            "deadline_after_commit" => reason == "deadline",
            _ => reason == "none",
        };
        if !reason_matches {
            return Err(invalid("cancellation_reason disagrees with status"));
        }
    }
    if receipt.committed != status_committed
        || receipt.outcome.committed != Some(receipt.committed)
        || receipt.outcome.committed_statements != Some(receipt.committed_statements)
        || receipt.outcome.first_commit_statement_index != receipt.first_commit_statement_index
        || receipt.outcome.last_commit_statement_index != receipt.last_commit_statement_index
        || receipt.outcome.completed_statements != Some(receipt.completed_statements)
        || receipt.outcome.statement_index != Some(receipt.statement_index)
    {
        return Err(invalid("top-level and outcome commit state disagree"));
    }
    let terminal_matches = match receipt.status.as_str() {
        "completed" | "committed" => receipt.terminal_error.is_none(),
        "cancelled_after_commit" => receipt.terminal_error.as_ref().is_some_and(|error| {
            error.code == "QUERY_CANCELLED_AFTER_COMMIT" && error.category == "cancellation"
        }),
        "deadline_after_commit" => receipt.terminal_error.as_ref().is_some_and(|error| {
            error.code == "DEADLINE_AFTER_COMMIT" && error.category == "deadline"
        }),
        "committed_with_error" | "partially_committed" => receipt.terminal_error.is_some(),
        _ => false,
    };
    if !terminal_matches {
        return Err(invalid("terminal error disagrees with status"));
    }
    receipt.last_commit_epoch = validated_receipt_epoch(
        receipt.last_commit_epoch_text.as_deref(),
        receipt.last_commit_epoch,
        "top-level",
    )?;
    receipt.outcome.last_commit_epoch = validated_receipt_epoch(
        receipt.outcome.last_commit_epoch_text.as_deref(),
        receipt.outcome.last_commit_epoch,
        "outcome",
    )?;
    if receipt.last_commit_epoch != receipt.outcome.last_commit_epoch {
        return Err(invalid("top-level and outcome commit epochs disagree"));
    }
    if receipt
        .first_commit_statement_index
        .zip(receipt.last_commit_statement_index)
        .is_some_and(|(first, last)| first > last)
    {
        return Err(invalid("commit statement index range is reversed"));
    }
    if receipt.committed {
        if receipt.committed_statements == 0
            || receipt.last_commit_epoch.is_none()
            || receipt.last_commit_epoch_text.is_none()
            || receipt.outcome.last_commit_epoch_text.is_none()
            || receipt.first_commit_statement_index.is_none()
            || receipt.last_commit_statement_index.is_none()
        {
            return Err(invalid("committed receipt has no durable commit metadata"));
        }
    } else if receipt.committed_statements != 0
        || receipt.last_commit_epoch.is_some()
        || receipt.first_commit_statement_index.is_some()
        || receipt.last_commit_statement_index.is_some()
    {
        return Err(invalid("non-committed receipt contains commit metadata"));
    }
    if let (Some(first), Some(last)) = (
        receipt.first_commit_statement_index,
        receipt.last_commit_statement_index,
    ) {
        if receipt.committed_statements > last.saturating_sub(first).saturating_add(1)
            || last > receipt.statement_index
        {
            return Err(invalid("commit statement indexes are invalid"));
        }
    }
    if receipt.statement_index > receipt.completed_statements
        || receipt.completed_statements > receipt.statement_index.saturating_add(1)
    {
        return Err(invalid("statement index and completed count disagree"));
    }
    let original_query_id_matches = match expected_original_query_id {
        Some(expected) if receipt.idempotency_replayed => receipt.original_query_id == expected,
        Some(_) => receipt.original_query_id == expected_query_id,
        None if receipt.idempotency_replayed => true,
        None => receipt.original_query_id == expected_query_id,
    };
    if !receipt.idempotency_persisted
        || receipt.idempotency_expires_at_ms == 0
        || receipt.retryable
        || !original_query_id_matches
    {
        return Err(invalid("idempotency metadata is invalid"));
    }
    receipt.last_commit_epoch = receipt
        .last_commit_epoch
        .or(receipt.outcome.last_commit_epoch);
    Ok(receipt)
}

fn validate_sql_page(
    page: RemoteSqlPage,
    initial_options: Option<&RemoteSqlPaginationOptions>,
) -> Result<RemoteSqlPage> {
    let invalid = |message: &str| KitError::Storage(format!("invalid SQL page: {message}"));
    let end = page
        .page
        .offset
        .checked_add(page.page.row_count)
        .ok_or_else(|| invalid("offset overflowed"))?;
    if page.status != "completed" {
        return Err(invalid("status is not completed"));
    }
    if page.page.row_count != page.rows.len() {
        return Err(invalid("row_count does not match rows"));
    }
    let projection: HashSet<_> = page.page.projection.iter().collect();
    let projection_bytes = page
        .page
        .projection
        .iter()
        .map(String::len)
        .fold(0usize, usize::saturating_add);
    if !(1..=128).contains(&page.page.projection.len())
        || projection_bytes > 16 * 1024
        || page
            .page
            .projection
            .iter()
            .any(|column| column.trim().is_empty() || column == "*" || column.len() > 256)
        || projection.len() != page.page.projection.len()
        || page.rows.iter().any(|row| {
            row.len() != projection.len()
                || projection.iter().any(|column| !row.contains_key(*column))
        })
    {
        return Err(invalid("rows do not exactly match the unique projection"));
    }
    let byte_count = page.rows.iter().try_fold(2usize, |bytes, row| {
        serde_json::to_vec(row)
            .map(|encoded| {
                bytes
                    .saturating_add(usize::from(bytes > 2))
                    .saturating_add(encoded.len())
            })
            .map_err(|error| invalid(&error.to_string()))
    })?;
    if page.page.byte_count != byte_count
        || page.page.estimated_tokens != byte_count.saturating_add(3) / 4
    {
        return Err(invalid("byte or token estimate is invalid"));
    }
    if page.page.offset > page.page.total_rows || end > page.page.total_rows {
        return Err(invalid("offset or row_count exceeds total_rows"));
    }
    if page.page.limits.rows == 0
        || page.page.limits.bytes == 0
        || page.page.limits.tokens == 0
        || page.page.limits.bytes > MAX_JSON_RESPONSE_BYTES
        || page.page.row_count > page.page.limits.rows
        || page.page.byte_count > page.page.limits.bytes
        || page.page.estimated_tokens > page.page.limits.tokens
    {
        return Err(invalid("declared limits are invalid or exceeded"));
    }
    if page.page.expires_at_ms == 0
        || page.page.snapshot != "retained_result"
        || page.page.token_estimate != "ceil(projected_json_bytes/4)"
    {
        return Err(invalid("metadata contains an empty required field"));
    }
    let has_more = end < page.page.total_rows;
    if (has_more && page.page.row_count == 0)
        || has_more != page.next_cursor.is_some()
        || page
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 2_048)
    {
        return Err(invalid("continuation cursor is inconsistent"));
    }
    if let Some(options) = initial_options {
        if page.page.offset != 0
            || page.page.projection != options.projection
            || page.page.limits.rows > options.page_size_rows
            || options
                .max_page_bytes
                .is_some_and(|limit| page.page.limits.bytes > limit)
            || options
                .max_page_tokens
                .is_some_and(|limit| page.page.limits.tokens > limit)
            || options
                .max_output_rows
                .is_some_and(|limit| page.page.total_rows > limit)
            || options
                .max_output_bytes
                .is_some_and(|limit| page.page.byte_count > limit)
        {
            return Err(invalid(
                "metadata does not match requested pagination options",
            ));
        }
    }
    Ok(page)
}

fn validate_sql_pagination_options(options: &RemoteSqlPaginationOptions) -> Result<()> {
    let mut projection = HashSet::new();
    let projection_bytes = options
        .projection
        .iter()
        .map(String::len)
        .fold(0usize, usize::saturating_add);
    if options.page_size_rows == 0
        || !(1..=128).contains(&options.projection.len())
        || projection_bytes > 16 * 1024
        || options.projection.iter().any(|column| {
            column.is_empty()
                || column == "*"
                || column.len() > 256
                || !projection.insert(column.as_str())
        })
        || options.max_page_bytes == Some(0)
        || options.max_page_tokens == Some(0)
        || options.max_output_rows == Some(0)
        || options.max_output_bytes == Some(0)
    {
        return Err(KitError::Validation(
            "pagination limits must be positive and projection must contain 1 to 128 unique explicit columns".into(),
        ));
    }
    Ok(())
}

enum IdempotentSqlAttemptError {
    Replay(KitError),
    Final(KitError),
}

impl IdempotentSqlAttemptError {
    fn into_inner(self) -> KitError {
        match self {
            Self::Replay(error) | Self::Final(error) => error,
        }
    }
}

fn idempotent_loss(error: KitError) -> IdempotentSqlAttemptError {
    IdempotentSqlAttemptError::Final(error)
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

    pub fn cancel(&self) -> Result<RemoteCancelOutcome> {
        self.database.cancel_sql(self.query_id)
    }

    pub fn status(&self) -> Result<Option<RemoteQueryStatus>> {
        self.database.sql_query_status(self.query_id)
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
#[serde(deny_unknown_fields)]
struct SchemaInfo {
    columns: Vec<ColumnMeta>,
    #[serde(default, rename = "schema_id")]
    _schema_id: Option<u64>,
    #[serde(default, rename = "indexes")]
    _indexes: Option<Vec<Value>>,
    #[serde(default, rename = "constraints")]
    _constraints: Option<Value>,
}

/// `POST /compact` response: `{ "compacted": N, "skipped": M }`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactResp {
    compacted: usize,
    skipped: usize,
}

/// `POST /tables/{name}/compact` response: `{ "status": "compacted"|"skipped" }`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactTableResp {
    status: String,
}

/// `GET/PUT /history/retention` response.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRetention {
    pub history_retention_epochs: u64,
    pub earliest_retained_epoch: u64,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnMeta {
    id: u16,
    name: String,
    primary_key: bool,
    #[serde(default, rename = "ty")]
    _ty: Option<String>,
    #[serde(default, rename = "nullable")]
    _nullable: bool,
    #[serde(default, rename = "auto_increment")]
    _auto_increment: bool,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AllSchemas {
    tables: Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTableResponse {
    table_id: u64,
    table_id_text: String,
}

fn remote_http_client(options: &RemoteOptions) -> Result<reqwest::blocking::Client> {
    validate_positive_duration("transport_timeout", options.transport_timeout)?;
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(auth) = &options.auth {
        let header = match auth {
            RemoteAuth::Bearer(token) => {
                if token.expose_secret().trim().is_empty() {
                    return Err(KitError::Validation(
                        "bearer token must not be empty".into(),
                    ));
                }
                Zeroizing::new(format!("Bearer {}", token.expose_secret()))
            }
            RemoteAuth::Basic { username, password } => {
                if username.is_empty() || username.contains(':') {
                    return Err(KitError::Validation(
                        "basic-auth username must be non-empty and contain no colon".into(),
                    ));
                }
                use base64::Engine as _;
                let credentials =
                    Zeroizing::new(format!("{username}:{}", password.expose_secret()));
                let encoded = Zeroizing::new(
                    base64::engine::general_purpose::STANDARD.encode(credentials.as_bytes()),
                );
                Zeroizing::new(format!("Basic {}", encoded.as_str()))
            }
        };
        let mut header = reqwest::header::HeaderValue::from_bytes(header.as_bytes())
            .map_err(|_| KitError::Validation("invalid authorization credentials".into()))?;
        header.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, header);
    }
    let mut client = reqwest::blocking::Client::builder().default_headers(headers);
    if let Some(timeout) = options.transport_timeout {
        client = client.timeout(timeout);
    }
    client.build().map_err(ioe)
}

impl RemoteDatabase {
    /// Connect to a daemon and load every table's schema metadata.
    pub fn connect(url: &str) -> Result<Self> {
        Self::connect_with_options(url, RemoteOptions::default())
    }

    pub fn connect_with_options(url: &str, options: RemoteOptions) -> Result<Self> {
        let mut parsed = reqwest::Url::parse(url)
            .map_err(|_| KitError::Validation("invalid remote URL".into()))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(KitError::Validation(
                "remote URL must use http:// or https://".into(),
            ));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(KitError::Validation(
                "credentials must use RemoteOptions.auth, not the URL".into(),
            ));
        }
        if parsed.query().is_some_and(|query| !query.is_empty())
            || parsed
                .fragment()
                .is_some_and(|fragment| !fragment.is_empty())
        {
            return Err(KitError::Validation(
                "remote URL must not include a query or fragment".into(),
            ));
        }
        parsed.set_query(None);
        parsed.set_fragment(None);
        let mut db = Self {
            base_url: parsed.as_str().trim_end_matches('/').to_string(),
            client: remote_http_client(&options)?,
            schemas: HashMap::new(),
            sql_cancellation: None,
            sql_idempotency: None,
            sql_pagination: None,
        };
        if let Some(capabilities) = db.fetch_capabilities()? {
            db.sql_cancellation = capabilities.sql_cancellation;
            db.sql_idempotency = capabilities.sql_idempotency;
            db.sql_pagination = capabilities.sql_pagination;
        }
        db.refresh()?;
        Ok(db)
    }

    fn fetch_capabilities(&self) -> Result<Option<CapabilitiesResponse>> {
        let response = self
            .client
            .get(self.url("/capabilities"))
            .send()
            .map_err(ioe)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        decode_control(response).map(Some)
    }

    pub fn sql_cancellation_capabilities(&self) -> Option<&SqlCancellationCapabilities> {
        self.sql_cancellation.as_ref()
    }

    pub fn sql_idempotency_capabilities(&self) -> Option<&SqlIdempotencyCapabilities> {
        self.sql_idempotency.as_ref()
    }

    pub fn sql_pagination_capabilities(&self) -> Option<&SqlPaginationCapabilities> {
        self.sql_pagination.as_ref()
    }

    fn validate_sql_cancellation_capability(
        capabilities: Option<&SqlCancellationCapabilities>,
    ) -> Result<&SqlCancellationCapabilities> {
        let capabilities = capabilities.ok_or_else(|| {
            KitError::CapabilityUnsupported(
                "server does not advertise SQL cancellation capability version 2".into(),
            )
        })?;
        if capabilities.version != 2
            || !capabilities.client_query_ids
            || !capabilities.cancel_endpoint
            || !capabilities.query_status
            || !capabilities.pre_registration_cancel
        {
            return Err(KitError::CapabilityUnsupported(
                "server SQL cancellation capability is incompatible".into(),
            ));
        }
        Ok(capabilities)
    }

    fn require_sql_cancellation(&self) -> Result<&SqlCancellationCapabilities> {
        Self::validate_sql_cancellation_capability(self.sql_cancellation.as_ref())
    }

    fn validate_sql_idempotency_capability(
        capabilities: Option<&SqlIdempotencyCapabilities>,
    ) -> Result<&SqlIdempotencyCapabilities> {
        let capabilities = capabilities.ok_or_else(|| {
            KitError::CapabilityUnsupported(
                "server does not advertise durable SQL idempotency".into(),
            )
        })?;
        if capabilities.version != 1
            || !capabilities.durable_pre_execution_intent
            || !capabilities.replay_committed_receipt
            || !capabilities.indeterminate_never_reexecutes
        {
            return Err(KitError::CapabilityUnsupported(
                "server SQL idempotency capability is incompatible".into(),
            ));
        }
        Ok(capabilities)
    }

    fn require_sql_idempotency(&self) -> Result<&SqlIdempotencyCapabilities> {
        Self::validate_sql_idempotency_capability(self.sql_idempotency.as_ref())
    }

    fn require_fresh_sql_idempotency(&self) -> Result<()> {
        let capabilities = self.fetch_capabilities()?;
        let cancellation = capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.sql_cancellation.as_ref());
        let idempotency = capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.sql_idempotency.as_ref());
        Self::validate_sql_cancellation_capability(cancellation)?;
        Self::validate_sql_idempotency_capability(idempotency)?;
        Ok(())
    }

    fn require_sql_pagination(&self) -> Result<&SqlPaginationCapabilities> {
        let capabilities = self.sql_pagination.as_ref().ok_or_else(|| {
            KitError::CapabilityUnsupported("server does not advertise SQL pagination".into())
        })?;
        if capabilities.version != 1
            || capabilities.continuation_endpoint != "/sql/continue"
            || !capabilities.retained_snapshot
            || !capabilities.projection_required
            || !capabilities.byte_and_token_hints
        {
            return Err(KitError::CapabilityUnsupported(
                "server SQL pagination capability is incompatible".into(),
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
        let mut schemas = HashMap::new();
        for (name, body) in &all.tables {
            if name.is_empty() {
                return Err(KitError::Storage(
                    "schema response contained an empty table name".into(),
                ));
            }
            let info: SchemaInfo = serde_json::from_value(body.clone())
                .map_err(|e| KitError::Storage(e.to_string()))?;
            let mut id_to_name = HashMap::new();
            let mut name_to_id = HashMap::new();
            let mut primary_key = None;
            for c in &info.columns {
                if c.name.is_empty()
                    || id_to_name.insert(c.id, c.name.clone()).is_some()
                    || name_to_id.insert(c.name.clone(), c.id).is_some()
                    || c.primary_key && primary_key.replace(c.id).is_some()
                {
                    return Err(KitError::Storage(
                        "schema response contained invalid or duplicate columns".into(),
                    ));
                }
            }
            schemas.insert(
                name.clone(),
                RemoteTable {
                    id_to_name,
                    name_to_id,
                    primary_key,
                },
            );
        }
        self.schemas = schemas;
        Ok(())
    }

    /// Create a constraint-bearing table over HTTP (`POST /kit/create_table`)
    /// and refresh the local schema cache. `body` is the full request JSON —
    /// `{name, columns:[{id,name,ty,primary_key,nullable,auto_increment,…}],
    /// constraints:{uniques,…,foreign_keys,…,checks:[{id,name,expr}]}}`.
    /// Returns the assigned table id.
    pub fn create_table(&mut self, body: &Value) -> Result<u64> {
        let operation = "table creation";
        let resp = self
            .client
            .post(self.url("/kit/create_table"))
            .json(body)
            .send()
            .map_err(|error| remote_write_outcome_unknown(operation, error))?;
        let response: CreateTableResponse = decode_write(resp, operation)?;
        if precise_epoch(Some(&response.table_id_text), Some(response.table_id))
            .map_err(|error| remote_write_outcome_unknown(operation, error))?
            != Some(response.table_id)
        {
            return Err(remote_write_outcome_unknown(
                operation,
                "response table ID fields disagreed",
            ));
        }
        self.refresh()
            .map_err(|error| committed_write_followup_error(operation, error))?;
        Ok(response.table_id)
    }

    pub fn table_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    pub fn create_procedure(&self, spec: &ProcedureSpec) -> Result<Value> {
        let operation = "procedure creation";
        let resp = self
            .client
            .post(self.url("/procedures"))
            .json(&json!({ "procedure": spec.json }))
            .send()
            .map_err(|error| remote_write_outcome_unknown(operation, error))?;
        decode_write(resp, operation)
    }

    pub fn replace_procedure(&self, name: &str, spec: &ProcedureSpec) -> Result<Value> {
        let operation = "procedure replacement";
        let resp = self
            .client
            .put(self.url(&format!("/procedures/{name}")))
            .json(&json!({ "procedure": spec.json }))
            .send()
            .map_err(|error| remote_write_outcome_unknown(operation, error))?;
        decode_write(resp, operation)
    }

    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        let operation = "procedure deletion";
        let resp = self
            .client
            .delete(self.url(&format!("/procedures/{name}")))
            .send()
            .map_err(|error| remote_write_outcome_unknown(operation, error))?;
        let _: Value = decode_write(resp, operation)?;
        Ok(())
    }

    pub fn call_procedure(&self, name: &str, args: Map<String, Value>) -> Result<Value> {
        let operation = "procedure call";
        let resp = self
            .client
            .post(self.url(&format!("/kit/procedures/{name}/call")))
            .json(&json!({ "args": args }))
            .send()
            .map_err(|error| remote_write_outcome_unknown(operation, error))?;
        let response = decode_write(resp, operation)?;
        validate_procedure_call_response(response, operation)
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
        let operation = "trigger creation";
        let resp = self
            .client
            .post(self.url("/triggers"))
            .json(&json!({ "trigger": spec.json }))
            .send()
            .map_err(|error| remote_write_outcome_unknown(operation, error))?;
        decode_write(resp, operation)
    }

    pub fn replace_trigger(&self, name: &str, spec: &TriggerSpec) -> Result<Value> {
        let operation = "trigger replacement";
        let resp = self
            .client
            .put(self.url(&format!("/triggers/{name}")))
            .json(&json!({ "trigger": spec.json }))
            .send()
            .map_err(|error| remote_write_outcome_unknown(operation, error))?;
        decode_write(resp, operation)
    }

    pub fn drop_trigger(&self, name: &str) -> Result<()> {
        let operation = "trigger deletion";
        let resp = self
            .client
            .delete(self.url(&format!("/triggers/{name}")))
            .send()
            .map_err(|error| remote_write_outcome_unknown(operation, error))?;
        let _: Value = decode_write(resp, operation)?;
        Ok(())
    }

    pub fn create_virtual_table(
        &mut self,
        table: &VirtualTableSpec,
    ) -> Result<Vec<Map<String, Value>>> {
        let rows = self.sql_rows(&table.create_sql())?;
        self.refresh()
            .map_err(|error| committed_write_followup_error("virtual table creation", error))?;
        Ok(rows)
    }

    pub fn drop_virtual_table(&mut self, name: &str) -> Result<Vec<Map<String, Value>>> {
        let rows = self.sql_rows(&format!(
            "DROP TABLE {}",
            mongreldb_kit_core::quote_ident(name)
        ))?;
        self.refresh()
            .map_err(|error| committed_write_followup_error("virtual table deletion", error))?;
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
        let operation = "history retention update";
        let resp = self
            .client
            .put(self.url("/history/retention"))
            .json(&json!({ "history_retention_epochs": epochs }))
            .send()
            .map_err(|error| remote_write_outcome_unknown(operation, error))?;
        decode_write(resp, operation)
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
        mut options: RemoteSqlOptions,
    ) -> Result<Vec<Map<String, Value>>> {
        // Row decoding happens client-side. Always retain a query ID so a
        // malformed response can be reconciled with the durable server receipt.
        if options.query_id.is_none() {
            options.query_id = Some(mongreldb_query::QueryId::random().map_err(KitError::from)?);
        }
        let query_id = options.query_id;
        let format = options.format;
        let bytes = self.sql_bytes_with_options(sql, options)?;
        let decoded = (|| {
            if format == RemoteSqlFormat::Json {
                return strict_json_from_slice(&bytes).map_err(KitError::from);
            }
            let batches = read_arrow_ipc(&bytes)?;
            let mut rows = Vec::new();
            for batch in &batches {
                rows.extend(batch_to_rows(batch)?);
            }
            Ok(rows)
        })();
        decoded.map_err(|error| match query_id {
            Some(query_id) => self.client_serialization_error(query_id, error.to_string()),
            None => error,
        })
    }

    fn client_serialization_error(
        &self,
        query_id: mongreldb_query::QueryId,
        message: String,
    ) -> KitError {
        self.client_serialization_error_with_status(query_id, message, None)
    }

    fn client_serialization_error_with_status(
        &self,
        query_id: mongreldb_query::QueryId,
        message: String,
        initial_status: Option<RemoteQueryStatus>,
    ) -> KitError {
        let Some(status) = self
            .terminal_status_after_loss_with_status(query_id, initial_status)
            .filter(RemoteQueryStatus::is_recovery_decisive)
        else {
            return KitError::OutcomeUnknown {
                query_id: query_id.to_string(),
                message,
                metadata: boxed_query_metadata(None, None, Some(false), None),
            };
        };
        if status.terminal_error.is_some() {
            if let Some(error) = remote_status_error(&status) {
                return error;
            }
        }
        if status.durable_commit_state().is_none() {
            return KitError::OutcomeUnknown {
                query_id: query_id.to_string(),
                message,
                metadata: remote_status_metadata(&status),
            };
        }
        let committed_statements = max_known(
            status.committed_statements,
            status.outcome.committed_statements,
        );
        let last_commit_epoch = status
            .last_commit_epoch
            .or(status.outcome.last_commit_epoch);
        let completed_statements = max_known(
            status.completed_statements,
            status.outcome.completed_statements,
        )
        .unwrap_or_default();
        let statement_index =
            max_known(status.statement_index, status.outcome.statement_index).unwrap_or_default();
        KitError::SerializationFailed {
            query_id: Some(query_id.to_string()),
            outcome: Box::new(QueryExecutionOutcome {
                committed: status.durably_committed(),
                committed_statements,
                last_commit_epoch,
                first_commit_statement_index: status
                    .first_commit_statement_index
                    .or(status.outcome.first_commit_statement_index),
                last_commit_statement_index: status
                    .last_commit_statement_index
                    .or(status.outcome.last_commit_statement_index),
                completed_statements,
                statement_index,
            }),
            message: message.into_boxed_str(),
            metadata: remote_status_metadata(&status),
        }
    }

    pub fn sql_arrow_with_options(
        &self,
        sql: &str,
        mut options: RemoteSqlOptions,
    ) -> Result<Vec<u8>> {
        if options.query_id.is_none() {
            options.query_id = Some(mongreldb_query::QueryId::random().map_err(KitError::from)?);
        }
        options.format = RemoteSqlFormat::Arrow;
        self.sql_bytes_with_options(sql, options)
    }

    /// Run one read-only `SELECT` and retain its projected JSON rows for
    /// bounded cursor pagination. The cursor is opaque and owner-bound.
    pub fn sql_page(
        &self,
        sql: &str,
        options: RemoteSqlPaginationOptions,
    ) -> Result<RemoteSqlPage> {
        self.require_sql_cancellation()?;
        self.require_sql_pagination()?;
        validate_positive_duration("timeout", options.timeout)?;
        validate_sql_pagination_options(&options)?;
        let query_id = match options.query_id {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random().map_err(KitError::from)?,
        };
        let response = match self
            .client
            .post(self.url("/sql"))
            .json(&json!({
                "sql": sql,
                "format": "json",
                "query_id": query_id.to_string(),
                "timeout_ms": options.timeout.map(duration_millis),
                "max_output_rows": options.max_output_rows,
                "max_output_bytes": options.max_output_bytes,
                "pagination": {
                    "page_size_rows": options.page_size_rows,
                    "projection": options.projection,
                    "max_page_bytes": options.max_page_bytes,
                    "max_page_tokens": options.max_page_tokens,
                },
            }))
            .send()
        {
            Ok(response) => response,
            Err(error) => {
                return Err(self.recover_after_transport_loss(query_id, error.to_string()))
            }
        };
        if !response.status().is_success() {
            return Err(self.map_sql_error_response(response, query_id));
        }
        if let Err(error) = validate_sql_query_id_header(response.headers(), query_id) {
            return Err(self.client_serialization_error(query_id, error));
        }
        strict_response_json(response)
            .map_err(KitError::Storage)
            .and_then(|page| validate_sql_page(page, Some(&options)))
            .map_err(|error| self.client_serialization_error(query_id, error.to_string()))
    }

    pub fn continue_sql_page(
        &self,
        cursor: &str,
        options: RemoteSqlControlOptions,
    ) -> Result<RemoteSqlPage> {
        self.require_sql_pagination()?;
        validate_positive_duration("timeout", options.timeout)?;
        if cursor.is_empty() || cursor.len() > 2_048 {
            return Err(KitError::Validation(
                "SQL continuation cursor must contain 1 to 2048 bytes".into(),
            ));
        }
        let query_id = match options.query_id {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random().map_err(KitError::from)?,
        };
        let response = match self
            .client
            .post(self.url("/sql/continue"))
            .json(&json!({
                "cursor": cursor,
                "operation_id": query_id.to_string(),
                "timeout_ms": options.timeout.map(duration_millis),
            }))
            .send()
        {
            Ok(response) => response,
            Err(error) => {
                return Err(self.recover_after_transport_loss(query_id, error.to_string()))
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let invalid = |message: String| KitError::SerializationFailed {
                query_id: Some(query_id.to_string()),
                outcome: Box::new(QueryExecutionOutcome {
                    committed: false,
                    committed_statements: Some(0),
                    last_commit_epoch: None,
                    first_commit_statement_index: None,
                    last_commit_statement_index: None,
                    completed_statements: 0,
                    statement_index: 0,
                }),
                message: message.into_boxed_str(),
                metadata: boxed_query_metadata(None, None, Some(false), Some("invalid_response")),
            };
            let body = response_text_with_limit(response, MAX_CONTROL_JSON_RESPONSE_BYTES)
                .map_err(invalid)?;
            let value =
                strict_json_from_str::<Value>(&body).map_err(|error| invalid(error.to_string()))?;
            validate_sql_cursor_error_envelope(&value).map_err(invalid)?;
            return Err(map_error_body(status, &body));
        }
        if let Err(error) = validate_sql_query_id_header(response.headers(), query_id) {
            return Err(self.client_serialization_error(query_id, error));
        }
        strict_response_json(response)
            .map_err(KitError::Storage)
            .and_then(|page| validate_sql_page(page, None))
            .map_err(|error| KitError::SerializationFailed {
                query_id: Some(query_id.to_string()),
                outcome: Box::new(QueryExecutionOutcome {
                    committed: false,
                    committed_statements: Some(0),
                    last_commit_epoch: None,
                    first_commit_statement_index: None,
                    last_commit_statement_index: None,
                    completed_statements: 0,
                    statement_index: 0,
                }),
                message: error.to_string().into_boxed_str(),
                metadata: boxed_query_metadata(None, None, Some(false), Some("failed")),
            })
    }

    /// Execute one write statement with an owner-bound, durable at-most-once
    /// key. Replays return the stored receipt instead of executing SQL again.
    pub fn execute_idempotent_sql(
        &self,
        sql: &str,
        options: RemoteIdempotentSqlOptions,
    ) -> Result<RemoteSqlWriteReceipt> {
        self.require_sql_cancellation()?;
        self.require_sql_idempotency()?;
        validate_positive_duration("timeout", options.timeout)?;
        if options.idempotency_key.is_empty()
            || options.idempotency_key.len() > 256
            || options.max_output_rows == Some(0)
            || options.max_output_bytes == Some(0)
        {
            return Err(KitError::Validation(
                "idempotency key and output limits must be non-empty and positive".into(),
            ));
        }
        let query_id = match options.query_id {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random().map_err(KitError::from)?,
        };
        match self.execute_idempotent_sql_once(sql, &options, query_id, None) {
            Ok(receipt) => Ok(receipt),
            Err(IdempotentSqlAttemptError::Final(error)) => Err(error),
            Err(IdempotentSqlAttemptError::Replay(_)) => {
                self.require_fresh_sql_idempotency()?;
                let mut replay_query_id =
                    mongreldb_query::QueryId::random().map_err(KitError::from)?;
                while replay_query_id == query_id {
                    replay_query_id = mongreldb_query::QueryId::random().map_err(KitError::from)?;
                }
                self.execute_idempotent_sql_once(sql, &options, replay_query_id, Some(query_id))
                    .map_err(IdempotentSqlAttemptError::into_inner)
            }
        }
    }

    fn execute_idempotent_sql_once(
        &self,
        sql: &str,
        options: &RemoteIdempotentSqlOptions,
        query_id: mongreldb_query::QueryId,
        expected_original_query_id: Option<mongreldb_query::QueryId>,
    ) -> std::result::Result<RemoteSqlWriteReceipt, IdempotentSqlAttemptError> {
        let response = match self
            .client
            .post(self.url("/sql"))
            .json(&json!({
                "sql": sql,
                "format": "json",
                "query_id": query_id.to_string(),
                "timeout_ms": options.timeout.map(duration_millis),
                "max_output_rows": options.max_output_rows,
                "max_output_bytes": options.max_output_bytes,
                "idempotency_key": &options.idempotency_key,
            }))
            .send()
        {
            Ok(response) => response,
            Err(error) => {
                let initial_status = self.idempotent_status_after_loss(query_id)?;
                return Err(idempotent_loss(
                    self.recover_after_transport_loss_with_status(
                        query_id,
                        error.to_string(),
                        initial_status,
                    ),
                ));
            }
        };
        if !response.status().is_success() {
            return Err(IdempotentSqlAttemptError::Final(
                self.map_sql_error_response(response, query_id),
            ));
        }
        let header_error = validate_sql_query_id_header(response.headers(), query_id).err();
        let expected_query_id = query_id.to_string();
        (|| {
            let receipt: RemoteSqlWriteReceipt =
                strict_control_response_json(response).map_err(KitError::Storage)?;
            let expected_original_query_id =
                expected_original_query_id.map(|query_id| query_id.to_string());
            let receipt = validate_sql_write_receipt(
                receipt,
                &expected_query_id,
                expected_original_query_id.as_deref(),
            )?;
            if let Some(error) = header_error {
                if receipt.committed {
                    return Err(KitError::CommitOutcome {
                        query_id: receipt.query_id.clone(),
                        code: "COMMIT_OUTCOME".into(),
                        outcome: Box::new(QueryExecutionOutcome {
                            committed: true,
                            committed_statements: Some(receipt.committed_statements),
                            last_commit_epoch: receipt.last_commit_epoch,
                            first_commit_statement_index: receipt.first_commit_statement_index,
                            last_commit_statement_index: receipt.last_commit_statement_index,
                            completed_statements: receipt.completed_statements,
                            statement_index: receipt.statement_index,
                        }),
                        message: error.into_boxed_str(),
                        metadata: boxed_query_metadata(
                            receipt.cancel_outcome.map(remote_cancel_outcome_name),
                            receipt.cancellation_reason.as_deref(),
                            Some(false),
                            receipt.server_state.as_deref(),
                        ),
                    });
                }
                return Err(KitError::Storage(error));
            }
            Ok(receipt)
        })()
        .map_err(|error| match error {
            error @ KitError::CommitOutcome { .. } => IdempotentSqlAttemptError::Final(error),
            error => match self.idempotent_status_after_loss(query_id) {
                Ok(initial_status) => idempotent_loss(self.client_serialization_error_with_status(
                    query_id,
                    error.to_string(),
                    initial_status,
                )),
                Err(error) => error,
            },
        })
    }

    fn sql_bytes_with_options(&self, sql: &str, options: RemoteSqlOptions) -> Result<Vec<u8>> {
        validate_positive_duration("timeout", options.timeout)?;
        validate_positive_duration("transport_timeout", options.transport_timeout)?;
        if options.max_output_rows == Some(0) || options.max_output_bytes == Some(0) {
            return Err(KitError::Validation(
                "max_output_rows and max_output_bytes must be positive".into(),
            ));
        }
        self.require_sql_cancellation()?;
        let query_id = match options.query_id {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random().map_err(KitError::from)?,
        };
        let timeout_ms = options.timeout.map(duration_millis);
        let body = json!({
            "sql": sql,
            "format": options.format.as_str(),
            "query_id": query_id.to_string(),
            "timeout_ms": timeout_ms,
            "max_output_rows": options.max_output_rows,
            "max_output_bytes": options.max_output_bytes,
        });
        let mut request = self.client.post(self.url("/sql")).json(&body);
        if let Some(timeout) = options.transport_timeout {
            request = request.timeout(timeout);
        }
        let response = match request.send() {
            Ok(response) => response,
            Err(error) => {
                return Err(self.recover_after_transport_loss(query_id, error.to_string()));
            }
        };
        if !response.status().is_success() {
            return Err(self.map_sql_error_response(response, query_id));
        }
        if let Err(error) = validate_sql_query_id_header(response.headers(), query_id) {
            return Err(self.recover_after_transport_loss(query_id, error));
        }
        let response_limit = sql_response_limit(options.max_output_bytes);
        let bytes = response_bytes_with_limit(response, response_limit);
        match bytes {
            Ok(bytes) => Ok(bytes),
            Err(error) => Err(self.recover_after_transport_loss(query_id, error.to_string())),
        }
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

    pub fn cancel_sql(&self, query_id: mongreldb_query::QueryId) -> Result<RemoteCancelOutcome> {
        self.require_sql_cancellation()?;
        let response = self
            .client
            .post(self.url(&format!("/queries/{query_id}/cancel")))
            .send()
            .map_err(ioe)?;
        let response_status = response.status();
        if !matches!(
            response_status,
            reqwest::StatusCode::OK
                | reqwest::StatusCode::ACCEPTED
                | reqwest::StatusCode::CONFLICT
                | reqwest::StatusCode::NOT_FOUND
        ) {
            return Err(map_error(response));
        }
        let body: Value = strict_control_response_json(response).map_err(KitError::Storage)?;
        validate_cancel_response(&body, query_id, response_status)
    }

    pub fn sql_query_status(
        &self,
        query_id: mongreldb_query::QueryId,
    ) -> Result<Option<RemoteQueryStatus>> {
        let capabilities = self.require_sql_cancellation()?;
        if !capabilities.query_status {
            return Err(KitError::CapabilityUnsupported(
                "server does not advertise SQL query status".into(),
            ));
        }
        let response = self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .send()
            .map_err(ioe)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            let body: Value = strict_control_response_json(response)
                .map_err(|error| invalid_query_status_error(query_id, error))?;
            validate_query_not_found_response(&body, query_id)
                .map_err(|error| invalid_query_status_error(query_id, error))?;
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(map_error(response));
        }
        let status = strict_control_response_json(response)
            .map_err(|error| invalid_query_status_error(query_id, error))?;
        validate_query_status(status, query_id)
            .map(Some)
            .map_err(|error| invalid_query_status_error(query_id, error))
    }

    fn recover_after_transport_loss(
        &self,
        query_id: mongreldb_query::QueryId,
        message: String,
    ) -> KitError {
        self.recover_after_transport_loss_with_status(query_id, message, None)
    }

    fn recover_after_transport_loss_with_status(
        &self,
        query_id: mongreldb_query::QueryId,
        message: String,
        initial_status: Option<RemoteQueryStatus>,
    ) -> KitError {
        let status = self.terminal_status_after_loss_with_status(query_id, initial_status);
        if let Some(error) = status.as_ref().and_then(remote_status_error) {
            return error;
        }
        if let Some(status) = status
            .as_ref()
            .filter(|status| status.is_recovery_decisive())
        {
            if status.state == "completed" {
                return serialization_error_from_status(status, message);
            }
        }
        KitError::OutcomeUnknown {
            query_id: query_id.to_string(),
            message,
            metadata: status.as_ref().map_or_else(
                || boxed_query_metadata(None, None, Some(false), None),
                remote_status_metadata,
            ),
        }
    }

    fn terminal_status_after_loss_with_status(
        &self,
        query_id: mongreldb_query::QueryId,
        initial_status: Option<RemoteQueryStatus>,
    ) -> Option<RemoteQueryStatus> {
        let deadline = Instant::now() + SQL_RECOVERY_WINDOW;
        let mut status =
            initial_status.or_else(|| self.query_status_for_recovery(query_id, deadline));
        if status
            .as_ref()
            .is_some_and(RemoteQueryStatus::is_recovery_decisive)
        {
            return status;
        }
        self.cancel_sql_for_recovery(query_id, deadline);
        while Instant::now() < deadline {
            std::thread::sleep(
                SQL_RECOVERY_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
            );
            status = self
                .query_status_for_recovery(query_id, deadline)
                .or(status);
            if status
                .as_ref()
                .is_some_and(RemoteQueryStatus::is_recovery_decisive)
            {
                break;
            }
        }
        status
    }

    fn idempotent_status_after_loss(
        &self,
        query_id: mongreldb_query::QueryId,
    ) -> std::result::Result<Option<RemoteQueryStatus>, IdempotentSqlAttemptError> {
        let response = match self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .timeout(SQL_RECOVERY_REQUEST_TIMEOUT)
            .send()
        {
            Ok(response) => response,
            Err(_) => return Ok(None),
        };
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            let body: Value = strict_control_response_json(response).map_err(|error| {
                IdempotentSqlAttemptError::Final(invalid_query_status_error(query_id, error))
            })?;
            validate_query_not_found_response(&body, query_id).map_err(|error| {
                IdempotentSqlAttemptError::Final(invalid_query_status_error(query_id, error))
            })?;
            return Err(IdempotentSqlAttemptError::Replay(
                KitError::OutcomeUnknown {
                    query_id: query_id.to_string(),
                    message: format!("query {query_id} is not retained"),
                    metadata: boxed_query_metadata(None, None, Some(false), None),
                },
            ));
        }
        if !response.status().is_success() {
            return Ok(None);
        }
        let status = strict_control_response_json(response).map_err(|error| {
            IdempotentSqlAttemptError::Final(invalid_query_status_error(query_id, error))
        })?;
        validate_query_status(status, query_id)
            .map(Some)
            .map_err(|error| {
                IdempotentSqlAttemptError::Final(invalid_query_status_error(query_id, error))
            })
    }

    fn recovery_timeout(deadline: Instant) -> Option<Duration> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        (!remaining.is_zero()).then(|| remaining.min(SQL_RECOVERY_REQUEST_TIMEOUT))
    }

    fn query_status_for_recovery(
        &self,
        query_id: mongreldb_query::QueryId,
        deadline: Instant,
    ) -> Option<RemoteQueryStatus> {
        let timeout = Self::recovery_timeout(deadline)?;
        let response = self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .timeout(timeout)
            .send()
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        validate_query_status(strict_control_response_json(response).ok()?, query_id).ok()
    }

    fn cancel_sql_for_recovery(&self, query_id: mongreldb_query::QueryId, deadline: Instant) {
        let Some(timeout) = Self::recovery_timeout(deadline) else {
            return;
        };
        let response = self
            .client
            .post(self.url(&format!("/queries/{query_id}/cancel")))
            .timeout(timeout)
            .send();
        if let Ok(response) = response {
            let status = response.status();
            if matches!(
                status,
                reqwest::StatusCode::OK
                    | reqwest::StatusCode::ACCEPTED
                    | reqwest::StatusCode::CONFLICT
                    | reqwest::StatusCode::NOT_FOUND
            ) {
                let _ = strict_control_response_json::<Value>(response)
                    .ok()
                    .and_then(|body| validate_cancel_response(&body, query_id, status).ok());
            }
        }
    }

    fn map_sql_error_response(
        &self,
        response: reqwest::blocking::Response,
        query_id: mongreldb_query::QueryId,
    ) -> KitError {
        let status = response.status();
        match response_text_with_limit(response, MAX_CONTROL_JSON_RESPONSE_BYTES) {
            Ok(body)
                if strict_json_from_str::<Value>(&body)
                    .ok()
                    .is_some_and(|value| validate_sql_error_envelope(&value, query_id).is_ok()) =>
            {
                map_error_body(status, &body)
            }
            Ok(_) => self.recover_after_transport_loss(
                query_id,
                format!("HTTP {status} SQL error response was malformed"),
            ),
            Err(error) => self.recover_after_transport_loss(query_id, error.to_string()),
        }
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
        #[serde(deny_unknown_fields)]
        struct Resp {
            truncated: bool,
            rows: Vec<RawRow>,
            #[serde(default)]
            next_cursor: Option<String>,
        }
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
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
        if parsed
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 2_048)
            || parsed.truncated != parsed.next_cursor.is_some()
        {
            return Err(KitError::Integrity(
                "/kit/query returned invalid continuation metadata".into(),
            ));
        }
        let info = self.require_table(table)?;
        let mut out = Vec::with_capacity(parsed.rows.len());
        for r in parsed.rows {
            out.push(RemoteQueryRow {
                row_id: r.row_id,
                values: decode_cells(&r.cells, &info.id_to_name)?,
            });
        }
        Ok(out)
    }
}

fn value_optional_usize(value: &Value) -> Option<Option<usize>> {
    if value.is_null() {
        Some(None)
    } else {
        value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .map(Some)
    }
}

fn value_optional_bool(value: &Value) -> Option<Option<bool>> {
    if value.is_null() {
        Some(None)
    } else {
        value.as_bool().map(Some)
    }
}

fn reject_unknown_fields(
    object: &Map<String, Value>,
    allowed: &[&str],
    scope: &str,
) -> std::result::Result<(), String> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!("{scope} contains unknown field {key:?}"));
    }
    Ok(())
}

fn value_exact_epoch(value: &Map<String, Value>) -> std::result::Result<Option<u64>, String> {
    let numeric = value
        .get("last_commit_epoch")
        .ok_or_else(|| "last_commit_epoch is missing".to_owned())?;
    let numeric = if numeric.is_null() {
        None
    } else {
        Some(
            numeric
                .as_u64()
                .ok_or_else(|| "last_commit_epoch is invalid".to_owned())?,
        )
    };
    let text = value
        .get("last_commit_epoch_text")
        .ok_or_else(|| "last_commit_epoch_text is missing".to_owned())?;
    let text = if text.is_null() {
        None
    } else {
        Some(
            text.as_str()
                .ok_or_else(|| "last_commit_epoch_text is invalid".to_owned())?,
        )
    };
    let exact = validated_status_epoch(text, numeric)?;
    if numeric.is_some_and(|numeric| Some(numeric) != exact) {
        return Err("numeric and exact commit epochs disagree".into());
    }
    Ok(exact)
}

fn validate_sql_error_envelope(
    value: &Value,
    expected_query_id: mongreldb_query::QueryId,
) -> std::result::Result<(), String> {
    let top = value
        .as_object()
        .ok_or_else(|| "query error is not an object".to_owned())?;
    reject_unknown_fields(
        top,
        &[
            "query_id",
            "status",
            "terminal_state",
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "cancel_outcome",
            "cancellation_reason",
            "retryable",
            "server_state",
            "outcome",
            "error",
            "max_rows",
            "max_bytes",
        ],
        "query error",
    )?;
    let outcome = top
        .get("outcome")
        .and_then(Value::as_object)
        .ok_or_else(|| "query error outcome is missing".to_owned())?;
    let error = top
        .get("error")
        .and_then(Value::as_object)
        .ok_or_else(|| "query error detail is missing".to_owned())?;
    reject_unknown_fields(
        outcome,
        &[
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "serialization",
        ],
        "query error outcome",
    )?;
    reject_unknown_fields(
        error,
        &[
            "code",
            "message",
            "query_id",
            "committed",
            "retryable",
            "max_rows",
            "max_bytes",
        ],
        "query error detail",
    )?;
    let expected = expected_query_id.to_string();
    if top.get("query_id").and_then(Value::as_str) != Some(expected.as_str())
        || error.get("query_id").and_then(Value::as_str) != Some(expected.as_str())
    {
        return Err("query error query_id does not match the request".into());
    }
    let status = top
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| "query error status is missing".to_owned())?;
    if top.get("terminal_state").and_then(Value::as_str) != Some(status) {
        return Err("query error terminal_state disagrees with status".into());
    }
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .filter(|code| !code.trim().is_empty())
        .ok_or_else(|| "query error code is missing".to_owned())?;
    if !matches!(
        outcome.get("serialization").and_then(Value::as_str),
        Some("not_started" | "in_progress" | "succeeded" | "failed" | "unknown")
    ) {
        return Err("query error outcome serialization is invalid".into());
    }
    if error
        .get("message")
        .and_then(Value::as_str)
        .is_none_or(|message| message.trim().is_empty())
    {
        return Err("query error message is missing".into());
    }
    let committed = value_optional_bool(
        top.get("committed")
            .ok_or_else(|| "query error committed is missing".to_owned())?,
    )
    .ok_or_else(|| "query error committed is invalid".to_owned())?;
    let outcome_committed = value_optional_bool(
        outcome
            .get("committed")
            .ok_or_else(|| "query error outcome committed is missing".to_owned())?,
    )
    .ok_or_else(|| "query error outcome committed is invalid".to_owned())?;
    let error_committed = value_optional_bool(
        error
            .get("committed")
            .ok_or_else(|| "query error detail committed is missing".to_owned())?,
    )
    .ok_or_else(|| "query error detail committed is invalid".to_owned())?;
    let field = |name: &str| -> std::result::Result<Option<usize>, String> {
        value_optional_usize(
            top.get(name)
                .ok_or_else(|| format!("query error {name} is missing"))?,
        )
        .ok_or_else(|| format!("query error {name} is invalid"))
    };
    let outcome_field = |name: &str| -> std::result::Result<Option<usize>, String> {
        value_optional_usize(
            outcome
                .get(name)
                .ok_or_else(|| format!("query error outcome {name} is missing"))?,
        )
        .ok_or_else(|| format!("query error outcome {name} is invalid"))
    };
    let committed_statements = field("committed_statements")?;
    let first = field("first_commit_statement_index")?;
    let last = field("last_commit_statement_index")?;
    let completed = field("completed_statements")?;
    let statement = field("statement_index")?;
    if committed != outcome_committed
        || committed != error_committed
        || committed_statements != outcome_field("committed_statements")?
        || first != outcome_field("first_commit_statement_index")?
        || last != outcome_field("last_commit_statement_index")?
        || completed != outcome_field("completed_statements")?
        || statement != outcome_field("statement_index")?
    {
        return Err("query error top-level, outcome, and error fields disagree".into());
    }
    for name in ["max_rows", "max_bytes"] {
        let limit = |object: &Map<String, Value>| -> std::result::Result<Option<usize>, String> {
            match object.get(name) {
                None | Some(Value::Null) => Ok(None),
                Some(value) => value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .filter(|value| *value > 0)
                    .map(Some)
                    .ok_or_else(|| format!("query error {name} is invalid")),
            }
        };
        let top_limit = limit(top)?;
        let error_limit = limit(error)?;
        if top_limit.is_some() && error_limit.is_some() && top_limit != error_limit {
            return Err(format!("query error {name} fields disagree"));
        }
    }
    let top_epoch = value_exact_epoch(top)?;
    let outcome_epoch = value_exact_epoch(outcome)?;
    if top_epoch != outcome_epoch {
        return Err("query error top-level and outcome commit epochs disagree".into());
    }
    let retryable = top
        .get("retryable")
        .and_then(Value::as_bool)
        .ok_or_else(|| "query error retryable is missing".to_owned())?;
    if error.get("retryable").and_then(Value::as_bool) != Some(retryable)
        || retryable
            != matches!(
                code,
                "QUERY_REGISTRY_FULL" | "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"
            )
    {
        return Err("query error retryable fields disagree".into());
    }
    let outcome_unknown = code == "QUERY_OUTCOME_UNKNOWN";
    match committed {
        Some(true) => {
            if outcome_unknown
                || committed_statements == Some(0)
                || committed_statements.is_none()
                || top_epoch.is_none()
                || top.get("last_commit_epoch_text").is_none_or(Value::is_null)
                || outcome
                    .get("last_commit_epoch_text")
                    .is_none_or(Value::is_null)
                || first.is_none()
                || last.is_none()
                || completed.is_none()
                || statement.is_none()
                || !matches!(
                    status,
                    "committed"
                        | "committed_with_error"
                        | "partially_committed"
                        | "cancelled_after_commit"
                        | "deadline_after_commit"
                )
            {
                return Err("committed query error has invalid durable metadata".into());
            }
        }
        Some(false) => {
            if outcome_unknown
                || committed_statements != Some(0)
                || top_epoch.is_some()
                || first.is_some()
                || last.is_some()
                || completed.is_none()
                || statement.is_none()
                || !matches!(
                    status,
                    "failed_before_commit"
                        | "cancelled_before_commit"
                        | "deadline_before_commit"
                        | "cancelled_before_start"
                )
            {
                return Err("non-committed query error has invalid durable metadata".into());
            }
        }
        None => {
            if !outcome_unknown
                || status != "outcome_unknown"
                || committed_statements.is_some()
                || top_epoch.is_some()
                || first.is_some()
                || last.is_some()
                || completed.is_some()
                || statement.is_some()
                || retryable
            {
                return Err("unknown query error contains contradictory metadata".into());
            }
        }
    }
    if let (Some(first), Some(last), Some(committed), Some(statement)) =
        (first, last, committed_statements, statement)
    {
        if first > last
            || committed > last.saturating_sub(first).saturating_add(1)
            || last > statement
        {
            return Err("query error commit statement indexes are invalid".into());
        }
    }
    if let (Some(completed), Some(statement)) = (completed, statement) {
        if statement > completed || completed > statement.saturating_add(1) {
            return Err("query error statement index and completed count disagree".into());
        }
    }
    let code_matches = match code {
        "QUERY_OUTCOME_UNKNOWN" => status == "outcome_unknown",
        "QUERY_CANCELLED_AFTER_COMMIT" => {
            status == "cancelled_after_commit" && committed == Some(true)
        }
        "DEADLINE_AFTER_COMMIT" => status == "deadline_after_commit" && committed == Some(true),
        "QUERY_CANCELLED" => {
            matches!(status, "cancelled_before_commit" | "cancelled_before_start")
        }
        "DEADLINE_EXCEEDED" => status == "deadline_before_commit",
        "COMMIT_OUTCOME" | "SERIALIZATION_FAILED_AFTER_COMMIT" => committed == Some(true),
        _ => true,
    };
    let status_matches_code = match status {
        "outcome_unknown" => code == "QUERY_OUTCOME_UNKNOWN",
        "cancelled_after_commit" => code == "QUERY_CANCELLED_AFTER_COMMIT",
        "deadline_after_commit" => code == "DEADLINE_AFTER_COMMIT",
        "cancelled_before_commit" | "cancelled_before_start" => code == "QUERY_CANCELLED",
        "deadline_before_commit" => code == "DEADLINE_EXCEEDED",
        _ => true,
    };
    if !code_matches || !status_matches_code {
        return Err("query error code and status disagree".into());
    }
    Ok(())
}

/// A row returned by [`RemoteDatabase::query`]: its physical row id plus the
/// projected cells keyed by column name.
#[derive(Debug, Clone)]
pub struct RemoteQueryRow {
    pub row_id: String,
    pub values: Map<String, Value>,
}

fn decode_cells(cells: &[Value], id_to_name: &HashMap<u16, String>) -> Result<Map<String, Value>> {
    if !cells.len().is_multiple_of(2) {
        return Err(KitError::Integrity(
            "/kit/query returned an odd-length row".into(),
        ));
    }
    let mut out = Map::new();
    for pair in cells.chunks_exact(2) {
        let id = pair[0]
            .as_u64()
            .and_then(|id| u16::try_from(id).ok())
            .ok_or_else(|| {
                KitError::Integrity("/kit/query returned an invalid column id".into())
            })?;
        let name = id_to_name.get(&id).ok_or_else(|| {
            KitError::Integrity(format!("/kit/query returned unknown column id {id}"))
        })?;
        if out.insert(name.clone(), pair[1].clone()).is_some() {
            return Err(KitError::Integrity(format!(
                "/kit/query returned duplicate column id {id}"
            )));
        }
    }
    Ok(out)
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
#[serde(deny_unknown_fields)]
struct TxnResponse {
    status: String,
    epoch: u64,
    epoch_text: String,
    results: Vec<OpResult>,
}

fn deserialize_required_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
enum OpResult {
    Put {
        #[serde(deserialize_with = "deserialize_required_option")]
        row_id: Option<String>,
        #[serde(deserialize_with = "deserialize_required_option")]
        auto_inc: Option<i64>,
        #[serde(default)]
        row: Option<Vec<Value>>,
    },
    Upsert {
        action: String,
        #[serde(rename = "auto_inc")]
        #[serde(deserialize_with = "deserialize_required_option")]
        _auto_inc: Option<i64>,
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

fn invalid_txn_success(message: impl Into<String>) -> KitError {
    KitError::OutcomeUnknown {
        query_id: "unknown".into(),
        message: message.into(),
        metadata: boxed_query_metadata(None, None, Some(false), Some("invalid_outcome")),
    }
}

fn committed_txn_decode_error(epoch: u64, message: impl Into<String>) -> KitError {
    KitError::CommitOutcome {
        query_id: "unknown".into(),
        code: "COMMIT_OUTCOME".into(),
        outcome: Box::new(QueryExecutionOutcome {
            committed: true,
            committed_statements: None,
            last_commit_epoch: Some(epoch),
            first_commit_statement_index: None,
            last_commit_statement_index: None,
            completed_statements: 0,
            statement_index: 0,
        }),
        message: message.into().into_boxed_str(),
        metadata: boxed_query_metadata(None, None, Some(false), Some("invalid_response")),
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
            return Err(map_txn_error(resp));
        }
        let txn: TxnResponse = strict_response_json(resp).map_err(|error| {
            invalid_txn_success(format!("invalid /kit/txn success response: {error}"))
        })?;
        let exact_epoch = precise_epoch(Some(&txn.epoch_text), Some(txn.epoch))
            .map_err(|error| invalid_txn_success(error.to_string()))?;
        if txn.status != "committed" || exact_epoch != Some(txn.epoch) {
            return Err(invalid_txn_success(
                "invalid /kit/txn committed response metadata",
            ));
        }
        if txn.results.len() != self.ops.len() {
            return Err(committed_txn_decode_error(
                txn.epoch,
                "committed /kit/txn result count did not match the request",
            ));
        }
        let mut results = Vec::with_capacity(txn.results.len());
        for (op, result) in self.ops.iter().zip(txn.results) {
            match (op, result) {
                (
                    TxnOp::Put {
                        table, returning, ..
                    },
                    OpResult::Put {
                        row_id,
                        auto_inc,
                        row,
                    },
                ) if row_id.is_none() && row.is_some() == *returning => {
                    results.push(RemoteOpResult::Put {
                        auto_inc,
                        row: decode_returning_row(self.db, table, row.as_deref()).map_err(
                            |error| committed_txn_decode_error(txn.epoch, error.to_string()),
                        )?,
                    });
                }
                (
                    TxnOp::Upsert {
                        table, returning, ..
                    },
                    OpResult::Upsert { action, row, .. },
                ) if matches!(action.as_str(), "inserted" | "updated" | "unchanged")
                    && row.is_some() == *returning =>
                {
                    results.push(RemoteOpResult::Upsert {
                        action,
                        row: decode_returning_row(self.db, table, row.as_deref()).map_err(
                            |error| committed_txn_decode_error(txn.epoch, error.to_string()),
                        )?,
                    });
                }
                (TxnOp::DeleteByPk { .. }, OpResult::Deleted) => {
                    results.push(RemoteOpResult::Deleted)
                }
                (TxnOp::DeleteByPk { .. }, OpResult::NotFound) => {
                    results.push(RemoteOpResult::NotFound)
                }
                _ => {
                    return Err(committed_txn_decode_error(
                        txn.epoch,
                        "/kit/txn result does not match its requested operation",
                    ))
                }
            }
        }
        Ok(RemoteBatch {
            epoch: txn.epoch,
            results,
        })
    }
}

/// Decode a `[col_id, val, col_id, val, …]` post-image into a name-keyed map.
fn decode_returning_row(
    db: &RemoteDatabase,
    table: &str,
    row: Option<&[Value]>,
) -> Result<Option<Map<String, Value>>> {
    let Some(row) = row else {
        return Ok(None);
    };
    let t = db.require_table(table)?;
    if row.len() % 2 != 0 {
        return Err(KitError::Integrity(
            "/kit/txn returned an odd-length row".into(),
        ));
    }
    let mut out = Map::new();
    for pair in row.chunks_exact(2) {
        let id = pair[0]
            .as_u64()
            .and_then(|id| u16::try_from(id).ok())
            .ok_or_else(|| KitError::Integrity("/kit/txn returned an invalid column id".into()))?;
        let name = t.id_to_name.get(&id).ok_or_else(|| {
            KitError::Integrity(format!("/kit/txn returned unknown column id {id}"))
        })?;
        if out.insert(name.clone(), pair[1].clone()).is_some() {
            return Err(KitError::Integrity(format!(
                "/kit/txn returned duplicate column id {id}"
            )));
        }
    }
    Ok(Some(out))
}

fn remote_status_error(status: &RemoteQueryStatus) -> Option<KitError> {
    if !status.is_recovery_decisive() {
        return None;
    }
    let committed_state = status.durable_commit_state();
    if status.state == "completed"
        && status.terminal_error.is_none()
        && committed_state == Some(false)
    {
        return None;
    }
    let code = status.terminal_error.as_ref().map_or_else(
        || {
            if committed_state == Some(true) {
                "COMMIT_OUTCOME"
            } else {
                "QUERY_FAILED"
            }
        },
        |error| error.code.as_str(),
    );
    let message = format!("server reported terminal query status {}", status.status);
    if code == "QUERY_OUTCOME_UNKNOWN" || committed_state.is_none() {
        return Some(KitError::OutcomeUnknown {
            query_id: status.query_id.clone(),
            message,
            metadata: remote_status_metadata(status),
        });
    }
    let committed = status.durably_committed();
    let committed_statements = max_known(
        status.committed_statements,
        status.outcome.committed_statements,
    );
    let last_commit_epoch = status
        .last_commit_epoch
        .or(status.outcome.last_commit_epoch);
    let first_commit_statement_index = status
        .first_commit_statement_index
        .or(status.outcome.first_commit_statement_index);
    let last_commit_statement_index = status
        .last_commit_statement_index
        .or(status.outcome.last_commit_statement_index);
    let completed_statements = max_known(
        status.completed_statements,
        status.outcome.completed_statements,
    )
    .unwrap_or_default();
    let statement_index =
        max_known(status.statement_index, status.outcome.statement_index).unwrap_or_default();
    let outcome = QueryExecutionOutcome {
        committed,
        committed_statements,
        last_commit_epoch,
        first_commit_statement_index,
        last_commit_statement_index,
        completed_statements,
        statement_index,
    };
    match code {
        "QUERY_CANCELLED" | "QUERY_CANCELLED_AFTER_COMMIT" => Some(KitError::Cancelled {
            query_id: status.query_id.clone().into_boxed_str(),
            reason: status
                .cancellation_reason
                .clone()
                .unwrap_or_else(|| "none".into())
                .into_boxed_str(),
            outcome: Box::new(outcome),
            metadata: remote_status_metadata(status),
        }),
        "DEADLINE_EXCEEDED" | "DEADLINE_AFTER_COMMIT" => Some(KitError::DeadlineExceeded {
            query_id: status.query_id.clone().into_boxed_str(),
            timeout_ms: None,
            outcome: Box::new(outcome),
            metadata: remote_status_metadata(status),
        }),
        "RESULT_LIMIT_EXCEEDED" => Some(KitError::ResultLimitExceeded {
            query_id: Some(status.query_id.clone().into_boxed_str()),
            max_rows: None,
            max_bytes: None,
            outcome: Box::new(outcome),
            message: message.into_boxed_str(),
            metadata: remote_status_metadata(status),
        }),
        "SERIALIZATION_FAILED" | "SERIALIZATION_FAILED_AFTER_COMMIT" => {
            Some(KitError::SerializationFailed {
                query_id: Some(status.query_id.clone()),
                outcome: Box::new(outcome),
                message: message.into_boxed_str(),
                metadata: remote_status_metadata(status),
            })
        }
        "TRANSACTION_ABORTED" => Some(KitError::TransactionAborted {
            query_id: Some(status.query_id.clone()),
            message,
            metadata: remote_status_metadata(status),
        }),
        _ if committed => Some(KitError::CommitOutcome {
            query_id: status.query_id.clone(),
            code: code.into(),
            outcome: Box::new(outcome),
            message: message.into_boxed_str(),
            metadata: remote_status_metadata(status),
        }),
        _ => Some(KitError::QueryFailed {
            query_id: status.query_id.clone(),
            code: code.into(),
            outcome: Box::new(outcome),
            message: message.into_boxed_str(),
            metadata: remote_status_metadata(status),
        }),
    }
}

fn serialization_error_from_status(status: &RemoteQueryStatus, message: String) -> KitError {
    if status.durable_commit_state().is_none() {
        return KitError::OutcomeUnknown {
            query_id: status.query_id.clone(),
            message,
            metadata: remote_status_metadata(status),
        };
    }
    KitError::SerializationFailed {
        query_id: Some(status.query_id.clone()),
        outcome: Box::new(QueryExecutionOutcome {
            committed: status.durably_committed(),
            committed_statements: max_known(
                status.committed_statements,
                status.outcome.committed_statements,
            ),
            last_commit_epoch: status
                .last_commit_epoch
                .or(status.outcome.last_commit_epoch),
            first_commit_statement_index: status
                .first_commit_statement_index
                .or(status.outcome.first_commit_statement_index),
            last_commit_statement_index: status
                .last_commit_statement_index
                .or(status.outcome.last_commit_statement_index),
            completed_statements: max_known(
                status.completed_statements,
                status.outcome.completed_statements,
            )
            .unwrap_or_default(),
            statement_index: max_known(status.statement_index, status.outcome.statement_index)
                .unwrap_or_default(),
        }),
        message: message.into_boxed_str(),
        metadata: remote_status_metadata(status),
    }
}

fn map_error(resp: reqwest::blocking::Response) -> KitError {
    let status = resp.status();
    let body = match response_text_with_limit(resp, MAX_CONTROL_JSON_RESPONSE_BYTES) {
        Ok(body) => body,
        Err(error) => {
            return KitError::Storage(format!(
                "failed to read HTTP {status} error response: {error}"
            ))
        }
    };
    map_error_body(status, &body)
}

fn map_txn_error(resp: reqwest::blocking::Response) -> KitError {
    let status = resp.status();
    let body = match response_text_with_limit(resp, MAX_CONTROL_JSON_RESPONSE_BYTES) {
        Ok(body) => body,
        Err(error) => {
            return KitError::OutcomeUnknown {
                query_id: "unknown".into(),
                message: format!("failed to read HTTP {status} transaction response: {error}"),
                metadata: boxed_query_metadata(None, None, Some(false), Some("invalid_outcome")),
            }
        }
    };
    match strict_json_from_str::<Value>(&body)
        .map_err(|error| error.to_string())
        .and_then(|value| {
            validate_txn_error_envelope(&value)?;
            Ok(value)
        }) {
        Ok(_) => map_error_body(status, &body),
        Err(error) => KitError::OutcomeUnknown {
            query_id: "unknown".into(),
            message: format!("invalid /kit/txn error response: {error}"),
            metadata: boxed_query_metadata(None, None, Some(false), Some("invalid_outcome")),
        },
    }
}

fn exact_object_fields(
    value: &Value,
    allowed: &[&str],
    required: &[&str],
    name: &str,
) -> std::result::Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{name} must be an object"))?;
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(format!("{name} contains unknown field {field:?}"));
    }
    if let Some(field) = required.iter().find(|field| !object.contains_key(**field)) {
        return Err(format!("{name} lacks required field {field:?}"));
    }
    Ok(())
}

fn validate_txn_error_envelope(value: &Value) -> std::result::Result<(), String> {
    let status = value["status"]
        .as_str()
        .ok_or_else(|| "transaction response lacks status".to_owned())?;
    let error = &value["error"];
    exact_object_fields(
        error,
        &["code", "message", "op_index"],
        &["code", "message"],
        "transaction error",
    )?;
    if error["code"].as_str().is_none_or(str::is_empty)
        || error["message"].as_str().is_none()
        || error
            .get("op_index")
            .is_some_and(|index| index.as_u64().is_none())
    {
        return Err("transaction error fields are invalid".into());
    }
    match status {
        "aborted" if value.get("committed").is_none() => exact_object_fields(
            value,
            &["status", "error"],
            &["status", "error"],
            "transaction error response",
        ),
        "aborted" => {
            exact_object_fields(
                value,
                &["status", "committed", "retryable", "error"],
                &["status", "committed", "retryable", "error"],
                "transaction error response",
            )?;
            if value["committed"].as_bool() != Some(false) || value["retryable"].as_bool().is_none()
            {
                return Err("aborted transaction metadata is invalid".into());
            }
            Ok(())
        }
        "committed" => {
            exact_object_fields(
                value,
                &[
                    "status",
                    "committed",
                    "epoch",
                    "epoch_text",
                    "results",
                    "retryable",
                    "error",
                ],
                &[
                    "status",
                    "committed",
                    "epoch",
                    "epoch_text",
                    "retryable",
                    "error",
                ],
                "transaction error response",
            )?;
            if value["committed"].as_bool() != Some(true)
                || value["retryable"].as_bool() != Some(false)
                || error["code"].as_str() != Some("COMMIT_OUTCOME")
                || precise_epoch(value["epoch_text"].as_str(), value["epoch"].as_u64())
                    .map_err(|error| error.to_string())?
                    != value["epoch"].as_u64()
                || value
                    .get("results")
                    .is_some_and(|results| !results.is_array())
            {
                return Err("committed transaction metadata is invalid".into());
            }
            Ok(())
        }
        "outcome_unknown" => {
            exact_object_fields(
                value,
                &[
                    "status",
                    "committed",
                    "epoch",
                    "epoch_text",
                    "retryable",
                    "error",
                ],
                &[
                    "status",
                    "committed",
                    "epoch",
                    "epoch_text",
                    "retryable",
                    "error",
                ],
                "transaction error response",
            )?;
            let epoch = precise_epoch(value["epoch_text"].as_str(), value["epoch"].as_u64())
                .map_err(|error| error.to_string())?;
            if !value["committed"].is_null()
                || value["retryable"].as_bool() != Some(false)
                || error["code"].as_str() != Some("QUERY_OUTCOME_UNKNOWN")
                || epoch != value["epoch"].as_u64()
            {
                return Err("unknown transaction metadata is invalid".into());
            }
            Ok(())
        }
        _ => Err("transaction response status is invalid".into()),
    }
}

fn exact_optional_bool(
    values: &[Option<&Value>],
    field: &str,
) -> std::result::Result<Option<bool>, String> {
    let mut exact = None;
    for value in values.iter().flatten() {
        if value.is_null() {
            continue;
        }
        let value = value
            .as_bool()
            .ok_or_else(|| format!("remote {field} is not a boolean"))?;
        if exact.is_some_and(|exact| exact != value) {
            return Err(format!("remote {field} fields disagree"));
        }
        exact = Some(value);
    }
    Ok(exact)
}

fn exact_optional_epoch(
    numbers: &[Option<&Value>],
    texts: &[Option<&Value>],
) -> std::result::Result<Option<u64>, String> {
    let mut exact = None;
    for value in numbers.iter().flatten() {
        if value.is_null() {
            continue;
        }
        let value = value
            .as_u64()
            .ok_or_else(|| "remote commit epoch is not an unsigned integer".to_owned())?;
        if exact.is_some_and(|exact| exact != value) {
            return Err("remote commit epoch fields disagree".into());
        }
        exact = Some(value);
    }
    for value in texts.iter().flatten() {
        if value.is_null() {
            continue;
        }
        let text = value
            .as_str()
            .ok_or_else(|| "remote commit epoch text is not a string".to_owned())?;
        let value = precise_epoch(Some(text), None)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "remote commit epoch text is missing".to_owned())?;
        if exact.is_some_and(|exact| exact != value) {
            return Err("remote commit epoch fields disagree".into());
        }
        exact = Some(value);
    }
    Ok(exact)
}

fn exact_optional_usize(
    values: &[Option<&Value>],
    field: &str,
) -> std::result::Result<Option<usize>, String> {
    let mut exact = None;
    for value in values.iter().flatten() {
        if value.is_null() {
            continue;
        }
        let value = value
            .as_u64()
            .ok_or_else(|| format!("remote {field} is not an unsigned integer"))?;
        let value = usize::try_from(value)
            .map_err(|_| format!("remote {field} exceeds the platform usize range"))?;
        if exact.is_some_and(|exact| exact != value) {
            return Err(format!("remote {field} fields disagree"));
        }
        exact = Some(value);
    }
    Ok(exact)
}

fn map_error_body(status: reqwest::StatusCode, body: &str) -> KitError {
    if let Ok(v) = strict_json_from_str::<Value>(body) {
        if matches!(
            v["status"].as_str(),
            Some("aborted" | "committed" | "outcome_unknown")
        ) {
            if let Err(message) = validate_txn_error_envelope(&v) {
                return KitError::OutcomeUnknown {
                    query_id: "unknown".into(),
                    message: format!("invalid durable error response: {message}"),
                    metadata: boxed_query_metadata(
                        None,
                        None,
                        Some(false),
                        Some("invalid_outcome"),
                    ),
                };
            }
        }
        let code = v["error"]["code"].as_str().unwrap_or("");
        let msg = v["error"]["message"]
            .as_str()
            .unwrap_or("remote transaction rejected")
            .to_string();
        let top_query_id = v["query_id"].as_str();
        let error_query_id = v["error"]["query_id"].as_str();
        let query_id = match (top_query_id, error_query_id) {
            (Some(top), Some(nested)) if top != nested => {
                return KitError::OutcomeUnknown {
                    query_id: "unknown".into(),
                    message: "remote query_id fields disagree".into(),
                    metadata: boxed_query_metadata(
                        None,
                        None,
                        Some(false),
                        Some("invalid_outcome"),
                    ),
                }
            }
            (Some(query_id), _) | (_, Some(query_id)) => query_id.to_string(),
            (None, None) => "unknown".to_string(),
        };
        let outcome_integer =
            |field: &str| exact_optional_usize(&[v.get(field), v["outcome"].get(field)], field);
        let malformed_outcome = |message: String| KitError::OutcomeUnknown {
            query_id: query_id.clone(),
            message,
            metadata: boxed_query_metadata(None, None, Some(false), Some("invalid_outcome")),
        };
        let committed = match exact_optional_bool(
            &[
                v.get("committed"),
                v["outcome"].get("committed"),
                v["error"].get("committed"),
            ],
            "committed",
        ) {
            Ok(committed) => committed,
            Err(message) => return malformed_outcome(message),
        };
        let implied_committed = match v["status"].as_str() {
            Some("committed") => Some(true),
            Some("aborted") => Some(false),
            Some("outcome_unknown") => None,
            _ => committed,
        };
        if committed.is_some() && implied_committed.is_some() && committed != implied_committed {
            return malformed_outcome("remote status and committed fields disagree".into());
        }
        let committed = committed.or(implied_committed);
        let explicit_outcome_known = match exact_optional_bool(
            &[v.get("outcome_known"), v["outcome"].get("outcome_known")],
            "outcome_known",
        ) {
            Ok(value) => value,
            Err(message) => return malformed_outcome(message),
        };
        if explicit_outcome_known.is_some_and(|known| known != committed.is_some()) {
            return malformed_outcome(
                "remote outcome_known disagrees with committed fields".into(),
            );
        }
        let committed_statements = match outcome_integer("committed_statements") {
            Ok(value) => value.unwrap_or(0),
            Err(message) => return malformed_outcome(message),
        };
        let last_commit_epoch = match exact_optional_epoch(
            &[
                v.get("last_commit_epoch"),
                v["outcome"].get("last_commit_epoch"),
                v.get("epoch"),
            ],
            &[
                v.get("last_commit_epoch_text"),
                v["outcome"].get("last_commit_epoch_text"),
                v.get("epoch_text"),
            ],
        ) {
            Ok(epoch) => epoch,
            Err(message) => return malformed_outcome(message),
        };
        let completed_statements = match outcome_integer("completed_statements") {
            Ok(value) => value.unwrap_or(0),
            Err(message) => return malformed_outcome(message),
        };
        let statement_index = match outcome_integer("statement_index") {
            Ok(value) => value.unwrap_or(0),
            Err(message) => return malformed_outcome(message),
        };
        let first_commit_statement_index = match outcome_integer("first_commit_statement_index") {
            Ok(value) => value,
            Err(message) => return malformed_outcome(message),
        };
        let last_commit_statement_index = match outcome_integer("last_commit_statement_index") {
            Ok(value) => value,
            Err(message) => return malformed_outcome(message),
        };
        if committed == Some(true) && last_commit_epoch.is_none() {
            return malformed_outcome("committed response lacks an exact commit epoch".into());
        }
        if committed == Some(false) && last_commit_epoch.is_some() {
            return malformed_outcome("non-committed response contains a commit epoch".into());
        }
        if code == "COMMIT_OUTCOME" && committed != Some(true) {
            return malformed_outcome("COMMIT_OUTCOME does not prove a commit".into());
        }
        if code == "QUERY_OUTCOME_UNKNOWN" && committed.is_some() {
            return malformed_outcome("unknown outcome claims a commit decision".into());
        }
        let code_requires_commit = matches!(
            code,
            "QUERY_CANCELLED_AFTER_COMMIT"
                | "DEADLINE_AFTER_COMMIT"
                | "COMMIT_OUTCOME"
                | "SERIALIZATION_FAILED_AFTER_COMMIT"
        );
        let code_requires_no_commit = matches!(
            code,
            "QUERY_CANCELLED" | "DEADLINE_EXCEEDED" | "SERIALIZATION_FAILED"
        );
        if code_requires_commit && committed != Some(true) {
            return malformed_outcome(format!("{code} does not prove a commit"));
        }
        if code_requires_no_commit && committed != Some(false) {
            return malformed_outcome(format!("{code} has an unknown commit outcome"));
        }
        let retryable = match exact_optional_bool(
            &[v.get("retryable"), v["error"].get("retryable")],
            "retryable",
        ) {
            Ok(value) => value,
            Err(message) => return malformed_outcome(message),
        };
        let cancellation_reason = v["cancellation_reason"]
            .as_str()
            .unwrap_or(&msg)
            .to_string();
        let metadata = boxed_query_metadata(
            v["cancel_outcome"].as_str(),
            v["cancellation_reason"].as_str(),
            retryable,
            v["server_state"].as_str(),
        );
        let max_rows = match exact_optional_usize(
            &[v.get("max_rows"), v["error"].get("max_rows")],
            "max_rows",
        ) {
            Ok(value) => value.map(Box::new),
            Err(message) => {
                return KitError::RemoteProtocol {
                    status: status.as_u16(),
                    code: "INVALID_REMOTE_RESPONSE".into(),
                    query_id: (query_id != "unknown").then_some(query_id),
                    message: message.into_boxed_str(),
                    metadata,
                }
            }
        };
        let max_bytes = match exact_optional_usize(
            &[v.get("max_bytes"), v["error"].get("max_bytes")],
            "max_bytes",
        ) {
            Ok(value) => value.map(Box::new),
            Err(message) => {
                return KitError::RemoteProtocol {
                    status: status.as_u16(),
                    code: "INVALID_REMOTE_RESPONSE".into(),
                    query_id: (query_id != "unknown").then_some(query_id),
                    message: message.into_boxed_str(),
                    metadata,
                }
            }
        };
        let execution_outcome = QueryExecutionOutcome {
            committed: committed.unwrap_or(false),
            committed_statements: Some(committed_statements),
            last_commit_epoch,
            first_commit_statement_index,
            last_commit_statement_index,
            completed_statements,
            statement_index,
        };
        match code {
            EC_UNIQUE => return KitError::Duplicate(msg),
            EC_FK => return KitError::ForeignKey(msg),
            EC_CHECK | EC_BAD => return KitError::Validation(msg),
            EC_CONFLICT => return KitError::Conflict(msg),
            EC_TRIGGER_VALIDATION => return KitError::TriggerValidation(msg),
            "QUERY_CANCELLED" | "QUERY_CANCELLED_AFTER_COMMIT" => {
                return KitError::Cancelled {
                    query_id: query_id.into_boxed_str(),
                    reason: cancellation_reason.into_boxed_str(),
                    outcome: Box::new(execution_outcome),
                    metadata,
                }
            }
            "DEADLINE_EXCEEDED" | "DEADLINE_AFTER_COMMIT" => {
                return KitError::DeadlineExceeded {
                    query_id: query_id.into_boxed_str(),
                    timeout_ms: None,
                    outcome: Box::new(execution_outcome),
                    metadata,
                }
            }
            "COMMIT_OUTCOME" => {
                return KitError::CommitOutcome {
                    query_id,
                    code: code.into(),
                    outcome: Box::new(execution_outcome),
                    message: msg.into_boxed_str(),
                    metadata,
                }
            }
            "QUERY_ID_CONFLICT" => return KitError::QueryConflict { query_id, metadata },
            "QUERY_REGISTRY_FULL" => {
                return KitError::QueryRegistryFull {
                    query_id: (query_id != "unknown").then_some(query_id),
                    message: msg.into_boxed_str(),
                    metadata,
                }
            }
            "RESULT_LIMIT_EXCEEDED" => {
                return KitError::ResultLimitExceeded {
                    query_id: (query_id != "unknown").then(|| query_id.into_boxed_str()),
                    max_rows,
                    max_bytes,
                    outcome: Box::new(execution_outcome),
                    message: msg.into_boxed_str(),
                    metadata,
                }
            }
            "SERIALIZATION_FAILED" => {
                return KitError::SerializationFailed {
                    query_id: (query_id != "unknown").then_some(query_id),
                    outcome: Box::new(execution_outcome),
                    message: msg.into_boxed_str(),
                    metadata,
                }
            }
            "SERIALIZATION_FAILED_AFTER_COMMIT" => {
                return KitError::SerializationFailed {
                    query_id: (query_id != "unknown").then_some(query_id),
                    outcome: Box::new(QueryExecutionOutcome {
                        committed: true,
                        ..execution_outcome
                    }),
                    message: msg.into_boxed_str(),
                    metadata,
                }
            }
            "CAPABILITY_UNSUPPORTED" => return KitError::CapabilityUnsupported(msg),
            "QUERY_OUTCOME_UNKNOWN" => {
                return KitError::OutcomeUnknown {
                    query_id,
                    message: msg,
                    metadata,
                }
            }
            "TRANSACTION_ABORTED" => {
                return KitError::TransactionAborted {
                    query_id: (query_id != "unknown").then_some(query_id),
                    message: msg,
                    metadata,
                }
            }
            _ if !code.is_empty() => {
                return KitError::RemoteProtocol {
                    status: status.as_u16(),
                    code: code.into(),
                    query_id: (query_id != "unknown").then_some(query_id),
                    message: msg.into_boxed_str(),
                    metadata,
                }
            }
            _ => {}
        }
    }
    KitError::Storage(format!("HTTP {status} error response was malformed"))
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().clamp(1, u128::from(u64::MAX)) as u64
}

fn validate_positive_duration(name: &str, duration: Option<Duration>) -> Result<()> {
    if duration.is_some_and(|duration| duration.is_zero()) {
        return Err(KitError::Validation(format!("{name} must be positive")));
    }
    Ok(())
}

fn ioe(e: reqwest::Error) -> KitError {
    KitError::Storage(e.to_string())
}

fn remote_write_outcome_unknown(operation: &str, message: impl std::fmt::Display) -> KitError {
    KitError::OutcomeUnknown {
        query_id: "unknown".into(),
        message: format!("remote {operation} outcome is unknown: {message}"),
        metadata: boxed_query_metadata(None, None, Some(false), Some("invalid_response")),
    }
}

fn committed_write_followup_error(operation: &str, message: impl std::fmt::Display) -> KitError {
    KitError::CommitOutcome {
        query_id: "unknown".into(),
        code: "COMMIT_OUTCOME".into(),
        outcome: Box::new(QueryExecutionOutcome {
            committed: true,
            ..QueryExecutionOutcome::default()
        }),
        message: format!("remote {operation} committed, but local follow-up failed: {message}")
            .into_boxed_str(),
        metadata: boxed_query_metadata(None, None, Some(false), Some("invalid_response")),
    }
}

fn decode_write<T: for<'de> Deserialize<'de>>(
    resp: reqwest::blocking::Response,
    operation: &str,
) -> Result<T> {
    if !resp.status().is_success() {
        return Err(map_error(resp));
    }
    strict_response_json(resp).map_err(|error| remote_write_outcome_unknown(operation, error))
}

fn validate_procedure_call_response(response: Value, operation: &str) -> Result<Value> {
    let invalid = |message: String| remote_write_outcome_unknown(operation, message);
    exact_object_fields(
        &response,
        &["status", "committed", "epoch", "epoch_text", "result"],
        &["status", "committed", "epoch", "epoch_text", "result"],
        "procedure call response",
    )
    .map_err(&invalid)?;
    let committed = response["committed"]
        .as_bool()
        .ok_or_else(|| invalid("procedure call committed field is invalid".into()))?;
    if response["status"].as_str() != Some("ok") {
        return Err(invalid("procedure call status is not ok".into()));
    }
    if committed {
        let epoch = response["epoch"]
            .as_u64()
            .ok_or_else(|| invalid("committed procedure call lacks numeric epoch".into()))?;
        let epoch_text = response["epoch_text"]
            .as_str()
            .ok_or_else(|| invalid("committed procedure call lacks exact epoch".into()))?;
        if precise_epoch(Some(epoch_text), Some(epoch))
            .map_err(|error| invalid(error.to_string()))?
            != Some(epoch)
        {
            return Err(invalid("procedure call epoch fields disagree".into()));
        }
    } else if !response["epoch"].is_null() || !response["epoch_text"].is_null() {
        return Err(invalid(
            "non-committed procedure call contains commit epoch".into(),
        ));
    }
    Ok(response)
}

fn decode<T: for<'de> Deserialize<'de>>(resp: reqwest::blocking::Response) -> Result<T> {
    if !resp.status().is_success() {
        return Err(map_error(resp));
    }
    let v: T = strict_response_json(resp).map_err(KitError::Storage)?;
    Ok(v)
}

fn decode_control<T: for<'de> Deserialize<'de>>(resp: reqwest::blocking::Response) -> Result<T> {
    if !resp.status().is_success() {
        return Err(map_error(resp));
    }
    strict_control_response_json(resp).map_err(KitError::Storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    const CAPABILITIES: &str = r#"{"sql_cancellation":{"version":2,"client_query_ids":true,"cancel_endpoint":true,"query_status":true,"pre_registration_cancel":true,"stream_disconnect_cancels":true},"sql_idempotency":{"version":1,"durable_pre_execution_intent":true,"replay_committed_receipt":true,"indeterminate_never_reexecutes":true},"sql_pagination":{"version":1,"continuation_endpoint":"/sql/continue","retained_snapshot":true,"projection_required":true,"byte_and_token_hints":true}}"#;

    fn query_not_found_response(query_id: &str) -> &'static str {
        Box::leak(
            json!({
                "query_id": query_id,
                "status": "unknown",
                "terminal_state": null,
                "committed": null,
                "committed_statements": null,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": null,
                "statement_index": null,
                "cancel_outcome": "not_found",
                "cancellation_reason": null,
                "retryable": false,
                "server_state": "not_found",
                "outcome": {
                    "committed": null,
                    "committed_statements": null,
                    "last_commit_epoch": null,
                    "last_commit_epoch_text": null,
                    "first_commit_statement_index": null,
                    "last_commit_statement_index": null,
                    "completed_statements": null,
                    "statement_index": null,
                    "serialization": "unknown"
                },
                "error": {
                    "code": "QUERY_NOT_FOUND",
                    "message": "query not found",
                    "query_id": query_id,
                    "committed": null,
                    "retryable": false
                }
            })
            .to_string()
            .into_boxed_str(),
        )
    }

    #[test]
    fn strict_json_rejects_duplicate_keys_at_any_depth() {
        assert!(strict_json_from_str::<Value>(r#"{"status":"a","status":"b"}"#).is_err());
        assert!(strict_json_from_str::<Value>(
            r#"{"outcome":{"committed":false,"committed":true}}"#
        )
        .is_err());
        assert_eq!(
            strict_json_from_str::<Value>(r#"{"status":"a","outcome":{"committed":true}}"#)
                .unwrap()["status"],
            "a"
        );
    }

    #[test]
    fn procedure_call_requires_explicit_exact_commit_state() {
        for response in [
            json!({
                "status": "ok",
                "committed": false,
                "epoch": null,
                "epoch_text": null,
                "result": null,
            }),
            json!({
                "status": "ok",
                "committed": true,
                "epoch": 9,
                "epoch_text": "9",
                "result": {},
            }),
        ] {
            assert!(validate_procedure_call_response(response, "procedure call").is_ok());
        }
        for response in [
            json!({"status": "ok", "epoch": null, "epoch_text": null, "result": null}),
            json!({
                "status": "ok",
                "committed": false,
                "epoch": 9,
                "epoch_text": "9",
                "result": null,
            }),
            json!({
                "status": "ok",
                "committed": true,
                "epoch": 9,
                "epoch_text": "09",
                "result": null,
            }),
            json!({
                "status": "ok",
                "committed": true,
                "epoch": null,
                "epoch_text": null,
                "result": null,
            }),
        ] {
            assert!(matches!(
                validate_procedure_call_response(response, "procedure call"),
                Err(KitError::OutcomeUnknown { .. })
            ));
        }
    }

    #[test]
    fn response_reader_enforces_body_limit_before_json_decode() {
        let (url, server) = mock_server(vec![("200 OK", r#"{"status":"completed"}"#)]);
        let response = reqwest::blocking::get(url).unwrap();
        let error = strict_response_json_with_limit::<Value>(response, 8).unwrap_err();
        assert!(error.contains("exceeded 8 bytes"));
        assert_eq!(server.join().unwrap().len(), 1);
    }

    #[test]
    fn query_not_found_requires_exact_matching_envelope() {
        let query_id: mongreldb_query::QueryId =
            "aaaabbbbccccddddeeeeffff00001111".parse().unwrap();
        let valid: Value =
            strict_json_from_str(query_not_found_response(&query_id.to_string())).unwrap();
        assert!(validate_query_not_found_response(&valid, query_id).is_ok());

        let mut wrong_id = valid.clone();
        wrong_id["error"]["query_id"] = Value::String("11112222333344445555666677778888".into());
        assert!(validate_query_not_found_response(&wrong_id, query_id).is_err());

        let mut unknown = valid;
        unknown["result"] = Value::Null;
        assert!(validate_query_not_found_response(&unknown, query_id).is_err());
    }

    #[test]
    fn cursor_errors_require_exact_non_committed_outcome() {
        let valid = json!({
            "status": "failed_before_commit",
            "terminal_state": "failed_before_commit",
            "server_state": "failed",
            "committed": false,
            "committed_statements": 0,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": 0,
            "statement_index": 0,
            "cancel_outcome": null,
            "cancellation_reason": null,
            "retryable": false,
            "outcome": {
                "committed": false,
                "committed_statements": 0,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": 0,
                "statement_index": 0,
                "serialization": "not_started"
            },
            "error": {
                "code": "SQL_CURSOR_NOT_FOUND",
                "message": "cursor missing",
                "committed": false,
                "retryable": false
            }
        });
        assert!(validate_sql_cursor_error_envelope(&valid).is_ok());
        let mut conflict = valid.clone();
        conflict["error"]["committed"] = Value::Bool(true);
        assert!(validate_sql_cursor_error_envelope(&conflict).is_err());
        let mut unknown = valid;
        unknown["unexpected"] = Value::Null;
        assert!(validate_sql_cursor_error_envelope(&unknown).is_err());
    }

    #[test]
    fn native_query_requires_cursor_when_truncated() {
        let (url, server) = mock_server(vec![(
            "200 OK",
            r#"{"truncated":true,"next_cursor":null,"rows":[]}"#,
        )]);
        let database = RemoteDatabase {
            base_url: url,
            client: reqwest::blocking::Client::new(),
            schemas: HashMap::new(),
            sql_cancellation: None,
            sql_idempotency: None,
            sql_pagination: None,
        };
        assert!(matches!(
            database.query("items", Vec::new(), None, Some(1)),
            Err(KitError::Integrity(message))
                if message.contains("continuation metadata")
        ));
        assert_eq!(server.join().unwrap().len(), 1);
    }

    #[test]
    fn malformed_query_not_found_never_authorizes_idempotent_replay() {
        let (url, server) = mock_server(vec![("404 Not Found", "{}")]);
        let database = RemoteDatabase {
            base_url: url,
            client: reqwest::blocking::Client::new(),
            schemas: HashMap::new(),
            sql_cancellation: None,
            sql_idempotency: None,
            sql_pagination: None,
        };
        let query_id = "aaaabbbbccccddddeeeeffff00001111".parse().unwrap();
        assert!(matches!(
            database.idempotent_status_after_loss(query_id),
            Err(IdempotentSqlAttemptError::Final(
                KitError::OutcomeUnknown { .. }
            ))
        ));
        assert_eq!(server.join().unwrap().len(), 1);
    }

    #[test]
    fn transaction_success_decode_failures_keep_durable_state() {
        let (url, server) = mock_server(vec![
            (
                "200 OK",
                r#"{"status":"committed","epoch":42,"epoch_text":"42","results":[{"kind":"deleted"}]}"#,
            ),
            ("200 OK", r#"{"status":"mystery"}"#),
        ]);
        let database = RemoteDatabase {
            base_url: url,
            client: reqwest::blocking::Client::new(),
            schemas: HashMap::new(),
            sql_cancellation: None,
            sql_idempotency: None,
            sql_pagination: None,
        };
        assert!(matches!(
            database.begin().commit(),
            Err(KitError::CommitOutcome { outcome, .. })
                if outcome.committed && outcome.last_commit_epoch == Some(42)
        ));
        assert!(matches!(
            database.begin().commit(),
            Err(KitError::OutcomeUnknown { .. })
        ));
        assert_eq!(server.join().unwrap().len(), 2);
    }

    #[test]
    fn default_sql_response_limit_is_finite() {
        assert_eq!(sql_response_limit(None), MAX_JSON_RESPONSE_BYTES);
        assert_eq!(
            sql_response_limit(Some(usize::MAX)),
            MAX_JSON_RESPONSE_BYTES
        );
        assert_eq!(sql_response_limit(Some(1024)), 1024);
    }

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
                let request = read_request(&stream);
                let query_header = request
                    .starts_with("POST /sql")
                    .then(|| request.split_once("\r\n\r\n").unwrap().1)
                    .and_then(|body| serde_json::from_str::<Value>(body).ok())
                    .and_then(|body| {
                        body["query_id"]
                            .as_str()
                            .or_else(|| body["operation_id"].as_str())
                            .map(str::to_owned)
                    })
                    .map(|query_id| format!("x-mongreldb-query-id: {query_id}\r\n"))
                    .unwrap_or_default();
                requests.push(request);
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{query_header}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            requests
        });
        (format!("http://{address}"), worker)
    }

    fn mock_server_with_headers(
        responses: Vec<(&'static str, &'static str, &'static str)>,
    ) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, headers, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&stream));
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            requests
        });
        (format!("http://{address}"), worker)
    }

    fn valid_status_json(query_id: &str) -> Value {
        json!({
            "query_id": query_id,
            "status": "committed",
            "state": "completed",
            "server_state": "completed",
            "terminal_state": "committed",
            "operation": "sql",
            "started_ms_ago": 12,
            "deadline_ms_remaining": null,
            "session_id": null,
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 17,
            "last_commit_epoch_text": "17",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "cancel_outcome": "already_finished",
            "cancellation_reason": "none",
            "retryable": false,
            "terminal_error": null,
            "trace": {
                "queue_duration_us": 1,
                "planning_duration_us": 2,
                "execution_duration_us": 3,
                "serialization_duration_us": 4,
                "cancel_requested_phase": null,
                "cancel_observed_phase": null,
                "commit_fence_outcome": "commit_won"
            },
            "outcome": {
                "committed": true,
                "committed_statements": 1,
                "last_commit_epoch": 17,
                "last_commit_epoch_text": "17",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "serialization": "succeeded"
            }
        })
    }

    fn valid_receipt_json(query_id: &str) -> Value {
        json!({
            "query_id": query_id,
            "original_query_id": query_id,
            "status": "committed",
            "terminal_state": "committed",
            "server_state": "completed",
            "cancel_outcome": "already_finished",
            "cancellation_reason": "none",
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 17,
            "last_commit_epoch_text": "17",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "retryable": false,
            "idempotency_replayed": false,
            "idempotency_persisted": true,
            "idempotency_expires_at_ms": 999,
            "outcome": {
                "committed": true,
                "committed_statements": 1,
                "last_commit_epoch": 17,
                "last_commit_epoch_text": "17",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "serialization": "succeeded"
            },
            "terminal_error": null
        })
    }

    fn valid_page_json() -> Value {
        json!({
            "status": "completed",
            "rows": [{"id": 1}],
            "next_cursor": null,
            "page": {
                "offset": 0,
                "row_count": 1,
                "total_rows": 1,
                "byte_count": 10,
                "estimated_tokens": 3,
                "limits": {"rows": 1, "bytes": 1024, "tokens": 256},
                "projection": ["id"],
                "expires_at_ms": 999,
                "snapshot": "retained_result",
                "token_estimate": "ceil(projected_json_bytes/4)"
            }
        })
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
            sql_idempotency: None,
            sql_pagination: None,
        };
        let mut row = Map::new();
        row.insert("id".into(), json!(5));
        row.insert("name".into(), json!("a"));
        let cells = db.cells("t", &row).unwrap();
        assert_eq!(cells, vec![json!(1), json!(5), json!(2), json!("a")]);
    }

    #[test]
    fn durable_receipt_validator_rejects_conflicting_fields() {
        let query_id = "abcdefabcdefabcdefabcdefabcdefab";
        let valid: RemoteSqlWriteReceipt =
            serde_json::from_value(valid_receipt_json(query_id)).unwrap();
        assert!(validate_sql_write_receipt(valid, query_id, None).is_ok());

        let mut invalid = Vec::new();
        let mut candidate = valid_receipt_json(query_id);
        candidate["last_commit_epoch_text"] = json!("18");
        invalid.push(candidate);
        let mut candidate = valid_receipt_json(query_id);
        candidate["outcome"]["last_commit_epoch"] = json!(18);
        candidate["outcome"]["last_commit_epoch_text"] = json!("18");
        invalid.push(candidate);
        let mut candidate = valid_receipt_json(query_id);
        candidate["first_commit_statement_index"] = json!(1);
        candidate["outcome"]["first_commit_statement_index"] = json!(1);
        invalid.push(candidate);
        let mut candidate = valid_receipt_json(query_id);
        candidate["outcome"]["completed_statements"] = json!(0);
        invalid.push(candidate);
        let mut candidate = valid_receipt_json(query_id);
        candidate["outcome"]["serialization"] = json!("");
        invalid.push(candidate);
        let mut candidate = valid_receipt_json(query_id);
        candidate["outcome"]["serialization"] = json!("completed");
        invalid.push(candidate);
        let mut candidate = valid_receipt_json(query_id);
        candidate["terminal_error"] = json!({"code": "", "category": "execution"});
        invalid.push(candidate);
        let mut candidate = valid_receipt_json(query_id);
        candidate["terminal_error"] = json!({"code": "QUERY_FAILED", "category": "execution"});
        invalid.push(candidate);

        let mut unknown = valid_receipt_json(query_id);
        unknown["outcome"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<RemoteSqlWriteReceipt>(unknown).is_err());
        let mut missing = valid_receipt_json(query_id);
        missing["outcome"]
            .as_object_mut()
            .unwrap()
            .remove("last_commit_epoch");
        assert!(serde_json::from_value::<RemoteSqlWriteReceipt>(missing).is_err());

        for candidate in invalid {
            let receipt = serde_json::from_value(candidate).unwrap();
            assert!(validate_sql_write_receipt(receipt, query_id, None).is_err());
        }
        let replay_query_id = "11112222333344445555666677778888";
        let receipt = serde_json::from_value(valid_receipt_json(replay_query_id)).unwrap();
        assert!(validate_sql_write_receipt(receipt, replay_query_id, Some(query_id)).is_ok());
        let mut wrong_fresh = valid_receipt_json(replay_query_id);
        wrong_fresh["original_query_id"] = json!(query_id);
        let receipt = serde_json::from_value(wrong_fresh).unwrap();
        assert!(validate_sql_write_receipt(receipt, replay_query_id, Some(query_id)).is_err());

        let mut replay = valid_receipt_json(replay_query_id);
        replay["original_query_id"] = json!(query_id);
        replay["idempotency_replayed"] = json!(true);
        let receipt = serde_json::from_value(replay.clone()).unwrap();
        assert!(validate_sql_write_receipt(receipt, replay_query_id, Some(query_id)).is_ok());
        replay["original_query_id"] = json!("99990000111122223333444455556666");
        let receipt = serde_json::from_value(replay).unwrap();
        assert!(validate_sql_write_receipt(receipt, replay_query_id, Some(query_id)).is_err());
    }

    #[test]
    fn query_status_validator_rejects_cached_and_conflicting_status() {
        let query_id: mongreldb_query::QueryId =
            "abcdefabcdefabcdefabcdefabcdefab".parse().unwrap();
        let valid = serde_json::from_value(valid_status_json(&query_id.to_string())).unwrap();
        assert!(validate_query_status(valid, query_id).is_ok());

        let mut invalid = Vec::new();
        for (field, value) in [
            ("query_id", json!("11111111111111111111111111111111")),
            ("status", json!("completed")),
            ("server_state", json!("failed")),
            ("terminal_state", json!("completed")),
            ("last_commit_epoch_text", json!("18")),
        ] {
            let mut candidate = valid_status_json(&query_id.to_string());
            candidate[field] = value;
            invalid.push(candidate);
        }
        let mut candidate = valid_status_json(&query_id.to_string());
        candidate["outcome"]["last_commit_epoch"] = json!(18);
        candidate["outcome"]["last_commit_epoch_text"] = json!("18");
        invalid.push(candidate);
        let mut candidate = valid_status_json(&query_id.to_string());
        candidate["outcome"]["completed_statements"] = json!(0);
        invalid.push(candidate);
        let mut candidate = valid_status_json(&query_id.to_string());
        candidate["first_commit_statement_index"] = json!(1);
        candidate["outcome"]["first_commit_statement_index"] = json!(1);
        invalid.push(candidate);
        let mut candidate = valid_status_json(&query_id.to_string());
        candidate["committed_statements"] = json!(2);
        candidate["outcome"]["committed_statements"] = json!(2);
        invalid.push(candidate);
        let mut candidate = valid_status_json(&query_id.to_string());
        candidate["last_commit_statement_index"] = json!(1);
        candidate["outcome"]["last_commit_statement_index"] = json!(1);
        invalid.push(candidate);
        let mut candidate = valid_status_json(&query_id.to_string());
        candidate["statement_index"] = json!(2);
        candidate["outcome"]["statement_index"] = json!(2);
        invalid.push(candidate);
        let mut candidate = valid_status_json(&query_id.to_string());
        candidate["terminal_error"] = json!({"code": "", "category": "execution"});
        invalid.push(candidate);
        let mut candidate = valid_status_json(&query_id.to_string());
        candidate["status"] = json!("committed_with_error");
        candidate["state"] = json!("failed");
        candidate["server_state"] = json!("failed");
        candidate["terminal_state"] = json!("committed_with_error");
        candidate["terminal_error"] =
            json!({"code": "QUERY_CANCELLED_AFTER_COMMIT", "category": "execution"});
        invalid.push(candidate);
        let mut candidate = valid_status_json(&query_id.to_string());
        candidate["trace"]["commit_fence_outcome"] = json!("unknown");
        invalid.push(candidate);

        let mut unknown = valid_status_json(&query_id.to_string());
        unknown["trace"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<RemoteQueryStatus>(unknown).is_err());
        let mut missing = valid_status_json(&query_id.to_string());
        missing["outcome"]
            .as_object_mut()
            .unwrap()
            .remove("last_commit_epoch");
        assert!(serde_json::from_value::<RemoteQueryStatus>(missing).is_err());
        let mut unsafe_number = valid_status_json(&query_id.to_string());
        unsafe_number["trace"]["execution_duration_us"] = json!(-1);
        assert!(serde_json::from_value::<RemoteQueryStatus>(unsafe_number).is_err());

        for candidate in invalid {
            let status = serde_json::from_value(candidate).unwrap();
            assert!(validate_query_status(status, query_id).is_err());
        }

        let mut cancelling = valid_status_json(&query_id.to_string());
        cancelling["state"] = json!("cancelling");
        cancelling["server_state"] = json!("cancelling");
        cancelling["terminal_state"] = Value::Null;
        cancelling["cancel_outcome"] = json!("accepted");
        cancelling["cancellation_reason"] = json!("deadline");
        cancelling["outcome"]["serialization"] = json!("in_progress");
        let status = serde_json::from_value(cancelling).unwrap();
        assert!(validate_query_status(status, query_id).is_ok());
    }

    #[test]
    fn retained_page_validator_rejects_conflicting_metadata() {
        let options = RemoteSqlPaginationOptions {
            query_id: None,
            timeout: None,
            page_size_rows: 1,
            projection: vec!["id".into()],
            max_page_bytes: Some(1024),
            max_page_tokens: Some(256),
            max_output_rows: Some(1),
            max_output_bytes: Some(1024),
        };
        let valid = serde_json::from_value(valid_page_json()).unwrap();
        assert!(validate_sql_page(valid, Some(&options)).is_ok());

        let mut invalid = Vec::new();
        let mut candidate = valid_page_json();
        candidate["page"]["row_count"] = json!(2);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["offset"] = json!(1);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["limits"]["rows"] = json!(0);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["byte_count"] = json!(1025);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["projection"] = json!(["other"]);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["snapshot"] = json!("live");
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["token_estimate"] = json!("unknown");
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["next_cursor"] = json!("unexpected");
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["limits"]["rows"] = json!(2);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["total_rows"] = json!(2);
        candidate["next_cursor"] = json!("cursor-1");
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["rows"] = json!([{"other": 1}]);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["projection"] = json!(["id", "id"]);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["projection"] = json!(["x".repeat(257)]);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["limits"]["bytes"] = json!(MAX_JSON_RESPONSE_BYTES + 1);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["total_rows"] = json!(2);
        candidate["next_cursor"] = json!("😀".repeat(600));
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["byte_count"] = json!(11);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["page"]["estimated_tokens"] = json!(4);
        invalid.push(candidate);
        let mut candidate = valid_page_json();
        candidate["rows"] = json!([]);
        candidate["page"]["row_count"] = json!(0);
        candidate["page"]["total_rows"] = json!(1);
        candidate["page"]["byte_count"] = json!(2);
        candidate["page"]["estimated_tokens"] = json!(1);
        candidate["next_cursor"] = json!("cursor-1");
        invalid.push(candidate);

        for candidate in invalid {
            let page = serde_json::from_value(candidate).unwrap();
            assert!(validate_sql_page(page, Some(&options)).is_err());
        }
    }

    #[test]
    fn cancel_and_sql_error_validators_reject_crossed_metadata() {
        let query_id: mongreldb_query::QueryId =
            "abcdefabcdefabcdefabcdefabcdefab".parse().unwrap();
        let valid_cancel = json!({
            "query_id": query_id.to_string(),
            "state": "cancellation_requested",
            "cancel_outcome": "accepted"
        });
        assert_eq!(
            validate_cancel_response(&valid_cancel, query_id, reqwest::StatusCode::ACCEPTED)
                .unwrap(),
            RemoteCancelOutcome::Accepted
        );
        let mut wrong_id = valid_cancel.clone();
        wrong_id["query_id"] = json!("11112222333344445555666677778888");
        assert!(
            validate_cancel_response(&wrong_id, query_id, reqwest::StatusCode::ACCEPTED).is_err()
        );
        let mut crossed = valid_cancel.clone();
        crossed["cancel_outcome"] = json!("too_late");
        assert!(
            validate_cancel_response(&crossed, query_id, reqwest::StatusCode::CONFLICT).is_err()
        );
        assert!(
            validate_cancel_response(&valid_cancel, query_id, reqwest::StatusCode::OK).is_err()
        );
        let mut invalid_field = valid_cancel.clone();
        invalid_field["cancel_outcome"] = json!("mystery");
        assert!(
            validate_cancel_response(&invalid_field, query_id, reqwest::StatusCode::ACCEPTED)
                .is_err()
        );
        let mut missing_field = valid_cancel.clone();
        missing_field
            .as_object_mut()
            .unwrap()
            .remove("cancel_outcome");
        assert!(
            validate_cancel_response(&missing_field, query_id, reqwest::StatusCode::ACCEPTED)
                .is_err()
        );
        let not_found = json!({
            "query_id": query_id.to_string(),
            "status": "unknown",
            "terminal_state": null,
            "committed": null,
            "committed_statements": null,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": null,
            "statement_index": null,
            "cancel_outcome": "not_found",
            "cancellation_reason": null,
            "retryable": false,
            "server_state": "not_found",
            "outcome": {
                "committed": null,
                "committed_statements": null,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": null,
                "statement_index": null,
                "serialization": "unknown"
            },
            "error": {
                "code": "QUERY_NOT_FOUND",
                "message": "query not found",
                "query_id": query_id.to_string(),
                "committed": null,
                "retryable": false
            }
        });
        assert_eq!(
            validate_cancel_response(&not_found, query_id, reqwest::StatusCode::NOT_FOUND).unwrap(),
            RemoteCancelOutcome::NotFound
        );
        let mut missing_outcome = not_found;
        missing_outcome["outcome"]
            .as_object_mut()
            .unwrap()
            .remove("last_commit_epoch");
        assert!(validate_cancel_response(
            &missing_outcome,
            query_id,
            reqwest::StatusCode::NOT_FOUND
        )
        .is_err());

        let valid_error = json!({
            "query_id": query_id.to_string(),
            "status": "cancelled_before_commit",
            "terminal_state": "cancelled_before_commit",
            "committed": false,
            "committed_statements": 0,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": 0,
            "statement_index": 0,
            "retryable": false,
            "outcome": {
                "committed": false,
                "committed_statements": 0,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": 0,
                "statement_index": 0,
                "serialization": "failed"
            },
            "error": {
                "code": "QUERY_CANCELLED",
                "message": "cancelled",
                "query_id": query_id.to_string(),
                "committed": false,
                "retryable": false
            }
        });
        assert!(validate_sql_error_envelope(&valid_error, query_id).is_ok());
        let mut missing_outcome = valid_error.clone();
        missing_outcome["outcome"]
            .as_object_mut()
            .unwrap()
            .remove("last_commit_epoch");
        assert!(validate_sql_error_envelope(&missing_outcome, query_id).is_err());
        let mut crossed = valid_error;
        crossed["error"]["code"] = json!("RESULT_LIMIT_EXCEEDED");
        assert!(validate_sql_error_envelope(&crossed, query_id).is_err());
    }

    #[test]
    fn typed_transaction_responses_fail_closed() {
        const SCHEMA: &str = r#"{"tables":{"users":{"columns":[{"id":0,"name":"id","primary_key":true},{"id":1,"name":"email","primary_key":false}]}}}"#;
        for response in [
            r#"{"status":"committed","epoch":9,"results":[{"kind":"put","row_id":null,"auto_inc":1,"row":[0,1,1,"a"]}]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"10","results":[{"kind":"put","row_id":null,"auto_inc":1,"row":[0,1,1,"a"]}]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"9","results":[]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"9","results":[{"kind":"upsert","action":"inserted","auto_inc":1,"row":[0,1,1,"a"]}]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"9","results":[{"kind":"put","row_id":null,"auto_inc":1}]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"9","results":[{"kind":"put","row_id":null,"auto_inc":1,"row":[0]}]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"9","results":[{"kind":"put","row_id":null,"auto_inc":1,"row":[99,1]}]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"9","results":[{"kind":"put","row_id":null,"auto_inc":1,"row":[0,1,0,2]}]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"9","results":[{"kind":"put","row_id":null,"row":[0,1,1,"a"]}]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"9","results":[{"kind":"put","auto_inc":1,"row":[0,1,1,"a"]}]}"#,
            r#"{"status":"committed","epoch":9,"epoch_text":"9","results":[{"kind":"put","row_id":null,"auto_inc":1,"row":[0,1,1,"a"]}],"extra":true}"#,
        ] {
            let (url, server) = mock_server(vec![
                ("200 OK", CAPABILITIES),
                ("200 OK", SCHEMA),
                ("200 OK", response),
            ]);
            let database = RemoteDatabase::connect(&url).unwrap();
            let transaction = database
                .begin()
                .insert_returning("users", Map::from_iter([("email".into(), json!("a"))]))
                .unwrap();
            assert!(transaction.commit().is_err(), "accepted {response}");
            assert_eq!(server.join().unwrap().len(), 3);
        }
    }

    #[test]
    fn outcome_unknown_status_keeps_commit_state_unknown() {
        let status: RemoteQueryStatus = serde_json::from_str(
            r#"{
                "query_id":"11112222333344445555666677778888",
                "status":"outcome_unknown",
                "state":"failed",
                "committed":null,
                "committed_statements":null,
                "last_commit_epoch":null,
                "last_commit_epoch_text":null,
                "first_commit_statement_index":null,
                "last_commit_statement_index":null,
                "completed_statements":null,
                "statement_index":null,
                "outcome":{
                    "committed":null,
                    "committed_statements":null,
                    "last_commit_epoch":null,
                    "last_commit_epoch_text":null,
                    "first_commit_statement_index":null,
                    "last_commit_statement_index":null,
                    "completed_statements":null,
                    "statement_index":null,
                    "serialization":"unknown"
                },
                "terminal_error":{"code":"QUERY_OUTCOME_UNKNOWN","category":"execution"}
            }"#,
        )
        .unwrap();

        assert_eq!(status.durable_commit_state(), None);
        assert!(matches!(
            remote_status_error(&status),
            Some(KitError::OutcomeUnknown { .. })
        ));
    }

    #[test]
    fn pre_cancelled_status_is_terminal_cancellation() {
        let status: RemoteQueryStatus = serde_json::from_str(
            r#"{
                "query_id":"11112222333344445555666677778888",
                "status":"cancelled_before_commit",
                "state":"pre_cancelled",
                "committed":false,
                "committed_statements":0,
                "last_commit_epoch":null,
                "last_commit_epoch_text":null,
                "first_commit_statement_index":null,
                "last_commit_statement_index":null,
                "completed_statements":0,
                "statement_index":0,
                "cancel_outcome":"pre_cancelled",
                "cancellation_reason":"client_request",
                "outcome":{"committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"serialization":"not_started"},
                "terminal_error":{"code":"QUERY_CANCELLED","category":"cancellation"}
            }"#,
        )
        .unwrap();

        assert!(status.is_terminal());
        assert!(matches!(
            remote_status_error(&status),
            Some(KitError::Cancelled {
                outcome,
                ..
            }) if !outcome.committed && outcome.committed_statements == Some(0)
        ));
    }

    #[test]
    fn lost_successful_read_response_is_serialization_failure() {
        let terminal = r#"{
                "query_id":"11112222333344445555666677778888",
                "status":"completed",
                "terminal_state":"completed",
                "state":"completed",
                "server_state":"completed",
                "committed":false,
                "committed_statements":0,
                "last_commit_epoch":null,
                "last_commit_epoch_text":null,
                "first_commit_statement_index":null,
                "last_commit_statement_index":null,
                "completed_statements":1,
                "statement_index":0,
                "cancel_outcome":"already_finished",
                "cancellation_reason":"none",
                "retryable":false,
                "outcome":{
                    "committed":false,
                    "committed_statements":0,
                    "last_commit_epoch":null,
                    "last_commit_epoch_text":null,
                    "first_commit_statement_index":null,
                    "last_commit_statement_index":null,
                    "completed_statements":1,
                    "statement_index":0,
                    "serialization":"succeeded"
                },
                "terminal_error":null
            }"#;
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("200 OK", terminal),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let query_id = "11112222333344445555666677778888".parse().unwrap();
        let error = database.recover_after_transport_loss(query_id, "response lost".into());

        assert!(matches!(
            error,
            KitError::SerializationFailed {
                outcome,
                ..
            } if !outcome.committed
                && outcome.committed_statements == Some(0)
                && outcome.completed_statements == 1
        ));
        assert!(
            server.join().unwrap()[2].starts_with("GET /queries/11112222333344445555666677778888 ")
        );
    }

    #[test]
    fn committed_serializing_status_is_immediately_decisive() {
        let status = r#"{
            "query_id":"11112222333344445555666677778888",
            "status":"committed",
            "terminal_state":null,
            "state":"serializing",
            "server_state":"serializing",
            "operation":"INSERT",
            "committed":true,
            "committed_statements":1,
            "last_commit_epoch":17,
            "last_commit_epoch_text":"17",
            "first_commit_statement_index":0,
            "last_commit_statement_index":0,
            "completed_statements":1,
            "statement_index":0,
            "cancel_outcome":null,
            "cancellation_reason":"none",
            "retryable":false,
            "outcome":{
                "committed":true,
                "committed_statements":1,
                "last_commit_epoch":17,
                "last_commit_epoch_text":"17",
                "first_commit_statement_index":0,
                "last_commit_statement_index":0,
                "completed_statements":1,
                "statement_index":0,
                "serialization":"in_progress"
            },
            "terminal_error":null,
            "trace":{}
        }"#;
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("200 OK", status),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let query_id = "11112222333344445555666677778888".parse().unwrap();
        let error = database.recover_after_transport_loss(query_id, "response lost".into());
        assert!(matches!(
            error,
            KitError::CommitOutcome { outcome, .. }
                if outcome.committed && outcome.last_commit_epoch == Some(17)
        ));
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].starts_with("GET /queries/11112222333344445555666677778888 "));
    }

    #[test]
    fn protocol_errors_preserve_current_server_codes() {
        for code in [
            "IDEMPOTENCY_KEY_REUSE_MISMATCH",
            "IDEMPOTENCY_REQUIRES_JSON",
            "IDEMPOTENCY_REQUIRES_SINGLE_WRITE",
            "IDEMPOTENCY_STORE_FULL",
            "IDEMPOTENCY_STORE_UNAVAILABLE",
            "IDEMPOTENCY_UNSUPPORTED_IN_TRANSACTION",
            "INVALID_IDEMPOTENCY_KEY",
            "INVALID_PAGE_OFFSET",
            "INVALID_PAGINATION_OPTIONS",
            "INVALID_SQL_CURSOR",
            "INVALID_SQL_PROJECTION",
            "PAGINATION_REQUIRES_JSON",
            "PAGINATION_REQUIRES_SINGLE_READ_QUERY",
            "SQL_CURSOR_EXPIRED",
            "SQL_CURSOR_NOT_FOUND",
            "SQL_PAGE_STORE_FULL",
            "ENTROPY_UNAVAILABLE",
            "INCOMPATIBLE_SQL_CONTROLS",
            "NO_SQL_TRANSACTION",
            "SAVEPOINT_NOT_FOUND",
            "SERIALIZATION_WORKER_FAILED",
        ] {
            let body = serde_json::json!({
                "error": {"code": code, "message": "rejected"}
            })
            .to_string();
            assert!(matches!(
                map_error_body(reqwest::StatusCode::BAD_REQUEST, &body),
                KitError::RemoteProtocol {
                    status: 400,
                    code: actual,
                    ..
                } if actual.as_ref() == code
            ));
        }
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
        assert!(matches!(error, KitError::CapabilityUnsupported(_)));
        assert_eq!(server.join().unwrap().len(), 2);
    }

    #[test]
    fn controlled_sql_sends_client_id_and_server_timeout() {
        let timeout = r#"{"query_id":"11112222333344445555666677778888","status":"deadline_before_commit","terminal_state":"deadline_before_commit","committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"cancel_outcome":"accepted","cancellation_reason":"deadline","retryable":false,"server_state":"cancelled","outcome":{"committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"serialization":"not_started"},"error":{"code":"DEADLINE_EXCEEDED","message":"timed out","query_id":"11112222333344445555666677778888","committed":false,"retryable":false}}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
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
    fn default_arrow_sql_always_sends_query_id() {
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("200 OK", ""),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        assert!(database
            .sql_arrow_with_options("SELECT 1", RemoteSqlOptions::default())
            .unwrap()
            .is_empty());
        let requests = server.join().unwrap();
        assert!(requests[2].contains(r#""query_id":""#));
        assert!(!requests[2].contains(r#""query_id":null"#));
    }

    #[test]
    fn pagination_sends_limits_authenticates_continuation_and_validates_capability() {
        let page = r#"{"status":"completed","rows":[{"id":1}],"next_cursor":"cursor-1","page":{"offset":0,"row_count":1,"total_rows":2,"byte_count":10,"estimated_tokens":3,"limits":{"rows":1,"bytes":1024,"tokens":256},"projection":["id"],"expires_at_ms":999,"snapshot":"retained_result","token_estimate":"ceil(projected_json_bytes/4)"}}"#;
        let next = r#"{"status":"completed","rows":[{"id":2}],"next_cursor":null,"page":{"offset":1,"row_count":1,"total_rows":2,"byte_count":10,"estimated_tokens":3,"limits":{"rows":1,"bytes":1024,"tokens":256},"projection":["id"],"expires_at_ms":999,"snapshot":"retained_result","token_estimate":"ceil(projected_json_bytes/4)"}}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("200 OK", page),
            ("200 OK", next),
        ]);
        let database = RemoteDatabase::connect_with_options(
            &url,
            RemoteOptions {
                auth: Some(RemoteAuth::Bearer(SecretString::from("secret".to_owned()))),
                transport_timeout: Some(Duration::from_secs(2)),
            },
        )
        .unwrap();
        let first = database
            .sql_page(
                "SELECT id FROM items",
                RemoteSqlPaginationOptions {
                    query_id: Some("1234567890abcdef1234567890abcdef".parse().unwrap()),
                    timeout: Some(Duration::from_millis(250)),
                    page_size_rows: 1,
                    projection: vec!["id".into()],
                    max_page_bytes: Some(1024),
                    max_page_tokens: Some(256),
                    max_output_rows: None,
                    max_output_bytes: None,
                },
            )
            .unwrap();
        assert_eq!(first.rows, vec![Map::from_iter([("id".into(), json!(1))])]);
        let second = database
            .continue_sql_page(
                first.next_cursor.as_deref().unwrap(),
                RemoteSqlControlOptions {
                    query_id: Some("11112222333344445555666677778888".parse().unwrap()),
                    timeout: None,
                },
            )
            .unwrap();
        assert_eq!(second.rows, vec![Map::from_iter([("id".into(), json!(2))])]);
        let requests = server.join().unwrap();
        assert!(requests[2].contains(r#""page_size_rows":1"#));
        assert!(requests[2].contains(r#""max_page_bytes":1024"#));
        assert!(requests[3].starts_with("POST /sql/continue "));
        assert!(requests[3]
            .to_ascii_lowercase()
            .contains("authorization: bearer secret"));
    }

    #[test]
    fn idempotent_sql_preserves_precise_epoch_and_rejects_missing_capability() {
        let receipt = r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","original_query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"9007199254740993","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"retryable":false,"idempotency_replayed":false,"idempotency_persisted":true,"idempotency_expires_at_ms":999,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"9007199254740993","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"serialization":"succeeded"},"terminal_error":null}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("200 OK", receipt),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let result = database
            .execute_idempotent_sql(
                "INSERT INTO items VALUES (1)",
                RemoteIdempotentSqlOptions {
                    query_id: Some("abcdefabcdefabcdefabcdefabcdefab".parse().unwrap()),
                    timeout: None,
                    idempotency_key: "insert-one".into(),
                    max_output_rows: Some(1),
                    max_output_bytes: Some(1024),
                },
            )
            .unwrap();
        assert_eq!(result.last_commit_epoch, Some(9_007_199_254_740_993));
        assert_eq!(result.outcome.last_commit_epoch, result.last_commit_epoch);
        assert_eq!(result.first_commit_statement_index, Some(0));
        assert_eq!(result.last_commit_statement_index, Some(0));
        let requests = server.join().unwrap();
        assert!(requests[2].contains(r#""idempotency_key":"insert-one""#));

        let cancellation_only = r#"{"sql_cancellation":{"version":2,"client_query_ids":true,"cancel_endpoint":true,"query_status":true,"pre_registration_cancel":true,"stream_disconnect_cancels":true}}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", cancellation_only),
            ("200 OK", r#"{"tables":{}}"#),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let error = database
            .execute_idempotent_sql(
                "INSERT INTO items VALUES (1)",
                RemoteIdempotentSqlOptions {
                    query_id: None,
                    timeout: None,
                    idempotency_key: "insert-one".into(),
                    max_output_rows: None,
                    max_output_bytes: None,
                },
            )
            .unwrap_err();
        assert!(matches!(error, KitError::CapabilityUnsupported(_)));
        assert_eq!(server.join().unwrap().len(), 2);
    }

    #[test]
    fn idempotent_sql_bad_query_header_keeps_exact_commit_proof() {
        const QUERY_ID: &str = "abcdefabcdefabcdefabcdefabcdefab";
        const RECEIPT: &str = r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","original_query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","committed":true,"committed_statements":1,"last_commit_epoch":29,"last_commit_epoch_text":"29","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"retryable":false,"idempotency_replayed":false,"idempotency_persisted":true,"idempotency_expires_at_ms":999,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch":29,"last_commit_epoch_text":"29","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"serialization":"succeeded"},"terminal_error":null}"#;
        for header in [
            "",
            "x-mongreldb-query-id: 11112222333344445555666677778888\r\n",
        ] {
            let (url, server) = mock_server_with_headers(vec![
                ("200 OK", "", CAPABILITIES),
                ("200 OK", "", r#"{"tables":{}}"#),
                ("200 OK", header, RECEIPT),
            ]);
            let database = RemoteDatabase::connect(&url).unwrap();
            let error = database
                .execute_idempotent_sql(
                    "INSERT INTO items VALUES (1)",
                    RemoteIdempotentSqlOptions {
                        query_id: Some(QUERY_ID.parse().unwrap()),
                        timeout: None,
                        idempotency_key: "insert-one".into(),
                        max_output_rows: None,
                        max_output_bytes: None,
                    },
                )
                .unwrap_err();
            assert!(matches!(
                error,
                KitError::CommitOutcome {
                    query_id,
                    outcome,
                    ..
                } if query_id == QUERY_ID
                    && outcome.committed
                    && outcome.committed_statements == Some(1)
                    && outcome.last_commit_epoch == Some(29)
            ));
            assert_eq!(server.join().unwrap().len(), 3);
        }
    }

    #[test]
    fn pagination_and_idempotency_reject_invalid_limits_before_post() {
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        assert!(matches!(
            database.sql_page(
                "SELECT id FROM items",
                RemoteSqlPaginationOptions {
                    query_id: None,
                    timeout: None,
                    page_size_rows: 0,
                    projection: Vec::new(),
                    max_page_bytes: None,
                    max_page_tokens: None,
                    max_output_rows: None,
                    max_output_bytes: None,
                },
            ),
            Err(KitError::Validation(_))
        ));
        assert!(matches!(
            database.execute_idempotent_sql(
                "INSERT INTO items VALUES (1)",
                RemoteIdempotentSqlOptions {
                    query_id: None,
                    timeout: None,
                    idempotency_key: String::new(),
                    max_output_rows: None,
                    max_output_bytes: None,
                },
            ),
            Err(KitError::Validation(_))
        ));
        assert_eq!(server.join().unwrap().len(), 2);
    }

    #[test]
    fn invalid_idempotent_receipts_recover_durable_outcome() {
        let status = r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","terminal_state":"committed","state":"completed","server_state":"completed","operation":"INSERT","committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"9007199254740993","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"cancel_outcome":"already_finished","cancellation_reason":"none","retryable":false,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"9007199254740993","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"serialization":"succeeded"},"terminal_error":null,"trace":{}}"#;
        for invalid_receipt in [
            "not-json",
            r#"{"query_id":"11111111111111111111111111111111","original_query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","committed":true,"committed_statements":1,"last_commit_epoch":17,"last_commit_epoch_text":"17","retryable":false,"idempotency_replayed":false,"idempotency_persisted":true,"idempotency_expires_at_ms":999,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch":17,"last_commit_epoch_text":"17","serialization":"succeeded"},"terminal_error":null}"#,
            r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","original_query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","committed":true,"committed_statements":1,"last_commit_epoch":17,"last_commit_epoch_text":"not-an-epoch","retryable":false,"idempotency_replayed":false,"idempotency_persisted":true,"idempotency_expires_at_ms":999,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch":17,"last_commit_epoch_text":"not-an-epoch","serialization":"succeeded"},"terminal_error":null}"#,
            r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","original_query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","committed":true,"committed_statements":1,"last_commit_epoch":17,"last_commit_epoch_text":"18","retryable":false,"idempotency_replayed":false,"idempotency_persisted":true,"idempotency_expires_at_ms":999,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch":18,"last_commit_epoch_text":"18","serialization":"succeeded"},"terminal_error":null}"#,
            r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","original_query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","committed":true,"committed_statements":1,"last_commit_epoch_text":"17","first_commit_statement_index":0,"retryable":false,"idempotency_replayed":false,"idempotency_persisted":true,"idempotency_expires_at_ms":999,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch_text":"18","first_commit_statement_index":1,"serialization":"succeeded"},"terminal_error":null}"#,
            r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","original_query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","committed":true,"committed_statements":1,"last_commit_epoch_text":"17","first_commit_statement_index":2,"last_commit_statement_index":1,"retryable":false,"idempotency_replayed":false,"idempotency_persisted":true,"idempotency_expires_at_ms":999,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch_text":"17","first_commit_statement_index":2,"last_commit_statement_index":1,"serialization":"succeeded"},"terminal_error":null}"#,
            r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","original_query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","committed":true,"committed_statements":1,"retryable":false,"idempotency_replayed":false,"idempotency_persisted":true,"idempotency_expires_at_ms":999,"outcome":{"committed":false,"committed_statements":0,"serialization":"succeeded"},"terminal_error":null}"#,
        ] {
            let (url, server) = mock_server(vec![
                ("200 OK", CAPABILITIES),
                ("200 OK", r#"{"tables":{}}"#),
                ("200 OK", invalid_receipt),
                ("200 OK", status),
            ]);
            let database = RemoteDatabase::connect(&url).unwrap();
            let error = database
                .execute_idempotent_sql(
                    "INSERT INTO items VALUES (1)",
                    RemoteIdempotentSqlOptions {
                        query_id: Some("abcdefabcdefabcdefabcdefabcdefab".parse().unwrap()),
                        timeout: None,
                        idempotency_key: "insert-one".into(),
                        max_output_rows: None,
                        max_output_bytes: None,
                    },
                )
                .unwrap_err();
            assert!(matches!(
                error,
                KitError::SerializationFailed {
                    outcome,
                    ..
                } if outcome.committed
                    && outcome.committed_statements == Some(1)
                    && outcome.last_commit_epoch == Some(9_007_199_254_740_993)
            ));
            let requests = server.join().unwrap();
            assert!(requests[3].starts_with("GET /queries/abcdefabcdefabcdefabcdefabcdefab "));
        }
    }

    #[test]
    fn idempotent_recovery_rejects_wrong_query_status_without_replay() {
        let wrong_status = r#"{"query_id":"11111111111111111111111111111111","status":"outcome_unknown","state":"failed","committed":null,"outcome":{"committed":null}}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("200 OK", "{}"),
            ("200 OK", wrong_status),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let error = database
            .execute_idempotent_sql(
                "INSERT INTO items VALUES (1)",
                RemoteIdempotentSqlOptions {
                    query_id: Some("abcdefabcdefabcdefabcdefabcdefab".parse().unwrap()),
                    timeout: None,
                    idempotency_key: "wrong-status".into(),
                    max_output_rows: None,
                    max_output_bytes: None,
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            KitError::OutcomeUnknown { metadata, .. }
                if metadata.server_state.as_deref() == Some("invalid_status")
        ));
        let requests = server.join().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.starts_with("POST /sql "))
                .count(),
            1
        );
        assert!(!requests.iter().any(|request| request.contains("/cancel ")));
    }

    #[test]
    fn malformed_pages_use_initial_and_continuation_serialization_errors() {
        let terminal = r#"{"query_id":"1234567890abcdef1234567890abcdef","status":"completed","terminal_state":"completed","state":"completed","server_state":"completed","committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":1,"statement_index":0,"cancel_outcome":"already_finished","cancellation_reason":"none","retryable":false,"outcome":{"committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":1,"statement_index":0,"serialization":"succeeded"},"terminal_error":null}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("200 OK", r#"{"status":"completed","rows":[]}"#),
            ("200 OK", terminal),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let error = database
            .sql_page(
                "SELECT id FROM items",
                RemoteSqlPaginationOptions {
                    query_id: Some("1234567890abcdef1234567890abcdef".parse().unwrap()),
                    timeout: None,
                    page_size_rows: 1,
                    projection: vec!["id".into()],
                    max_page_bytes: Some(1024),
                    max_page_tokens: Some(256),
                    max_output_rows: None,
                    max_output_bytes: None,
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            KitError::SerializationFailed {
                query_id: Some(query_id),
                outcome,
                ..
            } if query_id == "1234567890abcdef1234567890abcdef"
                && !outcome.committed
        ));
        assert_eq!(server.join().unwrap().len(), 4);

        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("200 OK", r#"{"status":"completed","rows":[]}"#),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let error = database
            .continue_sql_page(
                "cursor-1",
                RemoteSqlControlOptions {
                    query_id: Some("11112222333344445555666677778888".parse().unwrap()),
                    timeout: None,
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            KitError::SerializationFailed {
                query_id: Some(query_id),
                outcome,
                ..
            } if query_id == "11112222333344445555666677778888" && !outcome.committed
        ));
        assert_eq!(server.join().unwrap().len(), 3);
    }

    #[test]
    fn idempotent_indeterminate_terminal_status_does_not_replay() {
        let status = r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"outcome_unknown","terminal_state":"outcome_unknown","state":"failed","server_state":"failed","operation":"INSERT","committed":null,"committed_statements":null,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":null,"statement_index":null,"cancel_outcome":"already_finished","cancellation_reason":"none","retryable":false,"outcome":{"committed":null,"committed_statements":null,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":null,"statement_index":null,"serialization":"unknown"},"terminal_error":{"code":"QUERY_OUTCOME_UNKNOWN","category":"execution"},"trace":{}}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("200 OK", "{}"),
            ("200 OK", status),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let error = database
            .execute_idempotent_sql(
                "INSERT INTO items VALUES (1)",
                RemoteIdempotentSqlOptions {
                    query_id: Some("abcdefabcdefabcdefabcdefabcdefab".parse().unwrap()),
                    timeout: None,
                    idempotency_key: "insert-one".into(),
                    max_output_rows: None,
                    max_output_bytes: None,
                },
            )
            .unwrap_err();
        assert!(matches!(error, KitError::OutcomeUnknown { .. }));
        assert_eq!(
            server
                .join()
                .unwrap()
                .iter()
                .filter(|request| request.starts_with("POST /sql "))
                .count(),
            1
        );
    }

    #[test]
    fn idempotent_restart_replays_key_once_with_fresh_query_id() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let original_query_id = "abcdefabcdefabcdefabcdefabcdefab";
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for body in [CAPABILITIES, r#"{"tables":{}}"#] {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&stream));
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }

            let (stream, _) = listener.accept().unwrap();
            requests.push(read_request(&stream));
            drop(stream);

            let mut pre_cancelled = false;
            loop {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_request(&stream);
                let replay = request.starts_with("POST /sql ");
                requests.push(request.clone());
                if request.starts_with("GET /capabilities ") {
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{CAPABILITIES}",
                        CAPABILITIES.len()
                    )
                    .unwrap();
                    continue;
                }
                if replay {
                    let request_body = request.split_once("\r\n\r\n").unwrap().1;
                    let request_json: Value = serde_json::from_str(request_body).unwrap();
                    let replay_query_id = request_json["query_id"].as_str().unwrap();
                    let receipt = json!({
                        "query_id": replay_query_id,
                        "original_query_id": original_query_id,
                        "status": "committed",
                        "committed": true,
                        "committed_statements": 1,
                        "last_commit_epoch": null,
                        "last_commit_epoch_text": "29",
                        "first_commit_statement_index": 0,
                        "last_commit_statement_index": 0,
                        "completed_statements": 1,
                        "statement_index": 0,
                        "retryable": false,
                        "idempotency_replayed": true,
                        "idempotency_persisted": true,
                        "idempotency_expires_at_ms": 999,
                        "outcome": {
                            "committed": true,
                            "committed_statements": 1,
                            "last_commit_epoch": null,
                            "last_commit_epoch_text": "29",
                            "first_commit_statement_index": 0,
                            "last_commit_statement_index": 0,
                            "completed_statements": 1,
                            "statement_index": 0,
                            "serialization": "succeeded"
                        },
                        "terminal_error": null
                    })
                    .to_string();
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nx-mongreldb-query-id: {replay_query_id}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{receipt}",
                        receipt.len()
                    )
                    .unwrap();
                    break;
                }
                let (status, body) = if request.contains("/cancel ") {
                    pre_cancelled = true;
                    (
                        "202 Accepted",
                        r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","state":"pre_cancelled","cancel_outcome":"pre_cancelled"}"#,
                    )
                } else if pre_cancelled {
                    (
                        "200 OK",
                        r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"cancelled_before_start","terminal_state":"cancelled_before_start","state":"pre_cancelled","server_state":"pre_cancelled","committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"cancel_outcome":"pre_cancelled","cancellation_reason":"client_request","retryable":false,"outcome":{"committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"serialization":"not_started"},"terminal_error":{"code":"QUERY_CANCELLED","category":"cancellation"}}"#,
                    )
                } else {
                    (
                        "404 Not Found",
                        query_not_found_response("abcdefabcdefabcdefabcdefabcdefab"),
                    )
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            requests
        });

        let database = RemoteDatabase::connect(&format!("http://{address}")).unwrap();
        let receipt = database
            .execute_idempotent_sql(
                "INSERT INTO items VALUES (1)",
                RemoteIdempotentSqlOptions {
                    query_id: Some(original_query_id.parse().unwrap()),
                    timeout: Some(Duration::from_millis(250)),
                    idempotency_key: "insert-one".into(),
                    max_output_rows: Some(1),
                    max_output_bytes: Some(1024),
                },
            )
            .unwrap();
        assert_ne!(receipt.query_id, original_query_id);
        assert_eq!(receipt.original_query_id, original_query_id);
        assert!(receipt.idempotency_replayed);
        assert_eq!(receipt.last_commit_epoch, Some(29));

        let requests = server.join().unwrap();
        assert!(!requests.iter().any(|request| request.contains("/cancel ")));
        let sql_requests: Vec<Value> = requests
            .into_iter()
            .filter(|request| request.starts_with("POST /sql "))
            .map(|request| serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap())
            .collect();
        assert_eq!(sql_requests.len(), 2);
        assert_eq!(sql_requests[0]["query_id"], original_query_id);
        assert_eq!(sql_requests[1]["query_id"], receipt.query_id);
        for request in sql_requests {
            assert_eq!(request["sql"], "INSERT INTO items VALUES (1)");
            assert_eq!(request["idempotency_key"], "insert-one");
            assert_eq!(request["timeout_ms"], 250);
            assert_eq!(request["max_output_rows"], 1);
            assert_eq!(request["max_output_bytes"], 1024);
        }
    }

    #[test]
    fn idempotent_transport_loss_cancels_and_recovers_durable_outcome() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for body in [CAPABILITIES, r#"{"tables":{}}"#] {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&stream));
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            let (stream, _) = listener.accept().unwrap();
            requests.push(read_request(&stream));
            drop(stream);

            let active = r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"running","terminal_state":null,"state":"executing","server_state":"executing","operation":"INSERT","committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"cancel_outcome":null,"cancellation_reason":"none","retryable":false,"outcome":{"committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"serialization":"in_progress"},"terminal_error":null,"trace":{}}"#;
            let terminal = r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","terminal_state":"committed","state":"completed","server_state":"completed","operation":"INSERT","committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"17","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"cancel_outcome":"already_finished","cancellation_reason":"none","retryable":false,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"17","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"serialization":"succeeded"},"terminal_error":null,"trace":{}}"#;
            for (status, body) in [
                ("200 OK", active),
                (
                    "202 Accepted",
                    r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","state":"cancellation_requested","cancel_outcome":"accepted"}"#,
                ),
                ("200 OK", terminal),
            ] {
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
        let database = RemoteDatabase::connect(&format!("http://{address}")).unwrap();
        let error = database
            .execute_idempotent_sql(
                "INSERT INTO items VALUES (1)",
                RemoteIdempotentSqlOptions {
                    query_id: Some("abcdefabcdefabcdefabcdefabcdefab".parse().unwrap()),
                    timeout: None,
                    idempotency_key: "insert-one".into(),
                    max_output_rows: None,
                    max_output_bytes: None,
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            KitError::CommitOutcome {
                outcome,
                ..
            } if outcome.committed_statements == Some(1)
                && outcome.last_commit_epoch == Some(17)
                && outcome.first_commit_statement_index == Some(0)
                && outcome.last_commit_statement_index == Some(0)
        ));
        let requests = server.join().unwrap();
        assert!(requests[3].starts_with("GET /queries/abcdefabcdefabcdefabcdefabcdefab "));
        assert!(requests[4].starts_with("POST /queries/abcdefabcdefabcdefabcdefabcdefab/cancel "));
        assert!(requests[5].starts_with("GET /queries/abcdefabcdefabcdefabcdefabcdefab "));
    }

    #[test]
    fn truncated_sql_error_body_recovers_durable_outcome() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for body in [CAPABILITIES, r#"{"tables":{}}"#] {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&stream));
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            let (mut stream, _) = listener.accept().unwrap();
            requests.push(read_request(&stream));
            write!(
                stream,
                "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{{"
            )
            .unwrap();
            drop(stream);

            let terminal = r#"{"query_id":"abcdefabcdefabcdefabcdefabcdefab","status":"committed","terminal_state":"committed","state":"completed","server_state":"completed","operation":"INSERT","committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"19","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"cancel_outcome":"already_finished","cancellation_reason":"none","retryable":false,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"19","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"serialization":"succeeded"},"terminal_error":null,"trace":{}}"#;
            let (mut stream, _) = listener.accept().unwrap();
            requests.push(read_request(&stream));
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{terminal}",
                terminal.len()
            )
            .unwrap();
            requests
        });
        let database = RemoteDatabase::connect(&format!("http://{address}")).unwrap();
        let error = database
            .execute_idempotent_sql(
                "INSERT INTO items VALUES (1)",
                RemoteIdempotentSqlOptions {
                    query_id: Some("abcdefabcdefabcdefabcdefabcdefab".parse().unwrap()),
                    timeout: None,
                    idempotency_key: "insert-one".into(),
                    max_output_rows: None,
                    max_output_bytes: None,
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            KitError::CommitOutcome {
                outcome,
                ..
            } if outcome.last_commit_epoch == Some(19)
        ));
        assert!(
            server.join().unwrap()[3].starts_with("GET /queries/abcdefabcdefabcdefabcdefabcdefab ")
        );
    }

    #[test]
    fn cancel_maps_accepted_response() {
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            (
                "202 Accepted",
                r#"{"query_id":"aaaabbbbccccddddeeeeffff00001111","state":"cancellation_requested","cancel_outcome":"accepted"}"#,
            ),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let query_id = "aaaabbbbccccddddeeeeffff00001111".parse().unwrap();
        assert_eq!(
            database.cancel_sql(query_id).unwrap(),
            RemoteCancelOutcome::Accepted
        );
        let requests = server.join().unwrap();
        assert!(requests[2].starts_with("POST /queries/aaaabbbbccccddddeeeeffff00001111/cancel "));
    }

    #[test]
    fn transport_timeout_recovers_committed_status() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for body in [CAPABILITIES, r#"{"tables":{}}"#] {
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
            let active = r#"{"query_id":"3333444455556666777788889999aaaa","status":"running","terminal_state":null,"state":"executing","server_state":"executing","operation":"INSERT","committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"cancel_outcome":null,"cancellation_reason":"none","retryable":false,"outcome":{"committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"serialization":"in_progress"},"terminal_error":null,"trace":{}}"#;
            let terminal = r#"{"query_id":"3333444455556666777788889999aaaa","status":"committed","terminal_state":"committed","state":"completed","server_state":"completed","operation":"INSERT","committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"9007199254740993","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"cancel_outcome":"already_finished","cancellation_reason":"none","retryable":false,"outcome":{"committed":true,"committed_statements":1,"last_commit_epoch":null,"last_commit_epoch_text":"9007199254740993","first_commit_statement_index":0,"last_commit_statement_index":0,"completed_statements":1,"statement_index":0,"serialization":"succeeded"},"terminal_error":null,"trace":{}}"#;
            for (status, body) in [
                ("200 OK", active),
                (
                    "202 Accepted",
                    r#"{"query_id":"3333444455556666777788889999aaaa","state":"cancellation_requested","cancel_outcome":"accepted"}"#,
                ),
                ("200 OK", active),
                ("200 OK", terminal),
            ] {
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
        assert!(matches!(
            error,
            KitError::CommitOutcome {
                outcome,
                ..
            } if outcome.committed_statements == Some(1)
                && outcome.last_commit_epoch == Some(9_007_199_254_740_993)
        ));
        let requests = server.join().unwrap();
        assert!(requests[2].starts_with("POST /sql "));
        assert!(requests[3].starts_with("GET /queries/3333444455556666777788889999aaaa "));
        assert!(requests[4].starts_with("POST /queries/3333444455556666777788889999aaaa/cancel "));
        assert!(requests[6].starts_with("GET /queries/3333444455556666777788889999aaaa "));
    }

    #[test]
    fn recovery_window_bounds_unresponsive_control_requests() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let server_stop = std::sync::Arc::clone(&stop);
        let server = std::thread::spawn(move || {
            for body in [CAPABILITIES, r#"{"tables":{}}"#] {
                let (mut stream, _) = listener.accept().unwrap();
                read_request(&stream);
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            listener.set_nonblocking(true).unwrap();
            let mut held = Vec::new();
            while !server_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        read_request(&stream);
                        held.push(stream);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("recovery listener failed: {error}"),
                }
            }
            held.len()
        });
        let database = RemoteDatabase::connect(&format!("http://{address}")).unwrap();
        let query_id = "3333444455556666777788889999aaaa".parse().unwrap();
        let started = Instant::now();
        assert!(database
            .terminal_status_after_loss_with_status(query_id, None)
            .is_none());
        let elapsed = started.elapsed();
        stop.store(true, Ordering::Release);
        assert!(server.join().unwrap() >= 2);
        assert!(elapsed >= Duration::from_millis(1500));
        assert!(elapsed < Duration::from_secs(3));
    }

    #[test]
    fn shared_auth_reaches_control_routes() {
        for (auth, expected) in [
            (
                RemoteAuth::Bearer(SecretString::from("secret".to_owned())),
                "Authorization: Bearer secret",
            ),
            (
                RemoteAuth::Basic {
                    username: "alice".into(),
                    password: SecretString::from("secret".to_owned()),
                },
                "Authorization: Basic YWxpY2U6c2VjcmV0",
            ),
        ] {
            let (url, server) = mock_server(vec![
                ("200 OK", CAPABILITIES),
                ("200 OK", r#"{"tables":{}}"#),
                (
                    "202 Accepted",
                    r#"{"query_id":"aaaabbbbccccddddeeeeffff00001111","state":"pre_cancelled","cancel_outcome":"pre_cancelled"}"#,
                ),
                (
                    "404 Not Found",
                    query_not_found_response("aaaabbbbccccddddeeeeffff00001111"),
                ),
            ]);
            let database = RemoteDatabase::connect_with_options(
                &url,
                RemoteOptions {
                    auth: Some(auth),
                    transport_timeout: Some(Duration::from_secs(2)),
                },
            )
            .unwrap();
            let query_id = "aaaabbbbccccddddeeeeffff00001111".parse().unwrap();
            assert_eq!(
                database.cancel_sql(query_id).unwrap(),
                RemoteCancelOutcome::PreCancelled
            );
            assert!(database.sql_query_status(query_id).unwrap().is_none());
            for request in server.join().unwrap() {
                assert!(request
                    .to_ascii_lowercase()
                    .contains(&expected.to_ascii_lowercase()));
            }
        }
    }

    #[test]
    fn auth_options_reject_empty_or_ambiguous_credentials() {
        for auth in [
            RemoteAuth::Bearer(SecretString::from(String::new())),
            RemoteAuth::Basic {
                username: String::new(),
                password: SecretString::from("secret".to_owned()),
            },
            RemoteAuth::Basic {
                username: "alice:admin".into(),
                password: SecretString::from("secret".to_owned()),
            },
        ] {
            assert!(remote_http_client(&RemoteOptions {
                auth: Some(auth),
                transport_timeout: None,
            })
            .is_err());
        }
        assert!(remote_http_client(&RemoteOptions {
            auth: None,
            transport_timeout: Some(Duration::ZERO),
        })
        .is_err());
    }

    #[test]
    fn credentials_in_remote_url_are_rejected() {
        let result = RemoteDatabase::connect_with_options(
            "https://alice:secret@example.test",
            RemoteOptions::default(),
        );
        let error = match result {
            Ok(_) => panic!("credential-bearing URL was accepted"),
            Err(error) => error,
        };
        assert!(
            matches!(error, KitError::Validation(message) if message.contains("RemoteOptions.auth"))
        );
    }

    #[test]
    fn query_and_fragment_in_remote_url_are_rejected() {
        for url in [
            "https://example.test?token=secret",
            "https://example.test#token",
        ] {
            let result = RemoteDatabase::connect_with_options(url, RemoteOptions::default());
            let error = match result {
                Ok(_) => panic!("query- or fragment-bearing URL was accepted"),
                Err(error) => error,
            };
            assert!(
                matches!(error, KitError::Validation(message) if message.contains("query or fragment"))
            );
        }
    }

    #[test]
    fn cancel_maps_structured_conflicts_and_not_found() {
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            (
                "409 Conflict",
                r#"{"query_id":"aaaabbbbccccddddeeeeffff00001111","state":"cancellation_requested","cancel_outcome":"accepted"}"#,
            ),
            (
                "409 Conflict",
                r#"{"query_id":"aaaabbbbccccddddeeeeffff00001111","state":"commit_critical","cancel_outcome":"too_late","committed":true}"#,
            ),
            (
                "200 OK",
                r#"{"query_id":"aaaabbbbccccddddeeeeffff00001111","state":"finished","cancel_outcome":"already_finished"}"#,
            ),
            (
                "404 Not Found",
                r#"{"query_id":"aaaabbbbccccddddeeeeffff00001111","state":"not_found","cancel_outcome":"not_found"}"#,
            ),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let query_id = "aaaabbbbccccddddeeeeffff00001111".parse().unwrap();
        assert!(database.cancel_sql(query_id).is_err());
        assert_eq!(
            database.cancel_sql(query_id).unwrap(),
            RemoteCancelOutcome::TooLate
        );
        assert_eq!(
            database.cancel_sql(query_id).unwrap(),
            RemoteCancelOutcome::AlreadyFinished
        );
        assert_eq!(
            database.cancel_sql(query_id).unwrap(),
            RemoteCancelOutcome::NotFound
        );
        assert_eq!(server.join().unwrap().len(), 6);
    }

    #[test]
    fn accepted_cancel_keeps_worker_durable_outcome() {
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            (
                "202 Accepted",
                r#"{"query_id":"ddddccccbbbbaaaa9999888877776666","state":"cancellation_requested","cancel_outcome":"accepted"}"#,
            ),
        ]);
        let database = RemoteDatabase::connect(&url).unwrap();
        let query_id: mongreldb_query::QueryId =
            "ddddccccbbbbaaaa9999888877776666".parse().unwrap();
        let worker = std::thread::spawn(move || {
            Err(KitError::Cancelled {
                query_id: query_id.to_string().into_boxed_str(),
                reason: "client_request".into(),
                outcome: Box::new(QueryExecutionOutcome {
                    committed: true,
                    committed_statements: Some(1),
                    last_commit_epoch: Some(17),
                    first_commit_statement_index: Some(0),
                    last_commit_statement_index: Some(0),
                    completed_statements: 1,
                    statement_index: 1,
                }),
                metadata: boxed_query_metadata(
                    Some("accepted"),
                    Some("client_request"),
                    Some(false),
                    Some("cancelled"),
                ),
            })
        });
        let handle = RemoteSqlQueryHandle {
            query_id,
            database,
            worker: Some(worker),
        };

        assert_eq!(handle.cancel().unwrap(), RemoteCancelOutcome::Accepted);
        assert!(matches!(
            handle.wait(),
            Err(KitError::Cancelled {
                outcome,
                ..
            }) if outcome.committed
                && outcome.committed_statements == Some(1)
                && outcome.last_commit_epoch == Some(17)
                && outcome.completed_statements == 1
                && outcome.statement_index == 1
        ));
        assert_eq!(server.join().unwrap().len(), 3);
    }

    #[test]
    fn cancelled_error_uses_structured_cancellation_reason() {
        let error = map_error_body(
            reqwest::StatusCode::CONFLICT,
            r#"{
                "error":{"code":"QUERY_CANCELLED","message":"query stopped"},
                "query_id":"ddddccccbbbbaaaa9999888877776666",
                "committed":false,
                "cancel_outcome":"accepted",
                "cancellation_reason":"client_request",
                "retryable":false,
                "server_state":"cancelled"
            }"#,
        );
        assert!(matches!(
            error,
            KitError::Cancelled { reason, metadata, .. }
                if reason.as_ref() == "client_request"
                    && metadata.cancel_outcome.as_deref() == Some("accepted")
                    && metadata.cancellation_reason.as_deref() == Some("client_request")
                    && metadata.retryable == Some(false)
                    && metadata.server_state.as_deref() == Some("cancelled")
        ));
    }

    #[test]
    fn non_sql_commit_outcome_keeps_committed_epoch_and_retryability() {
        let error = map_error_body(
            reqwest::StatusCode::CONFLICT,
            r#"{
                "status":"committed",
                "committed":true,
                "epoch":42,
                "epoch_text":"42",
                "retryable":false,
                "error":{"code":"COMMIT_OUTCOME","message":"commit completed"}
            }"#,
        );
        assert!(matches!(
            error,
            KitError::CommitOutcome {
                query_id,
                code,
                outcome,
                metadata,
                ..
            } if query_id == "unknown"
                && code.as_ref() == "COMMIT_OUTCOME"
                && outcome.committed
                && outcome.last_commit_epoch == Some(42)
                && metadata.retryable == Some(false)
        ));
    }

    #[test]
    fn transaction_procedure_and_trigger_routes_keep_commit_outcomes() {
        const BODY: &str = r#"{
            "status":"committed",
            "committed":true,
            "epoch":42,
            "epoch_text":"42",
            "retryable":false,
            "error":{"code":"COMMIT_OUTCOME","message":"commit completed"}
        }"#;
        let (url, server) = mock_server(vec![
            ("409 Conflict", BODY),
            ("409 Conflict", BODY),
            ("409 Conflict", BODY),
        ]);
        let database = RemoteDatabase {
            base_url: url,
            client: reqwest::blocking::Client::new(),
            schemas: HashMap::new(),
            sql_cancellation: None,
            sql_idempotency: None,
            sql_pagination: None,
        };
        let errors = [
            database
                .begin()
                .with_idempotency_key("txn-key")
                .commit()
                .unwrap_err(),
            database
                .create_procedure(&ProcedureSpec::new(json!({"name":"p"})))
                .unwrap_err(),
            database
                .create_trigger(&TriggerSpec::new(json!({"name":"t"})))
                .unwrap_err(),
        ];
        for error in errors {
            assert!(matches!(
                error,
                KitError::CommitOutcome {
                    outcome,
                    metadata,
                    ..
                } if outcome.committed
                    && outcome.last_commit_epoch == Some(42)
                    && metadata.retryable == Some(false)
            ));
        }
        let requests = server.join().unwrap();
        assert!(requests[0].starts_with("POST /kit/txn "));
        assert!(requests[0].contains("\"idempotency_key\":\"txn-key\""));
        assert!(requests[1].starts_with("POST /procedures "));
        assert!(requests[2].starts_with("POST /triggers "));
    }

    #[test]
    fn malformed_non_sql_write_success_is_outcome_unknown() {
        let (url, server) = mock_server(vec![("200 OK", "{")]);
        let database = RemoteDatabase {
            base_url: url,
            client: reqwest::blocking::Client::new(),
            schemas: HashMap::new(),
            sql_cancellation: None,
            sql_idempotency: None,
            sql_pagination: None,
        };
        let error = database
            .create_trigger(&TriggerSpec::new(json!({"name":"t"})))
            .unwrap_err();
        assert!(matches!(error, KitError::OutcomeUnknown { .. }));
        assert!(server.join().unwrap()[0].starts_with("POST /triggers "));
    }

    #[test]
    fn create_table_refresh_failure_preserves_known_commit() {
        let (url, server) = mock_server(vec![
            ("200 OK", r#"{"table_id":7,"table_id_text":"7"}"#),
            ("200 OK", r#"{}"#),
        ]);
        let mut database = RemoteDatabase {
            base_url: url,
            client: reqwest::blocking::Client::new(),
            schemas: HashMap::new(),
            sql_cancellation: None,
            sql_idempotency: None,
            sql_pagination: None,
        };
        let error = database
            .create_table(&json!({"name":"items","columns":[]}))
            .unwrap_err();
        assert!(matches!(
            error,
            KitError::CommitOutcome { outcome, .. } if outcome.committed
        ));
        let requests = server.join().unwrap();
        assert!(requests[0].starts_with("POST /kit/create_table "));
        assert!(requests[1].starts_with("GET /kit/schema "));
    }

    #[test]
    fn non_sql_unknown_outcome_stays_typed_and_non_retryable() {
        let error = map_error_body(
            reqwest::StatusCode::CONFLICT,
            r#"{
                "status":"outcome_unknown",
                "committed":null,
                "retryable":false,
                "error":{"code":"QUERY_OUTCOME_UNKNOWN","message":"commit status unknown"}
            }"#,
        );
        assert!(matches!(
            error,
            KitError::OutcomeUnknown {
                query_id,
                metadata,
                ..
            } if query_id == "unknown" && metadata.retryable == Some(false)
        ));
    }

    #[test]
    fn non_sql_commit_outcome_rejects_conflicting_exact_epoch() {
        let error = map_error_body(
            reqwest::StatusCode::CONFLICT,
            r#"{
                "status":"committed",
                "committed":true,
                "epoch":41,
                "epoch_text":"42",
                "retryable":false,
                "error":{"code":"COMMIT_OUTCOME","message":"commit completed"}
            }"#,
        );
        assert!(matches!(
            error,
            KitError::OutcomeUnknown {
                message,
                metadata,
                ..
            } if message.contains("conflicting or non-canonical exact commit epoch")
                && metadata.retryable == Some(false)
                && metadata.server_state.as_deref() == Some("invalid_outcome")
        ));
    }

    #[test]
    fn generic_error_rejects_conflicting_durable_fields() {
        for body in [
            r#"{
                "status":"committed",
                "committed":true,
                "epoch":42,
                "epoch_text":"42",
                "outcome":{"committed":false},
                "retryable":false,
                "error":{"code":"COMMIT_OUTCOME","message":"commit completed"}
            }"#,
            r#"{
                "status":"committed",
                "committed":true,
                "epoch":42,
                "epoch_text":"42",
                "last_commit_epoch":43,
                "last_commit_epoch_text":"43",
                "retryable":false,
                "error":{"code":"COMMIT_OUTCOME","message":"commit completed"}
            }"#,
            r#"{
                "status":"committed",
                "committed":true,
                "epoch":42,
                "epoch_text":"42",
                "outcome_known":false,
                "retryable":false,
                "error":{"code":"COMMIT_OUTCOME","message":"commit completed"}
            }"#,
        ] {
            assert!(matches!(
                map_error_body(reqwest::StatusCode::CONFLICT, body),
                KitError::OutcomeUnknown { .. }
            ));
        }
    }

    #[test]
    fn transaction_error_envelope_rejects_unknown_fields_and_false_commit_claims() {
        let valid = serde_json::json!({
            "status": "committed",
            "committed": true,
            "epoch": 42,
            "epoch_text": "42",
            "retryable": false,
            "error": {"code": "COMMIT_OUTCOME", "message": "published"}
        });
        assert!(validate_txn_error_envelope(&valid).is_ok());

        let mut unknown = valid.clone();
        unknown["committed_statements"] = serde_json::json!(1);
        assert!(validate_txn_error_envelope(&unknown).is_err());

        let mut false_claim = valid;
        false_claim["committed"] = serde_json::Value::Bool(false);
        assert!(validate_txn_error_envelope(&false_claim).is_err());
    }

    #[test]
    fn result_limit_error_keeps_structured_limits() {
        let body = r#"{"query_id":"aaaabbbbccccddddeeeeffff00001111","status":"failed_before_commit","terminal_state":"failed_before_commit","committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"cancel_outcome":"already_finished","cancellation_reason":"none","retryable":false,"server_state":"failed","outcome":{"committed":false,"committed_statements":0,"last_commit_epoch":null,"last_commit_epoch_text":null,"first_commit_statement_index":null,"last_commit_statement_index":null,"completed_statements":0,"statement_index":0,"serialization":"failed"},"error":{"code":"RESULT_LIMIT_EXCEEDED","message":"row limit exceeded","query_id":"aaaabbbbccccddddeeeeffff00001111","committed":false,"retryable":false,"max_rows":10,"max_bytes":1024}}"#;
        let (url, server) = mock_server(vec![
            ("200 OK", CAPABILITIES),
            ("200 OK", r#"{"tables":{}}"#),
            ("413 Payload Too Large", body),
        ]);
        let error = RemoteDatabase::connect(&url)
            .unwrap()
            .sql_rows_with_options(
                "SELECT * FROM t",
                RemoteSqlOptions {
                    query_id: Some("aaaabbbbccccddddeeeeffff00001111".parse().unwrap()),
                    ..RemoteSqlOptions::default()
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            KitError::ResultLimitExceeded {
                query_id: Some(query_id),
                max_rows: Some(max_rows),
                max_bytes: Some(max_bytes),
                outcome,
                ..
            } if query_id.as_ref() == "aaaabbbbccccddddeeeeffff00001111"
                && *max_rows == 10
                && *max_bytes == 1024
                && !outcome.committed
        ));
        assert_eq!(server.join().unwrap().len(), 3);
    }
}
