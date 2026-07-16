//! Error model for `mongreldb-kit`.
//!
//! Storage errors from MongrelDB core and validation errors from the core model
//! are folded into a small, stable set of categories so consumers can handle
//! failures without depending on internal crate details.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, KitError>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryErrorMetadata {
    pub cancel_outcome: Option<String>,
    pub cancellation_reason: Option<String>,
    pub retryable: Option<bool>,
    pub server_state: Option<String>,
}

/// Durable and statement-level execution facts shared by structured SQL errors.
/// Boxed inside [`KitError`] so ordinary `Result<T>` values stay small enough
/// for efficient propagation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryExecutionOutcome {
    pub committed: bool,
    pub committed_statements: Option<usize>,
    pub last_commit_epoch: Option<u64>,
    pub first_commit_statement_index: Option<usize>,
    pub last_commit_statement_index: Option<usize>,
    pub completed_statements: usize,
    pub statement_index: usize,
}

/// A storage/transaction error in kit terminology.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum KitError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("duplicate key: {0}")]
    Duplicate(String),
    #[error("foreign key violation: {0}")]
    ForeignKey(String),
    #[error("restrict violation: {0}")]
    Restrict(String),
    #[error("migration error: {0}")]
    Migration(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("trigger validation error: {0}")]
    TriggerValidation(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("database locked: {0}")]
    DatabaseLocked(String),
    #[error("integrity error: {0}")]
    Integrity(String),
    #[error("authentication required: {0}")]
    AuthRequired(String),
    #[error("authentication not required: {0}")]
    AuthNotRequired(String),
    #[error("invalid credentials: {0}")]
    InvalidCredentials(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("query {query_id} cancelled: {reason}")]
    Cancelled {
        query_id: Box<str>,
        reason: Box<str>,
        outcome: Box<QueryExecutionOutcome>,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("query {query_id} deadline exceeded")]
    DeadlineExceeded {
        query_id: Box<str>,
        timeout_ms: Option<u64>,
        outcome: Box<QueryExecutionOutcome>,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("query id conflict: {query_id}")]
    QueryConflict {
        query_id: String,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("SQL query registry full: {message}")]
    QueryRegistryFull {
        query_id: Option<String>,
        message: Box<str>,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("query {query_id} commit outcome {code}: {message}")]
    CommitOutcome {
        query_id: String,
        code: Box<str>,
        outcome: Box<QueryExecutionOutcome>,
        message: Box<str>,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("query {query_id} failed ({code}): {message}")]
    QueryFailed {
        query_id: String,
        code: Box<str>,
        outcome: Box<QueryExecutionOutcome>,
        message: Box<str>,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("remote protocol error {code} (HTTP {status}): {message}")]
    RemoteProtocol {
        status: u16,
        code: Box<str>,
        query_id: Option<String>,
        message: Box<str>,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("query {query_id:?} result limit exceeded: {message}")]
    ResultLimitExceeded {
        query_id: Option<Box<str>>,
        max_rows: Option<Box<usize>>,
        max_bytes: Option<Box<usize>>,
        outcome: Box<QueryExecutionOutcome>,
        message: Box<str>,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("query {query_id:?} serialization failed: {message}")]
    SerializationFailed {
        query_id: Option<String>,
        outcome: Box<QueryExecutionOutcome>,
        message: Box<str>,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("query {query_id} outcome unknown: {message}")]
    OutcomeUnknown {
        query_id: String,
        message: String,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("transaction aborted: {message}")]
    TransactionAborted {
        query_id: Option<String>,
        message: String,
        metadata: Box<QueryErrorMetadata>,
    },
    #[error("unsupported feature: {0}")]
    Unsupported(String),
    #[error("capability unsupported: {0}")]
    CapabilityUnsupported(String),
    #[error("transport error for query {query_id}: {message}")]
    Transport {
        query_id: String,
        message: String,
        metadata: Box<QueryErrorMetadata>,
    },
}

impl KitError {
    pub fn query_outcome(&self) -> Option<&QueryExecutionOutcome> {
        match self {
            Self::Cancelled { outcome, .. }
            | Self::DeadlineExceeded { outcome, .. }
            | Self::CommitOutcome { outcome, .. }
            | Self::QueryFailed { outcome, .. }
            | Self::ResultLimitExceeded { outcome, .. }
            | Self::SerializationFailed { outcome, .. } => Some(outcome),
            _ => None,
        }
    }

    pub fn query_metadata(&self) -> Option<&QueryErrorMetadata> {
        match self {
            Self::Cancelled { metadata, .. }
            | Self::DeadlineExceeded { metadata, .. }
            | Self::QueryConflict { metadata, .. }
            | Self::QueryRegistryFull { metadata, .. }
            | Self::CommitOutcome { metadata, .. }
            | Self::QueryFailed { metadata, .. }
            | Self::RemoteProtocol { metadata, .. }
            | Self::ResultLimitExceeded { metadata, .. }
            | Self::SerializationFailed { metadata, .. }
            | Self::OutcomeUnknown { metadata, .. }
            | Self::TransactionAborted { metadata, .. }
            | Self::Transport { metadata, .. } => Some(metadata),
            _ => None,
        }
    }

    fn query_metadata_mut(&mut self) -> Option<&mut QueryErrorMetadata> {
        match self {
            Self::Cancelled { metadata, .. }
            | Self::DeadlineExceeded { metadata, .. }
            | Self::QueryConflict { metadata, .. }
            | Self::QueryRegistryFull { metadata, .. }
            | Self::CommitOutcome { metadata, .. }
            | Self::QueryFailed { metadata, .. }
            | Self::RemoteProtocol { metadata, .. }
            | Self::ResultLimitExceeded { metadata, .. }
            | Self::SerializationFailed { metadata, .. }
            | Self::OutcomeUnknown { metadata, .. }
            | Self::TransactionAborted { metadata, .. }
            | Self::Transport { metadata, .. } => Some(metadata),
            _ => None,
        }
    }
}

pub(crate) fn boxed_query_metadata(
    cancel_outcome: Option<&str>,
    cancellation_reason: Option<&str>,
    retryable: Option<bool>,
    server_state: Option<&str>,
) -> Box<QueryErrorMetadata> {
    Box::new(QueryErrorMetadata {
        cancel_outcome: cancel_outcome.map(str::to_owned),
        cancellation_reason: cancellation_reason.map(str::to_owned),
        retryable,
        server_state: server_state.map(str::to_owned),
    })
}

impl From<std::io::Error> for KitError {
    fn from(e: std::io::Error) -> Self {
        KitError::Storage(e.to_string())
    }
}

impl From<mongreldb_core::MongrelError> for KitError {
    fn from(e: mongreldb_core::MongrelError) -> Self {
        use mongreldb_core::MongrelError;
        match e {
            MongrelError::TriggerValidation(msg) => KitError::TriggerValidation(msg),
            MongrelError::Conflict(msg) => KitError::Conflict(msg),
            MongrelError::InvalidArgument(msg) => KitError::Validation(msg),
            MongrelError::Schema(msg) => KitError::Validation(msg),
            MongrelError::ColumnNotFound(msg) => KitError::Integrity(msg),
            MongrelError::NotFound(msg) => KitError::Integrity(msg),
            MongrelError::Io(e) => KitError::Storage(e.to_string()),
            MongrelError::DatabaseLocked { .. } => KitError::DatabaseLocked(e.to_string()),
            MongrelError::Serialization(e) => KitError::Storage(e.to_string()),
            MongrelError::ChecksumMismatch { .. }
            | MongrelError::MagicMismatch { .. }
            | MongrelError::CorruptWal { .. }
            | MongrelError::TornWrite { .. } => KitError::Integrity(e.to_string()),
            MongrelError::EncryptionDisabled
            | MongrelError::Encryption(_)
            | MongrelError::Decryption(_) => KitError::Integrity(e.to_string()),
            MongrelError::Full(msg) => KitError::Storage(msg),
            MongrelError::Other(msg) => KitError::Storage(msg),
            MongrelError::AuthRequired => KitError::AuthRequired(e.to_string()),
            MongrelError::AuthNotRequired => KitError::AuthNotRequired(e.to_string()),
            MongrelError::InvalidCredentials { username } => KitError::InvalidCredentials(username),
            MongrelError::PermissionDenied {
                required,
                principal,
            } => KitError::PermissionDenied(format!("{principal} lacks {required}")),
            MongrelError::DurableCommit { epoch, message } => KitError::CommitOutcome {
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
                message: message.into_boxed_str(),
                metadata: boxed_query_metadata(None, None, Some(false), None),
            },
            MongrelError::CommitOutcomeUnknown { message, .. } => KitError::OutcomeUnknown {
                query_id: "unknown".into(),
                message,
                metadata: boxed_query_metadata(None, None, Some(false), None),
            },
            _ => KitError::Storage(e.to_string()),
        }
    }
}

impl From<mongreldb_kit_core::schema::SchemaError> for KitError {
    fn from(e: mongreldb_kit_core::schema::SchemaError) -> Self {
        KitError::Validation(e.to_string())
    }
}

impl From<mongreldb_kit_core::validation::ValidationError> for KitError {
    fn from(e: mongreldb_kit_core::validation::ValidationError) -> Self {
        KitError::Validation(e.to_string())
    }
}

impl From<mongreldb_kit_core::planner::PlannerError> for KitError {
    fn from(e: mongreldb_kit_core::planner::PlannerError) -> Self {
        match e {
            mongreldb_kit_core::planner::PlannerError::TableNotFound(msg) => {
                KitError::Integrity(msg)
            }
            mongreldb_kit_core::planner::PlannerError::CircularDelete(msg) => {
                KitError::Restrict(msg)
            }
        }
    }
}

impl From<serde_json::Error> for KitError {
    fn from(e: serde_json::Error) -> Self {
        KitError::Storage(e.to_string())
    }
}

impl From<mongreldb_query::MongrelQueryError> for KitError {
    fn from(e: mongreldb_query::MongrelQueryError) -> Self {
        use mongreldb_query::MongrelQueryError;
        match e {
            // Core errors carry the engine's declarative constraint failures
            // (unique / FK / check / conflict) — route them through the same
            // mapping as direct core errors so callers see the right category.
            MongrelQueryError::Core(core) => KitError::from(core),
            MongrelQueryError::Schema(msg) => KitError::Validation(msg),
            MongrelQueryError::Arrow(msg) | MongrelQueryError::DataFusion(msg) => {
                KitError::Storage(msg)
            }
            MongrelQueryError::QueryCancelled {
                query_id,
                reason,
                committed,
                committed_statements,
                last_commit_epoch,
                first_commit_statement_index,
                last_commit_statement_index,
                completed_statements,
                cancelled_statement_index,
            } => KitError::Cancelled {
                query_id: query_id.to_string().into_boxed_str(),
                reason: cancellation_reason_name(reason).into(),
                outcome: Box::new(QueryExecutionOutcome {
                    committed,
                    committed_statements: Some(committed_statements),
                    last_commit_epoch,
                    first_commit_statement_index,
                    last_commit_statement_index,
                    completed_statements,
                    statement_index: cancelled_statement_index,
                }),
                metadata: boxed_query_metadata(
                    None,
                    Some(cancellation_reason_name(reason)),
                    Some(false),
                    None,
                ),
            },
            MongrelQueryError::DeadlineExceeded {
                query_id,
                timeout_ms,
                committed,
                committed_statements,
                last_commit_epoch,
                first_commit_statement_index,
                last_commit_statement_index,
                completed_statements,
                cancelled_statement_index,
            } => KitError::DeadlineExceeded {
                query_id: query_id.to_string().into_boxed_str(),
                timeout_ms,
                outcome: Box::new(QueryExecutionOutcome {
                    committed,
                    committed_statements: Some(committed_statements),
                    last_commit_epoch,
                    first_commit_statement_index,
                    last_commit_statement_index,
                    completed_statements,
                    statement_index: cancelled_statement_index,
                }),
                metadata: boxed_query_metadata(None, Some("deadline"), Some(false), None),
            },
            MongrelQueryError::QueryIdConflict { query_id } => KitError::QueryConflict {
                query_id: query_id.to_string(),
                metadata: boxed_query_metadata(None, None, Some(false), None),
            },
            MongrelQueryError::QueryRegistryFull => KitError::QueryRegistryFull {
                query_id: None,
                message: "SQL query registry is full".into(),
                metadata: boxed_query_metadata(None, None, Some(true), None),
            },
            MongrelQueryError::CommitOutcome {
                query_id,
                committed,
                committed_statements,
                last_commit_epoch,
                first_commit_statement_index,
                last_commit_statement_index,
                completed_statements,
                statement_index,
                message,
            } => KitError::CommitOutcome {
                query_id: query_id.to_string(),
                code: "COMMIT_OUTCOME".into(),
                outcome: Box::new(QueryExecutionOutcome {
                    committed,
                    committed_statements: Some(committed_statements),
                    last_commit_epoch,
                    first_commit_statement_index,
                    last_commit_statement_index,
                    completed_statements,
                    statement_index,
                }),
                message: message.into_boxed_str(),
                metadata: boxed_query_metadata(None, None, Some(false), None),
            },
            MongrelQueryError::ResultLimitExceeded {
                query_id,
                committed,
                committed_statements,
                last_commit_epoch,
                first_commit_statement_index,
                last_commit_statement_index,
                completed_statements,
                statement_index,
                message,
            } => KitError::ResultLimitExceeded {
                query_id: Some(query_id.to_string().into_boxed_str()),
                max_rows: None,
                max_bytes: None,
                outcome: Box::new(QueryExecutionOutcome {
                    committed,
                    committed_statements: Some(committed_statements),
                    last_commit_epoch,
                    first_commit_statement_index,
                    last_commit_statement_index,
                    completed_statements,
                    statement_index,
                }),
                message: message.into_boxed_str(),
                metadata: boxed_query_metadata(None, None, Some(false), None),
            },
            MongrelQueryError::OutcomeUnknown { query_id, message } => KitError::OutcomeUnknown {
                query_id: query_id.to_string(),
                message,
                metadata: boxed_query_metadata(None, None, Some(false), None),
            },
            MongrelQueryError::TransactionAborted => KitError::TransactionAborted {
                query_id: None,
                message: "ROLLBACK or ROLLBACK TO SAVEPOINT is required".into(),
                metadata: boxed_query_metadata(None, None, Some(false), None),
            },
            _ => KitError::Storage(e.to_string()),
        }
    }
}

fn cancellation_reason_name(reason: mongreldb_core::CancellationReason) -> &'static str {
    use mongreldb_core::CancellationReason;
    match reason {
        CancellationReason::None => "none",
        CancellationReason::ClientRequest => "client_request",
        CancellationReason::Deadline => "deadline",
        CancellationReason::ClientDisconnected => "client_disconnected",
        CancellationReason::SessionClosed => "session_closed",
        CancellationReason::ServerShutdown => "server_shutdown",
    }
}

fn query_phase_name(phase: mongreldb_query::SqlQueryPhase) -> &'static str {
    use mongreldb_query::SqlQueryPhase;
    match phase {
        SqlQueryPhase::Queued => "queued",
        SqlQueryPhase::Planning => "planning",
        SqlQueryPhase::Executing => "executing",
        SqlQueryPhase::Streaming => "streaming",
        SqlQueryPhase::Serializing => "serializing",
        SqlQueryPhase::CommitCritical => "commit_critical",
        SqlQueryPhase::Cancelling => "cancelling",
        SqlQueryPhase::Completed => "completed",
        SqlQueryPhase::Failed => "failed",
        SqlQueryPhase::Cancelled => "cancelled",
    }
}

fn execution_outcome_from_status(status: &mongreldb_query::QueryStatus) -> QueryExecutionOutcome {
    QueryExecutionOutcome {
        committed: status.durable_outcome.committed,
        committed_statements: Some(status.durable_outcome.committed_statements),
        last_commit_epoch: status.durable_outcome.last_commit_epoch,
        first_commit_statement_index: status.durable_outcome.first_commit_statement_index,
        last_commit_statement_index: status.durable_outcome.last_commit_statement_index,
        completed_statements: status.completed_statements,
        statement_index: status.statement_index,
    }
}

fn merge_execution_outcome(
    mut outcome: QueryExecutionOutcome,
    status: Option<&mongreldb_query::QueryStatus>,
) -> QueryExecutionOutcome {
    let Some(status) = status else {
        return outcome;
    };
    let current = execution_outcome_from_status(status);
    outcome.committed |= current.committed;
    outcome.committed_statements =
        merge_count(outcome.committed_statements, current.committed_statements);
    outcome.last_commit_epoch = current.last_commit_epoch.or(outcome.last_commit_epoch);
    outcome.first_commit_statement_index = current
        .first_commit_statement_index
        .or(outcome.first_commit_statement_index);
    outcome.last_commit_statement_index = current
        .last_commit_statement_index
        .or(outcome.last_commit_statement_index);
    outcome.completed_statements = current.completed_statements;
    outcome.statement_index = current.statement_index;
    outcome
}

pub(crate) fn query_error_with_status(
    error: mongreldb_query::MongrelQueryError,
    status: Option<&mongreldb_query::QueryStatus>,
) -> KitError {
    let committed = status.is_some_and(|status| status.durable_outcome.committed);
    let query_id = status.map(|status| status.query_id.to_string());
    let mut error = KitError::from(error);
    if let (Some(metadata), Some(status)) = (error.query_metadata_mut(), status) {
        metadata
            .cancellation_reason
            .get_or_insert_with(|| cancellation_reason_name(status.cancellation_reason).into());
        metadata
            .server_state
            .get_or_insert_with(|| query_phase_name(status.phase).into());
    }
    if status.is_some_and(|status| status.outcome_unknown) {
        return KitError::OutcomeUnknown {
            query_id: query_id.unwrap_or_else(|| "unknown".into()),
            message: error.to_string(),
            metadata: status.map_or_else(
                || boxed_query_metadata(None, None, Some(false), None),
                |status| {
                    boxed_query_metadata(
                        None,
                        Some(cancellation_reason_name(status.cancellation_reason)),
                        Some(false),
                        Some(query_phase_name(status.phase)),
                    )
                },
            ),
        };
    }
    match error {
        KitError::Cancelled {
            query_id,
            reason,
            outcome,
            metadata,
        } => KitError::Cancelled {
            query_id,
            reason,
            outcome: Box::new(merge_execution_outcome(*outcome, status)),
            metadata,
        },
        KitError::DeadlineExceeded {
            query_id,
            timeout_ms,
            outcome,
            metadata,
        } => KitError::DeadlineExceeded {
            query_id,
            timeout_ms,
            outcome: Box::new(merge_execution_outcome(*outcome, status)),
            metadata,
        },
        KitError::CommitOutcome {
            query_id: error_query_id,
            code,
            outcome,
            message,
            metadata,
        } => KitError::CommitOutcome {
            query_id: if error_query_id == "unknown" {
                query_id.unwrap_or(error_query_id)
            } else {
                error_query_id
            },
            code,
            outcome: Box::new(merge_execution_outcome(*outcome, status)),
            message,
            metadata,
        },
        KitError::ResultLimitExceeded {
            query_id: error_query_id,
            max_rows,
            max_bytes,
            outcome,
            message,
            metadata,
        } => KitError::ResultLimitExceeded {
            query_id: error_query_id.or_else(|| query_id.map(String::into_boxed_str)),
            max_rows,
            max_bytes,
            outcome: Box::new(merge_execution_outcome(*outcome, status)),
            message,
            metadata,
        },
        KitError::SerializationFailed {
            query_id: error_query_id,
            outcome,
            message,
            metadata,
        } => KitError::SerializationFailed {
            query_id: error_query_id.or(query_id),
            outcome: Box::new(merge_execution_outcome(*outcome, status)),
            message,
            metadata,
        },
        KitError::TransactionAborted {
            query_id: error_query_id,
            message,
            metadata,
        } => KitError::TransactionAborted {
            query_id: error_query_id.or(query_id),
            message,
            metadata,
        },
        KitError::OutcomeUnknown {
            query_id: error_query_id,
            message,
            metadata,
        } => KitError::OutcomeUnknown {
            query_id: if error_query_id == "unknown" {
                query_id.unwrap_or(error_query_id)
            } else {
                error_query_id
            },
            message,
            metadata,
        },
        error if committed => KitError::CommitOutcome {
            query_id: query_id.unwrap_or_else(|| "unknown".into()),
            code: "COMMIT_OUTCOME".into(),
            outcome: Box::new(
                status
                    .map(execution_outcome_from_status)
                    .unwrap_or_default(),
            ),
            message: error.to_string().into_boxed_str(),
            metadata: status.map_or_else(
                || boxed_query_metadata(None, None, Some(false), None),
                |status| {
                    boxed_query_metadata(
                        None,
                        Some(cancellation_reason_name(status.cancellation_reason)),
                        Some(false),
                        Some(query_phase_name(status.phase)),
                    )
                },
            ),
        },
        error => error,
    }
}

fn merge_count(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

#[cfg(test)]
mod tests {
    use super::{query_error_with_status, KitError};

    #[test]
    fn maps_trigger_core_errors_without_message_parsing() {
        let trigger = KitError::from(mongreldb_core::MongrelError::TriggerValidation(
            "trigger raised".into(),
        ));
        assert_eq!(
            trigger,
            KitError::TriggerValidation("trigger raised".into())
        );

        let conflict = KitError::from(mongreldb_core::MongrelError::Conflict(
            "unrelated conflict contains trigger wording".into(),
        ));
        assert_eq!(
            conflict,
            KitError::Conflict("unrelated conflict contains trigger wording".into())
        );

        let invalid = KitError::from(mongreldb_core::MongrelError::InvalidArgument(
            "unrelated invalid argument mentions external trigger bridge".into(),
        ));
        assert_eq!(
            invalid,
            KitError::Validation(
                "unrelated invalid argument mentions external trigger bridge".into()
            )
        );
    }

    #[test]
    fn maps_query_registry_and_commit_outcomes_without_strings() {
        assert!(matches!(
            KitError::from(mongreldb_query::MongrelQueryError::QueryRegistryFull),
            KitError::QueryRegistryFull { query_id: None, .. }
        ));
        let query_id = mongreldb_query::QueryId::random().unwrap();
        assert!(matches!(
            KitError::from(mongreldb_query::MongrelQueryError::CommitOutcome {
                query_id,
                committed: true,
                committed_statements: 1,
                last_commit_epoch: Some(7),
                first_commit_statement_index: Some(0),
                last_commit_statement_index: Some(0),
                completed_statements: 1,
                statement_index: 0,
                message: "post-commit refresh failed".into(),
            }),
            KitError::CommitOutcome {
                outcome,
                ..
            } if outcome.committed
                && outcome.committed_statements == Some(1)
                && outcome.last_commit_epoch == Some(7)
        ));
    }

    #[test]
    fn committed_status_wraps_unknown_post_commit_errors() {
        let registry = std::sync::Arc::new(mongreldb_query::SqlQueryRegistry::default());
        let query = registry
            .register(mongreldb_query::SqlQueryOptions::default())
            .unwrap();
        let query_id = query.id().to_string();
        query.record_commit(2, 7);
        let status = query.status();

        let error = query_error_with_status(
            mongreldb_query::MongrelQueryError::Schema("refresh failed".into()),
            Some(&status),
        );
        assert!(matches!(
            error,
            KitError::CommitOutcome {
                query_id: actual,
                code,
                outcome,
                ..
            } if actual == query_id
                && code.as_ref() == "COMMIT_OUTCOME"
                && outcome.committed
                && outcome.committed_statements == Some(1)
                && outcome.last_commit_epoch == Some(7)
                && outcome.first_commit_statement_index == Some(2)
                && outcome.last_commit_statement_index == Some(2)
        ));
    }

    #[test]
    fn cancellation_keeps_exact_durable_receipt() {
        let query_id = mongreldb_query::QueryId::random().unwrap();
        let error = KitError::from(mongreldb_query::MongrelQueryError::QueryCancelled {
            query_id,
            reason: mongreldb_core::CancellationReason::ClientRequest,
            committed: true,
            committed_statements: 2,
            last_commit_epoch: Some(19),
            first_commit_statement_index: Some(0),
            last_commit_statement_index: Some(3),
            completed_statements: 3,
            cancelled_statement_index: 4,
        });
        assert!(matches!(
            error,
            KitError::Cancelled {
                outcome,
                ..
            } if outcome.committed
                && outcome.committed_statements == Some(2)
                && outcome.last_commit_epoch == Some(19)
                && outcome.first_commit_statement_index == Some(0)
                && outcome.last_commit_statement_index == Some(3)
                && outcome.completed_statements == 3
                && outcome.statement_index == 4
        ));
    }

    #[test]
    fn kit_error_stays_compact() {
        assert!(std::mem::size_of::<KitError>() <= 128);
    }
}
