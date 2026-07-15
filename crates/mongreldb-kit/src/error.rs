//! Error model for `mongreldb-kit`.
//!
//! Storage errors from MongrelDB core and validation errors from the core model
//! are folded into a small, stable set of categories so consumers can handle
//! failures without depending on internal crate details.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, KitError>;

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
    Cancelled { query_id: String, reason: String },
    #[error("query {query_id} deadline exceeded")]
    DeadlineExceeded {
        query_id: String,
        timeout_ms: Option<u64>,
    },
    #[error("query id conflict: {0}")]
    QueryConflict(String),
    #[error("transaction aborted: {0}")]
    TransactionAborted(String),
    #[error("unsupported feature: {0}")]
    Unsupported(String),
    #[error("transport error for query {query_id}: {message}")]
    Transport { query_id: String, message: String },
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
            MongrelError::Conflict(msg) if is_trigger_error(&msg) => {
                KitError::TriggerValidation(msg)
            }
            MongrelError::Conflict(msg) => KitError::Conflict(msg),
            MongrelError::InvalidArgument(msg) if is_trigger_error(&msg) => {
                KitError::TriggerValidation(msg)
            }
            MongrelError::InvalidArgument(msg) => KitError::Validation(msg),
            MongrelError::Schema(msg) => KitError::Validation(msg),
            MongrelError::ColumnNotFound(msg) => KitError::Integrity(msg),
            MongrelError::NotFound(msg) => KitError::Integrity(msg),
            MongrelError::Io(e) => KitError::Storage(e.to_string()),
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
            _ => KitError::Storage(e.to_string()),
        }
    }
}

fn is_trigger_error(message: &str) -> bool {
    message.contains("trigger ")
        || message.contains("Trigger ")
        || message.contains("external trigger bridge")
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
                query_id, reason, ..
            } => KitError::Cancelled {
                query_id: query_id.to_string(),
                reason: format!("{reason:?}"),
            },
            MongrelQueryError::DeadlineExceeded {
                query_id,
                timeout_ms,
                ..
            } => KitError::DeadlineExceeded {
                query_id: query_id.to_string(),
                timeout_ms,
            },
            MongrelQueryError::QueryIdConflict { query_id } => {
                KitError::QueryConflict(query_id.to_string())
            }
            MongrelQueryError::TransactionAborted => {
                KitError::TransactionAborted("only ROLLBACK is allowed".into())
            }
            _ => KitError::Storage(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KitError;

    #[test]
    fn maps_trigger_core_errors_to_trigger_validation() {
        let conflict = KitError::from(mongreldb_core::MongrelError::Conflict(
            "trigger raised".into(),
        ));
        assert_eq!(
            conflict,
            KitError::TriggerValidation("trigger raised".into())
        );

        let invalid = KitError::from(mongreldb_core::MongrelError::InvalidArgument(
            "external trigger bridge rejected".into(),
        ));
        assert_eq!(
            invalid,
            KitError::TriggerValidation("external trigger bridge rejected".into())
        );
    }
}
