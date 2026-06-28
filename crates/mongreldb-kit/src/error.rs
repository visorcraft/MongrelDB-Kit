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
    #[error("storage error: {0}")]
    Storage(String),
    #[error("integrity error: {0}")]
    Integrity(String),
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
            MongrelError::Conflict(msg) => KitError::Conflict(msg),
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
