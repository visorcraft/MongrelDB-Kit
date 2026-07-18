//! Storage-backed MongrelDB Kit crate.
//!
//! This crate wraps MongrelDB core with the kit schema model, transaction
//! semantics, query execution, and migration runner.
pub mod arrow_util;
pub mod db;
pub mod error;
pub(crate) mod internal;
pub mod migrate;
pub mod pushdown;
pub mod query;
#[cfg(feature = "remote")]
pub mod remote;
pub mod schema;
pub mod search;
pub mod tsv;
pub mod txn;

pub use db::{
    ApproxAggKind, ApproxAggregate, Database, ExplainPlan, IncrementalAggKind,
    IncrementalAggregate, OpenOptions, SimilarRow, SqlOptions, SqlOutputLimits, SqlQueryHandle,
};
// Re-export the engine tuning/config types so kit consumers (and the Python
// binding, which depends only on this crate) can reach them without a direct
// `mongreldb-core` dependency.
pub use error::{KitError, QueryErrorMetadata, QueryExecutionOutcome, Result};
pub use migrate::migrate;
pub use mongreldb_core::auth::{Permission, RoleEntry, UserEntry};
pub use mongreldb_core::auth_state::{AuthState, RequiredPermission, TableAuthChecker};
pub use mongreldb_core::cache::CacheStats;
pub use mongreldb_core::{
    CancellationReason, EmbeddingError, EmbeddingModelMeta, EmbeddingProvider,
    EmbeddingProviderRegistry, EmbeddingSource as CoreEmbeddingSource, FixedVectorProvider,
    IndexBuildPolicy, TriggerConfig,
};
pub use mongreldb_query::{
    CancelOutcome, QueryId, QueryTerminalErrorCategory, QueryTerminalState, SerializationOutcome,
    SqlQueryPhase,
};
pub use query::JoinRow;
#[cfg(feature = "remote")]
pub use remote::{
    RemoteAuth, RemoteBatch, RemoteCancelOutcome, RemoteDatabase, RemoteIdempotentSqlOptions,
    RemoteOpResult, RemoteOptions, RemoteQueryRow, RemoteQueryStatus, RemoteSqlFormat,
    RemoteSqlOptions, RemoteSqlPage, RemoteSqlPageInfo, RemoteSqlPageLimits,
    RemoteSqlPaginationOptions, RemoteSqlQueryHandle, RemoteSqlReceiptError, RemoteSqlWriteReceipt,
    RemoteTransaction, SecretString, SqlCancellationCapabilities, SqlIdempotencyCapabilities,
    SqlPaginationCapabilities,
};
pub use schema::Row;
pub use search::{
    SearchComponent, SearchHit, SearchMetric, SearchRerank, SearchRetriever, SearchSpec,
};
pub use txn::Transaction;

// Re-export the core model so downstream consumers can depend on a single crate.
pub use mongreldb_kit_core::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct BuildInfo {
    pub artifact_version: &'static str,
    pub engine_version: &'static str,
    pub query_version: &'static str,
    pub kit_version: &'static str,
    pub mongreldb_git_sha: &'static str,
    pub kit_git_sha: &'static str,
}

pub fn build_info() -> BuildInfo {
    BuildInfo {
        artifact_version: env!("CARGO_PKG_VERSION"),
        engine_version: env!("CARGO_PKG_VERSION"),
        query_version: env!("CARGO_PKG_VERSION"),
        kit_version: env!("CARGO_PKG_VERSION"),
        mongreldb_git_sha: env!("MONGRELDB_GIT_SHA"),
        kit_git_sha: env!("MONGRELDB_KIT_GIT_SHA"),
    }
}

#[cfg(test)]
mod build_info_tests {
    #[test]
    fn build_info_reports_one_component_train() {
        let info = super::build_info();
        assert_eq!(info.artifact_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(info.engine_version, info.query_version);
        assert_eq!(info.query_version, info.kit_version);
        assert!(info.mongreldb_git_sha == "unknown" || info.mongreldb_git_sha.len() == 40);
        assert_eq!(info.kit_git_sha.len(), 40);
    }
}
