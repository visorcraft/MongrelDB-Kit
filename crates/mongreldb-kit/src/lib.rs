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
pub mod tsv;
pub mod txn;

pub use db::{
    ApproxAggKind, ApproxAggregate, Database, ExplainPlan, IncrementalAggKind,
    IncrementalAggregate, OpenOptions, SimilarRow, SqlOptions, SqlQueryHandle,
};
// Re-export the engine tuning/config types so kit consumers (and the Python
// binding, which depends only on this crate) can reach them without a direct
// `mongreldb-core` dependency.
pub use error::{KitError, Result};
pub use migrate::migrate;
pub use mongreldb_core::auth::{Permission, RoleEntry, UserEntry};
pub use mongreldb_core::auth_state::{AuthState, RequiredPermission, TableAuthChecker};
pub use mongreldb_core::cache::CacheStats;
pub use mongreldb_core::{IndexBuildPolicy, TriggerConfig};
pub use mongreldb_query::{CancelOutcome, QueryId};
pub use query::JoinRow;
#[cfg(feature = "remote")]
pub use remote::{RemoteBatch, RemoteDatabase, RemoteOpResult, RemoteQueryRow, RemoteTransaction};
pub use schema::Row;
pub use txn::Transaction;

// Re-export the core model so downstream consumers can depend on a single crate.
pub use mongreldb_kit_core::*;
