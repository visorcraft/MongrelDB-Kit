//! Storage-backed MongrelDB Kit crate.
//!
//! This crate wraps MongrelDB core with the kit schema model, transaction
//! semantics, query execution, and migration runner.

pub mod db;
pub mod error;
pub(crate) mod internal;
pub mod migrate;
pub mod query;
pub mod schema;
pub mod txn;

pub use db::Database;
pub use error::{KitError, Result};
pub use migrate::migrate;
pub use query::JoinRow;
pub use schema::Row;
pub use txn::Transaction;

// Re-export the core model so downstream consumers can depend on a single crate.
pub use mongreldb_kit_core::*;
