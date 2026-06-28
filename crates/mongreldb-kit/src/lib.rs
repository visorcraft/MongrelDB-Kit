//! Storage-backed MongrelDB Kit crate.
//!
//! This crate builds on `mongreldb-kit-core` and will eventually provide the
//! native database adapter, transaction runner, query builder, and migration
//! engine. For now it re-exports the core model so downstream consumers can
//! depend on a single crate.

pub use mongreldb_kit_core::*;
