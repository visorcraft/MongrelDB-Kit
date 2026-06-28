//! Core, language-neutral model for MongrelDB Kit.
//!
//! This crate contains the schema model, key encoding, validation, constraint
//! planning, migration planning, and query AST used by the storage-backed
//! `mongreldb-kit` crate and by language bindings.

pub mod keys;
pub mod migrations;
pub mod planner;
pub mod query;
pub mod schema;
pub mod validation;

pub use keys::{encode_pk, encode_row_guard_key, encode_unique_key};
pub use migrations::{checksum, plan_migrations, Migration, MigrationOp};
pub use planner::{plan_delete, DeletePlan, PlannerError, RestrictedConstraint, RowRef, SetNullUpdate};
pub use query::{Delete, Direction, Expr, Insert, Literal, OrderBy, Query, Select, Update};
pub use schema::{
    CheckConstraint, Column, ColumnType, DefaultKind, ForeignKey, ForeignKeyAction, Index, Schema,
    SchemaError, Sequence, Table, UniqueConstraint,
};
pub use validation::{validate_row, ValidationError};
