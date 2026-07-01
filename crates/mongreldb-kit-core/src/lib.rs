//! Core, language-neutral model for MongrelDB Kit.
//!
//! This crate contains the schema model, key encoding, validation, constraint
//! planning, migration planning, and query AST used by the storage-backed
//! `mongreldb-kit` crate and by language bindings.

pub mod check;
pub mod keys;
pub mod migrations;
pub mod planner;
pub mod query;
pub mod schema;
pub mod validation;

pub use check::{eval_check, CheckParseError};
pub use keys::{
    decode_pk, encode_component, encode_pk, encode_row_guard_key, encode_unique_key, KeyComponent,
    KIT_KEY_VERSION,
};
pub use migrations::{migration_checksum, plan_migrations, Migration, MigrationOp};
pub use planner::{
    plan_delete, DeletePlan, PlannerError, RestrictedConstraint, RowRef, SetNullUpdate,
};
pub use query::{
    AggFunc, Aggregate, AggregateQuery, Cte, Delete, Direction, Expr, Insert, Join, JoinKind,
    JoinQuery, Literal, OnConflict, OrderBy, Query, Select, Update, Upsert,
};
pub use schema::{
    CheckConstraint, Column, ColumnType, DefaultKind, ForeignKey, ForeignKeyAction, Index,
    IndexKind, Schema, SchemaError, Sequence, Table, UniqueConstraint,
};
pub use validation::{validate_row, ValidationError};
