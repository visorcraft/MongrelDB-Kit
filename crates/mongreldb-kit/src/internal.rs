//! Reserved internal kit tables.
//!
//! MongrelDB Kit owns a set of `__kit_*` tables that back migrations, the schema
//! catalog, sequences, unique-key guards, row guards, and migration locks. These
//! mirror the TypeScript kit's `internalTables.ts` so all three languages share
//! the same on-disk shapes. They are ordinary core tables but are never exposed
//! through application table enumeration.

use crate::error::Result;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema as CoreSchema, TypeId};
use mongreldb_core::Database as CoreDatabase;

pub(crate) const MIGRATIONS_TABLE: &str = "__kit_schema_migrations";
pub(crate) const CATALOG: &str = "__kit_schema_catalog";
pub(crate) const SEQUENCES: &str = "__kit_sequences";
pub(crate) const UNIQUE_KEYS: &str = "__kit_unique_keys";
pub(crate) const ROW_GUARDS: &str = "__kit_row_guards";
pub(crate) const MIGRATION_LOCKS: &str = "__kit_migration_locks";

/// Column ids for the reserved tables. Ids are stable per table.
pub(crate) mod cols {
    // __kit_schema_migrations
    pub const MIG_VERSION: u16 = 1;
    pub const MIG_NAME: u16 = 2;
    pub const MIG_CHECKSUM: u16 = 3;
    pub const MIG_APPLIED: u16 = 4;
    pub const MIG_KIT_VERSION: u16 = 5;
    pub const MIG_STATUS: u16 = 6;

    // __kit_schema_catalog
    pub const CAT_VERSION: u16 = 1;
    pub const CAT_JSON: u16 = 2;
    pub const CAT_CHECKSUM: u16 = 3;
    pub const CAT_WRITTEN: u16 = 4;

    // __kit_sequences
    pub const SEQ_NAME: u16 = 1;
    pub const SEQ_NEXT: u16 = 2;
    pub const SEQ_UPDATED: u16 = 3;

    // __kit_unique_keys
    pub const UQ_ENCODED: u16 = 1;
    pub const UQ_CONSTRAINT: u16 = 2;
    pub const UQ_OWNER_TABLE: u16 = 3;
    pub const UQ_OWNER_PK: u16 = 4;
    pub const UQ_CREATED: u16 = 5;

    // __kit_row_guards
    pub const RG_ENCODED: u16 = 1;
    pub const RG_TABLE: u16 = 2;
    pub const RG_PK: u16 = 3;
    pub const RG_VERSION: u16 = 4;
    pub const RG_UPDATED: u16 = 5;

    // __kit_migration_locks
    pub const LOCK_NAME: u16 = 1;
    pub const LOCK_HOLDER: u16 = 2;
    pub const LOCK_ACQUIRED: u16 = 3;
    pub const LOCK_EXPIRES: u16 = 4;
}

fn pk(id: u16, name: &str, ty: TypeId) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        default_value: None,
    }
}

fn col(id: u16, name: &str, ty: TypeId) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: ColumnFlags::empty(),
        default_value: None,
    }
}

/// The reserved tables in `(name, schema)` form. Order is stable.
pub(crate) fn internal_tables_core() -> Vec<(&'static str, CoreSchema)> {
    vec![
        (
            MIGRATIONS_TABLE,
            CoreSchema {
                schema_id: u64::MAX - 1,
                columns: vec![
                    pk(cols::MIG_VERSION, "version", TypeId::Int64),
                    col(cols::MIG_NAME, "name", TypeId::Bytes),
                    col(cols::MIG_CHECKSUM, "checksum", TypeId::Bytes),
                    col(cols::MIG_APPLIED, "applied_at", TypeId::Bytes),
                    col(cols::MIG_KIT_VERSION, "kit_version", TypeId::Bytes),
                    col(cols::MIG_STATUS, "status", TypeId::Bytes),
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
        ),
        (
            CATALOG,
            CoreSchema {
                schema_id: u64::MAX - 2,
                columns: vec![
                    pk(cols::CAT_VERSION, "schema_version", TypeId::Int64),
                    col(cols::CAT_JSON, "schema_json", TypeId::Bytes),
                    col(cols::CAT_CHECKSUM, "checksum", TypeId::Bytes),
                    col(cols::CAT_WRITTEN, "written_at", TypeId::Bytes),
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
        ),
        (
            SEQUENCES,
            CoreSchema {
                schema_id: u64::MAX - 3,
                columns: vec![
                    pk(cols::SEQ_NAME, "sequence_name", TypeId::Bytes),
                    col(cols::SEQ_NEXT, "next_value", TypeId::Int64),
                    col(cols::SEQ_UPDATED, "updated_at", TypeId::Bytes),
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
        ),
        (
            UNIQUE_KEYS,
            CoreSchema {
                schema_id: u64::MAX - 4,
                columns: vec![
                    pk(cols::UQ_ENCODED, "encoded_key", TypeId::Bytes),
                    col(cols::UQ_CONSTRAINT, "constraint_name", TypeId::Bytes),
                    col(cols::UQ_OWNER_TABLE, "owner_table", TypeId::Bytes),
                    col(cols::UQ_OWNER_PK, "owner_pk", TypeId::Bytes),
                    col(cols::UQ_CREATED, "created_at", TypeId::Bytes),
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
        ),
        (
            ROW_GUARDS,
            CoreSchema {
                schema_id: u64::MAX - 5,
                columns: vec![
                    pk(cols::RG_ENCODED, "encoded_guard_key", TypeId::Bytes),
                    col(cols::RG_TABLE, "table_name", TypeId::Bytes),
                    col(cols::RG_PK, "primary_key", TypeId::Bytes),
                    col(cols::RG_VERSION, "version", TypeId::Int64),
                    col(cols::RG_UPDATED, "updated_at", TypeId::Bytes),
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
        ),
        (
            MIGRATION_LOCKS,
            CoreSchema {
                schema_id: u64::MAX - 6,
                columns: vec![
                    pk(cols::LOCK_NAME, "lock_name", TypeId::Bytes),
                    col(cols::LOCK_HOLDER, "holder", TypeId::Bytes),
                    col(cols::LOCK_ACQUIRED, "acquired_at", TypeId::Bytes),
                    col(cols::LOCK_EXPIRES, "expires_at", TypeId::Bytes),
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
        ),
    ]
}

/// Create any reserved tables that are missing. Idempotent.
pub(crate) fn ensure_internal_tables(db: &CoreDatabase) -> Result<()> {
    for (name, schema) in internal_tables_core() {
        crate::db::create_core_table(db, name, schema)?;
    }
    Ok(())
}

/// An RFC-3339 / ISO-8601 UTC timestamp string, e.g. `2024-01-02T03:04:05Z`.
///
/// Computed without external date crates using Howard Hinnant's civil-from-days
/// algorithm so guard/sequence rows and `now` defaults carry a human-readable,
/// cross-language timestamp.
pub(crate) fn iso_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (hour, minute, second) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Days since 1970-01-01 → (year, month, day). Valid for all reasonable dates.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}
