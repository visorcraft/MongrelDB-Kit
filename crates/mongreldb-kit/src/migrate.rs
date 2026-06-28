//! Migration runner for `mongreldb-kit`.

use crate::db::internal_bytes;
use crate::error::{KitError, Result};
use crate::internal::{
    cols, ensure_internal_tables, iso_now, MIGRATIONS_TABLE, ROW_GUARDS, UNIQUE_KEYS,
};
use crate::schema::{core_row_to_json, to_core_schema, Row as KitRow};
use crate::txn::{encoded_pk_for, fk_values_null, parent_pk_components, unique_key};
use mongreldb_core::memtable::{Row as CoreRow, Value as CoreValue};
use mongreldb_core::Database as CoreDatabase;
use mongreldb_kit_core::keys::{encode_pk, encode_row_guard_key, KeyComponent};
use mongreldb_kit_core::migrations::{plan_migrations, Migration, MigrationOp};
use mongreldb_kit_core::schema::{Schema as KitSchema, Table as KitTable};
use std::collections::{HashMap, HashSet};

/// Run pending migrations against `db`.
///
/// Creates internal tables if missing, applies each pending migration in
/// version order, and records it in `_migrations`.
pub fn migrate(db: &mut crate::db::Database, migrations: &[Migration]) -> Result<()> {
    let core = db.core_db();

    // Ensure internal tables exist (idempotent on an already-created DB).
    ensure_internal_tables(core)?;

    let applied = load_applied_migrations(core)?;
    let pending = plan_migrations(&applied, migrations);

    if pending.is_empty() {
        return Ok(());
    }

    // Apply DDL ops directly. Each op is individually durable in core; the
    // migration record transaction below makes the applied versions atomic.
    for migration in &pending {
        apply_migration_ops(core, migration, &db.schema)?;
    }

    // Record all newly-applied migrations in one transaction.
    let mut txn = core.begin();
    for migration in &pending {
        record_migration(&mut txn, migration)?;
    }
    txn.commit().map_err(KitError::from)?;

    // Persist the updated schema and reload it.
    crate::db::persist_schema(db, &db.schema)?;
    let fresh = crate::db::load_schema(db.root())?;
    db.set_schema(fresh);
    Ok(())
}

pub fn load_applied_migrations(core: &CoreDatabase) -> Result<Vec<Migration>> {
    let handle = core.table(MIGRATIONS_TABLE).map_err(KitError::from)?;
    let guard = handle.lock();
    let snapshot = guard.snapshot();
    let rows = guard.visible_rows(snapshot).map_err(KitError::from)?;

    let mut out: Vec<Migration> = rows
        .into_iter()
        .filter_map(|r| migration_from_row(&r).ok())
        .collect();
    out.sort_by_key(|m| m.version);
    Ok(out)
}

fn migration_from_row(row: &CoreRow) -> Result<Migration> {
    let version = match row.columns.get(&1).cloned().unwrap_or(CoreValue::Null) {
        CoreValue::Int64(i) => i,
        _ => return Err(KitError::Integrity("migration version missing".into())),
    };
    let name = bytes_string(row.columns.get(&2))?.unwrap_or_default();
    Ok(Migration {
        version,
        name,
        ops: Vec::new(),
    })
}

fn bytes_string(value: Option<&CoreValue>) -> Result<Option<String>> {
    match value {
        Some(CoreValue::Bytes(b)) => Ok(Some(
            String::from_utf8(b.clone()).map_err(|e| KitError::Integrity(e.to_string()))?,
        )),
        Some(CoreValue::Null) | None => Ok(None),
        _ => Err(KitError::Integrity("expected bytes value".into())),
    }
}

fn apply_migration_ops(
    core: &CoreDatabase,
    migration: &Migration,
    schema: &KitSchema,
) -> Result<()> {
    for op in &migration.ops {
        match op {
            MigrationOp::CreateTable { name } => {
                if let Some(table) = schema.table(name) {
                    if core.table_id(name).is_err() {
                        core.create_table(name, to_core_schema(table))
                            .map_err(KitError::from)?;
                    }
                }
            }
            MigrationOp::DropTable { name } => {
                // Drop is best-effort (idempotent), then forget the table's
                // unique-key and row guards.
                let _ = core.drop_table(name);
                clean_table_guards(core, name)?;
            }
            MigrationOp::AddColumn { table, column } => {
                if let Some(t) = schema.table(table) {
                    if let Some(col) = t.column(column) {
                        let handle = core.table(table).map_err(KitError::from)?;
                        let mut guard = handle.lock();
                        guard
                            .add_column(column, crate::schema::to_core_type(col.storage_type))
                            .map_err(KitError::from)?;
                    }
                }
            }
            MigrationOp::AddUnique { table, constraint } => {
                backfill_unique(core, schema, table, constraint)?;
            }
            MigrationOp::DropUnique { table, constraint } => {
                drop_unique_guards(core, table, constraint)?;
            }
            MigrationOp::AddForeignKey { table, constraint } => {
                backfill_foreign_key(core, schema, table, constraint)?;
            }
            MigrationOp::AddCheck { .. }
            | MigrationOp::DropCheck { .. }
            | MigrationOp::DropForeignKey { .. } => {
                // Metadata-only: check evaluation and foreign-key enforcement are
                // driven by the re-persisted schema, so there is no catalog or
                // guard mutation to perform here.
            }
            MigrationOp::DropColumn { table, column } => {
                return Err(KitError::Migration(format!(
                    "migration op drop_column ({table}.{column}) requires a table rebuild \
                     and is not supported by the Rust runner yet"
                )));
            }
            MigrationOp::AddIndex { table, index } => {
                return Err(KitError::Migration(format!(
                    "migration op add_index ({index} on {table}) requires a table rebuild \
                     and is not supported by the Rust runner yet"
                )));
            }
            MigrationOp::DropIndex { table, index } => {
                return Err(KitError::Migration(format!(
                    "migration op drop_index ({index} on {table}) requires a table rebuild \
                     and is not supported by the Rust runner yet"
                )));
            }
            MigrationOp::RawSql(sql) => {
                return Err(KitError::Migration(format!(
                    "RawSql migration op is not supported: {sql}"
                )));
            }
        }
    }
    Ok(())
}

/// All visible rows of an internal `__kit_*` table as raw core rows.
fn visible_internal_rows(core: &CoreDatabase, name: &str) -> Result<Vec<CoreRow>> {
    let handle = core.table(name).map_err(KitError::from)?;
    let guard = handle.lock();
    let snapshot = guard.snapshot();
    guard.visible_rows(snapshot).map_err(KitError::from)
}

/// All visible rows of an application table as kit JSON rows.
fn visible_app_rows(core: &CoreDatabase, table: &KitTable) -> Result<Vec<KitRow>> {
    visible_internal_rows(core, &table.name)?
        .iter()
        .map(|r| core_row_to_json(r, table))
        .collect()
}

/// Backfill `__kit_unique_keys` guards for an added unique constraint (PLAN
/// "Migrations"). Rows whose unique columns are all non-null reserve a guard;
/// two rows producing the same key means existing data already violates the
/// constraint, which rejects the migration. Existing guards are left untouched,
/// so re-running is idempotent.
fn backfill_unique(
    core: &CoreDatabase,
    schema: &KitSchema,
    table_name: &str,
    constraint: &str,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| {
        KitError::Migration(format!("add_unique: table {table_name} not found in schema"))
    })?;
    let uq = table
        .unique_constraints
        .iter()
        .find(|u| u.name == constraint)
        .ok_or_else(|| {
            KitError::Migration(format!(
                "add_unique: unique constraint {constraint} not found on table {table_name}"
            ))
        })?;

    let rows = visible_app_rows(core, table)?;
    let mut seen: HashMap<String, String> = HashMap::new();
    let mut to_insert: Vec<(String, String)> = Vec::new();
    for row in &rows {
        let Some(key) = unique_key(table, uq, &row.values) else {
            continue;
        };
        let owner_pk = encoded_pk_for(table, &row.values);
        match seen.get(&key) {
            Some(existing) if existing != &owner_pk => {
                return Err(KitError::Migration(format!(
                    "cannot add unique constraint {constraint} on {table_name}: \
                     existing rows violate it"
                )));
            }
            Some(_) => {}
            None => {
                seen.insert(key.clone(), owner_pk.clone());
                to_insert.push((key, owner_pk));
            }
        }
    }

    let existing_keys: HashSet<String> = visible_internal_rows(core, UNIQUE_KEYS)?
        .iter()
        .filter_map(|g| internal_bytes(g, cols::UQ_ENCODED))
        .collect();

    let now = iso_now();
    let mut txn = core.begin();
    for (key, owner_pk) in to_insert {
        if existing_keys.contains(&key) {
            continue;
        }
        txn.put(
            UNIQUE_KEYS,
            vec![
                (cols::UQ_ENCODED, CoreValue::Bytes(key.into_bytes())),
                (
                    cols::UQ_CONSTRAINT,
                    CoreValue::Bytes(constraint.as_bytes().to_vec()),
                ),
                (
                    cols::UQ_OWNER_TABLE,
                    CoreValue::Bytes(table_name.as_bytes().to_vec()),
                ),
                (cols::UQ_OWNER_PK, CoreValue::Bytes(owner_pk.into_bytes())),
                (
                    cols::UQ_CREATED,
                    CoreValue::Bytes(now.clone().into_bytes()),
                ),
            ],
        )
        .map_err(KitError::from)?;
    }
    txn.commit().map_err(KitError::from)?;
    Ok(())
}

/// Delete every `__kit_unique_keys` guard for a dropped unique constraint.
fn drop_unique_guards(core: &CoreDatabase, table_name: &str, constraint: &str) -> Result<()> {
    let existing = visible_internal_rows(core, UNIQUE_KEYS)?;
    let mut txn = core.begin();
    for g in &existing {
        let g_table = internal_bytes(g, cols::UQ_OWNER_TABLE).unwrap_or_default();
        let g_constraint = internal_bytes(g, cols::UQ_CONSTRAINT).unwrap_or_default();
        if g_table == table_name && g_constraint == constraint {
            txn.delete(UNIQUE_KEYS, g.row_id).map_err(KitError::from)?;
        }
    }
    txn.commit().map_err(KitError::from)?;
    Ok(())
}

/// Backfill parent `__kit_row_guards` for an added foreign key (PLAN
/// "Migrations"). Every existing child row with a non-null FK must reference an
/// existing parent; a missing parent rejects the migration. The referenced
/// parent's row guard is touched so a later concurrent parent delete conflicts.
fn backfill_foreign_key(
    core: &CoreDatabase,
    schema: &KitSchema,
    table_name: &str,
    constraint: &str,
) -> Result<()> {
    let table = schema.table(table_name).ok_or_else(|| {
        KitError::Migration(format!("add_foreign_key: table {table_name} not found in schema"))
    })?;
    let fk = table
        .foreign_keys
        .iter()
        .find(|f| f.name == constraint)
        .ok_or_else(|| {
            KitError::Migration(format!(
                "add_foreign_key: foreign key {constraint} not found on table {table_name}"
            ))
        })?;
    let parent = schema.table(&fk.references_table).ok_or_else(|| {
        KitError::Migration(format!(
            "add_foreign_key: referenced table {} not found in schema",
            fk.references_table
        ))
    })?;

    let child_rows = visible_app_rows(core, table)?;
    let parent_pks: HashSet<String> = visible_app_rows(core, parent)?
        .iter()
        .map(|p| encoded_pk_for(parent, &p.values))
        .collect();

    let mut to_touch: Vec<Vec<KeyComponent>> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for child in &child_rows {
        if fk_values_null(fk, &child.values) {
            continue;
        }
        let comps = parent_pk_components(&child.values, fk, parent);
        let encoded = encode_pk(&comps);
        if !parent_pks.contains(&encoded) {
            return Err(KitError::ForeignKey(format!(
                "{} references missing parent {}({})",
                fk.name, fk.references_table, encoded
            )));
        }
        if seen.insert(encoded) {
            to_touch.push(comps);
        }
    }

    let existing = visible_internal_rows(core, ROW_GUARDS)?;
    let now = iso_now();
    let mut txn = core.begin();
    for comps in &to_touch {
        let encoded_pk = encode_pk(comps);
        let guard_key = encode_row_guard_key(&parent.name, &encoded_pk);
        let mut version = 1i64;
        for g in &existing {
            if internal_bytes(g, cols::RG_ENCODED).as_deref() == Some(guard_key.as_str()) {
                if let Some(CoreValue::Int64(v)) = g.columns.get(&cols::RG_VERSION) {
                    version = v + 1;
                }
                txn.delete(ROW_GUARDS, g.row_id).map_err(KitError::from)?;
            }
        }
        txn.put(
            ROW_GUARDS,
            vec![
                (cols::RG_ENCODED, CoreValue::Bytes(guard_key.into_bytes())),
                (
                    cols::RG_TABLE,
                    CoreValue::Bytes(parent.name.as_bytes().to_vec()),
                ),
                (cols::RG_PK, CoreValue::Bytes(encoded_pk.into_bytes())),
                (cols::RG_VERSION, CoreValue::Int64(version)),
                (
                    cols::RG_UPDATED,
                    CoreValue::Bytes(now.clone().into_bytes()),
                ),
            ],
        )
        .map_err(KitError::from)?;
    }
    txn.commit().map_err(KitError::from)?;
    Ok(())
}

/// Delete every unique-key and row guard owned by a dropped table.
fn clean_table_guards(core: &CoreDatabase, table_name: &str) -> Result<()> {
    let unique = visible_internal_rows(core, UNIQUE_KEYS)?;
    let guards = visible_internal_rows(core, ROW_GUARDS)?;
    let mut txn = core.begin();
    for g in &unique {
        if internal_bytes(g, cols::UQ_OWNER_TABLE).as_deref() == Some(table_name) {
            txn.delete(UNIQUE_KEYS, g.row_id).map_err(KitError::from)?;
        }
    }
    for g in &guards {
        if internal_bytes(g, cols::RG_TABLE).as_deref() == Some(table_name) {
            txn.delete(ROW_GUARDS, g.row_id).map_err(KitError::from)?;
        }
    }
    txn.commit().map_err(KitError::from)?;
    Ok(())
}

fn record_migration(
    txn: &mut mongreldb_core::txn::Transaction<'_>,
    migration: &Migration,
) -> Result<()> {
    let now = crate::internal::iso_now();
    let cells = vec![
        (cols::MIG_VERSION, CoreValue::Int64(migration.version)),
        (cols::MIG_NAME, CoreValue::Bytes(migration.name.clone().into_bytes())),
        (
            cols::MIG_CHECKSUM,
            CoreValue::Bytes(migration.checksum().into_bytes()),
        ),
        (cols::MIG_APPLIED, CoreValue::Bytes(now.into_bytes())),
        (
            cols::MIG_KIT_VERSION,
            CoreValue::Bytes(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
        ),
        (cols::MIG_STATUS, CoreValue::Bytes(b"applied".to_vec())),
    ];
    txn.put(MIGRATIONS_TABLE, cells).map_err(KitError::from)?;
    Ok(())
}
