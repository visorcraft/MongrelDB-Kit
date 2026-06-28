//! Migration runner for `mongreldb-kit`.

use crate::error::{KitError, Result};
use crate::schema::to_core_schema;
use mongreldb_core::memtable::{Row as CoreRow, Value as CoreValue};
use mongreldb_core::Database as CoreDatabase;
use mongreldb_kit_core::migrations::{plan_migrations, Migration, MigrationOp};
use mongreldb_kit_core::schema::Schema as KitSchema;

pub(crate) const MIGRATIONS_TABLE: &str = "_migrations";

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

fn ensure_internal_tables(core: &CoreDatabase) -> Result<()> {
    if core.table_id(MIGRATIONS_TABLE).is_err() {
        core.create_table(MIGRATIONS_TABLE, crate::db::migrations_table_core())
            .map_err(KitError::from)?;
    }
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
                let _ = core.drop_table(name);
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
            MigrationOp::DropColumn { .. }
            | MigrationOp::AddIndex { .. }
            | MigrationOp::DropIndex { .. }
            | MigrationOp::AddUnique { .. }
            | MigrationOp::DropUnique { .. }
            | MigrationOp::AddForeignKey { .. }
            | MigrationOp::DropForeignKey { .. }
            | MigrationOp::AddCheck { .. }
            | MigrationOp::DropCheck { .. } => {
                // Schema-level changes are reflected by re-creating/re-opening
                // tables from the updated schema. For this minimal runner we
                // rely on the caller to pass a schema that matches the migration
                // set and re-persist it after all ops complete.
            }
            MigrationOp::RawSql(_) => {
                return Err(KitError::Migration(
                    "RawSql migrations are not supported".into(),
                ));
            }
        }
    }
    Ok(())
}

fn record_migration(
    txn: &mut mongreldb_core::txn::Transaction<'_>,
    migration: &Migration,
) -> Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let cells = vec![
        (1, CoreValue::Int64(migration.version)),
        (2, CoreValue::Bytes(migration.name.clone().into_bytes())),
        (3, CoreValue::Bytes(migration.checksum().into_bytes())),
        (4, CoreValue::Bytes(now.to_string().into_bytes())),
    ];
    txn.put(MIGRATIONS_TABLE, cells).map_err(KitError::from)?;
    Ok(())
}
