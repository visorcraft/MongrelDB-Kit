//! Migration runner for `mongreldb-kit`.

use crate::db::internal_bytes;
use crate::error::{KitError, Result};
use crate::internal::{
    cols, ensure_internal_tables, iso_now, MIGRATIONS_TABLE, ROW_GUARDS, UNIQUE_KEYS,
};
use crate::schema::{core_row_to_json, to_core_schema, Row as KitRow};
use crate::txn::{encoded_pk_for, fk_values_null, parent_pk_components, unique_key};
use mongreldb_core::memtable::{Row as CoreRow, Value as CoreValue};
use mongreldb_core::{AlterColumn, Database as CoreDatabase};
use mongreldb_kit_core::keys::{encode_pk, encode_row_guard_key, KeyComponent};
use mongreldb_kit_core::migrations::{plan_migrations, Migration, MigrationOp};
use mongreldb_kit_core::schema::{Schema as KitSchema, Table as KitTable};
use std::collections::{HashMap, HashSet};

/// Run pending migrations against `db`.
///
/// Creates internal tables if missing, applies each pending migration in
/// version order, and records it in `__kit_schema_migrations`.
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
        apply_migration_ops(db, migration, &db.schema)?;
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
    db: &crate::db::Database,
    migration: &Migration,
    schema: &KitSchema,
) -> Result<()> {
    let core = db.core_db();
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
                            .add_column(
                                column,
                                crate::schema::to_core_type(col.storage_type),
                                crate::schema::to_core_flags(t, col),
                            )
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
            MigrationOp::AlterColumn { table, column } => {
                alter_column(core, schema, table, column)?;
            }
            MigrationOp::AddCheck { .. }
            | MigrationOp::DropCheck { .. }
            | MigrationOp::DropForeignKey { .. } => {
                // Metadata-only: check evaluation, foreign-key enforcement, and
                // dropped foreign-key definitions are driven by the re-persisted
                // schema, so there is no catalog or guard mutation to perform
                // here.
            }
            MigrationOp::CreateProcedure { procedure, .. } => {
                let parsed: mongreldb_core::StoredProcedure =
                    serde_json::from_value(procedure.json.clone()).map_err(KitError::from)?;
                let procedure = mongreldb_core::StoredProcedure::new(
                    parsed.name,
                    parsed.mode,
                    parsed.params,
                    parsed.body,
                    0,
                )
                .map_err(KitError::from)?;
                core.create_procedure(procedure).map_err(KitError::from)?;
            }
            MigrationOp::ReplaceProcedure { name: _, procedure } => {
                let parsed: mongreldb_core::StoredProcedure =
                    serde_json::from_value(procedure.json.clone()).map_err(KitError::from)?;
                let procedure = mongreldb_core::StoredProcedure::new(
                    parsed.name,
                    parsed.mode,
                    parsed.params,
                    parsed.body,
                    0,
                )
                .map_err(KitError::from)?;
                core.create_or_replace_procedure(procedure)
                    .map_err(KitError::from)?;
            }
            MigrationOp::DropProcedure { name } => {
                let _ = core.drop_procedure(name);
            }
            MigrationOp::CreateTrigger { trigger, .. } => {
                core.create_trigger(core_trigger(trigger)?)
                    .map_err(KitError::from)?;
            }
            MigrationOp::ReplaceTrigger { trigger, .. } => {
                core.create_or_replace_trigger(core_trigger(trigger)?)
                    .map_err(KitError::from)?;
            }
            MigrationOp::DropTrigger { name } => {
                let _ = core.drop_trigger(name);
            }
            MigrationOp::CreateVirtualTable { table } => {
                // SQL-backed: run `CREATE VIRTUAL TABLE ...` through the
                // embedded session, then refresh so the new table is visible
                // to subsequent `sql()` calls.
                db.sql(&table.create_sql())?;
                db.refresh_sql_session()?;
            }
            MigrationOp::DropVirtualTable { name } => {
                db.sql(&format!("DROP TABLE IF EXISTS {name}"))?;
                db.refresh_sql_session()?;
            }
            MigrationOp::CreateView { view, .. } => {
                // The engine's `CREATE VIEW` overwrites any existing entry, so
                // create and replace are the same SQL (see `ReplaceView`).
                db.sql(&view.create_sql())?;
            }
            MigrationOp::ReplaceView { view, .. } => {
                db.sql(&view.create_sql())?;
            }
            MigrationOp::DropView { name } => {
                // `IF EXISTS` so dropping an already-absent view is a no-op
                // (idempotent re-applies of the same migration).
                db.sql(&format!("DROP VIEW IF EXISTS {name}"))?;
            }
            MigrationOp::DropColumn { table, column } => {
                let target = schema.table(table).ok_or_else(|| {
                    KitError::Migration(format!("drop_column: table {table} not found in schema"))
                })?;
                if target.column(column).is_some() {
                    return Err(KitError::Migration(format!(
                        "drop_column: target schema still contains {table}.{column}"
                    )));
                }
                rebuild_table(core, target)?;
                drop_stale_unique_guards(core, target)?;
            }
            MigrationOp::AddIndex { table, index } => {
                let target = schema.table(table).ok_or_else(|| {
                    KitError::Migration(format!("add_index: table {table} not found in schema"))
                })?;
                let idx = target
                    .indexes
                    .iter()
                    .find(|idx| idx.name == *index)
                    .ok_or_else(|| {
                        KitError::Migration(format!(
                            "add_index: index {index} not found on table {table} in schema"
                        ))
                    })?;
                if idx.unique {
                    backfill_unique(core, schema, table, index)?;
                }
                rebuild_table(core, target)?;
            }
            MigrationOp::DropIndex { table, index } => {
                let target = schema.table(table).ok_or_else(|| {
                    KitError::Migration(format!("drop_index: table {table} not found in schema"))
                })?;
                if target.indexes.iter().any(|idx| idx.name == *index) {
                    return Err(KitError::Migration(format!(
                        "drop_index: target schema still contains index {index} on {table}"
                    )));
                }
                rebuild_table(core, target)?;
                drop_stale_unique_guards(core, target)?;
            }
            MigrationOp::RawSql(sql) => {
                // Run arbitrary SQL (DDL or DML) through the embedded session.
                // Useful for engine features without a dedicated migration op.
                // Schema-affecting statements should be followed by
                // `refresh_sql_session` if later ops in this migration query
                // the new structure via SQL.
                db.sql(sql)?;
            }
        }
    }
    Ok(())
}

fn core_trigger(spec: &mongreldb_kit_core::TriggerSpec) -> Result<mongreldb_core::StoredTrigger> {
    let parsed: mongreldb_core::StoredTrigger =
        serde_json::from_value(spec.json.clone()).map_err(KitError::from)?;
    mongreldb_core::StoredTrigger::new(
        parsed.name,
        mongreldb_core::TriggerDefinition {
            target: parsed.target,
            timing: parsed.timing,
            event: parsed.event,
            update_of: parsed.update_of,
            target_columns: parsed.target_columns,
            when: parsed.when,
            program: parsed.program,
        },
        0,
    )
    .map_err(KitError::from)
}

fn alter_column(
    core: &CoreDatabase,
    schema: &KitSchema,
    table_name: &str,
    column_name: &str,
) -> Result<()> {
    let table = schema
        .table(table_name)
        .ok_or_else(|| KitError::Migration(format!("table {table_name:?} not found")))?;

    let handle = core.table(table_name).map_err(KitError::from)?;
    let guard = handle.lock();
    let current_columns = guard.schema().columns.clone();
    drop(guard);

    let target = match table.column(column_name) {
        Some(col) => col,
        None => {
            let current = current_columns
                .iter()
                .find(|col| col.name == column_name)
                .ok_or_else(|| {
                    KitError::Migration(format!(
                        "column {table_name}.{column_name} not found in current or target schema"
                    ))
                })?;
            table
                .columns
                .iter()
                .find(|col| col.id as u16 == current.id)
                .ok_or_else(|| {
                    KitError::Migration(format!(
                        "target column for {table_name}.{column_name} with id {} not found",
                        current.id
                    ))
                })?
        }
    };

    let source_name = current_columns
        .iter()
        .find(|col| col.id == target.id as u16)
        .map(|col| col.name.as_str())
        .unwrap_or(column_name);

    core.alter_column(
        table_name,
        source_name,
        AlterColumn {
            name: Some(target.name.clone()),
            ty: Some(crate::schema::to_core_type(target.storage_type)),
            flags: Some(crate::schema::to_core_flags(table, target)),
        },
    )
    .map_err(KitError::from)?;

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

fn copy_rows_to_table(
    core: &CoreDatabase,
    table_name: &str,
    table: &KitTable,
    rows: &[CoreRow],
) -> Result<()> {
    let mut txn = core.begin();
    for row in rows {
        let cells: Vec<(u16, CoreValue)> = table
            .columns
            .iter()
            .map(|col| {
                let id = col.id as u16;
                (id, row.columns.get(&id).cloned().unwrap_or(CoreValue::Null))
            })
            .collect();
        txn.put(table_name, cells).map_err(KitError::from)?;
    }
    txn.commit().map_err(KitError::from)?;
    Ok(())
}

fn temp_rebuild_name(core: &CoreDatabase, table_name: &str) -> String {
    for attempt in 0.. {
        let name = format!("__kit_tmp_rebuild_{table_name}_{attempt}");
        if core.table_id(&name).is_err() {
            return name;
        }
    }
    unreachable!("unbounded rebuild temp name search must return")
}

fn rebuild_table(core: &CoreDatabase, target: &KitTable) -> Result<()> {
    let rows = visible_internal_rows(core, &target.name)?;
    let target_schema = to_core_schema(target);
    let temp_name = temp_rebuild_name(core, &target.name);

    core.create_table(&temp_name, target_schema.clone())
        .map_err(KitError::from)?;
    let result = (|| -> Result<()> {
        copy_rows_to_table(core, &temp_name, target, &rows)?;
        core.drop_table(&target.name).map_err(KitError::from)?;
        core.create_table(&target.name, target_schema)
            .map_err(KitError::from)?;
        copy_rows_to_table(core, &target.name, target, &rows)?;
        Ok(())
    })();

    let cleanup = core.drop_table(&temp_name).map_err(KitError::from);
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), _) => Err(err),
        (Ok(()), Err(err)) => Err(err),
    }
}

fn drop_stale_unique_guards(core: &CoreDatabase, target: &KitTable) -> Result<()> {
    let live_constraints: HashSet<&str> = target
        .unique_constraints
        .iter()
        .map(|constraint| constraint.name.as_str())
        .collect();
    let existing = visible_internal_rows(core, UNIQUE_KEYS)?;
    let mut txn = core.begin();
    for guard in &existing {
        let guard_table = internal_bytes(guard, cols::UQ_OWNER_TABLE).unwrap_or_default();
        if guard_table != target.name {
            continue;
        }
        let guard_constraint = internal_bytes(guard, cols::UQ_CONSTRAINT).unwrap_or_default();
        if !live_constraints.contains(guard_constraint.as_str()) {
            txn.delete(UNIQUE_KEYS, guard.row_id)
                .map_err(KitError::from)?;
        }
    }
    txn.commit().map_err(KitError::from)?;
    Ok(())
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
        KitError::Migration(format!(
            "add_unique: table {table_name} not found in schema"
        ))
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
                (cols::UQ_CREATED, CoreValue::Bytes(now.clone().into_bytes())),
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
        KitError::Migration(format!(
            "add_foreign_key: table {table_name} not found in schema"
        ))
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
                (cols::RG_UPDATED, CoreValue::Bytes(now.clone().into_bytes())),
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
        (
            cols::MIG_NAME,
            CoreValue::Bytes(migration.name.clone().into_bytes()),
        ),
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
