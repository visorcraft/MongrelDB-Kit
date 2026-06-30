//! Kit transaction wrapper around a MongrelDB core transaction.
//!
//! Constraints are enforced with the guard-table architecture shared with the
//! TypeScript kit:
//!
//! * Unique constraints reserve a row in `__kit_unique_keys` keyed by the typed
//!   encoded unique key. Concurrent inserts of the same value collide on that
//!   key and one transaction retries.
//! * Foreign keys verify the parent row exists and then *touch* the parent's
//!   `__kit_row_guards` row. A concurrent parent delete also writes that guard
//!   key, forcing a write-write conflict so the unsafe snapshot interleaving is
//!   impossible.
//! * Primary keys are handled like the TypeScript kit. An auto-assigned
//!   (sequence-default) primary key is guaranteed unique and needs no check. An
//!   explicit single-column primary key is checked directly against the visible
//!   rows (no guard row). Only an explicit composite primary key reserves a
//!   `__pk_<table>` guard in `__kit_unique_keys` (it has no single native key to
//!   probe), so a duplicate-PK insert is rejected instead of silently upserting.
//!
//! Reads inside a transaction use the transaction's read snapshot; writes staged
//! earlier in the same transaction are tracked in memory so read-your-writes
//! behaves correctly even though the core transaction cannot read its own
//! staging.

use crate::db::internal_bytes;
use crate::error::{KitError, Result};
use crate::internal::{cols, iso_now, ROW_GUARDS, UNIQUE_KEYS};
use crate::query::{project_distinct, run_aggregate, run_join, run_select, ExecCtx, JoinRow};
use crate::schema::{core_row_to_json, json_to_core, pk_to_map, row_to_core_cells, Row};
use mongreldb_core::memtable::{Row as CoreRow, Value as CoreValue};
use mongreldb_core::query::Condition;
use mongreldb_core::RowId;
use mongreldb_kit_core::keys::{
    decode_pk, encode_pk, encode_row_guard_key, encode_unique_key, KeyComponent, KIT_KEY_VERSION,
};
use mongreldb_kit_core::planner::{plan_delete, DeletePlan};
use mongreldb_kit_core::query::{AggregateQuery, Cte, Expr, JoinQuery, OnConflict, Query, Select};
use mongreldb_kit_core::schema::{
    Column, ColumnType, DefaultKind, ForeignKey, Table as KitTable, UniqueConstraint,
};
use serde_json::{Map, Value};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

/// A kit transaction.
pub struct Transaction<'a> {
    db: &'a crate::db::Database,
    core: mongreldb_core::txn::Transaction<'a>,
    staged: Vec<StagedOp>,
    /// Unique keys reserved within this (uncommitted) transaction.
    staged_unique: Vec<StagedUnique>,
    /// Row-guard keys already touched in this transaction (dedupe).
    touched_guards: HashSet<String>,
    next_temp_id: u64,
}

#[derive(Debug, Clone)]
enum StagedOp {
    Insert {
        table: String,
        values: Map<String, Value>,
    },
    Update {
        table: String,
        old_pk: String,
        row_id: u64,
        values: Map<String, Value>,
    },
    /// A delete staged in this transaction. Tracked so `staged_row_exists`
    /// can ignore rows removed earlier in the same transaction.
    Delete {
        table: String,
        pk: String,
    },
    Truncate {
        table: String,
    },
}

#[derive(Debug, Clone)]
struct StagedUnique {
    encoded_key: String,
    owner_table: String,
    owner_pk: String,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(
        db: &'a crate::db::Database,
        core: mongreldb_core::txn::Transaction<'a>,
    ) -> Self {
        Self {
            db,
            core,
            staged: Vec::new(),
            staged_unique: Vec::new(),
            touched_guards: HashSet::new(),
            next_temp_id: 1,
        }
    }

    /// Insert a row into `table`.
    pub fn insert(&mut self, table: &str, row: Map<String, Value>) -> Result<Row> {
        let t = self.require_table(table)?.clone();
        self.do_insert(table, &t, row, None)
    }

    pub fn insert_returning(
        &mut self,
        table: &str,
        row: Map<String, Value>,
        returning: Vec<String>,
    ) -> Result<Value> {
        let inserted = self.insert(table, row)?;
        project_returning(&inserted, &returning)
    }

    /// Insert many rows into `table` within this single transaction.
    ///
    /// Each row still passes through defaults, validation, and constraint checks,
    /// but the whole batch is staged in one transaction (the caller commits once)
    /// — far faster than a row-at-a-time begin/commit loop for bulk loads. For a
    /// single-column primary key the existing primary keys are loaded once into a
    /// set so the per-row duplicate check stays O(1) instead of re-scanning the
    /// table for every row. Mirrors the TypeScript kit's `insertInto().valuesMany`.
    pub fn insert_many(&mut self, table: &str, rows: Vec<Map<String, Value>>) -> Result<Vec<Row>> {
        let t = self.require_table(table)?.clone();
        // Preload the visible single-column primary keys once; explicit-PK rows in
        // the batch are then checked (and staged) against this in-memory set.
        let mut pk_seen: Option<HashSet<String>> = if t.primary_key.len() == 1 {
            let mut set = HashSet::new();
            for r in self.snapshot_rows(table)? {
                set.insert(encoded_pk_for(&t, &r.values));
            }
            Some(set)
        } else {
            None
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(self.do_insert(table, &t, row, pk_seen.as_mut())?);
        }
        Ok(out)
    }

    /// Core insert path shared by [`insert`](Self::insert) and
    /// [`insert_many`](Self::insert_many). `pk_seen`, when present, is the batch's
    /// in-memory set of single-column primary keys used for an O(1) duplicate
    /// check (and is updated as explicit-PK rows are staged).
    fn do_insert(
        &mut self,
        table: &str,
        t: &KitTable,
        mut row: Map<String, Value>,
        pk_seen: Option<&mut HashSet<String>>,
    ) -> Result<Row> {
        // A primary key is "explicit" when the caller supplied all of its columns
        // in the original input (before defaults are applied); only an explicit PK
        // can collide. An auto-assigned (sequence) PK is guaranteed unique.
        let pk_explicit = pk_is_explicit(t, &row);

        self.apply_defaults(&mut row, t)?;
        // Normalize any column still unset to explicit null, so the stored row and
        // the returned row agree (an omitted nullable column reads back as null).
        for col in &t.columns {
            row.entry(col.name.clone()).or_insert(Value::Null);
        }
        mongreldb_kit_core::validation::validate_row(t, &row)?;

        // Validate all constraints before staging any writes.
        self.check_unique_constraints(t, &row, None)?;
        self.check_pk(t, &row, pk_explicit, pk_seen)?;
        self.check_foreign_keys(t, &row)?;

        // Stage guard rows + the application row atomically.
        self.reserve_unique_guards(t, &row, None)?;
        self.reserve_pk_guard(t, &row, pk_explicit)?;
        self.touch_foreign_key_guards(t, &row)?;

        let cells = row_to_core_cells(&row, t)?;
        self.core.put(table, cells).map_err(KitError::from)?;
        let temp_id = self.alloc_temp_id();
        self.staged.push(StagedOp::Insert {
            table: table.to_string(),
            values: row.clone(),
        });
        Ok(Row {
            row_id: temp_id,
            values: row,
        })
    }

    /// Update the row in `table` identified by `pk` with `patch`.
    pub fn update(&mut self, table: &str, pk: &Value, patch: Map<String, Value>) -> Result<Row> {
        let t = self.require_table(table)?.clone();
        let pk_map = pk_to_map(pk, &t)?;
        let old_row = self
            .get_by_pk_internal(table, &pk_map)?
            .ok_or_else(|| KitError::Integrity(format!("row not found in {table}")))?;

        let patch_keys: HashSet<String> = patch.keys().cloned().collect();
        let mut values = old_row.values.clone();
        for (k, v) in patch {
            if t.column(&k).is_some() {
                values.insert(k, v);
            }
        }
        self.apply_update_defaults(&mut values, &patch_keys, &t);
        mongreldb_kit_core::validation::validate_row(&t, &values)?;
        self.check_unique_constraints(&t, &values, Some(&old_row.values))?;
        self.check_foreign_keys(&t, &values)?;

        // Reserve new unique keys and delete stale ones.
        self.reserve_unique_guards(&t, &values, Some(&old_row.values))?;
        self.touch_foreign_key_guards(&t, &values)?;

        let cells = row_to_core_cells(&values, &t)?;
        self.core
            .delete(table, RowId(old_row.row_id))
            .map_err(KitError::from)?;
        self.core.put(table, cells).map_err(KitError::from)?;
        let temp_id = self.alloc_temp_id();
        self.staged.push(StagedOp::Update {
            table: table.to_string(),
            old_pk: encoded_pk_for(&t, &old_row.values),
            row_id: old_row.row_id,
            values: values.clone(),
        });
        Ok(Row {
            row_id: temp_id,
            values,
        })
    }

    /// Delete the row in `table` identified by `pk`.
    pub fn delete(&mut self, table: &str, pk: &Value) -> Result<()> {
        let t = self.require_table(table)?.clone();
        let pk_map = pk_to_map(pk, &t)?;
        let row = self
            .get_by_pk_internal(table, &pk_map)?
            .ok_or_else(|| KitError::Integrity(format!("row not found in {table}")))?;

        let (plan, row_cache) = self.plan_delete(table, &row)?;
        if !plan.restricted.is_empty() {
            let msg = format!(
                "delete restricted by {}",
                plan.restricted
                    .iter()
                    .map(|r| format!("{}.{}", r.table, r.constraint))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            return Err(KitError::Restrict(msg));
        }

        // Apply set-null updates first, then cascade deletes, finally the target.
        // Each affected child row was already fetched during planning, so reuse it
        // from the cache rather than re-reading it by PK.
        for set_null in &plan.set_null {
            let key = format!("{}:{}", set_null.table, set_null.pk);
            if let Some(child_row) = row_cache.get(&key).cloned() {
                self.apply_set_null(set_null, &child_row)?;
            }
        }
        for del in &plan.delete {
            if del.table == table {
                continue; // deleted at the end
            }
            let child_table = self.require_table(&del.table)?.clone();
            let key = format!("{}:{}", del.table, del.pk);
            if let Some(child_row) = row_cache.get(&key) {
                let row_id = child_row.row_id;
                let values = child_row.values.clone();
                self.delete_guards_for(&child_table, &values)?;
                self.core
                    .delete(&del.table, RowId(row_id))
                    .map_err(KitError::from)?;
                self.staged.push(StagedOp::Delete {
                    table: del.table.clone(),
                    pk: del.pk.clone(),
                });
            }
        }

        // Clean the target's guards and force a conflict with any concurrent
        // child insert by touching its row guard.
        self.delete_guards_for(&t, &row.values)?;
        self.touch_row_guard(table, &pk_components(&t, &row.values))?;
        self.core
            .delete(table, RowId(row.row_id))
            .map_err(KitError::from)?;
        self.staged.push(StagedOp::Delete {
            table: table.to_string(),
            pk: encoded_pk_for(&t, &row.values),
        });
        Ok(())
    }

    pub fn truncate(&mut self, table: &str) -> Result<()> {
        let t = self.require_table(table)?.clone();
        if self.has_staged_for(table) {
            return Err(KitError::Validation(format!(
                "truncate cannot be combined with prior writes on {table}"
            )));
        }
        if self.db.schema.tables.iter().any(|other| {
            other.name != t.name
                && other
                    .foreign_keys
                    .iter()
                    .any(|fk| fk.references_table == t.name)
        }) {
            return Err(KitError::Restrict(format!(
                "table {} is referenced by a foreign key",
                t.name
            )));
        }
        let rows = self
            .db
            .visible_core_rows_at(table, self.core.read_snapshot())?;
        self.delete_all_guards_for_table(&t)?;
        for row in rows {
            self.core
                .delete(table, row.row_id)
                .map_err(KitError::from)?;
        }
        self.staged.push(StagedOp::Truncate { table: t.name });
        Ok(())
    }

    pub fn upsert(
        &mut self,
        table: &str,
        row: Map<String, Value>,
        on_conflict: OnConflict,
        returning: Vec<String>,
    ) -> Result<Value> {
        let t = self.require_table(table)?.clone();
        let mut values = row;
        self.apply_defaults(&mut values, &t)?;
        for col in &t.columns {
            values.entry(col.name.clone()).or_insert(Value::Null);
        }
        mongreldb_kit_core::validation::validate_row(&t, &values)?;
        let pk_map = pk_values_map(&t, &values);
        let existing = self.get_by_pk_internal(table, &pk_map)?;
        match (existing, on_conflict) {
            (Some(old), OnConflict::DoNothing) => project_returning(&old, &returning),
            (Some(old), OnConflict::DoUpdate(patch)) => {
                let updated = self.update(table, &old.pk(&t).unwrap_or(Value::Null), patch)?;
                project_returning(&updated, &returning)
            }
            (None, _) => {
                let inserted = self.insert(table, values)?;
                project_returning(&inserted, &returning)
            }
        }
    }

    pub fn update_where(
        &mut self,
        table: &str,
        filter: Option<Expr>,
        patch: Map<String, Value>,
        returning: Vec<String>,
    ) -> Result<Vec<Value>> {
        let t = self.require_table(table)?.clone();
        let rows = self.select(&Query::Select(select_all(table, filter)))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let pk = row.pk(&t).unwrap_or(Value::Null);
            let updated = self.update(table, &pk, patch.clone())?;
            out.push(project_returning(&updated, &returning)?);
        }
        Ok(out)
    }

    pub fn delete_where(
        &mut self,
        table: &str,
        filter: Option<Expr>,
        returning: Vec<String>,
    ) -> Result<Vec<Value>> {
        let t = self.require_table(table)?.clone();
        let rows = self.select(&Query::Select(select_all(table, filter)))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(project_returning(&row, &returning)?);
            let pk = row.pk(&t).unwrap_or(Value::Null);
            self.delete(table, &pk)?;
        }
        Ok(out)
    }

    /// Read a row by primary key.
    pub fn get_by_pk(&self, table: &str, pk: &Value) -> Result<Option<Row>> {
        let t = self.require_table(table)?;
        let pk_map = pk_to_map(pk, t)?;
        self.get_by_pk_internal(table, &pk_map)
    }

    /// Execute a `Select` query. Subqueries (`IN (subquery)`, `EXISTS`) and
    /// `like`/`contains`/`not in` predicates resolve against this transaction's
    /// read snapshot, so they may reference other tables.
    pub fn select(&self, query: &Query) -> Result<Vec<Row>> {
        let select = match query {
            Query::Select(s) => s,
            _ => return Err(KitError::Validation("only SELECT supported".into())),
        };
        let ctx = self.exec_ctx();
        run_select(&ctx, select)
    }

    /// Like [`select`](Self::select) but drops duplicate rows. When the select
    /// projects columns, duplicates are decided on the projection (true
    /// `SELECT DISTINCT col, ...`); otherwise on the whole row.
    pub fn select_distinct(&self, query: &Query) -> Result<Vec<Row>> {
        let select = match query {
            Query::Select(s) => s,
            _ => return Err(KitError::Validation("only SELECT supported".into())),
        };
        let ctx = self.exec_ctx();
        let rows = run_select(&ctx, select)?;
        Ok(project_distinct(select, rows))
    }

    /// Materialize each CTE in order (a later CTE may read an earlier one) and
    /// run `body` with those named results available as virtual tables.
    pub fn select_with(&self, ctes: &[Cte], body: &Select) -> Result<Vec<Row>> {
        let mut ctx = self.exec_ctx();
        for cte in ctes {
            let rows = run_select(&ctx, &cte.query)?;
            ctx.add_cte(cte.name.clone(), rows);
        }
        run_select(&ctx, body)
    }

    /// Run an aggregate / group-by / having query. Returns one row per group
    /// (group-key columns plus the aggregate aliases); with no `group_by` the
    /// whole filtered table is a single group.
    pub fn aggregate(&self, query: &AggregateQuery) -> Result<Vec<Row>> {
        let ctx = self.exec_ctx();
        run_aggregate(&ctx, query)
    }

    /// Run a nested-loop join. Each result row is a map keyed by table alias; see
    /// [`JoinQuery`] for the shape. Supports inner, left, and cross joins.
    pub fn join(&self, query: &JoinQuery) -> Result<Vec<JoinRow>> {
        let ctx = self.exec_ctx();
        run_join(&ctx, query)
    }

    pub fn execute(&mut self, query: &Query) -> Result<Vec<Value>> {
        match query {
            Query::Select(_) => Err(KitError::Validation("use select() for SELECT".into())),
            Query::Insert(insert) => Ok(vec![self.insert_returning(
                &insert.table,
                insert.values.clone(),
                insert.returning.clone(),
            )?]),
            Query::Upsert(upsert) => Ok(vec![self.upsert(
                &upsert.table,
                upsert.values.clone(),
                upsert.on_conflict.clone(),
                upsert.returning.clone(),
            )?]),
            Query::Update(update) => self.update_where(
                &update.table,
                update.filter.clone(),
                update.set.clone(),
                update.returning.clone(),
            ),
            Query::Delete(delete) => self.delete_where(
                &delete.table,
                delete.filter.clone(),
                delete.returning.clone(),
            ),
            Query::Aggregate(_) | Query::Join(_) => Err(KitError::Validation(
                "aggregate/join are not mutating statements".into(),
            )),
        }
    }

    /// Build an execution context whose table fetcher reads visible rows at this
    /// transaction's read snapshot. When conditions are provided, the fetcher
    /// resolves them via native indexes (Kit Priority 1 pushdown).
    fn exec_ctx(&self) -> ExecCtx<'_> {
        ExecCtx::new(
            Some(&self.db.schema),
            |name: &str, conds: Option<&[Condition]>| match conds {
                Some(c) if !c.is_empty() => self.snapshot_rows_pushed(name, c),
                _ => self.snapshot_rows(name),
            },
        )
    }

    /// Commit the transaction.
    pub fn commit(self) -> Result<()> {
        self.core.commit().map_err(KitError::from).map(|_| ())
    }

    /// Roll back the transaction.
    pub fn rollback(self) {
        self.core.rollback();
    }

    // ── internals ──────────────────────────────────────────────────────────

    fn alloc_temp_id(&mut self) -> u64 {
        let id = self.next_temp_id;
        self.next_temp_id += 1;
        id
    }

    fn require_table(&self, name: &str) -> Result<&KitTable> {
        self.db
            .table(name)
            .ok_or_else(|| KitError::Integrity(format!("table {name} not found")))
    }

    fn snapshot_rows(&self, table: &str) -> Result<Vec<Row>> {
        let t = self.require_table(table)?;
        let core_rows = self
            .db
            .visible_core_rows_at(table, self.core.read_snapshot())?;
        let mut rows = core_rows
            .into_iter()
            .map(|r| core_row_to_json(&r, t))
            .collect::<Result<Vec<_>>>()?;
        self.replay_staged_rows(t, &mut rows);
        Ok(rows)
    }

    /// Fetch rows for `table` with native `conditions` resolved by the engine
    /// (Kit Priority 1 pushdown). Avoids the full scan that `snapshot_rows`
    /// does — the engine resolves conditions via HOT/bitmap/range indexes.
    fn snapshot_rows_pushed(&self, table: &str, conditions: &[Condition]) -> Result<Vec<Row>> {
        let t = self.require_table(table)?;
        if self.has_staged_for(table) {
            return Ok(self
                .snapshot_rows(table)?
                .into_iter()
                .filter(|r| row_matches_conditions(t, r, conditions))
                .collect());
        }
        let core_rows = self
            .db
            .query_core_rows_at(table, conditions, self.core.read_snapshot())?;
        core_rows
            .into_iter()
            .map(|r| core_row_to_json(&r, t))
            .collect()
    }

    fn get_by_pk_internal(&self, table: &str, pk_map: &Map<String, Value>) -> Result<Option<Row>> {
        let t = self.require_table(table)?;
        if self.has_staged_for(table) {
            let rows = self.snapshot_rows(table)?;
            return Ok(rows.into_iter().find(|r| pk_matches(&r.values, pk_map, t)));
        }
        // Kit Priority 1 pushdown: use native PK index (O(1) HOT probe) instead
        // of O(N) full scan. Falls back to scan if conditions can't be built.
        if let Some(conditions) = crate::pushdown::pk_conditions(t, pk_map) {
            let core_rows =
                self.db
                    .query_core_rows_at(table, &conditions, self.core.read_snapshot())?;
            return Ok(core_rows
                .into_iter()
                .filter_map(|r| core_row_to_json(&r, t).ok())
                .find(|r| pk_matches(&r.values, pk_map, t)));
        }
        let rows = self.snapshot_rows(table)?;
        Ok(rows.into_iter().find(|r| pk_matches(&r.values, pk_map, t)))
    }

    fn internal_rows(&self, table: &str) -> Result<Vec<CoreRow>> {
        self.db
            .visible_core_rows_at(table, self.core.read_snapshot())
    }

    // ── unique constraints ─────────────────────────────────────────────────

    /// Reject the row if any of its unique keys is already owned by a different
    /// row, either in committed state or in this transaction's staging.
    fn check_unique_constraints(
        &self,
        table: &KitTable,
        values: &Map<String, Value>,
        old_values: Option<&Map<String, Value>>,
    ) -> Result<()> {
        let owner_pk = encoded_pk_for(table, values);
        let committed = self.internal_rows(UNIQUE_KEYS)?;
        for uq in &table.unique_constraints {
            let Some(key) = unique_key(table, uq, values) else {
                continue;
            };
            // Unchanged keys (update where the value did not move) are fine.
            if let Some(old) = old_values {
                if unique_key(table, uq, old).as_deref() == Some(key.as_str()) {
                    continue;
                }
            }
            self.assert_key_free(&committed, table, uq, &key, &owner_pk)?;
        }
        Ok(())
    }

    fn assert_key_free(
        &self,
        committed: &[CoreRow],
        table: &KitTable,
        uq: &UniqueConstraint,
        key: &str,
        owner_pk: &str,
    ) -> Result<()> {
        for guard in committed {
            if internal_bytes(guard, cols::UQ_ENCODED).as_deref() == Some(key) {
                let g_table = internal_bytes(guard, cols::UQ_OWNER_TABLE).unwrap_or_default();
                let g_pk = internal_bytes(guard, cols::UQ_OWNER_PK).unwrap_or_default();
                if g_table != table.name || g_pk != owner_pk {
                    return Err(KitError::Duplicate(format!(
                        "unique constraint {} on {}",
                        uq.name, table.name
                    )));
                }
            }
        }
        for staged in &self.staged_unique {
            if staged.encoded_key == key
                && (staged.owner_table != table.name || staged.owner_pk != owner_pk)
            {
                return Err(KitError::Duplicate(format!(
                    "unique constraint {} on {}",
                    uq.name, table.name
                )));
            }
        }
        Ok(())
    }

    /// Reserve new unique guard rows for `values`, deleting any stale guards that
    /// belonged to the previous version of the row (on update).
    fn reserve_unique_guards(
        &mut self,
        table: &KitTable,
        values: &Map<String, Value>,
        old_values: Option<&Map<String, Value>>,
    ) -> Result<()> {
        let owner_pk = encoded_pk_for(table, values);
        for uq in &table.unique_constraints {
            let new_key = unique_key(table, uq, values);
            let old_key = old_values.and_then(|old| unique_key(table, uq, old));
            if new_key == old_key {
                continue;
            }
            if let Some(key) = &new_key {
                self.put_unique_guard(&uq.name, key, &table.name, &owner_pk)?;
            }
            if let Some(key) = &old_key {
                self.delete_unique_guard(key)?;
            }
        }
        Ok(())
    }

    fn put_unique_guard(
        &mut self,
        constraint: &str,
        encoded_key: &str,
        owner_table: &str,
        owner_pk: &str,
    ) -> Result<()> {
        let now = iso_now();
        self.core
            .put(
                UNIQUE_KEYS,
                vec![
                    (cols::UQ_ENCODED, CoreValue::Bytes(encoded_key.into())),
                    (cols::UQ_CONSTRAINT, CoreValue::Bytes(constraint.into())),
                    (cols::UQ_OWNER_TABLE, CoreValue::Bytes(owner_table.into())),
                    (cols::UQ_OWNER_PK, CoreValue::Bytes(owner_pk.into())),
                    (cols::UQ_CREATED, CoreValue::Bytes(now.into_bytes())),
                ],
            )
            .map_err(KitError::from)?;
        self.staged_unique.push(StagedUnique {
            encoded_key: encoded_key.to_string(),
            owner_table: owner_table.to_string(),
            owner_pk: owner_pk.to_string(),
        });
        Ok(())
    }

    fn delete_unique_guard(&mut self, encoded_key: &str) -> Result<()> {
        let committed = self.internal_rows(UNIQUE_KEYS)?;
        for guard in &committed {
            if internal_bytes(guard, cols::UQ_ENCODED).as_deref() == Some(encoded_key) {
                self.core
                    .delete(UNIQUE_KEYS, guard.row_id)
                    .map_err(KitError::from)?;
            }
        }
        self.staged_unique.retain(|s| s.encoded_key != encoded_key);
        Ok(())
    }

    /// Delete every unique guard owned by the given row (used on row delete).
    fn delete_unique_guards_for_owner(&mut self, table: &KitTable, owner_pk: &str) -> Result<()> {
        let constraint_names: HashSet<&str> = table
            .unique_constraints
            .iter()
            .map(|u| u.name.as_str())
            .collect();
        let committed = self.internal_rows(UNIQUE_KEYS)?;
        for guard in &committed {
            let g_table = internal_bytes(guard, cols::UQ_OWNER_TABLE).unwrap_or_default();
            let g_pk = internal_bytes(guard, cols::UQ_OWNER_PK).unwrap_or_default();
            let g_constraint = internal_bytes(guard, cols::UQ_CONSTRAINT).unwrap_or_default();
            if g_table == table.name
                && g_pk == owner_pk
                && (constraint_names.contains(g_constraint.as_str())
                    || g_constraint == pk_guard_constraint(table))
            {
                self.core
                    .delete(UNIQUE_KEYS, guard.row_id)
                    .map_err(KitError::from)?;
            }
        }
        let owner_pk = owner_pk.to_string();
        let name = table.name.clone();
        self.staged_unique
            .retain(|s| !(s.owner_table == name && s.owner_pk == owner_pk));
        Ok(())
    }

    // ── primary-key handling ───────────────────────────────────────────────
    //
    // Matches the TypeScript kit: an auto-assigned (sequence) primary key is
    // guaranteed unique and skipped; an explicit single-column primary key is
    // checked directly against the visible rows (no guard row); only an explicit
    // composite primary key reserves a `__pk_<table>` guard in `__kit_unique_keys`
    // (it has no single native key to probe), making the duplicate insert throw
    // and stay conflict-safe.

    fn check_pk(
        &self,
        table: &KitTable,
        values: &Map<String, Value>,
        pk_explicit: bool,
        pk_seen: Option<&mut HashSet<String>>,
    ) -> Result<()> {
        // An auto-assigned primary key is unique by construction; nothing to do.
        if table.primary_key.is_empty() || !pk_explicit {
            return Ok(());
        }

        if table.primary_key.len() == 1 {
            // A single-column explicit PK has a native key, so check whether a row
            // with that PK already exists. A batch passes a pre-loaded set so the
            // check stays O(1) per row; a single insert checks the visible rows
            // (committed plus this transaction's in-flight staging) directly.
            let duplicate = match pk_seen {
                Some(seen) => {
                    let key = encoded_pk_for(table, values);
                    if seen.contains(&key) {
                        true
                    } else {
                        seen.insert(key);
                        false
                    }
                }
                None => {
                    self.parent_exists(&table.name, values)?
                        || self.staged_row_exists(table, values)
                }
            };
            if duplicate {
                return Err(KitError::Duplicate(format!(
                    "primary key {} on {}",
                    encoded_pk_for(table, values),
                    table.name
                )));
            }
            return Ok(());
        }

        // A composite explicit PK uses a guard row (conflict-safe), like the
        // unique-constraint machinery.
        let key = pk_guard_key(table, values);
        let owner_pk = encoded_pk_for(table, values);
        let committed = self.internal_rows(UNIQUE_KEYS)?;
        for guard in &committed {
            if internal_bytes(guard, cols::UQ_ENCODED).as_deref() == Some(key.as_str()) {
                return Err(KitError::Duplicate(format!(
                    "primary key {} on {}",
                    owner_pk, table.name
                )));
            }
        }
        for staged in &self.staged_unique {
            if staged.encoded_key == key {
                return Err(KitError::Duplicate(format!(
                    "primary key {} on {}",
                    owner_pk, table.name
                )));
            }
        }
        Ok(())
    }

    fn reserve_pk_guard(
        &mut self,
        table: &KitTable,
        values: &Map<String, Value>,
        pk_explicit: bool,
    ) -> Result<()> {
        // Only an explicit composite primary key needs a guard row. A single-column
        // PK is checked directly, and an auto-assigned PK is guaranteed unique.
        if table.primary_key.len() < 2 || !pk_explicit {
            return Ok(());
        }
        let key = pk_guard_key(table, values);
        let owner_pk = encoded_pk_for(table, values);
        let constraint = pk_guard_constraint(table);
        self.put_unique_guard(&constraint, &key, &table.name, &owner_pk)
    }

    // ── foreign keys ───────────────────────────────────────────────────────

    fn check_foreign_keys(&self, table: &KitTable, values: &Map<String, Value>) -> Result<()> {
        for fk in &table.foreign_keys {
            if fk_values_null(fk, values) {
                continue;
            }
            let parent = self.require_table(&fk.references_table)?;
            let parent_pk = parent_pk_value(values, fk, parent)?;
            let parent_pk_map = pk_to_map(&parent_pk, parent)?;
            if self.parent_exists(&fk.references_table, &parent_pk_map)?
                || self.staged_row_exists(parent, &parent_pk_map)
            {
                continue;
            }
            return Err(KitError::ForeignKey(format!(
                "{} references {}({})",
                fk.name,
                fk.references_table,
                fk.references_columns.join(",")
            )));
        }
        Ok(())
    }

    fn touch_foreign_key_guards(
        &mut self,
        table: &KitTable,
        values: &Map<String, Value>,
    ) -> Result<()> {
        for fk in table.foreign_keys.clone() {
            if fk_values_null(&fk, values) {
                continue;
            }
            let parent = self.require_table(&fk.references_table)?.clone();
            let components = parent_pk_components(values, &fk, &parent);
            self.touch_row_guard(&parent.name, &components)?;
        }
        Ok(())
    }

    fn parent_exists(&self, table: &str, pk_map: &Map<String, Value>) -> Result<bool> {
        let t = self.require_table(table)?;
        if self.has_staged_for(table) {
            let rows = self.snapshot_rows(table)?;
            return Ok(rows.iter().any(|r| pk_matches(&r.values, pk_map, t)));
        }
        // Kit Priority 1 pushdown: O(1) HOT probe instead of O(N) full scan for
        // FK parent existence checks.
        if let Some(conditions) = crate::pushdown::pk_conditions(t, pk_map) {
            let core_rows =
                self.db
                    .query_core_rows_at(table, &conditions, self.core.read_snapshot())?;
            return Ok(!core_rows.is_empty());
        }
        let rows = self.snapshot_rows(table)?;
        Ok(rows.iter().any(|r| pk_matches(&r.values, pk_map, t)))
    }

    /// Whether a row identified by `pk_map` exists in `table`'s in-flight staging
    /// for this transaction. Replays the staging in order (table-scoped) so a row
    /// inserted and then deleted within the same transaction is not treated as
    /// present. Used both for foreign-key parent checks and for the single-column
    /// primary-key duplicate check.
    fn staged_row_exists(&self, table: &KitTable, pk_map: &Map<String, Value>) -> bool {
        let target = encode_pk(&pk_components(table, pk_map));
        let mut exists = false;
        for staged in &self.staged {
            match staged {
                StagedOp::Insert { table: t, values }
                | StagedOp::Update {
                    table: t, values, ..
                } => {
                    if t == &table.name && pk_matches(values, pk_map, table) {
                        exists = true;
                    }
                }
                StagedOp::Delete { table: t, pk } => {
                    if t == &table.name && *pk == target {
                        exists = false;
                    }
                }
                StagedOp::Truncate { table: t } => {
                    if t == &table.name {
                        exists = false;
                    }
                }
            }
        }
        exists
    }

    // ── row guards ─────────────────────────────────────────────────────────

    fn touch_row_guard(&mut self, table: &str, pk_components: &[KeyComponent]) -> Result<()> {
        let encoded_pk = encode_pk(pk_components);
        let guard_key = encode_row_guard_key(table, &encoded_pk);
        if !self.touched_guards.insert(guard_key.clone()) {
            return Ok(());
        }
        // Replace any existing committed guard, bumping the version.
        let mut version = 1i64;
        let committed = self.internal_rows(ROW_GUARDS)?;
        for guard in &committed {
            if internal_bytes(guard, cols::RG_ENCODED).as_deref() == Some(guard_key.as_str()) {
                if let Some(CoreValue::Int64(v)) = guard.columns.get(&cols::RG_VERSION) {
                    version = v + 1;
                }
                self.core
                    .delete(ROW_GUARDS, guard.row_id)
                    .map_err(KitError::from)?;
            }
        }
        let now = iso_now();
        self.core
            .put(
                ROW_GUARDS,
                vec![
                    (cols::RG_ENCODED, CoreValue::Bytes(guard_key.into_bytes())),
                    (cols::RG_TABLE, CoreValue::Bytes(table.into())),
                    (cols::RG_PK, CoreValue::Bytes(encoded_pk.into_bytes())),
                    (cols::RG_VERSION, CoreValue::Int64(version)),
                    (cols::RG_UPDATED, CoreValue::Bytes(now.into_bytes())),
                ],
            )
            .map_err(KitError::from)?;
        Ok(())
    }

    fn has_staged_for(&self, table: &str) -> bool {
        self.staged.iter().any(|op| match op {
            StagedOp::Insert { table: t, .. }
            | StagedOp::Update { table: t, .. }
            | StagedOp::Delete { table: t, .. }
            | StagedOp::Truncate { table: t } => t == table,
        })
    }

    fn replay_staged_rows(&self, table: &KitTable, rows: &mut Vec<Row>) {
        for staged in &self.staged {
            match staged {
                StagedOp::Insert { table: t, values } if t == &table.name => {
                    let pk = encoded_pk_for(table, values);
                    rows.retain(|r| encoded_pk_for(table, &r.values) != pk);
                    rows.push(Row {
                        row_id: 0,
                        values: values.clone(),
                    });
                }
                StagedOp::Update {
                    table: t,
                    old_pk,
                    row_id,
                    values,
                } if t == &table.name => {
                    let new_pk = encoded_pk_for(table, values);
                    rows.retain(|r| {
                        let pk = encoded_pk_for(table, &r.values);
                        pk != *old_pk && pk != new_pk
                    });
                    rows.push(Row {
                        row_id: *row_id,
                        values: values.clone(),
                    });
                }
                StagedOp::Delete { table: t, pk } if t == &table.name => {
                    rows.retain(|r| encoded_pk_for(table, &r.values) != *pk);
                }
                StagedOp::Truncate { table: t } if t == &table.name => {
                    rows.clear();
                }
                _ => {}
            }
        }
    }

    /// Remove unique + pk guards for a row that is being deleted.
    fn delete_guards_for(&mut self, table: &KitTable, values: &Map<String, Value>) -> Result<()> {
        let owner_pk = encoded_pk_for(table, values);
        self.delete_unique_guards_for_owner(table, &owner_pk)
    }

    fn delete_all_guards_for_table(&mut self, table: &KitTable) -> Result<()> {
        for guard in self.internal_rows(UNIQUE_KEYS)? {
            let owner_table = internal_bytes(&guard, cols::UQ_OWNER_TABLE).unwrap_or_default();
            if owner_table == table.name {
                self.core
                    .delete(UNIQUE_KEYS, guard.row_id)
                    .map_err(KitError::from)?;
            }
        }
        for guard in self.internal_rows(ROW_GUARDS)? {
            let owner_table = internal_bytes(&guard, cols::RG_TABLE).unwrap_or_default();
            if owner_table == table.name {
                self.core
                    .delete(ROW_GUARDS, guard.row_id)
                    .map_err(KitError::from)?;
            }
        }
        self.staged_unique.retain(|s| s.owner_table != table.name);
        self.touched_guards
            .retain(|key| !key.starts_with(&format!("{}:", table.name)));
        Ok(())
    }

    // ── delete planning ────────────────────────────────────────────────────

    fn plan_delete(&self, table: &str, row: &Row) -> Result<(DeletePlan, HashMap<String, Row>)> {
        let schema = &self.db.schema;
        let t = self.require_table(table)?;
        let pk_str = encoded_pk_for(t, &row.values);
        // Cache every child row discovered while planning, keyed by
        // "<table>:<encoded_pk>", so the apply phase can reuse it instead of
        // re-reading each row by PK — which would make a bulk cascade / set-null
        // delete O(n^2) (one full table scan per affected child row).
        let row_cache: RefCell<HashMap<String, Row>> = RefCell::new(HashMap::new());
        let find_children =
            |child_table: &KitTable, fk: &ForeignKey, parent_pk: &str| -> Vec<(String, String)> {
                let parent_pk_value = match pk_string_to_value(parent_pk, t) {
                    Ok(v) => v,
                    Err(_) => return Vec::new(),
                };
                let parent_pk_map = match pk_to_map(&parent_pk_value, t) {
                    Ok(m) => m,
                    Err(_) => return Vec::new(),
                };
                let child_rows = match self.snapshot_rows(&child_table.name) {
                    Ok(r) => r,
                    Err(_) => return Vec::new(),
                };
                let mut out = Vec::new();
                for child_row in child_rows {
                    if fk_matches(&child_row.values, fk, &parent_pk_map, t) {
                        let child_pk = encoded_pk_for(child_table, &child_row.values);
                        row_cache
                            .borrow_mut()
                            .insert(format!("{}:{}", child_table.name, child_pk), child_row);
                        out.push((child_pk, parent_pk.to_string()));
                    }
                }
                out
            };
        let plan = plan_delete(schema, table, &pk_str, find_children).map_err(KitError::from)?;
        Ok((plan, row_cache.into_inner()))
    }

    fn apply_set_null(
        &mut self,
        set_null: &mongreldb_kit_core::planner::SetNullUpdate,
        child_row: &Row,
    ) -> Result<()> {
        let child_table = self.require_table(&set_null.table)?.clone();
        let mut values = child_row.values.clone();
        for col in &set_null.columns {
            let col_def = child_table
                .column(col)
                .ok_or_else(|| KitError::Integrity(format!("set-null column {col} not found")))?;
            if !col_def.nullable {
                return Err(KitError::Restrict(format!(
                    "set-null on non-nullable column {col}"
                )));
            }
            values.insert(col.clone(), Value::Null);
        }
        // Re-run validation (including checks) on the patched child row.
        mongreldb_kit_core::validation::validate_row(&child_table, &values)?;
        // Recompute unique guards for the patched row. The row itself survives a
        // set-null (only its FK columns change), so its primary-key guard is
        // re-reserved after `delete_guards_for` clears it.
        self.delete_guards_for(&child_table, &child_row.values)?;
        let cells = row_to_core_cells(&values, &child_table)?;
        self.core
            .delete(&set_null.table, RowId(child_row.row_id))
            .map_err(KitError::from)?;
        self.core
            .put(&set_null.table, cells)
            .map_err(KitError::from)?;
        self.reserve_unique_guards(&child_table, &values, None)?;
        // The row keeps its full primary key (only FK columns changed), so it is
        // "explicit"; this re-reserves a composite PK guard and is a no-op for a
        // single-column PK.
        self.reserve_pk_guard(&child_table, &values, true)?;
        self.staged.push(StagedOp::Update {
            table: set_null.table.clone(),
            old_pk: encoded_pk_for(&child_table, &child_row.values),
            row_id: child_row.row_id,
            values: values.clone(),
        });
        Ok(())
    }

    // ── defaults ───────────────────────────────────────────────────────────

    fn apply_defaults(&self, row: &mut Map<String, Value>, table: &KitTable) -> Result<()> {
        for col in &table.columns {
            if row.contains_key(&col.name) && row.get(&col.name) != Some(&Value::Null) {
                continue;
            }
            let Some(default) = &col.default else {
                continue;
            };
            let value = match default {
                DefaultKind::Static(v) => v.clone(),
                DefaultKind::Now => {
                    let now = iso_now();
                    if col.storage_type == ColumnType::Date {
                        Value::String(now[..10].to_string())
                    } else {
                        Value::String(now)
                    }
                }
                DefaultKind::Uuid => Value::String(uuid::Uuid::new_v4().to_string()),
                DefaultKind::Sequence(name) => {
                    let start = self.db.allocate_sequence(name, 1)?;
                    Value::Number(start.into())
                }
                DefaultKind::CustomName(name) => {
                    let provider = self.db.default_providers.get(name).ok_or_else(|| {
                        KitError::Validation(format!("custom default \"{name}\" is not registered"))
                    })?;
                    provider()
                }
            };
            row.insert(col.name.clone(), value);
        }
        Ok(())
    }

    /// Refresh write-managed `now` columns on update.
    ///
    /// Only a `generated` column whose default is `now` (e.g. `updatedAt`) is a
    /// write-managed timestamp that refreshes on every update. A plain
    /// `default: now` column (e.g. `createdAt`) is an insert-time value and must
    /// NOT change on update. A column already present in the caller's patch is
    /// left as supplied. Mirrors the TypeScript kit's `applyUpdateDefaults`.
    fn apply_update_defaults(
        &self,
        merged: &mut Map<String, Value>,
        patch_keys: &HashSet<String>,
        table: &KitTable,
    ) {
        let mut now: Option<String> = None;
        for col in &table.columns {
            if patch_keys.contains(&col.name) {
                continue;
            }
            if col.generated && matches!(col.default, Some(DefaultKind::Now)) {
                let stamp = now.get_or_insert_with(iso_now);
                let value = if col.storage_type == ColumnType::Date {
                    Value::String(stamp[..10].to_string())
                } else {
                    Value::String(stamp.clone())
                };
                merged.insert(col.name.clone(), value);
            }
        }
    }
}

// ── free helpers ───────────────────────────────────────────────────────────

fn project_returning(row: &Row, columns: &[String]) -> Result<Value> {
    let mut out = Map::new();
    for c in columns {
        out.insert(c.clone(), row.values.get(c).cloned().unwrap_or(Value::Null));
    }
    Ok(Value::Object(out))
}

fn pk_values_map(table: &KitTable, values: &Map<String, Value>) -> Map<String, Value> {
    let mut out = Map::new();
    for name in &table.primary_key {
        out.insert(
            name.clone(),
            values.get(name).cloned().unwrap_or(Value::Null),
        );
    }
    out
}

fn select_all(table: &str, filter: Option<Expr>) -> Select {
    Select {
        table: table.to_string(),
        columns: vec![],
        filter,
        order_by: vec![],
        limit: None,
        offset: None,
    }
}

fn pk_matches(values: &Map<String, Value>, pk_map: &Map<String, Value>, table: &KitTable) -> bool {
    for name in &table.primary_key {
        if values.get(name) != pk_map.get(name) {
            return false;
        }
    }
    true
}

/// Whether the caller supplied every primary-key column (non-null) in `row`.
///
/// Mirrors the TypeScript kit's `pkExplicit` flag: a primary key whose columns
/// all came from a sequence default (i.e. were not supplied) is auto-assigned and
/// guaranteed unique, so it is neither checked nor guarded.
fn pk_is_explicit(table: &KitTable, row: &Map<String, Value>) -> bool {
    !table.primary_key.is_empty()
        && table
            .primary_key
            .iter()
            .all(|name| matches!(row.get(name), Some(v) if !v.is_null()))
}

fn pk_guard_constraint(table: &KitTable) -> String {
    format!("__pk_{}", table.name)
}

fn pk_guard_key(table: &KitTable, values: &Map<String, Value>) -> String {
    encode_unique_key(
        KIT_KEY_VERSION,
        &pk_guard_constraint(table),
        &pk_components(table, values),
    )
}

/// Build the typed key component for a column value.
pub(crate) fn key_component(col: &Column, value: Option<&Value>) -> KeyComponent {
    match value {
        None | Some(Value::Null) => KeyComponent::Null,
        Some(v) => match col.storage_type {
            ColumnType::Int8
            | ColumnType::Int16
            | ColumnType::Int32
            | ColumnType::Int64
            | ColumnType::TimestampNanos => KeyComponent::Int(v.as_i64().unwrap_or(0)),
            _ => KeyComponent::Text(value_to_text(v)),
        },
    }
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn pk_components(table: &KitTable, values: &Map<String, Value>) -> Vec<KeyComponent> {
    table
        .primary_key
        .iter()
        .map(|name| {
            let col = table.column(name);
            match col {
                Some(c) => key_component(c, values.get(name)),
                None => KeyComponent::Null,
            }
        })
        .collect()
}

pub(crate) fn encoded_pk_for(table: &KitTable, values: &Map<String, Value>) -> String {
    encode_pk(&pk_components(table, values))
}

pub(crate) fn unique_key(
    table: &KitTable,
    uq: &UniqueConstraint,
    values: &Map<String, Value>,
) -> Option<String> {
    let mut components = Vec::with_capacity(uq.columns.len());
    for name in &uq.columns {
        let col = table.column(name)?;
        let component = key_component(col, values.get(name));
        if component == KeyComponent::Null {
            return None; // nullable-unique: nulls never collide
        }
        components.push(component);
    }
    Some(encode_unique_key(KIT_KEY_VERSION, &uq.name, &components))
}

pub(crate) fn fk_values_null(fk: &ForeignKey, values: &Map<String, Value>) -> bool {
    fk.columns
        .iter()
        .any(|c| values.get(c).map(|v| v.is_null()).unwrap_or(true))
}

pub(crate) fn parent_pk_components(
    values: &Map<String, Value>,
    fk: &ForeignKey,
    parent: &KitTable,
) -> Vec<KeyComponent> {
    fk.columns
        .iter()
        .zip(&parent.primary_key)
        .map(|(child_col, parent_col)| {
            let col = parent.column(parent_col);
            match col {
                Some(c) => key_component(c, values.get(child_col)),
                None => KeyComponent::Null,
            }
        })
        .collect()
}

fn parent_pk_value(
    values: &Map<String, Value>,
    fk: &ForeignKey,
    parent: &KitTable,
) -> Result<Value> {
    if parent.primary_key.len() == 1 {
        let child_col = fk
            .columns
            .first()
            .ok_or_else(|| KitError::Integrity("fk has no columns".into()))?;
        Ok(values.get(child_col).cloned().unwrap_or(Value::Null))
    } else {
        let mut obj = Map::new();
        for (child_col, parent_col) in fk.columns.iter().zip(&parent.primary_key) {
            obj.insert(
                parent_col.clone(),
                values.get(child_col).cloned().unwrap_or(Value::Null),
            );
        }
        Ok(Value::Object(obj))
    }
}

fn fk_matches(
    child_values: &Map<String, Value>,
    fk: &ForeignKey,
    parent_pk_map: &Map<String, Value>,
    parent_table: &KitTable,
) -> bool {
    for (child_col, parent_col) in fk.columns.iter().zip(&parent_table.primary_key) {
        if child_values.get(child_col) != parent_pk_map.get(parent_col) {
            return false;
        }
    }
    true
}

/// Decode an encoded primary key string back into a JSON value.
///
/// Single-column keys return the scalar value; composite keys return an object
/// keyed by primary-key column name.
fn pk_string_to_value(encoded: &str, table: &KitTable) -> Result<Value> {
    let components = decode_pk(encoded);
    if components.len() != table.primary_key.len() {
        return Err(KitError::Validation(format!(
            "encoded pk \"{encoded}\" has {} components, expected {}",
            components.len(),
            table.primary_key.len()
        )));
    }
    if table.primary_key.len() == 1 {
        return Ok(component_to_value(&components[0]));
    }
    let mut obj = Map::new();
    for (name, component) in table.primary_key.iter().zip(&components) {
        obj.insert(name.clone(), component_to_value(component));
    }
    Ok(Value::Object(obj))
}

fn component_to_value(component: &KeyComponent) -> Value {
    match component {
        KeyComponent::Null => Value::Null,
        KeyComponent::Int(i) => Value::Number((*i).into()),
        KeyComponent::Text(s) => Value::String(s.clone()),
    }
}

fn row_matches_conditions(table: &KitTable, row: &Row, conditions: &[Condition]) -> bool {
    conditions
        .iter()
        .all(|condition| row_matches_condition(table, row, condition))
}

fn row_matches_condition(table: &KitTable, row: &Row, condition: &Condition) -> bool {
    match condition {
        Condition::Pk(key) => {
            let Some(pk_name) = table.primary_key.first() else {
                return false;
            };
            let Some(col) = table.column(pk_name) else {
                return false;
            };
            value_index_key(col, &row.values).as_deref() == Some(key.as_slice())
        }
        Condition::BitmapEq { column_id, value } => {
            let Some(col) = column_by_id(table, *column_id) else {
                return false;
            };
            value_index_key(col, &row.values).as_deref() == Some(value.as_slice())
        }
        Condition::BitmapIn { column_id, values } => {
            let Some(col) = column_by_id(table, *column_id) else {
                return false;
            };
            let Some(key) = value_index_key(col, &row.values) else {
                return false;
            };
            values.iter().any(|value| value == &key)
        }
        Condition::Range { column_id, lo, hi } => {
            let Some(col) = column_by_id(table, *column_id) else {
                return false;
            };
            matches!(
                json_to_core(
                    row.values.get(&col.name).unwrap_or(&Value::Null),
                    col.storage_type
                ),
                Ok(CoreValue::Int64(v)) if v >= *lo && v <= *hi
            )
        }
        Condition::RangeF64 {
            column_id,
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
        } => {
            let Some(col) = column_by_id(table, *column_id) else {
                return false;
            };
            let Ok(CoreValue::Float64(v)) = json_to_core(
                row.values.get(&col.name).unwrap_or(&Value::Null),
                col.storage_type,
            ) else {
                return false;
            };
            let ge_lo = if *lo_inclusive { v >= *lo } else { v > *lo };
            let le_hi = if *hi_inclusive { v <= *hi } else { v < *hi };
            ge_lo && le_hi
        }
        Condition::FmContains { column_id, pattern } => {
            let Some(col) = column_by_id(table, *column_id) else {
                return false;
            };
            if pattern.is_empty() {
                return true;
            }
            match json_to_core(
                row.values.get(&col.name).unwrap_or(&Value::Null),
                col.storage_type,
            ) {
                Ok(CoreValue::Bytes(bytes)) => bytes
                    .windows(pattern.len())
                    .any(|window| window == pattern.as_slice()),
                _ => false,
            }
        }
        Condition::Ann { .. } | Condition::SparseMatch { .. } => true,
    }
}

fn column_by_id(table: &KitTable, column_id: u16) -> Option<&Column> {
    table.columns.iter().find(|col| col.id as u16 == column_id)
}

fn value_index_key(col: &Column, values: &Map<String, Value>) -> Option<Vec<u8>> {
    let value = values.get(&col.name).unwrap_or(&Value::Null);
    if value.is_null() {
        return None;
    }
    json_to_core(value, col.storage_type)
        .ok()
        .map(|value| value.encode_key())
}
