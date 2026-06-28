//! Kit transaction wrapper around a MongrelDB core transaction.

use crate::error::{KitError, Result};
use crate::query::execute_select;
use crate::schema::{
    core_row_to_json, pk_to_map, row_to_core_cells, Row,
};
use mongreldb_core::RowId;
use mongreldb_kit_core::planner::{plan_delete, DeletePlan};
use mongreldb_kit_core::query::Query;
use mongreldb_kit_core::schema::{DefaultKind, Table as KitTable};
use serde_json::{Map, Value};

/// A kit transaction.
///
/// Wraps a core cross-table transaction and enforces kit-level defaults,
/// validation, unique constraints, and foreign keys.
pub struct Transaction<'a> {
    db: &'a crate::db::Database,
    core: mongreldb_core::txn::Transaction<'a>,
    staged: Vec<StagedOp>,
    next_temp_id: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum StagedOp {
    Insert {
        table: String,
        values: Map<String, Value>,
        temp_id: u64,
    },
    Update {
        table: String,
        old_row_id: u64,
        values: Map<String, Value>,
        temp_id: u64,
    },
    Delete {
        table: String,
        row_id: u64,
    },
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
            next_temp_id: 1,
        }
    }

    /// Insert a row into `table`.
    pub fn insert(&mut self, table: &str, mut row: Map<String, Value>) -> Result<Row> {
        let t = self.require_table(table)?.clone();
        apply_defaults(&mut row, &t)?;
        mongreldb_kit_core::validation::validate_row(&t, &row)?;
        self.check_unique_constraints(table, &row, None)?;
        self.check_foreign_keys(table, &row)?;

        let temp_id = self.next_temp_id;
        self.next_temp_id += 1;
        let cells = row_to_core_cells(&row, &t)?;
        self.core.put(table, cells).map_err(KitError::from)?;
        self.staged.push(StagedOp::Insert {
            table: table.to_string(),
            values: row.clone(),
            temp_id,
        });
        Ok(Row {
            row_id: temp_id,
            values: row,
        })
    }

    /// Update the row in `table` identified by `pk` with `patch`.
    pub fn update(
        &mut self,
        table: &str,
        pk: &Value,
        patch: Map<String, Value>,
    ) -> Result<Row> {
        let t = self.require_table(table)?.clone();
        let pk_map = pk_to_map(pk, &t)?;
        let old_row = self
            .get_by_pk_internal(table, &pk_map)?
            .ok_or_else(|| KitError::Integrity(format!("row not found in {table}")))?;

        let mut values = old_row.values.clone();
        for (k, v) in patch {
            if t.column(&k).is_some() {
                values.insert(k, v);
            }
        }
        mongreldb_kit_core::validation::validate_row(&t, &values)?;
        self.check_unique_constraints(table, &values, Some(old_row.row_id))?;
        self.check_foreign_keys(table, &values)?;

        let temp_id = self.next_temp_id;
        self.next_temp_id += 1;
        let cells = row_to_core_cells(&values, &t)?;
        self.core
            .delete(table, RowId(old_row.row_id))
            .map_err(KitError::from)?;
        self.core.put(table, cells).map_err(KitError::from)?;
        self.staged.push(StagedOp::Update {
            table: table.to_string(),
            old_row_id: old_row.row_id,
            values: values.clone(),
            temp_id,
        });
        Ok(Row {
            row_id: temp_id,
            values,
        })
    }

    /// Delete the row in `table` identified by `pk`.
    pub fn delete(&mut self, table: &str, pk: &Value) -> Result<()> {
        let t = self.require_table(table)?;
        let pk_map = pk_to_map(pk, t)?;
        let row = self
            .get_by_pk_internal(table, &pk_map)?
            .ok_or_else(|| KitError::Integrity(format!("row not found in {table}")))?;

        let plan = self.plan_delete(table, &row)?;
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

        // Apply set-null updates first, then cascade deletes, finally the parent.
        for set_null in &plan.set_null {
            self.apply_set_null(set_null)?;
        }
        for del in &plan.delete {
            if del.table == table {
                continue; // deleted at the end
            }
            let child_table = self.require_table(&del.table)?;
            let child_pk = pk_string_to_value(&del.pk, child_table)?;
            let child_pk_map = pk_to_map(&child_pk, child_table)?;
            if let Some(child_row) = self.get_by_pk_internal(&del.table, &child_pk_map)? {
                self.core
                    .delete(&del.table, RowId(child_row.row_id))
                    .map_err(KitError::from)?;
                self.staged.push(StagedOp::Delete {
                    table: del.table.clone(),
                    row_id: child_row.row_id,
                });
            }
        }

        self.core
            .delete(table, RowId(row.row_id))
            .map_err(KitError::from)?;
        self.staged.push(StagedOp::Delete {
            table: table.to_string(),
            row_id: row.row_id,
        });
        Ok(())
    }

    /// Read a row by primary key.
    pub fn get_by_pk(&self, table: &str, pk: &Value) -> Result<Option<Row>> {
        let t = self.require_table(table)?;
        let pk_map = pk_to_map(pk, t)?;
        self.get_by_pk_internal(table, &pk_map)
    }

    /// Execute a `Select` query.
    pub fn select(&self, query: &Query) -> Result<Vec<Row>> {
        let select = match query {
            Query::Select(s) => s,
            _ => return Err(KitError::Validation("only SELECT supported".into())),
        };
        let t = self.require_table(&select.table)?;
        let visible = self.db.visible_core_rows(&select.table)?;
        execute_select(t, visible, select)
    }

    /// Commit the transaction.
    pub fn commit(self) -> Result<()> {
        self.core.commit().map_err(KitError::from).map(|_| ())
    }

    /// Roll back the transaction.
    pub fn rollback(self) {
        self.core.rollback();
    }

    fn require_table(&self, name: &str) -> Result<&KitTable> {
        self.db
            .table(name)
            .ok_or_else(|| KitError::Integrity(format!("table {name} not found")))
    }

    fn visible_rows(&self, table: &str) -> Result<Vec<Row>> {
        let t = self.require_table(table)?;
        let core_rows = self.db.visible_core_rows(table)?;
        core_rows
            .into_iter()
            .map(|r| core_row_to_json(&r, t))
            .collect()
    }

    fn get_by_pk_internal(&self, table: &str, pk_map: &Map<String, Value>) -> Result<Option<Row>> {
        let t = self.require_table(table)?;
        let rows = self.visible_rows(table)?;
        Ok(rows.into_iter().find(|r| pk_matches(&r.values, pk_map, t)))
    }

    fn check_unique_constraints(
        &self,
        table: &str,
        values: &Map<String, Value>,
        exclude_row_id: Option<u64>,
    ) -> Result<()> {
        let t = self.require_table(table)?;
        let existing = self.visible_rows(table)?;
        for uq in &t.unique_constraints {
            let key = uq_key(values, t, uq);
            for row in &existing {
                if Some(row.row_id) == exclude_row_id {
                    continue;
                }
                if uq_key(&row.values, t, uq) == key && !key.is_empty() {
                    return Err(KitError::Duplicate(format!(
                        "unique constraint {} on {}",
                        uq.name, table
                    )));
                }
            }
            for staged in &self.staged {
                let staged_values = match staged {
                    StagedOp::Insert { values: v, .. } | StagedOp::Update { values: v, .. } => {
                        if let StagedOp::Update { old_row_id, .. } = staged {
                            if Some(*old_row_id) == exclude_row_id {
                                continue;
                            }
                        }
                        v
                    }
                    StagedOp::Delete { .. } => continue,
                };
                if uq_key(staged_values, t, uq) == key && !key.is_empty() {
                    return Err(KitError::Duplicate(format!(
                        "unique constraint {} on {} (staged)",
                        uq.name, table
                    )));
                }
            }
        }
        Ok(())
    }

    fn check_foreign_keys(&self, table: &str, values: &Map<String, Value>) -> Result<()> {
        let schema = &self.db.schema;
        let t = self.require_table(table)?.clone();
        for fk in &t.foreign_keys {
            let parent = schema.table(&fk.references_table).ok_or_else(|| {
                KitError::Integrity(format!("referenced table {} not found", fk.references_table))
            })?;
            // A null foreign-key reference is allowed; it represents an optional relationship.
            let child_values: Vec<&Value> = fk
                .columns
                .iter()
                .map(|col| values.get(col).unwrap_or(&Value::Null))
                .collect();
            if child_values.iter().any(|v| v.is_null()) {
                continue;
            }
            let parent_value = pk_value_from_fk(values, &t, fk, parent)?;
            let parent_exists = self.parent_exists(&fk.references_table, &parent_value)?;
            if parent_exists {
                continue;
            }
            // Also check staged inserts/updates in this transaction.
            if self.staged_parent_exists(&fk.references_table, &parent_value)? {
                continue;
            }
            return Err(KitError::ForeignKey(format!(
                "{} references {}({})",
                fk.name, fk.references_table, fk.references_columns.join(",")
            )));
        }
        Ok(())
    }

    fn parent_exists(&self, table: &str, pk: &Value) -> Result<bool> {
        let t = self.require_table(table)?;
        let pk_map = pk_to_map(pk, t)?;
        let rows = self.visible_rows(table)?;
        Ok(rows.iter().any(|r| pk_matches(&r.values, &pk_map, t)))
    }

    fn staged_parent_exists(&self, table: &str, pk: &Value) -> Result<bool> {
        let t = self.require_table(table)?;
        let pk_map = pk_to_map(pk, t)?;
        for staged in &self.staged {
            let values = match staged {
                StagedOp::Insert { values: v, .. } | StagedOp::Update { values: v, .. } => v,
                StagedOp::Delete { .. } => continue,
            };
            if pk_matches(values, &pk_map, t) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn plan_delete(&self, table: &str, row: &Row) -> Result<DeletePlan> {
        let schema = &self.db.schema;
        let t = self.require_table(table)?;
        let pk = row.pk(t).ok_or_else(|| KitError::Integrity("row has no pk".into()))?;
        let pk_str = pk_value_to_string(&pk, t)?;
        let find_children = |child_table: &KitTable, fk: &mongreldb_kit_core::schema::ForeignKey, parent_pk: &str| {
            let parent_pk_value = pk_string_to_value(parent_pk, t).ok();
            if parent_pk_value.is_none() {
                return Vec::new();
            }
            let parent_pk_value = parent_pk_value.unwrap();
            let parent_pk_map = pk_to_map(&parent_pk_value, t).ok();
            if parent_pk_map.is_none() {
                return Vec::new();
            }
            let parent_pk_map = parent_pk_map.unwrap();

            let child_rows = match self.visible_rows(&child_table.name) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            let mut out = Vec::new();
            for child_row in child_rows {
                if fk_matches(&child_row.values, child_table, fk, &parent_pk_map, t) {
                    if let Some(child_pk) = child_row.pk(child_table) {
                        if let Ok(s) = pk_value_to_string(&child_pk, child_table) {
                            out.push((s, parent_pk.to_string()));
                        }
                    }
                }
            }
            out
        };
        plan_delete(schema, table, &pk_str, find_children).map_err(KitError::from)
    }

    fn apply_set_null(
        &mut self,
        set_null: &mongreldb_kit_core::planner::SetNullUpdate,
    ) -> Result<()> {
        let child_table = self.require_table(&set_null.table)?;
        let child_pk = pk_string_to_value(&set_null.pk, child_table)?;
        let child_pk_map = pk_to_map(&child_pk, child_table)?;
        let Some(child_row) = self.get_by_pk_internal(&set_null.table, &child_pk_map)? else {
            return Ok(());
        };
        let mut values = child_row.values.clone();
        for col in &set_null.columns {
            let col_def = child_table.column(col).ok_or_else(|| {
                KitError::Integrity(format!("set-null column {col} not found"))
            })?;
            if !col_def.nullable {
                return Err(KitError::Restrict(format!(
                    "set-null on non-nullable column {col}"
                )));
            }
            values.insert(col.clone(), Value::Null);
        }
        mongreldb_kit_core::validation::validate_row(child_table, &values)?;
        let cells = row_to_core_cells(&values, child_table)?;
        self.core
            .delete(&set_null.table, RowId(child_row.row_id))
            .map_err(KitError::from)?;
        self.core.put(&set_null.table, cells).map_err(KitError::from)?;
        self.staged.push(StagedOp::Update {
            table: set_null.table.clone(),
            old_row_id: child_row.row_id,
            values,
            temp_id: self.next_temp_id,
        });
        self.next_temp_id += 1;
        Ok(())
    }
}

fn apply_defaults(row: &mut Map<String, Value>, table: &KitTable) -> Result<()> {
    for col in &table.columns {
        if row.contains_key(&col.name) && row.get(&col.name) != Some(&Value::Null) {
            continue;
        }
        if let Some(default) = &col.default {
            let value = match default {
                DefaultKind::Static(v) => v.clone(),
                DefaultKind::Now => {
                    let _now = std::time::SystemTime::now();
                    let dt = std::time::UNIX_EPOCH.elapsed().map(|d| d.as_secs() as i64).unwrap_or(0);
                    Value::String(format!("{dt}"))
                }
                DefaultKind::Uuid => Value::String(uuid::Uuid::new_v4().to_string()),
                DefaultKind::Sequence(name) => {
                    return Err(KitError::Validation(format!(
                        "sequence default {name} not implemented"
                    )))
                }
                DefaultKind::CustomName(name) => {
                    return Err(KitError::Validation(format!(
                        "custom default {name} not implemented"
                    )))
                }
            };
            row.insert(col.name.clone(), value);
        }
    }
    Ok(())
}

fn pk_matches(values: &Map<String, Value>, pk_map: &Map<String, Value>, table: &KitTable) -> bool {
    for name in &table.primary_key {
        let expected = pk_map.get(name);
        let actual = values.get(name);
        if expected != actual {
            return false;
        }
    }
    true
}

fn uq_key(values: &Map<String, Value>, _table: &KitTable, uq: &mongreldb_kit_core::schema::UniqueConstraint) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(uq.columns.len());
    for name in &uq.columns {
        let v = values.get(name).cloned().unwrap_or(Value::Null);
        parts.push(value_to_key(&v));
    }
    parts.join(":")
}

fn value_to_key(value: &Value) -> String {
    match value {
        Value::Null => "\0".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(a) => a.iter().map(value_to_key).collect::<Vec<_>>().join(","),
        Value::Object(o) => {
            let mut keys: Vec<String> = o.keys().cloned().collect();
            keys.sort();
            keys.iter()
                .map(|k| format!("{k}={}", value_to_key(o.get(k).unwrap())))
                .collect::<Vec<_>>()
                .join(",")
        }
    }
}

fn pk_value_to_string(pk: &Value, table: &KitTable) -> Result<String> {
    if table.primary_key.len() == 1 {
        match pk {
            Value::String(s) => Ok(s.clone()),
            Value::Number(n) => Ok(n.to_string()),
            Value::Bool(b) => Ok(b.to_string()),
            _ => Err(KitError::Validation("unsupported pk type".into())),
        }
    } else {
        match pk {
            Value::Object(obj) => {
                let mut parts = Vec::new();
                for name in &table.primary_key {
                    let v = obj
                        .get(name)
                        .cloned()
                        .unwrap_or(Value::Null);
                    parts.push(value_to_key(&v));
                }
                Ok(parts.join(":"))
            }
            _ => Err(KitError::Validation("composite pk must be object".into())),
        }
    }
}

fn pk_string_to_value(pk: &str, table: &KitTable) -> Result<Value> {
    if table.primary_key.len() == 1 {
        let col = table.column(&table.primary_key[0]).ok_or_else(|| {
            KitError::Integrity(format!("pk column {} not found", table.primary_key[0]))
        })?;
        match col.storage_type {
            mongreldb_kit_core::schema::ColumnType::Text
            | mongreldb_kit_core::schema::ColumnType::Date
            | mongreldb_kit_core::schema::ColumnType::DateTime => Ok(Value::String(pk.to_string())),
            mongreldb_kit_core::schema::ColumnType::Int8
            | mongreldb_kit_core::schema::ColumnType::Int16
            | mongreldb_kit_core::schema::ColumnType::Int32
            | mongreldb_kit_core::schema::ColumnType::Int64
            | mongreldb_kit_core::schema::ColumnType::TimestampNanos => Ok(Value::Number(
                pk.parse::<i64>().map_err(|_| KitError::Validation(format!("invalid int pk {pk}")))?.into(),
            )),
            _ => Err(KitError::Validation("unsupported pk type".into())),
        }
    } else {
        Err(KitError::Validation("composite pk not supported from string".into()))
    }
}

fn pk_value_from_fk(
    values: &Map<String, Value>,
    _child_table: &KitTable,
    fk: &mongreldb_kit_core::schema::ForeignKey,
    parent_table: &KitTable,
) -> Result<Value> {
    if parent_table.primary_key.len() == 1 {
        let child_col = fk
            .columns
            .first()
            .ok_or_else(|| KitError::Integrity("fk has no columns".into()))?;
        let v = values
            .get(child_col)
            .cloned()
            .unwrap_or(Value::Null);
        Ok(v)
    } else {
        let mut obj = Map::new();
        for (child_col, parent_col) in fk.columns.iter().zip(&parent_table.primary_key) {
            let v = values
                .get(child_col)
                .cloned()
                .unwrap_or(Value::Null);
            obj.insert(parent_col.clone(), v);
        }
        Ok(Value::Object(obj))
    }
}

fn fk_matches(
    child_values: &Map<String, Value>,
    _child_table: &KitTable,
    fk: &mongreldb_kit_core::schema::ForeignKey,
    parent_pk_map: &Map<String, Value>,
    parent_table: &KitTable,
) -> bool {
    for (child_col, parent_col) in fk.columns.iter().zip(&parent_table.primary_key) {
        let child_val = child_values.get(child_col);
        let parent_val = parent_pk_map.get(parent_col);
        if child_val != parent_val {
            return false;
        }
    }
    true
}
