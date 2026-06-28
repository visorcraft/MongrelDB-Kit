//! Delete planning: compute the set of rows affected by a delete, including
//! cascade, set-null, and restrict foreign-key actions.

use crate::schema::{ForeignKey, ForeignKeyAction, Schema, Table};

/// A row identified for deletion planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRef {
    pub table: String,
    pub pk: String,
}

/// A child row that must be updated to set its foreign-key columns to null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetNullUpdate {
    pub table: String,
    pub pk: String,
    pub columns: Vec<String>,
}

/// A foreign-key constraint that blocks the delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestrictedConstraint {
    pub table: String,
    pub constraint: String,
}

/// Result of planning a delete against a schema.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeletePlan {
    /// Rows to delete, in an order that respects dependencies (children first).
    pub delete: Vec<RowRef>,
    /// Child rows whose foreign-key columns must be nulled before the parent is deleted.
    pub set_null: Vec<SetNullUpdate>,
    /// Constraints that would be violated if the delete proceeded.
    pub restricted: Vec<RestrictedConstraint>,
}

/// Errors returned by delete planning.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlannerError {
    #[error("table \"{0}\" not found")]
    TableNotFound(String),
    #[error("circular delete detected involving table \"{0}\"")]
    CircularDelete(String),
}

/// Plan the deletion of a single row identified by its table and primary key.
///
/// The plan does not interact with storage; callers supply child row lookup via
/// the `find_children` closure. This keeps the core crate storage-agnostic.
pub fn plan_delete<F>(
    schema: &Schema,
    table_name: &str,
    pk: &str,
    find_children: F,
) -> Result<DeletePlan, PlannerError>
where
    F: Fn(&Table, &ForeignKey, &str) -> Vec<(String, String)>,
{
    let mut plan = DeletePlan::default();
    let mut path = Vec::new();
    let mut deleted = std::collections::HashSet::new();

    plan_delete_recursive(
        schema,
        table_name,
        pk,
        &find_children,
        &mut path,
        &mut deleted,
        &mut plan,
    )?;

    Ok(plan)
}

fn plan_delete_recursive<F>(
    schema: &Schema,
    table_name: &str,
    pk: &str,
    find_children: &F,
    path: &mut Vec<String>,
    deleted: &mut std::collections::HashSet<String>,
    plan: &mut DeletePlan,
) -> Result<bool, PlannerError>
where
    F: Fn(&Table, &ForeignKey, &str) -> Vec<(String, String)>,
{
    let visit_key = format!("{table_name}:{pk}");

    if deleted.contains(&visit_key) {
        return Ok(true);
    }
    if path.contains(&visit_key) {
        return Err(PlannerError::CircularDelete(table_name.into()));
    }

    path.push(visit_key.clone());

    let _table = schema
        .table(table_name)
        .ok_or_else(|| PlannerError::TableNotFound(table_name.into()))?
        .clone();

    let mut restricted = false;

    for child_table in &schema.tables {
        for fk in &child_table.foreign_keys {
            if fk.references_table != table_name {
                continue;
            }

            for (child_pk, _parent_pk_hint) in find_children(child_table, fk, pk) {
                match fk.on_delete {
                    ForeignKeyAction::Restrict => {
                        plan.restricted.push(RestrictedConstraint {
                            table: child_table.name.clone(),
                            constraint: fk.name.clone(),
                        });
                        restricted = true;
                    }
                    ForeignKeyAction::SetNull => {
                        plan.set_null.push(SetNullUpdate {
                            table: child_table.name.clone(),
                            pk: child_pk,
                            columns: fk.columns.clone(),
                        });
                    }
                    ForeignKeyAction::Cascade => {
                        let child_deletable = plan_delete_recursive(
                            schema,
                            &child_table.name,
                            &child_pk,
                            find_children,
                            path,
                            deleted,
                            plan,
                        )?;
                        if !child_deletable {
                            restricted = true;
                        }
                    }
                }
            }
        }
    }

    path.pop();

    if restricted {
        return Ok(false);
    }

    plan.delete.push(RowRef {
        table: table_name.into(),
        pk: pk.into(),
    });
    deleted.insert(visit_key);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, ColumnType, ForeignKey, Table};

    fn table(name: &str, id: u32, pk_col: &str) -> Table {
        Table {
            id,
            name: name.into(),
            columns: vec![Column::new(1, pk_col, ColumnType::Text)],
            primary_key: vec![pk_col.into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }
    }

    fn fk(name: &str, cols: &[&str], parent: &str, parent_cols: &[&str], action: ForeignKeyAction) -> ForeignKey {
        ForeignKey {
            name: name.into(),
            columns: cols.iter().map(|c| (*c).into()).collect(),
            references_table: parent.into(),
            references_columns: parent_cols.iter().map(|c| (*c).into()).collect(),
            on_delete: action,
        }
    }

    /// Child lookup that returns a single hard-coded child per FK.
    fn single_child(child_pk: &str) -> impl Fn(&Table, &ForeignKey, &str) -> Vec<(String, String)> + use<'_> {
        move |_table: &Table, _fk: &ForeignKey, _parent_pk: &str| {
            vec![(child_pk.into(), _parent_pk.into())]
        }
    }

    #[test]
    fn restrict_blocks_delete() {
        let parent = table("parent", 1, "id");
        let mut child = table("child", 2, "id");
        child.foreign_keys = vec![fk("fk_child_parent", &["parent_id"], "parent", &["id"], ForeignKeyAction::Restrict)];
        child.columns.push(Column::new(2, "parent_id", ColumnType::Text));

        let schema = Schema::new(vec![parent, child]).unwrap();
        let plan = plan_delete(&schema, "parent", "p1", single_child("c1")).unwrap();

        assert!(plan.delete.is_empty());
        assert!(plan.set_null.is_empty());
        assert_eq!(
            plan.restricted,
            vec![RestrictedConstraint {
                table: "child".into(),
                constraint: "fk_child_parent".into(),
            }]
        );
    }

    #[test]
    fn cascade_deletes_children() {
        let parent = table("parent", 1, "id");
        let mut child = table("child", 2, "id");
        child.foreign_keys = vec![fk("fk_child_parent", &["parent_id"], "parent", &["id"], ForeignKeyAction::Cascade)];
        child.columns.push(Column::new(2, "parent_id", ColumnType::Text));

        let schema = Schema::new(vec![parent, child]).unwrap();
        let plan = plan_delete(&schema, "parent", "p1", single_child("c1")).unwrap();

        assert!(plan.set_null.is_empty());
        assert!(plan.restricted.is_empty());
        assert_eq!(plan.delete.len(), 2);
        assert_eq!(plan.delete[0].table, "child");
        assert_eq!(plan.delete[1].table, "parent");
    }

    #[test]
    fn set_null_updates_children() {
        let parent = table("parent", 1, "id");
        let mut child = table("child", 2, "id");
        child.foreign_keys = vec![fk("fk_child_parent", &["parent_id"], "parent", &["id"], ForeignKeyAction::SetNull)];
        child.columns.push(Column::new(2, "parent_id", ColumnType::Text));

        let schema = Schema::new(vec![parent, child]).unwrap();
        let plan = plan_delete(&schema, "parent", "p1", single_child("c1")).unwrap();

        assert!(plan.restricted.is_empty());
        assert_eq!(plan.delete.len(), 1);
        assert_eq!(plan.set_null.len(), 1);
        assert_eq!(plan.set_null[0].columns, vec!["parent_id"]);
    }

    #[test]
    fn detects_circular_delete() {
        let mut a = table("a", 1, "id");
        a.foreign_keys = vec![fk("fk_a_b", &["b_id"], "b", &["id"], ForeignKeyAction::Cascade)];
        a.columns.push(Column::new(2, "b_id", ColumnType::Text));

        let mut b = table("b", 2, "id");
        b.foreign_keys = vec![fk("fk_b_a", &["a_id"], "a", &["id"], ForeignKeyAction::Cascade)];
        b.columns.push(Column::new(2, "a_id", ColumnType::Text));

        let schema = Schema::new(vec![a, b]).unwrap();
        let lookup = |_table: &Table, _fk: &ForeignKey, parent_pk: &str| {
            vec![(parent_pk.to_string().replace('p', "other"), parent_pk.into())]
        };
        let err = plan_delete(&schema, "a", "a1", lookup).unwrap_err();
        assert!(matches!(err, PlannerError::CircularDelete(_)));
    }
}
