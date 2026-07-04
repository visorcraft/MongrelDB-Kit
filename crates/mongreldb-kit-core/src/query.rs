//! Query AST for the language-neutral Kit model.
//!
//! This module defines expression and statement trees. Execution is left to
//! the storage-backed `mongreldb-kit` crate; `mongreldb-kit-core` only
//! validates structure and provides serialization.

use serde::{Deserialize, Serialize};

/// A scalar literal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(serde_json::Value),
}

/// A query expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expr {
    /// Reference to a table column.
    Column(String),
    /// A constant value.
    Literal(Literal),
    /// Logical conjunction.
    And(Vec<Expr>),
    /// Logical disjunction.
    Or(Vec<Expr>),
    /// Logical negation.
    Not(Box<Expr>),
    /// Equality.
    Eq(Box<Expr>, Box<Expr>),
    /// Inequality.
    Ne(Box<Expr>, Box<Expr>),
    /// Greater than.
    Gt(Box<Expr>, Box<Expr>),
    /// Greater than or equal.
    Gte(Box<Expr>, Box<Expr>),
    /// Less than.
    Lt(Box<Expr>, Box<Expr>),
    /// Less than or equal.
    Lte(Box<Expr>, Box<Expr>),
    /// Membership in a list.
    In(Box<Expr>, Vec<Literal>),
    /// Non-membership in a list.
    NotIn(Box<Expr>, Vec<Literal>),
    /// IS NULL test.
    IsNull(Box<Expr>),
    /// IS NOT NULL test.
    IsNotNull(Box<Expr>),
    /// SQL `LIKE` pattern match (`%`/`_` wildcards).
    Like(Box<Expr>, String),
    /// Case-sensitive substring containment. Equivalent to `LIKE '%needle%'`
    /// but without treating `%`/`_` in the needle as wildcards.
    Contains(Box<Expr>, String),
    /// Anchored prefix match on a Bytes column with a bitmap index — the
    /// exact equivalent of `LIKE 'prefix%'` (no wildcards in `prefix`). Pushes
    /// down to the engine's `Condition::BytesPrefix` exactly (no residual
    /// re-check). Falls back to a `starts_with` residual when the column lacks
    /// a bitmap index or pushdown is bypassed (e.g. a CTE-materialized source).
    BytesPrefix(Box<Expr>, String),
    /// Membership in the rows produced by a sub-`SELECT` (its first projected
    /// column). The subquery is evaluated against the same execution context, so
    /// it may read other tables or materialized CTEs.
    InSubquery(Box<Expr>, Box<Select>),
    /// `EXISTS (subquery)` — true when the subquery yields at least one row.
    Exists(Box<Select>),
    /// `NOT EXISTS (subquery)` — true when the subquery yields no rows.
    NotExists(Box<Select>),
}

/// Sort direction for `ORDER BY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Asc,
    Desc,
}

/// An `ORDER BY` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBy {
    pub expr: Expr,
    pub direction: Direction,
}

/// A `SELECT` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Select {
    pub table: String,
    pub columns: Vec<Expr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Expr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<OrderBy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
}

/// An `INSERT` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Insert {
    pub table: String,
    pub values: serde_json::Map<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// An `UPDATE` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    pub set: serde_json::Map<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Expr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
    /// Optional single-row target: the primary key value to update.
    /// Mutually exclusive with `filter`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pk: Option<serde_json::Value>,
}

/// A `DELETE` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Expr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
    /// Optional single-row target: the primary key value to delete.
    /// Mutually exclusive with `filter`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pk: Option<serde_json::Value>,
}

/// An `UPSERT` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Upsert {
    pub table: String,
    pub values: serde_json::Map<String, serde_json::Value>,
    pub on_conflict: OnConflict,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// SQL-style conflict behavior for `UPSERT`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnConflict {
    DoNothing,
    DoUpdate(serde_json::Map<String, serde_json::Value>),
}

/// Top-level query statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Query {
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Upsert(Upsert),
    Aggregate(AggregateQuery),
    Join(JoinQuery),
}

/// An aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// A single aggregate output column, e.g. `SUM(total) AS total_sum`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Aggregate {
    pub func: AggFunc,
    /// Source column. `None` means `COUNT(*)` (only valid for `Count`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// Output column name in each result row.
    pub alias: String,
    /// `DISTINCT` modifier, e.g. `COUNT(DISTINCT col)`. Requires a `column`; it
    /// is a no-op for `MIN`/`MAX`. Defaults to `false` so existing serialized
    /// queries deserialize unchanged.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub distinct: bool,
}

/// A grouped/aggregated query. Rows are scanned, optionally filtered, grouped by
/// `group_by` key columns, reduced with `aggregates`, and finally filtered by
/// `having`. With no `group_by`, the whole (filtered) table is one group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateQuery {
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Expr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    pub aggregates: Vec<Aggregate>,
    /// Predicate applied to each produced group row. Column references resolve to
    /// the group key columns or aggregate aliases.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having: Option<Expr>,
}

/// The kind of join to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    #[default]
    Inner,
    Left,
    Cross,
}

/// One joined table in a [`JoinQuery`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Join {
    #[serde(default)]
    pub kind: JoinKind,
    pub table: String,
    /// Optional alias; defaults to the table name when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// Join predicate. Column references must be qualified (`alias.column`).
    /// Ignored (and may be omitted) for `Cross` joins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<Expr>,
}

/// A nested-loop join query. The result is a list of combined rows; each combined
/// row is a JSON object keyed by table alias whose values are that source's row
/// object (or JSON `null` for an unmatched right side of a `LEFT` join). Column
/// references in `filter`/`order_by`/join `on` must be qualified (`alias.column`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinQuery {
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub joins: Vec<Join>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Expr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<OrderBy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
}

/// A common table expression: a named, materialized subquery. A later query can
/// read from `name` as if it were a table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cte {
    pub name: String,
    pub query: Box<Select>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_serializes_to_expected_shape() {
        let q = Query::Select(Select {
            table: "users".into(),
            columns: vec![Expr::Column("id".into()), Expr::Column("email".into())],
            filter: Some(Expr::Eq(
                Box::new(Expr::Column("id".into())),
                Box::new(Expr::Literal(Literal::Int(1))),
            )),
            order_by: vec![OrderBy {
                expr: Expr::Column("id".into()),
                direction: Direction::Desc,
            }],
            limit: Some(10),
            offset: Some(5),
        });

        let json = serde_json::to_value(&q).unwrap();
        assert_eq!(json["select"]["table"], "users");
        assert_eq!(json["select"]["columns"][0]["column"], "id");
        assert_eq!(json["select"]["filter"]["eq"][0]["column"], "id");
        assert_eq!(json["select"]["filter"]["eq"][1]["literal"]["int"], 1);
        assert_eq!(json["select"]["order_by"][0]["direction"], "desc");
        assert_eq!(json["select"]["limit"], 10);
    }

    #[test]
    fn query_roundtrips() {
        let q = Query::Select(Select {
            table: "t".into(),
            columns: vec![Expr::Column("x".into())],
            filter: Some(Expr::And(vec![
                Expr::Not(Box::new(Expr::IsNull(Box::new(Expr::Column("y".into()))))),
                Expr::Like(Box::new(Expr::Column("z".into())), "%test%".into()),
            ])),
            order_by: vec![],
            limit: None,
            offset: None,
        });
        let json = serde_json::to_string(&q).unwrap();
        let back: Query = serde_json::from_str(&json).unwrap();
        assert_eq!(q, back);
    }

    #[test]
    fn extended_exprs_and_queries_roundtrip() {
        let sub = Select {
            table: "orders".into(),
            columns: vec![Expr::Column("user_id".into())],
            filter: Some(Expr::Gt(
                Box::new(Expr::Column("total".into())),
                Box::new(Expr::Literal(Literal::Int(100))),
            )),
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let select = Select {
            table: "users".into(),
            columns: vec![],
            filter: Some(Expr::And(vec![
                Expr::Contains(Box::new(Expr::Column("email".into())), "@x".into()),
                Expr::InSubquery(Box::new(Expr::Column("id".into())), Box::new(sub.clone())),
                Expr::Exists(Box::new(sub.clone())),
                Expr::Not(Box::new(Expr::NotExists(Box::new(sub.clone())))),
            ])),
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let q = Query::Select(select);
        let back: Query = serde_json::from_str(&serde_json::to_string(&q).unwrap()).unwrap();
        assert_eq!(q, back);

        let agg = Query::Aggregate(AggregateQuery {
            table: "orders".into(),
            filter: None,
            group_by: vec!["user_id".into()],
            aggregates: vec![
                Aggregate {
                    func: AggFunc::Count,
                    column: None,
                    alias: "n".into(),
                    distinct: false,
                },
                Aggregate {
                    func: AggFunc::Sum,
                    column: Some("total".into()),
                    alias: "total_sum".into(),
                    distinct: false,
                },
            ],
            having: Some(Expr::Gt(
                Box::new(Expr::Column("n".into())),
                Box::new(Expr::Literal(Literal::Int(1))),
            )),
        });
        let back: Query = serde_json::from_str(&serde_json::to_string(&agg).unwrap()).unwrap();
        assert_eq!(agg, back);

        let join = Query::Join(JoinQuery {
            table: "users".into(),
            alias: None,
            joins: vec![Join {
                kind: JoinKind::Left,
                table: "orders".into(),
                alias: None,
                on: Some(Expr::Eq(
                    Box::new(Expr::Column("users.id".into())),
                    Box::new(Expr::Column("orders.user_id".into())),
                )),
            }],
            filter: None,
            order_by: vec![],
            limit: None,
            offset: None,
        });
        let back: Query = serde_json::from_str(&serde_json::to_string(&join).unwrap()).unwrap();
        assert_eq!(join, back);

        let cte = Cte {
            name: "big".into(),
            query: Box::new(sub),
        };
        let back: Cte = serde_json::from_str(&serde_json::to_string(&cte).unwrap()).unwrap();
        assert_eq!(cte, back);
    }

    #[test]
    fn insert_and_delete_roundtrip() {
        let mut values = serde_json::Map::new();
        values.insert("name".into(), serde_json::Value::String("alice".into()));

        let q = Query::Insert(Insert {
            table: "users".into(),
            values,
            returning: vec![],
        });
        let back: Query = serde_json::from_str(&serde_json::to_string(&q).unwrap()).unwrap();
        assert_eq!(q, back);

        let d = Query::Delete(Delete {
            table: "users".into(),
            filter: Some(Expr::In(
                Box::new(Expr::Column("id".into())),
                vec![Literal::Int(1), Literal::Int(2)],
            )),
            returning: vec![],
            pk: None,
        });
        let back: Query = serde_json::from_str(&serde_json::to_string(&d).unwrap()).unwrap();
        assert_eq!(d, back);
    }
}
