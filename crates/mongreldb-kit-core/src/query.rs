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
    /// SQL `LIKE` pattern match.
    Like(Box<Expr>, String),
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
}

/// An `UPDATE` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    pub set: serde_json::Map<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Expr>,
}

/// A `DELETE` statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Expr>,
}

/// Top-level query statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Query {
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
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
    fn insert_and_delete_roundtrip() {
        let mut values = serde_json::Map::new();
        values.insert("name".into(), serde_json::Value::String("alice".into()));

        let q = Query::Insert(Insert {
            table: "users".into(),
            values,
        });
        let back: Query = serde_json::from_str(&serde_json::to_string(&q).unwrap()).unwrap();
        assert_eq!(q, back);

        let d = Query::Delete(Delete {
            table: "users".into(),
            filter: Some(Expr::In(
                Box::new(Expr::Column("id".into())),
                vec![Literal::Int(1), Literal::Int(2)],
            )),
        });
        let back: Query = serde_json::from_str(&serde_json::to_string(&d).unwrap()).unwrap();
        assert_eq!(d, back);
    }
}
