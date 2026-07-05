//! Predicate pushdown: translate Kit `Expr` predicates into MongrelDB core
//! `Condition`s so the native engine can resolve them via indexes instead of a
//! full-scan + Rust evaluation (Kit Priority 1).
//!
//! ## Design
//!
//! The Kit's query layer materializes every visible row and evaluates
//! predicates in Rust. This module is the bridge that lets simple, index-
//! served predicates bypass that full scan. It produces [`PushdownPlan`] — a
//! list of native [`Condition`]s that the core engine can resolve via HOT /
//! bitmap / range indexes, plus a flag indicating whether the translation
//! covered the entire predicate.
//!
//! Core's native `Query` is a **conjunction** (AND of conditions) that resolves
//! to a row-id set. Conditions always return a **superset** of the matching
//! rows; the Kit must still re-apply the original `Expr` filter in Rust on the
//! survivors unless [`PushdownPlan::fully_translated`] is true. This makes
//! partial translation safe by construction.
//!
//! ## Supported translations
//!
//! | Kit `Expr` | Core `Condition` | Requirement |
//! |---|---|---|
//! | `Eq(Column, Literal)` on PK | `Pk` | single-column PK |
//! | `Eq(Column, Literal)` | `BitmapEq` | bitmap-indexed column |
//! | `In(Column, [literals])` | `BitmapIn` | bitmap-indexed column |
//! | `Lt/Lte/Gt/Gte(Column, Literal)` int | `Range` | int-typed column |
//! | `Lt/Lte/Gt/Gte(Column, Literal)` float | `RangeF64` | float-typed column |
//! | `Contains(Column, needle)` | `FmContains` | FM-indexed column (residual re-check) |
//! | `Like(Column, pattern)` | `FmContainsAll` | FM-indexed column (residual re-check) |
//! | `IsNull` / `IsNotNull(Column)` | `IsNull` / `IsNotNull` | page-stat-aware (residual re-check) |
//! | `And([sub-exprs])` | recurse each | each part translated independently |
//!
//! Unsupported (`Or`, `Not`, `Ne`, `NotIn`, `InSubquery`, `Exists`, cross-column
//! comparisons) are left as residual Rust evaluation — the caller falls back to
//! a full scan for those branches.

use mongreldb_core::query::Condition;
use mongreldb_kit_core::query::{Expr, Literal};
use mongreldb_kit_core::schema::{ColumnType, IndexKind as KitIndexKind, Table as KitTable};
use serde_json::{Map, Value};

/// The result of translating a Kit predicate into core conditions.
#[derive(Debug, Clone)]
pub struct PushdownPlan {
    /// Native conditions to push to core (ANDed together).
    pub conditions: Vec<Condition>,
    /// Whether the entire source `Expr` was translated. When `true`, the core
    /// query result is exact and no Rust-side re-filtering is needed.
    pub fully_translated: bool,
}

impl PushdownPlan {
    /// Whether there are conditions worth pushing (vs. a full scan).
    pub fn can_push(&self) -> bool {
        !self.conditions.is_empty()
    }
}

/// Attempt to translate `expr` into native `Condition`s for `table`. Returns
/// `None` when no part of the expression could be translated (caller falls back
/// to a full scan + Rust evaluation).
pub fn translate_predicate(table: &KitTable, expr: &Expr) -> Option<PushdownPlan> {
    let mut conditions = Vec::new();
    let fully = collect_conditions(table, expr, &mut conditions);
    if conditions.is_empty() {
        return None;
    }
    Some(PushdownPlan {
        conditions,
        fully_translated: fully,
    })
}

/// Recursively collect translatable conditions from `expr`. Returns `true` if
/// the entire sub-expression was translated (no residual needed for it).
fn collect_conditions(table: &KitTable, expr: &Expr, out: &mut Vec<Condition>) -> bool {
    match expr {
        Expr::And(parts) => {
            let mut all = true;
            for part in parts {
                if !collect_conditions(table, part, out) {
                    all = false;
                }
            }
            all
        }
        Expr::Eq(a, b) => push_if_some(out, try_translate_eq(table, a, b)),
        Expr::Lt(a, b) => push_if_some(out, try_translate_cmp(table, a, b, CmpOp::Lt)),
        Expr::Lte(a, b) => push_if_some(out, try_translate_cmp(table, a, b, CmpOp::Lte)),
        Expr::Gt(a, b) => push_if_some(out, try_translate_cmp(table, a, b, CmpOp::Gt)),
        Expr::Gte(a, b) => push_if_some(out, try_translate_cmp(table, a, b, CmpOp::Gte)),
        Expr::In(a, list) => push_if_some(out, try_translate_in(table, a, list)),
        // BytesPrefix is exact in the engine (no residual re-check): the
        // bitmap's distinct keys are enumerated and filtered by prefix. Returns
        // `true` only when a bitmap-indexed Bytes column matched.
        Expr::BytesPrefix(a, prefix) => {
            push_if_some(out, try_translate_bytes_prefix(table, a, prefix))
        }
        Expr::Contains(a, needle) => {
            // FM substring: push `FmContains` when the column has an FM index,
            // but keep `Contains` as a residual (the engine returns a superset)
            // by reporting the sub-expression as not fully translated.
            if let Some(c) = try_translate_contains(table, a, needle) {
                out.push(c);
            }
            false
        }
        Expr::Like(a, pattern) => {
            // Multi-segment LIKE: push `FmContainsAll` of the literal runs (a
            // superset — order and wildcards are re-checked by the residual).
            if let Some(c) = try_translate_like(table, a, pattern) {
                out.push(c);
            }
            false
        }
        Expr::IsNull(a) => {
            if let Some(c) = try_translate_null(table, a, true) {
                out.push(c);
            }
            false
        }
        Expr::IsNotNull(a) => {
            if let Some(c) = try_translate_null(table, a, false) {
                out.push(c);
            }
            false
        }
        // Unsupported: Or, Not, Ne, NotIn, Like, InSubquery, Exists, NotExists →
        // leave as residual Rust evaluation.
        _ => false,
    }
}

/// Translate `IsNull(Column)` / `IsNotNull(Column)` into the engine's page-stat-
/// aware null conditions. Kept as a residual (the engine returns a superset).
fn try_translate_null(table: &KitTable, a: &Expr, is_null: bool) -> Option<Condition> {
    let Expr::Column(col_name) = a else {
        return None;
    };
    let column_id = table.column(col_name)?.id as u16;
    Some(if is_null {
        Condition::IsNull { column_id }
    } else {
        Condition::IsNotNull { column_id }
    })
}

/// Push `opt` into `out` if `Some`, returning whether it was pushed.
fn push_if_some(out: &mut Vec<Condition>, opt: Option<Condition>) -> bool {
    match opt {
        Some(c) => {
            out.push(c);
            true
        }
        None => false,
    }
}

#[derive(Clone, Copy)]
enum CmpOp {
    Lt,
    Lte,
    Gt,
    Gte,
}

/// Extract a `(column_name, literal)` pair from two expression sides, handling
/// both `Column op Literal` and `Literal op Column` orderings.
fn extract_column_literal<'a>(a: &'a Expr, b: &'a Expr) -> Option<(&'a str, &'a Literal)> {
    match (a, b) {
        (Expr::Column(name), Expr::Literal(lit)) => Some((name.as_str(), lit)),
        (Expr::Literal(lit), Expr::Column(name)) => Some((name.as_str(), lit)),
        _ => None, // Column-op-Column is not translatable.
    }
}

/// Translate `Eq(Column, Literal)` into `Pk` (single-col PK) or `BitmapEq`.
fn try_translate_eq(table: &KitTable, a: &Expr, b: &Expr) -> Option<Condition> {
    let (col_name, lit) = extract_column_literal(a, b)?;
    let col = table.column(col_name)?;
    let col_id = col.id as u16;
    let ty = col.storage_type;

    // Single-column PK → O(1) HOT probe via Condition::Pk.
    if table.primary_key.len() == 1 && table.primary_key[0] == col_name {
        let key = literal_to_index_key(lit, ty)?;
        return Some(Condition::Pk(key));
    }

    // Bitmap-indexed column → BitmapEq. The Kit creates bitmap indexes for
    // every declared index and unique constraint column (see schema::to_core_schema).
    if has_bitmap_index(table, col_name) {
        let value = literal_to_index_key(lit, ty)?;
        return Some(Condition::BitmapEq {
            column_id: col_id,
            value,
        });
    }

    None
}

/// Translate `Lt/Lte/Gt/Gte(Column, Literal)` into `Range` (int) or `RangeF64`.
fn try_translate_cmp(table: &KitTable, a: &Expr, b: &Expr, op: CmpOp) -> Option<Condition> {
    let (col_name, lit) = extract_column_literal(a, b)?;
    let col = table.column(col_name)?;
    let col_id = col.id as u16;
    let ty = col.storage_type;

    if is_int_type(ty) {
        let v = literal_to_i64(lit)?;
        let (lo, hi) = match op {
            CmpOp::Lt => (i64::MIN, v.saturating_sub(1)),
            CmpOp::Lte => (i64::MIN, v),
            CmpOp::Gt => (v.saturating_add(1), i64::MAX),
            CmpOp::Gte => (v, i64::MAX),
        };
        Some(Condition::Range {
            column_id: col_id,
            lo,
            hi,
        })
    } else if is_float_type(ty) {
        let v = literal_to_f64(lit)?;
        let (lo, lo_inc, hi, hi_inc) = match op {
            CmpOp::Lt => (f64::NEG_INFINITY, false, v, false),
            CmpOp::Lte => (f64::NEG_INFINITY, false, v, true),
            CmpOp::Gt => (v, false, f64::INFINITY, false),
            CmpOp::Gte => (v, true, f64::INFINITY, false),
        };
        Some(Condition::RangeF64 {
            column_id: col_id,
            lo,
            lo_inclusive: lo_inc,
            hi,
            hi_inclusive: hi_inc,
        })
    } else {
        None
    }
}

/// Whether `col_name` has a declared FM (substring) index.
fn has_fm_index(table: &KitTable, col_name: &str) -> bool {
    table
        .indexes
        .iter()
        .any(|idx| idx.kind == KitIndexKind::Fm && idx.columns.iter().any(|c| c == col_name))
}

/// Translate `Like(Column, pattern)` into `FmContainsAll` of the pattern's
/// literal runs (the text between `%`/`_` wildcards) when the column has an FM
/// index. Every literal run is a required substring, so the result is a superset
/// of the real `LIKE`; the Kit re-checks the pattern in Rust. Escaped patterns
/// (containing `\`) are left to a full scan.
fn try_translate_like(table: &KitTable, a: &Expr, pattern: &str) -> Option<Condition> {
    let Expr::Column(col_name) = a else {
        return None;
    };
    let col = table.column(col_name)?;
    if !has_fm_index(table, col_name) || pattern.contains('\\') {
        return None;
    }
    let patterns: Vec<Vec<u8>> = pattern
        .split(['%', '_'])
        .filter(|s| !s.is_empty())
        .map(|s| s.as_bytes().to_vec())
        .collect();
    if patterns.is_empty() {
        return None;
    }
    Some(Condition::FmContainsAll {
        column_id: col.id as u16,
        patterns,
    })
}

/// Translate `Contains(Column, needle)` into `FmContains` when the column has an
/// FM index. The engine returns rows whose column contains `needle` as a
/// substring — a superset the Kit re-checks in Rust.
fn try_translate_contains(table: &KitTable, a: &Expr, needle: &str) -> Option<Condition> {
    let Expr::Column(col_name) = a else {
        return None;
    };
    let col = table.column(col_name)?;
    if !has_fm_index(table, col_name) {
        return None;
    }
    Some(Condition::FmContains {
        column_id: col.id as u16,
        pattern: needle.as_bytes().to_vec(),
    })
}

/// Translate `BytesPrefix(Column, prefix)` into the engine's exact
/// `Condition::BytesPrefix` — requires a bitmap index on the column (the
/// engine enumerates the bitmap's distinct keys and keeps those starting with
/// `prefix`). Returns `None` (→ residual `starts_with` evaluation) when the
/// column has no bitmap index or the operand isn't a bare column reference.
fn try_translate_bytes_prefix(table: &KitTable, a: &Expr, prefix: &str) -> Option<Condition> {
    let Expr::Column(col_name) = a else {
        return None;
    };
    let col = table.column(col_name)?;
    if !has_bitmap_index(table, col_name) {
        return None;
    }
    Some(Condition::BytesPrefix {
        column_id: col.id as u16,
        prefix: prefix.as_bytes().to_vec(),
    })
}

/// Translate `In(Column, [literals])` into `BitmapIn`.
fn try_translate_in(table: &KitTable, a: &Expr, list: &[Literal]) -> Option<Condition> {
    let Expr::Column(col_name) = a else {
        return None;
    };
    let col = table.column(col_name)?;
    let col_id = col.id as u16;
    let ty = col.storage_type;
    if !has_bitmap_index(table, col_name) {
        return None;
    }
    let mut values = Vec::with_capacity(list.len());
    for lit in list {
        values.push(literal_to_index_key(lit, ty)?);
    }
    Some(Condition::BitmapIn {
        column_id: col_id,
        values,
    })
}

/// Build a `Condition::Pk` (or bitmap fallback) from a PK value map. Used by
/// `get_by_pk_internal` and FK `parent_exists` — the highest-value pushdown
/// targets since they currently do O(N) linear scans with the PK in hand.
///
/// For a single-column PK, produces one `Condition::Pk` (O(1) HOT probe).
/// For a composite PK, produces `BitmapEq` conditions for each PK column
/// (intersected by the core query).
pub fn pk_conditions(table: &KitTable, pk_map: &Map<String, Value>) -> Option<Vec<Condition>> {
    if pk_map.is_empty() || table.primary_key.is_empty() {
        return None;
    }
    let mut conditions = Vec::with_capacity(table.primary_key.len());
    for pk_name in &table.primary_key {
        let value = pk_map.get(pk_name)?;
        let col = table.column(pk_name)?;
        let lit = json_to_literal(value)?;
        let key = literal_to_index_key(&lit, col.storage_type)?;
        if table.primary_key.len() == 1 {
            // Single-column PK: use HOT for O(1) lookup.
            conditions.push(Condition::Pk(key));
        } else {
            // Composite PK: use bitmap eq on each PK column.
            conditions.push(Condition::BitmapEq {
                column_id: col.id as u16,
                value: key,
            });
        }
    }
    Some(conditions)
}

// ── value encoding helpers ──────────────────────────────────────────────

/// Check if the Kit table has a bitmap index on `col_name` (declared index or
/// unique constraint). Mirrors the logic in `schema::to_core_schema`.
fn has_bitmap_index(table: &KitTable, col_name: &str) -> bool {
    table
        .indexes
        .iter()
        .any(|idx| {
            // Only a real bitmap index (the default kind) backs `BitmapEq`/
            // `BitmapIn`. A `LearnedRange`/`Ann`/`Fm`/`Sparse`/`MinHash` index
            // on the same column does NOT — the engine returns an empty set
            // for `BitmapEq` against a non-bitmap column, so treating those as
            // bitmaps here would silently drop matching rows.
            // Only a real bitmap index (the default kind) backs `BitmapEq`/
            // `BitmapIn`. A `LearnedRange`/`Ann`/`Fm`/`Sparse`/`MinHash` index
            // on the same column does NOT — the engine returns an empty set
            // for `BitmapEq` against a non-bitmap column, so treating those as
            // bitmaps here would silently drop matching rows.
            idx.kind == KitIndexKind::Bitmap && idx.columns.iter().any(|c| c == col_name)
        })
        || table
            .unique_constraints
            .iter()
            .any(|uq| uq.columns.iter().any(|c| c == col_name))
        || table.primary_key.contains(&col_name.to_string())
}

/// A column backed by a real **bitmap** index (declared index or unique
/// constraint) — unlike [`has_bitmap_index`], excludes the primary key, which
/// gets a HOT (not bitmap) index in `to_core_schema`. `BitmapIn` on a column
/// without a bitmap index returns an empty set (not a superset), so the FK-join
/// probe must only build one when this is true.
pub(crate) fn has_declared_bitmap_index(table: &KitTable, col_name: &str) -> bool {
    table
        .indexes
        .iter()
        .any(|idx| idx.columns.iter().any(|c| c == col_name))
        || table
            .unique_constraints
            .iter()
            .any(|uq| uq.columns.iter().any(|c| c == col_name))
}

/// Encode a JSON value into the bitmap-index key bytes for column type `ty`,
/// matching [`literal_to_index_key`]. Returns `None` for nulls / unencodable
/// values (which then simply don't contribute a probe key).
pub(crate) fn value_index_key(v: &Value, ty: ColumnType) -> Option<Vec<u8>> {
    let lit = match v {
        Value::Bool(b) => Literal::Bool(*b),
        Value::Number(n) => n
            .as_i64()
            .map(Literal::Int)
            .or_else(|| n.as_f64().map(Literal::Float))?,
        Value::String(s) => Literal::Text(s.clone()),
        _ => return None,
    };
    literal_to_index_key(&lit, ty)
}

fn is_int_type(ty: ColumnType) -> bool {
    matches!(
        ty,
        ColumnType::Int8
            | ColumnType::Int16
            | ColumnType::Int32
            | ColumnType::Int64
            | ColumnType::Bool
            | ColumnType::TimestampNanos
    )
}

fn is_float_type(ty: ColumnType) -> bool {
    matches!(ty, ColumnType::Float32 | ColumnType::Float64)
}

/// Encode a `Literal` to the byte form that core's bitmap/HOT indexes use
/// (matching [`mongreldb_core::memtable::Value::encode_key`]).
fn literal_to_index_key(lit: &Literal, ty: ColumnType) -> Option<Vec<u8>> {
    match lit {
        Literal::Null => None, // Nulls are not indexed.
        Literal::Bool(b) => Some(vec![*b as u8]),
        Literal::Int(n) => {
            if is_float_type(ty) {
                // The column stores Float64; encode as f64 bits.
                Some((*n as f64).to_bits().to_be_bytes().to_vec())
            } else {
                Some(n.to_be_bytes().to_vec())
            }
        }
        Literal::Float(f) => Some(f.to_bits().to_be_bytes().to_vec()),
        Literal::Text(s) => Some(s.as_bytes().to_vec()),
        Literal::Json(v) => Some(serde_json::to_vec(v).ok()?),
    }
}

fn literal_to_i64(lit: &Literal) -> Option<i64> {
    match lit {
        Literal::Int(n) => Some(*n),
        Literal::Bool(b) => Some(*b as i64),
        _ => None,
    }
}

fn literal_to_f64(lit: &Literal) -> Option<f64> {
    match lit {
        Literal::Int(n) => Some(*n as f64),
        Literal::Float(f) => Some(*f),
        _ => None,
    }
}

/// Convert a JSON `Value` to a `Literal` for the PK-conditions builder.
fn json_to_literal(value: &Value) -> Option<Literal> {
    match value {
        Value::Null => Some(Literal::Null),
        Value::Bool(b) => Some(Literal::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Literal::Int(i))
            } else {
                Some(Literal::Float(n.as_f64()?))
            }
        }
        Value::String(s) => Some(Literal::Text(s.clone())),
        Value::Array(_) | Value::Object(_) => Some(Literal::Json(value.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongreldb_kit_core::query::Expr;
    use mongreldb_kit_core::schema::{Column, Index, Table as KitTable};

    fn sample_table() -> KitTable {
        KitTable {
            id: 1,
            name: "users".into(),
            columns: vec![
                {
                    let mut c = Column::new(1, "id", ColumnType::Int64);
                    c.primary_key = true;
                    c
                },
                Column::new(2, "email", ColumnType::Text),
                {
                    let mut c = Column::new(3, "age", ColumnType::Int64);
                    c.nullable = true;
                    c
                },
                {
                    let mut c = Column::new(4, "score", ColumnType::Float64);
                    c.nullable = true;
                    c
                },
            ],
            primary_key: vec!["id".into()],
            indexes: vec![Index {
                name: "idx_email".into(),
                columns: vec!["email".into()],
                unique: false,
                kind: Default::default(),
            }],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }
    }

    #[test]
    fn translate_pk_eq() {
        let t = sample_table();
        let expr = Expr::Eq(
            Box::new(Expr::Column("id".into())),
            Box::new(Expr::Literal(Literal::Int(42))),
        );
        let plan = translate_predicate(&t, &expr).expect("should translate");
        assert!(plan.fully_translated);
        assert_eq!(plan.conditions.len(), 1);
        assert!(matches!(plan.conditions[0], Condition::Pk(_)));
    }

    #[test]
    fn translate_bitmap_eq() {
        let t = sample_table();
        let expr = Expr::Eq(
            Box::new(Expr::Column("email".into())),
            Box::new(Expr::Literal(Literal::Text("a@b.com".into()))),
        );
        let plan = translate_predicate(&t, &expr).expect("should translate");
        assert!(plan.fully_translated);
        assert!(matches!(
            plan.conditions[0],
            Condition::BitmapEq { column_id: 2, .. }
        ));
    }

    #[test]
    fn translate_int_range() {
        let t = sample_table();
        let expr = Expr::Gte(
            Box::new(Expr::Column("age".into())),
            Box::new(Expr::Literal(Literal::Int(18))),
        );
        let plan = translate_predicate(&t, &expr).expect("should translate");
        assert!(plan.fully_translated);
        assert!(matches!(
            plan.conditions[0],
            Condition::Range {
                column_id: 3,
                lo: 18,
                ..
            }
        ));
    }

    #[test]
    fn translate_and_of_translatable() {
        let t = sample_table();
        let expr = Expr::And(vec![
            Expr::Eq(
                Box::new(Expr::Column("email".into())),
                Box::new(Expr::Literal(Literal::Text("a@b.com".into()))),
            ),
            Expr::Gt(
                Box::new(Expr::Column("age".into())),
                Box::new(Expr::Literal(Literal::Int(21))),
            ),
        ]);
        let plan = translate_predicate(&t, &expr).expect("should translate");
        assert!(plan.fully_translated);
        assert_eq!(plan.conditions.len(), 2);
    }

    #[test]
    fn translate_and_partial() {
        let t = sample_table();
        // email = 'a@b.com' AND (age > 21 OR score < 5)
        let expr = Expr::And(vec![
            Expr::Eq(
                Box::new(Expr::Column("email".into())),
                Box::new(Expr::Literal(Literal::Text("a@b.com".into()))),
            ),
            Expr::Or(vec![
                Expr::Gt(
                    Box::new(Expr::Column("age".into())),
                    Box::new(Expr::Literal(Literal::Int(21))),
                ),
                Expr::Lt(
                    Box::new(Expr::Column("score".into())),
                    Box::new(Expr::Literal(Literal::Float(5.0))),
                ),
            ]),
        ]);
        let plan = translate_predicate(&t, &expr).expect("should partially translate");
        assert!(!plan.fully_translated); // OR is not translatable
        assert_eq!(plan.conditions.len(), 1); // only the bitmap eq pushed
    }

    #[test]
    fn translate_unsupported_returns_none() {
        let t = sample_table();
        let expr = Expr::Or(vec![
            Expr::Eq(
                Box::new(Expr::Column("email".into())),
                Box::new(Expr::Literal(Literal::Text("a@b.com".into()))),
            ),
            Expr::Eq(
                Box::new(Expr::Column("email".into())),
                Box::new(Expr::Literal(Literal::Text("c@d.com".into()))),
            ),
        ]);
        assert!(translate_predicate(&t, &expr).is_none());
    }

    #[test]
    fn translate_in_to_bitmap_in() {
        let t = sample_table();
        let expr = Expr::In(
            Box::new(Expr::Column("email".into())),
            vec![
                Literal::Text("a@b.com".into()),
                Literal::Text("c@d.com".into()),
            ],
        );
        let plan = translate_predicate(&t, &expr).expect("should translate");
        assert!(plan.fully_translated);
        assert!(matches!(
            plan.conditions[0],
            Condition::BitmapIn { column_id: 2, .. }
        ));
    }

    #[test]
    fn pk_conditions_single_col() {
        let t = sample_table();
        let mut pk_map = Map::new();
        pk_map.insert("id".into(), Value::Number(42.into()));
        let conds = pk_conditions(&t, &pk_map).expect("should build");
        assert_eq!(conds.len(), 1);
        assert!(matches!(conds[0], Condition::Pk(_)));
    }
}
