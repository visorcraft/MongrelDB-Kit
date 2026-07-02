//! Query execution for kit statements.
//!
//! The implementation is intentionally simple: it materializes every visible row
//! from the target table(s), evaluates predicates in Rust, then sorts, groups,
//! joins, and reduces in memory. This keeps the crate independent of MongrelDB
//! core's native query primitives while remaining correct for the supported
//! subset.
//!
//! ponytail: every operator here is computed in-memory after a full scan; there
//! is no predicate/projection pushdown into MongrelDB native conditions on the
//! Rust path. That is the deliberate non-pushdown ceiling — the async SQL engine
//! path is out of scope for this crate.

use crate::error::{KitError, Result};
use crate::schema::{core_row_to_json, Row};
use mongreldb_core::memtable::Row as CoreRow;
use mongreldb_core::query::Condition;
use mongreldb_kit_core::query::{
    AggFunc, Aggregate, AggregateQuery, Direction, Expr, JoinKind, JoinQuery, Literal, OrderBy,
    Query, Select,
};
use mongreldb_kit_core::schema::{Schema as KitSchema, Table as KitTable};
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// A combined join row: a JSON object keyed by table alias whose values are the
/// matched source rows (or JSON `null` for an unmatched right side of a `LEFT`
/// join). See [`JoinQuery`] for the documented shape.
pub type JoinRow = Map<String, Value>;

/// Fetches the visible rows for a real (non-CTE) table at a read snapshot.
/// When `Some(conditions)` is provided, the fetcher resolves them via native
/// indexes and returns only matching rows (Kit Priority 1 pushdown).
type TableFetch<'a> = Box<dyn Fn(&str, Option<&[Condition]>) -> Result<Vec<Row>> + 'a>;

/// Resolves the base rows for a table name and carries materialized CTEs.
///
/// CTE materializations shadow real tables, so a later query can read a `with`
/// result by name. Subqueries and joins evaluate against the same context, which
/// is how cross-table reads stay consistent at the transaction's read snapshot.
pub(crate) struct ExecCtx<'a> {
    schema: Option<&'a KitSchema>,
    fetch: TableFetch<'a>,
    ctes: HashMap<String, Vec<Row>>,
}

impl<'a> ExecCtx<'a> {
    pub(crate) fn new(
        schema: Option<&'a KitSchema>,
        fetch: impl Fn(&str, Option<&[Condition]>) -> Result<Vec<Row>> + 'a,
    ) -> Self {
        Self {
            schema,
            fetch: Box::new(fetch),
            ctes: HashMap::new(),
        }
    }

    pub(crate) fn add_cte(&mut self, name: String, rows: Vec<Row>) {
        self.ctes.insert(name, rows);
    }

    fn table_rows(&self, name: &str) -> Result<Vec<Row>> {
        if let Some(rows) = self.ctes.get(name) {
            return Ok(rows.clone());
        }
        (self.fetch)(name, None)
    }

    /// Fetch rows for `name` with native `conditions` pushed to the engine.
    /// Falls back to an unfiltered fetch for CTEs (which are already
    /// materialized) or empty conditions.
    fn table_rows_filtered(&self, name: &str, conditions: &[Condition]) -> Result<Vec<Row>> {
        if conditions.is_empty() || self.ctes.contains_key(name) {
            return self.table_rows(name);
        }
        (self.fetch)(name, Some(conditions))
    }

    fn table_def(&self, name: &str) -> Option<&KitTable> {
        self.schema.and_then(|s| s.table(name))
    }
}

/// A name-resolution scope for predicate/value evaluation.
trait Scope {
    fn get(&self, name: &str) -> Value;
}

/// Resolves bare column names against a single flat row.
struct FlatScope<'a>(&'a Map<String, Value>);

impl Scope for FlatScope<'_> {
    fn get(&self, name: &str) -> Value {
        self.0.get(name).cloned().unwrap_or(Value::Null)
    }
}

/// Resolves `alias.column` (and, as a fallback, bare column) names against a
/// combined join row keyed by table alias.
struct JoinScope<'a>(&'a Map<String, Value>);

impl Scope for JoinScope<'_> {
    fn get(&self, name: &str) -> Value {
        if let Some((alias, col)) = name.split_once('.') {
            return self
                .0
                .get(alias)
                .and_then(|t| t.as_object())
                .and_then(|o| o.get(col))
                .cloned()
                .unwrap_or(Value::Null);
        }
        for value in self.0.values() {
            if let Some(obj) = value.as_object() {
                if let Some(v) = obj.get(name) {
                    return v.clone();
                }
            }
        }
        Value::Null
    }
}

// ── public entry points ─────────────────────────────────────────────────────

/// Execute a kit [`Query::Select`] against the supplied visible rows.
///
/// `rows` must be the newest visible version of every non-deleted row in the
/// target table at the transaction's read snapshot. This standalone form cannot
/// resolve other tables, so subqueries/joins referencing a different table fail.
pub fn execute_select(
    table: &KitTable,
    visible_rows: Vec<CoreRow>,
    select: &Select,
) -> Result<Vec<Row>> {
    let rows: Vec<Row> = visible_rows
        .into_iter()
        .map(|r| core_row_to_json(&r, table))
        .collect::<Result<Vec<_>>>()?;
    let mut ctx = ExecCtx::new(None, |name: &str, _conds: Option<&[Condition]>| {
        Err(KitError::Validation(format!(
            "table {name} is not available outside a transaction context"
        )))
    });
    ctx.add_cte(table.name.clone(), rows);
    run_select(&ctx, select)
}

/// Execute any supported kit query statement against visible rows.
pub fn execute_query(
    table: &KitTable,
    visible_rows: Vec<CoreRow>,
    query: &Query,
) -> Result<Vec<Row>> {
    match query {
        Query::Select(select) => execute_select(table, visible_rows, select),
        _ => Err(KitError::Validation(
            "only SELECT queries are supported by execute_query".into(),
        )),
    }
}

// ── select ──────────────────────────────────────────────────────────────────

pub(crate) fn run_select(ctx: &ExecCtx, select: &Select) -> Result<Vec<Row>> {
    // Kit Priority 1 pushdown: translate the filter into native Conditions so
    // the engine resolves them via indexes (HOT/bitmap/range) instead of a
    // full scan. Core conditions return a superset, so the original filter is
    // re-applied in Rust when the translation was partial.
    let (mut rows, residual_needed) = match &select.filter {
        Some(filter) => {
            let pushed = ctx
                .table_def(&select.table)
                .and_then(|t| crate::pushdown::translate_predicate(t, filter));
            match pushed {
                Some(plan) if plan.can_push() => {
                    let fetched = ctx.table_rows_filtered(&select.table, &plan.conditions)?;
                    (fetched, !plan.fully_translated)
                }
                _ => (ctx.table_rows(&select.table)?, true),
            }
        }
        None => (ctx.table_rows(&select.table)?, false),
    };

    // Residual Rust evaluation for partially-translated predicates.
    if residual_needed {
        if let Some(filter) = &select.filter {
            let mut kept = Vec::with_capacity(rows.len());
            for r in rows {
                if eval_pred(filter, &FlatScope(&r.values), ctx)? {
                    kept.push(r);
                }
            }
            rows = kept;
        }
    }

    for order in &select.order_by {
        sort_rows(ctx, &mut rows, &select.table, order)?;
    }

    apply_limit_offset(&mut rows, select.limit, select.offset);
    Ok(rows)
}

/// Project to `select.columns` (when given) then drop duplicate rows, preserving
/// first-seen order. Used by `select_distinct`.
pub(crate) fn project_distinct(select: &Select, rows: Vec<Row>) -> Vec<Row> {
    let cols: Vec<String> = select
        .columns
        .iter()
        .filter_map(|e| match e {
            Expr::Column(n) => Some(n.clone()),
            _ => None,
        })
        .collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for r in rows {
        let values = if cols.is_empty() {
            r.values
        } else {
            let mut m = Map::new();
            for c in &cols {
                m.insert(c.clone(), r.values.get(c).cloned().unwrap_or(Value::Null));
            }
            m
        };
        let key = serde_json::to_string(&values).unwrap_or_default();
        if seen.insert(key) {
            out.push(Row { row_id: 0, values });
        }
    }
    out
}

// ── aggregates / group by / having ──────────────────────────────────────────

pub(crate) fn run_aggregate(ctx: &ExecCtx, query: &AggregateQuery) -> Result<Vec<Row>> {
    // Kit Priority 1 pushdown: same strategy as run_select.
    let (mut rows, residual_needed) = match &query.filter {
        Some(filter) => {
            let pushed = ctx
                .table_def(&query.table)
                .and_then(|t| crate::pushdown::translate_predicate(t, filter));
            match pushed {
                Some(plan) if plan.can_push() => {
                    let fetched = ctx.table_rows_filtered(&query.table, &plan.conditions)?;
                    (fetched, !plan.fully_translated)
                }
                _ => (ctx.table_rows(&query.table)?, true),
            }
        }
        None => (ctx.table_rows(&query.table)?, false),
    };

    if residual_needed {
        if let Some(filter) = &query.filter {
            let mut kept = Vec::with_capacity(rows.len());
            for r in rows {
                if eval_pred(filter, &FlatScope(&r.values), ctx)? {
                    kept.push(r);
                }
            }
            rows = kept;
        }
    }

    // Group rows by the key columns, preserving first-seen group order.
    let mut groups: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
    if query.group_by.is_empty() {
        groups.push((Vec::new(), rows));
    } else {
        let mut index: HashMap<String, usize> = HashMap::new();
        for r in rows {
            let key_vals: Vec<Value> = query
                .group_by
                .iter()
                .map(|c| r.values.get(c).cloned().unwrap_or(Value::Null))
                .collect();
            let key_str = serde_json::to_string(&key_vals).unwrap_or_default();
            match index.get(&key_str) {
                Some(&i) => groups[i].1.push(r),
                None => {
                    index.insert(key_str, groups.len());
                    groups.push((key_vals, vec![r]));
                }
            }
        }
    }

    let mut out = Vec::with_capacity(groups.len());
    for (key_vals, group_rows) in groups {
        let mut values = Map::new();
        for (col, val) in query.group_by.iter().zip(key_vals.iter()) {
            values.insert(col.clone(), val.clone());
        }
        for agg in &query.aggregates {
            values.insert(agg.alias.clone(), compute_aggregate(agg, &group_rows)?);
        }
        if let Some(having) = &query.having {
            if !eval_pred(having, &FlatScope(&values), ctx)? {
                continue;
            }
        }
        out.push(Row { row_id: 0, values });
    }
    Ok(out)
}

fn compute_aggregate(agg: &Aggregate, rows: &[Row]) -> Result<Value> {
    match agg.func {
        AggFunc::Count => {
            let n = match (&agg.column, agg.distinct) {
                // COUNT(*) — DISTINCT is meaningless without a column.
                (None, _) => rows.len(),
                // COUNT(DISTINCT col): unique non-null values.
                (Some(col), true) => distinct_non_null(rows, col).len(),
                // COUNT(col): non-null values.
                (Some(col), false) => rows
                    .iter()
                    .filter(|r| !r.values.get(col).map(Value::is_null).unwrap_or(true))
                    .count(),
            };
            Ok(Value::Number((n as i64).into()))
        }
        AggFunc::Sum | AggFunc::Avg => {
            let col = require_agg_column(agg)?;
            // DISTINCT ⇒ aggregate over the distinct non-null values.
            let (nums, all_int): (Vec<f64>, bool) = if agg.distinct {
                let vals = distinct_non_null(rows, col);
                let all_int = vals.iter().all(|v| v.as_i64().is_some());
                (vals.iter().filter_map(|&v| num_of(v)).collect(), all_int)
            } else {
                let all_int = rows
                    .iter()
                    .filter_map(|r| r.values.get(col))
                    .filter(|v| !v.is_null())
                    .all(|v| v.as_i64().is_some());
                (numeric_values(rows, col), all_int)
            };
            if nums.is_empty() {
                return Ok(Value::Null);
            }
            let sum: f64 = nums.iter().sum();
            if matches!(agg.func, AggFunc::Avg) {
                return Ok(number_value(sum / nums.len() as f64));
            }
            // Preserve integer-ness when every summand was an integer.
            if all_int {
                Ok(Value::Number((sum as i64).into()))
            } else {
                Ok(number_value(sum))
            }
        }
        AggFunc::Min | AggFunc::Max => {
            let col = require_agg_column(agg)?;
            let mut best: Option<&Value> = None;
            for r in rows {
                let Some(v) = r.values.get(col) else { continue };
                if v.is_null() {
                    continue;
                }
                best = Some(match best {
                    None => v,
                    Some(cur) => {
                        let take = matches!(
                            (json_cmp(v, cur), agg.func),
                            (Some(Ordering::Less), AggFunc::Min)
                                | (Some(Ordering::Greater), AggFunc::Max)
                        );
                        if take {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            Ok(best.cloned().unwrap_or(Value::Null))
        }
    }
}

fn require_agg_column(agg: &Aggregate) -> Result<&String> {
    agg.column
        .as_ref()
        .ok_or_else(|| KitError::Validation(format!("aggregate {:?} requires a column", agg.func)))
}

fn numeric_values(rows: &[Row], col: &str) -> Vec<f64> {
    rows.iter()
        .filter_map(|r| r.values.get(col))
        .filter_map(num_of)
        .collect()
}

fn num_of(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn number_value(f: f64) -> Value {
    serde_json::Number::from_f64(f)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// Distinct non-null values of `col` across `rows`, deduplicated by their
/// canonical JSON form — the same value identity GROUP BY keys use. Backs the
/// `DISTINCT` aggregates (`COUNT`/`SUM`/`AVG`).
fn distinct_non_null<'a>(rows: &'a [Row], col: &str) -> Vec<&'a Value> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for r in rows {
        if let Some(v) = r.values.get(col) {
            if v.is_null() {
                continue;
            }
            if seen.insert(serde_json::to_string(v).unwrap_or_default()) {
                out.push(v);
            }
        }
    }
    out
}

// ── joins ───────────────────────────────────────────────────────────────────

/// When a join `ON` is `right.col = left.col` (column = column) and the right
/// column has a declared bitmap index, build a `BitmapIn` over the distinct
/// left-side key values so the right table is fetched by an index probe instead
/// of a full scan. `eval_pred` still re-checks the predicate for every combined
/// row, so this only shrinks the candidate set — the result is identical.
/// Returns `None` (→ full scan) for any non-FK-equality shape, a right column
/// without a bitmap index, or an empty probe set.
fn fk_join_condition(
    right_table: &KitTable,
    right_alias: &str,
    on: &Expr,
    left_rows: &[JoinRow],
) -> Option<Condition> {
    let Expr::Eq(a, b) = on else { return None };
    let (Expr::Column(ca), Expr::Column(cb)) = (a.as_ref(), b.as_ref()) else {
        return None;
    };
    let prefix = format!("{right_alias}.");
    // Exactly one side must reference the right (joined) alias.
    let (right_qual, left_qual) = match (ca.starts_with(&prefix), cb.starts_with(&prefix)) {
        (true, false) => (ca.as_str(), cb.as_str()),
        (false, true) => (cb.as_str(), ca.as_str()),
        _ => return None,
    };
    let right_col = right_qual.strip_prefix(&prefix)?;
    if !crate::pushdown::has_declared_bitmap_index(right_table, right_col) {
        return None;
    }
    let col = right_table.column(right_col)?;
    let (left_alias, left_col) = left_qual.split_once('.')?;

    let mut seen = std::collections::HashSet::new();
    let mut values = Vec::new();
    for row in left_rows {
        let key = row
            .get(left_alias)
            .and_then(|t| t.as_object())
            .and_then(|o| o.get(left_col))
            .and_then(|v| crate::pushdown::value_index_key(v, col.storage_type));
        if let Some(key) = key {
            if seen.insert(key.clone()) {
                values.push(key);
            }
        }
    }
    if values.is_empty() {
        return None;
    }
    Some(Condition::BitmapIn {
        column_id: col.id as u16,
        values,
    })
}

/// Flatten an `Expr::And` chain into its conjuncts. A non-And expr yields a
/// single-element vec.
fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::And(parts) => parts.iter().flat_map(conjuncts).collect(),
        other => vec![other],
    }
}

/// Collect every `alias.column` alias referenced in `expr`. Bare column names
/// contribute no alias (they're ambiguous in a join context).
fn collect_aliases(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Column(name) => {
            if let Some((alias, _)) = name.split_once('.') {
                out.insert(alias.to_string());
            }
        }
        Expr::And(parts) | Expr::Or(parts) => {
            for p in parts {
                collect_aliases(p, out);
            }
        }
        Expr::Not(e) => collect_aliases(e, out),
        Expr::Eq(a, b)
        | Expr::Ne(a, b)
        | Expr::Gt(a, b)
        | Expr::Gte(a, b)
        | Expr::Lt(a, b)
        | Expr::Lte(a, b) => {
            collect_aliases(a, out);
            collect_aliases(b, out);
        }
        Expr::In(a, _)
        | Expr::NotIn(a, _)
        | Expr::Like(a, _)
        | Expr::Contains(a, _)
        | Expr::IsNull(a)
        | Expr::IsNotNull(a) => collect_aliases(a, out),
        Expr::InSubquery(a, _) => collect_aliases(a, out),
        Expr::Exists(_) | Expr::NotExists(_) => {
            out.insert("__subquery__".to_string());
        }
        Expr::Literal(_) => {}
    };
}

/// If `expr` references columns from exactly one alias (all `alias.column`
/// for the same alias), return that alias. Conjuncts with bare column names
/// or subqueries return `None` (conservative — left as post-join residuals).
fn single_alias(expr: &Expr) -> Option<String> {
    let mut set = HashSet::new();
    collect_aliases(expr, &mut set);
    if set.len() == 1 {
        return set.into_iter().next();
    }
    None
}

/// Rewrite `alias.column` → `column` in a cloned expression tree, so the
/// conjunct can be translated against a flat table row / `translate_predicate`.
fn strip_alias_prefix(expr: &Expr, alias: &str) -> Expr {
    match expr {
        Expr::Column(name) => match name.split_once('.') {
            Some((a, col)) if a == alias => Expr::Column(col.to_string()),
            _ => Expr::Column(name.clone()),
        },
        Expr::And(parts) => Expr::And(parts.iter().map(|p| strip_alias_prefix(p, alias)).collect()),
        Expr::Or(parts) => Expr::Or(parts.iter().map(|p| strip_alias_prefix(p, alias)).collect()),
        Expr::Not(e) => Expr::Not(Box::new(strip_alias_prefix(e, alias))),
        Expr::Eq(a, b) => Expr::Eq(
            Box::new(strip_alias_prefix(a, alias)),
            Box::new(strip_alias_prefix(b, alias)),
        ),
        Expr::Ne(a, b) => Expr::Ne(
            Box::new(strip_alias_prefix(a, alias)),
            Box::new(strip_alias_prefix(b, alias)),
        ),
        Expr::Gt(a, b) => Expr::Gt(
            Box::new(strip_alias_prefix(a, alias)),
            Box::new(strip_alias_prefix(b, alias)),
        ),
        Expr::Gte(a, b) => Expr::Gte(
            Box::new(strip_alias_prefix(a, alias)),
            Box::new(strip_alias_prefix(b, alias)),
        ),
        Expr::Lt(a, b) => Expr::Lt(
            Box::new(strip_alias_prefix(a, alias)),
            Box::new(strip_alias_prefix(b, alias)),
        ),
        Expr::Lte(a, b) => Expr::Lte(
            Box::new(strip_alias_prefix(a, alias)),
            Box::new(strip_alias_prefix(b, alias)),
        ),
        Expr::In(a, list) => Expr::In(Box::new(strip_alias_prefix(a, alias)), list.clone()),
        Expr::NotIn(a, list) => Expr::NotIn(Box::new(strip_alias_prefix(a, alias)), list.clone()),
        Expr::IsNull(a) => Expr::IsNull(Box::new(strip_alias_prefix(a, alias))),
        Expr::IsNotNull(a) => Expr::IsNotNull(Box::new(strip_alias_prefix(a, alias))),
        Expr::Like(a, pat) => Expr::Like(Box::new(strip_alias_prefix(a, alias)), pat.clone()),
        Expr::Contains(a, needle) => {
            Expr::Contains(Box::new(strip_alias_prefix(a, alias)), needle.clone())
        }
        Expr::InSubquery(a, sub) => {
            Expr::InSubquery(Box::new(strip_alias_prefix(a, alias)), sub.clone())
        }
        other => other.clone(),
    }
}

/// Split a join filter into per-alias groups. Returns:
/// - `side_filters`: conjuncts pushable to a single alias (stripped of prefix).
/// - `residual`: conjuncts that span multiple aliases (or have bare refs /
///   subqueries) — must be evaluated post-join.
fn split_join_filter(filter: Option<&Expr>) -> (HashMap<String, Vec<Expr>>, Vec<&Expr>) {
    let mut side_filters: HashMap<String, Vec<Expr>> = HashMap::new();
    let mut residual: Vec<&Expr> = Vec::new();
    let parts = match filter {
        Some(f) => conjuncts(f),
        None => return (side_filters, residual),
    };
    for c in parts {
        match single_alias(c) {
            Some(alias) => {
                let stripped = strip_alias_prefix(c, &alias);
                side_filters.entry(alias).or_default().push(stripped);
            }
            None => residual.push(c),
        }
    }
    (side_filters, residual)
}

/// Translate a group of stripped conjuncts for `table` into native Conditions
/// (for engine pushdown). Returns the combined conditions (may be partial —
/// the caller should still re-evaluate the conjuncts in Rust).
fn side_conditions(ctx: &ExecCtx, table: &str, conjuncts: &[Expr]) -> Vec<Condition> {
    let Some(t) = ctx.table_def(table) else {
        return Vec::new();
    };
    conjuncts
        .iter()
        .filter_map(|c| crate::pushdown::translate_predicate(t, c))
        .flat_map(|p| p.conditions)
        .collect()
}

/// Apply a group of stripped conjuncts in Rust against a flat row.
fn passes_side_filter(conjuncts: &[Expr], row: &Map<String, Value>, ctx: &ExecCtx) -> Result<bool> {
    if conjuncts.is_empty() {
        return Ok(true);
    }
    let scope = FlatScope(row);
    for c in conjuncts {
        if !eval_pred(c, &scope, ctx)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn run_join(ctx: &ExecCtx, query: &JoinQuery) -> Result<Vec<JoinRow>> {
    let base_alias = query.alias.clone().unwrap_or_else(|| query.table.clone());

    // P5: split the post-join filter into per-alias groups. Single-alias
    // conjuncts are pushed to the engine (via Conditions) AND re-applied in
    // Rust on the fetched rows; multi-alias conjuncts stay as post-join
    // residuals. This avoids materializing rows that the filter would drop.
    let (side_filters, residual) = split_join_filter(query.filter.as_ref());

    // ── base side ─────────────────────────────────────────────────────────
    let base_conjuncts = side_filters.get(&base_alias).cloned().unwrap_or_default();
    let base_conds = side_conditions(ctx, &query.table, &base_conjuncts);
    let base_rows = if base_conds.is_empty() {
        ctx.table_rows(&query.table)?
    } else {
        ctx.table_rows_filtered(&query.table, &base_conds)?
    };
    let mut acc: Vec<JoinRow> = Vec::with_capacity(base_rows.len());
    for r in base_rows {
        if passes_side_filter(&base_conjuncts, &r.values, ctx)? {
            let mut m = Map::new();
            m.insert(base_alias.clone(), Value::Object(r.values));
            acc.push(m);
        }
    }

    for join in &query.joins {
        let right_alias = join.alias.clone().unwrap_or_else(|| join.table.clone());
        // P5: push right-side filter conjuncts alongside the FK-equality probe.
        let right_conjuncts = side_filters.get(&right_alias).cloned().unwrap_or_default();
        let mut right_conds = side_conditions(ctx, &join.table, &right_conjuncts);
        // For an FK-equality ON with a bitmap-indexed right column, probe the
        // right table by `BitmapIn` over the accumulated left keys instead of a
        // full scan; otherwise fall back to the full scan.
        let fk_cond = match (join.kind, join.on.as_ref()) {
            (JoinKind::Cross, _) | (_, None) => None,
            (_, Some(on)) => ctx
                .table_def(&join.table)
                .and_then(|t| fk_join_condition(t, &right_alias, on, &acc)),
        };
        if let Some(c) = fk_cond {
            right_conds.push(c);
        }
        let right_rows_fetched = if right_conds.is_empty() {
            ctx.table_rows(&join.table)?
        } else {
            ctx.table_rows_filtered(&join.table, &right_conds)?
        };
        // Apply right-side conjuncts in Rust (handles partial translation).
        let mut right_rows: Vec<Row> = Vec::with_capacity(right_rows_fetched.len());
        for r in right_rows_fetched {
            if passes_side_filter(&right_conjuncts, &r.values, ctx)? {
                right_rows.push(r);
            }
        }
        let mut next = Vec::new();
        for left in acc {
            let mut matched = false;
            for rr in &right_rows {
                let mut combined = left.clone();
                combined.insert(right_alias.clone(), Value::Object(rr.values.clone()));
                let keep = match join.kind {
                    JoinKind::Cross => true,
                    _ => match &join.on {
                        Some(on) => eval_pred(on, &JoinScope(&combined), ctx)?,
                        None => true,
                    },
                };
                if keep {
                    matched = true;
                    next.push(combined);
                }
            }
            if !matched && join.kind == JoinKind::Left {
                let mut combined = left;
                combined.insert(right_alias.clone(), Value::Null);
                next.push(combined);
            }
        }
        acc = next;
    }

    // P5: only multi-alias conjuncts remain as post-join residuals.
    if !residual.is_empty() {
        let mut kept = Vec::with_capacity(acc.len());
        for row in acc {
            let ok = residual.iter().try_fold(true, |acc, c| {
                eval_pred(c, &JoinScope(&row), ctx).map(|v| acc && v)
            })?;
            if ok {
                kept.push(row);
            }
        }
        acc = kept;
    }

    for order in &query.order_by {
        let key = match &order.expr {
            Expr::Column(n) => n.clone(),
            other => {
                return Err(KitError::Validation(format!(
                    "unsupported order by: {other:?}"
                )))
            }
        };
        acc.sort_by(|a, b| {
            let av = JoinScope(a).get(&key);
            let bv = JoinScope(b).get(&key);
            let ord = json_cmp(&av, &bv).unwrap_or(Ordering::Equal);
            match order.direction {
                Direction::Asc => ord,
                Direction::Desc => ord.reverse(),
            }
        });
    }

    apply_limit_offset(&mut acc, query.limit, query.offset);
    Ok(acc)
}

// ── expression evaluation ───────────────────────────────────────────────────

fn eval_pred<S: Scope>(expr: &Expr, scope: &S, ctx: &ExecCtx) -> Result<bool> {
    Ok(match expr {
        Expr::Column(name) => truthy(&scope.get(name)),
        Expr::Literal(lit) => truthy(&literal_to_value(lit)),
        Expr::And(parts) => {
            for p in parts {
                if !eval_pred(p, scope, ctx)? {
                    return Ok(false);
                }
            }
            true
        }
        Expr::Or(parts) => {
            for p in parts {
                if eval_pred(p, scope, ctx)? {
                    return Ok(true);
                }
            }
            false
        }
        Expr::Not(inner) => !eval_pred(inner, scope, ctx)?,
        Expr::Eq(a, b) => cmp(a, b, scope, ctx)? == Some(Ordering::Equal),
        Expr::Ne(a, b) => cmp(a, b, scope, ctx)? != Some(Ordering::Equal),
        Expr::Gt(a, b) => cmp(a, b, scope, ctx)? == Some(Ordering::Greater),
        Expr::Gte(a, b) => cmp(a, b, scope, ctx)?.is_some_and(|o| o != Ordering::Less),
        Expr::Lt(a, b) => cmp(a, b, scope, ctx)? == Some(Ordering::Less),
        Expr::Lte(a, b) => cmp(a, b, scope, ctx)?.is_some_and(|o| o != Ordering::Greater),
        Expr::In(col, list) => {
            let v = eval_val(col, scope, ctx)?;
            list.iter().any(|lit| v == literal_to_value(lit))
        }
        Expr::NotIn(col, list) => {
            let v = eval_val(col, scope, ctx)?;
            list.iter().all(|lit| v != literal_to_value(lit))
        }
        Expr::IsNull(inner) => eval_val(inner, scope, ctx)?.is_null(),
        Expr::IsNotNull(inner) => !eval_val(inner, scope, ctx)?.is_null(),
        Expr::Like(col, pattern) => match eval_val(col, scope, ctx)? {
            Value::String(s) => like(&s, pattern),
            _ => false,
        },
        Expr::Contains(col, needle) => match eval_val(col, scope, ctx)? {
            Value::String(s) => s.contains(needle.as_str()),
            _ => false,
        },
        Expr::InSubquery(col, sub) => {
            // ponytail: subqueries are uncorrelated — the sub-SELECT is evaluated
            // once against the same snapshot/CTEs and cannot reference the outer
            // row. That covers `id IN (SELECT ...)` and EXISTS-of-a-condition; a
            // correlated executor is the deferred ceiling.
            let v = eval_val(col, scope, ctx)?;
            let key = subquery_column(sub);
            run_select(ctx, sub)?
                .iter()
                .any(|r| subquery_value(r, key.as_deref()) == v)
        }
        Expr::Exists(sub) => !run_select(ctx, sub)?.is_empty(),
        Expr::NotExists(sub) => run_select(ctx, sub)?.is_empty(),
    })
}

fn eval_val<S: Scope>(expr: &Expr, scope: &S, _ctx: &ExecCtx) -> Result<Value> {
    Ok(match expr {
        Expr::Column(name) => scope.get(name),
        Expr::Literal(lit) => literal_to_value(lit),
        other => {
            return Err(KitError::Validation(format!(
                "expression {other:?} cannot be used as a scalar value"
            )))
        }
    })
}

fn cmp<S: Scope>(a: &Expr, b: &Expr, scope: &S, ctx: &ExecCtx) -> Result<Option<Ordering>> {
    Ok(json_cmp(
        &eval_val(a, scope, ctx)?,
        &eval_val(b, scope, ctx)?,
    ))
}

/// The column name a subquery exposes for `IN`: the first projected `Column`, or
/// `None` to fall back to the first value of each result row.
fn subquery_column(select: &Select) -> Option<String> {
    select.columns.iter().find_map(|e| match e {
        Expr::Column(n) => Some(n.clone()),
        _ => None,
    })
}

fn subquery_value(row: &Row, key: Option<&str>) -> Value {
    match key {
        Some(k) => row.values.get(k).cloned().unwrap_or(Value::Null),
        None => row.values.values().next().cloned().unwrap_or(Value::Null),
    }
}

fn literal_to_value(lit: &Literal) -> Value {
    match lit {
        Literal::Null => Value::Null,
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Int(i) => Value::Number((*i).into()),
        Literal::Float(f) => serde_json::to_value(*f).unwrap_or(Value::Null),
        Literal::Text(s) => Value::String(s.clone()),
        Literal::Json(v) => v.clone(),
    }
}

fn json_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                Some(ai.cmp(&bi))
            } else {
                let af = a.as_f64().unwrap_or(f64::NAN);
                let bf = b.as_f64().unwrap_or(f64::NAN);
                af.partial_cmp(&bf)
            }
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Array(a), Value::Array(b)) => compare_arrays(a, b),
        (Value::Object(a), Value::Object(b)) => compare_objects(a, b),
        _ => None,
    }
}

fn compare_arrays(a: &[Value], b: &[Value]) -> Option<Ordering> {
    let len_cmp = a.len().partial_cmp(&b.len())?;
    if len_cmp != Ordering::Equal {
        return Some(len_cmp);
    }
    for (x, y) in a.iter().zip(b.iter()) {
        match json_cmp(x, y) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
    }
    Some(Ordering::Equal)
}

fn compare_objects(a: &Map<String, Value>, b: &Map<String, Value>) -> Option<Ordering> {
    let len_cmp = a.len().partial_cmp(&b.len())?;
    if len_cmp != Ordering::Equal {
        return Some(len_cmp);
    }
    let mut a_keys: Vec<&String> = a.keys().collect();
    let mut b_keys: Vec<&String> = b.keys().collect();
    a_keys.sort();
    b_keys.sort();
    for (ak, bk) in a_keys.iter().zip(b_keys.iter()) {
        match ak.cmp(bk) {
            Ordering::Equal => {}
            other => return Some(other),
        }
        let av = a.get(*ak).unwrap();
        let bv = b.get(*bk).unwrap();
        match json_cmp(av, bv) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
    }
    Some(Ordering::Equal)
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn sort_rows(ctx: &ExecCtx, rows: &mut [Row], table: &str, order: &OrderBy) -> Result<()> {
    let col_name = match &order.expr {
        Expr::Column(name) => name.clone(),
        other => {
            return Err(KitError::Validation(format!(
                "unsupported order by: {other:?}"
            )))
        }
    };
    // Validate against the schema when the table is known; CTE/virtual tables are
    // not in the schema and are sorted leniently (missing values sort as null).
    if let Some(t) = ctx.table_def(table) {
        if t.column(&col_name).is_none() {
            return Err(KitError::Validation(format!(
                "unknown order column {col_name}"
            )));
        }
    }

    rows.sort_by(|a, b| {
        let av = a.values.get(&col_name).cloned().unwrap_or(Value::Null);
        let bv = b.values.get(&col_name).cloned().unwrap_or(Value::Null);
        let ord = json_cmp(&av, &bv).unwrap_or(Ordering::Equal);
        match order.direction {
            Direction::Asc => ord,
            Direction::Desc => ord.reverse(),
        }
    });
    Ok(())
}

fn apply_limit_offset<T>(rows: &mut Vec<T>, limit: Option<usize>, offset: Option<usize>) {
    let offset = offset.unwrap_or(0);
    if offset > 0 || limit.is_some() {
        let start = offset.min(rows.len());
        let end = limit
            .map(|l| start + l)
            .unwrap_or(rows.len())
            .min(rows.len());
        *rows = rows.drain(start..end).collect();
    }
}

fn like(text: &str, pattern: &str) -> bool {
    let regex = match regex_like(pattern) {
        Ok(re) => re,
        Err(_) => return false,
    };
    regex.is_match(text)
}

fn regex_like(pattern: &str) -> Result<regex::Regex> {
    let mut out = String::with_capacity(pattern.len() * 2);
    out.push('^');
    for ch in pattern.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            c => {
                if regex::escape(&c.to_string()).len() > 1 {
                    out.push_str(&regex::escape(&c.to_string()));
                } else {
                    out.push(c);
                }
            }
        }
    }
    out.push('$');
    regex::Regex::new(&out).map_err(|e| KitError::Validation(format!("invalid LIKE pattern: {e}")))
}
