use mongreldb_kit::{
    encode_pk, encode_row_guard_key, encode_unique_key, Aggregate, AggregateQuery, Cte, Database,
    Direction, Expr, JoinQuery, KeyComponent, KitError, Literal, Migration, OnConflict, OrderBy,
    Query, Row, Schema, Select,
};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

mod remote;
pub use remote::run_remote;

#[derive(Debug, serde::Deserialize)]
struct Scenario {
    name: String,
    table: String,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    row: Option<Map<String, Value>>,
    #[serde(default)]
    pk: Option<Value>,
    #[serde(default)]
    patch: Option<Map<String, Value>>,
    #[serde(default)]
    filter: Option<Map<String, Value>>,
    #[serde(default)]
    on_conflict: Option<Value>,
    #[serde(default)]
    returning: Option<Vec<String>>,
    #[serde(default)]
    order: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    select: Option<Vec<String>>,
    #[serde(default)]
    count: Option<bool>,
    #[serde(default)]
    expected: Option<Map<String, Value>>,
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures")
}

fn load_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    serde_json::from_str(&s).map_err(|e| format!("parse {}: {}", path.display(), e))
}

fn error_code(e: &KitError) -> String {
    match e {
        KitError::Validation(_) => "VALIDATION",
        KitError::Duplicate(_) => "DUPLICATE",
        KitError::ForeignKey(_) => "FOREIGN_KEY",
        KitError::Restrict(_) => "RESTRICT",
        KitError::Migration(_) => "MIGRATION",
        KitError::Conflict(_) => "CONFLICT",
        KitError::TriggerValidation(_) => "TRIGGER_VALIDATION",
        KitError::Storage(_) => "STORAGE",
        KitError::Integrity(_) => "INTEGRITY",
        KitError::AuthRequired(_) => "AUTH_REQUIRED",
        KitError::AuthNotRequired(_) => "AUTH_NOT_REQUIRED",
        KitError::InvalidCredentials(_) => "INVALID_CREDENTIALS",
        KitError::PermissionDenied(_) => "PERMISSION_DENIED",
    }
    .to_string()
}

fn assert_eq_json(scenario: &str, actual: &Value, expected: &Value) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{} mismatch\n  actual:   {}\n  expected: {}",
            scenario,
            serde_json::to_string(actual).unwrap_or_default(),
            serde_json::to_string(expected).unwrap_or_default()
        ))
    }
}

fn value_to_literal(v: &Value) -> Literal {
    match v {
        Value::Null => Literal::Null,
        Value::Bool(b) => Literal::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Literal::Int(i)
            } else {
                Literal::Float(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::String(s) => Literal::Text(s.clone()),
        Value::Array(_) | Value::Object(_) => Literal::Json(v.clone()),
    }
}

fn object_filter_to_expr(map: &Map<String, Value>) -> Result<Expr, String> {
    let mut parts = Vec::new();
    for (col, val) in map {
        match val {
            Value::Object(op_map) if op_map.len() == 1 => {
                let (op, operand) = op_map.iter().next().unwrap();
                let operand_lit = value_to_literal(operand);
                let col_expr = Expr::Column(col.clone());
                let expr = match op.as_str() {
                    "eq" => Expr::Eq(Box::new(col_expr), Box::new(Expr::Literal(operand_lit))),
                    "ne" => Expr::Ne(Box::new(col_expr), Box::new(Expr::Literal(operand_lit))),
                    "gt" => Expr::Gt(Box::new(col_expr), Box::new(Expr::Literal(operand_lit))),
                    "gte" => Expr::Gte(Box::new(col_expr), Box::new(Expr::Literal(operand_lit))),
                    "lt" => Expr::Lt(Box::new(col_expr), Box::new(Expr::Literal(operand_lit))),
                    "lte" => Expr::Lte(Box::new(col_expr), Box::new(Expr::Literal(operand_lit))),
                    "is_null" => Expr::IsNull(Box::new(col_expr)),
                    "is_not_null" => Expr::IsNotNull(Box::new(col_expr)),
                    "like" => Expr::Like(
                        Box::new(col_expr),
                        operand.as_str().unwrap_or_default().to_string(),
                    ),
                    other => return Err(format!("unknown operator {}", other)),
                };
                parts.push(expr);
            }
            Value::Null => {
                parts.push(Expr::IsNull(Box::new(Expr::Column(col.clone()))));
            }
            _ => {
                parts.push(Expr::Eq(
                    Box::new(Expr::Column(col.clone())),
                    Box::new(Expr::Literal(value_to_literal(val))),
                ));
            }
        }
    }
    if parts.is_empty() {
        Ok(Expr::Literal(Literal::Bool(true)))
    } else if parts.len() == 1 {
        Ok(parts.into_iter().next().unwrap())
    } else {
        Ok(Expr::And(parts))
    }
}

fn parse_order(order: &str) -> Vec<OrderBy> {
    order
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            let (direction, col) = if let Some(rest) = part.strip_prefix('+') {
                (Direction::Asc, rest)
            } else if let Some(rest) = part.strip_prefix('-') {
                (Direction::Desc, rest)
            } else {
                (Direction::Asc, part)
            };
            Some(OrderBy {
                expr: Expr::Column(col.to_string()),
                direction,
            })
        })
        .collect()
}

fn build_select_query(scenario: &Scenario) -> Result<Query, String> {
    let filter = scenario
        .filter
        .as_ref()
        .map(object_filter_to_expr)
        .transpose()?;
    let order_by = scenario
        .order
        .as_ref()
        .map(|o| parse_order(o))
        .unwrap_or_default();
    Ok(Query::Select(Select {
        table: scenario.table.clone(),
        columns: Vec::new(),
        filter,
        order_by,
        limit: scenario.limit,
        offset: scenario.offset,
    }))
}

fn project(row: &Map<String, Value>, cols: &[String]) -> Map<String, Value> {
    cols.iter()
        .filter_map(|c| row.get(c).map(|v| (c.clone(), v.clone())))
        .collect()
}

fn query_all(db: &Database, table_name: &str) -> Result<Vec<Map<String, Value>>, String> {
    let txn = db.begin().map_err(|e| e.to_string())?;
    let query = Query::Select(Select {
        table: table_name.to_string(),
        columns: Vec::new(),
        filter: None,
        order_by: vec![OrderBy {
            expr: Expr::Column("id".to_string()),
            direction: Direction::Asc,
        }],
        limit: None,
        offset: None,
    });
    let rows = txn.select(&query).map_err(|e| e.to_string())?;
    txn.commit().map_err(|e| e.to_string())?;
    Ok(rows.into_iter().map(|r| r.values).collect())
}

fn run_insert(
    db: &mut Database,
    scenario: &Scenario,
    expected: Option<&Value>,
) -> Result<(), String> {
    let row = scenario.row.clone().ok_or("insert missing row")?;
    let exp = expected.ok_or("missing expected for insert")?;
    let mut txn = db.begin().map_err(|e| e.to_string())?;
    match txn.insert(&scenario.table, row) {
        Ok(result) => {
            txn.commit().map_err(|e| e.to_string())?;
            if let Some(err) = exp.get("error") {
                return Err(format!(
                    "{} expected error {} but succeeded",
                    scenario.name, err
                ));
            }
            let actual = row_to_value(&result);
            assert_eq_json(&scenario.name, &actual, exp.get("row").unwrap_or(exp))?;
        }
        Err(e) => {
            txn.rollback();
            if let Some(err) = exp.get("error") {
                assert_eq_json(&scenario.name, &Value::String(error_code(&e)), err)?;
            } else {
                return Err(format!("{} unexpected error: {}", scenario.name, e));
            }
        }
    }
    Ok(())
}

fn run_update(
    db: &mut Database,
    scenario: &Scenario,
    expected: Option<&Value>,
) -> Result<(), String> {
    let pk = scenario.pk.clone().ok_or("update missing pk")?;
    let patch = scenario.patch.clone().ok_or("update missing patch")?;
    let exp = expected.ok_or("missing expected for update")?;
    let mut txn = db.begin().map_err(|e| e.to_string())?;
    match txn.update(&scenario.table, &pk, patch) {
        Ok(result) => {
            txn.commit().map_err(|e| e.to_string())?;
            if let Some(err) = exp.get("error") {
                return Err(format!(
                    "{} expected error {} but succeeded",
                    scenario.name, err
                ));
            }
            let actual = row_to_value(&result);
            assert_eq_json(&scenario.name, &actual, exp.get("row").unwrap_or(exp))?;
        }
        Err(e) => {
            txn.rollback();
            if let Some(err) = exp.get("error") {
                assert_eq_json(&scenario.name, &Value::String(error_code(&e)), err)?;
            } else {
                return Err(format!("{} unexpected error: {}", scenario.name, e));
            }
        }
    }
    Ok(())
}

fn run_delete(
    db: &mut Database,
    scenario: &Scenario,
    expected: Option<&Value>,
) -> Result<(), String> {
    let pk = scenario.pk.clone().ok_or("delete missing pk")?;
    let exp = expected.ok_or("missing expected for delete")?;
    let mut txn = db.begin().map_err(|e| e.to_string())?;
    match txn.delete(&scenario.table, &pk) {
        Ok(()) => {
            txn.commit().map_err(|e| e.to_string())?;
            if let Some(err) = exp.get("error") {
                return Err(format!(
                    "{} expected error {} but succeeded",
                    scenario.name, err
                ));
            }
            for table_name in ["users", "posts", "comments"] {
                let rows = query_all(db, table_name)?;
                let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
                let exp_table = exp.get(table_name).ok_or(format!(
                    "{} missing expected for {}",
                    scenario.name, table_name
                ))?;
                assert_eq_json(
                    &format!("{}.{}", scenario.name, table_name),
                    &actual,
                    exp_table,
                )?;
            }
        }
        Err(e) => {
            txn.rollback();
            if let Some(err) = exp.get("error") {
                assert_eq_json(&scenario.name, &Value::String(error_code(&e)), err)?;
            } else {
                return Err(format!("{} unexpected error: {}", scenario.name, e));
            }
        }
    }
    Ok(())
}

fn run_query(
    db: &mut Database,
    scenario: &Scenario,
    expected: Option<&Value>,
) -> Result<(), String> {
    let exp = expected.ok_or("missing expected for query")?;
    let query = build_select_query(scenario)?;
    let txn = db.begin().map_err(|e| e.to_string())?;
    let rows = txn.select(&query).map_err(|e| e.to_string())?;
    txn.commit().map_err(|e| e.to_string())?;

    if scenario.count == Some(true) {
        let actual = Value::Number((rows.len() as i64).into());
        assert_eq_json(
            &scenario.name,
            &actual,
            exp.get("count").unwrap_or(&Value::Null),
        )?;
    } else {
        let selected: Vec<Map<String, Value>> = rows
            .iter()
            .map(|r| {
                scenario
                    .select
                    .as_ref()
                    .map(|cols| project(&r.values, cols))
                    .unwrap_or_else(|| r.values.clone())
            })
            .collect();
        let actual = Value::Array(selected.into_iter().map(Value::Object).collect());
        assert_eq_json(
            &scenario.name,
            &actual,
            exp.get("rows").unwrap_or(&Value::Null),
        )?;
    }
    Ok(())
}

fn row_to_value(row: &Row) -> Value {
    Value::Object(row.values.clone())
}

// ---------------------------------------------------------------------------
// Key-encoding conformance
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct KeyCase {
    name: String,
    kind: String,
    components: Vec<Value>,
    #[serde(default)]
    version: Option<u32>,
    #[serde(default)]
    constraint: Option<String>,
    #[serde(default)]
    table: Option<String>,
    expected: String,
}

#[derive(Debug, serde::Deserialize)]
struct KeyFixture {
    cases: Vec<KeyCase>,
}

fn parse_key_component(value: &Value) -> Result<KeyComponent, String> {
    if let Some(i) = value.get("int") {
        let n = i
            .as_i64()
            .ok_or_else(|| format!("int component not an i64: {value}"))?;
        Ok(KeyComponent::Int(n))
    } else if let Some(t) = value.get("text") {
        let s = t
            .as_str()
            .ok_or_else(|| format!("text component not a string: {value}"))?;
        Ok(KeyComponent::Text(s.to_string()))
    } else if value.get("null").is_some() {
        Ok(KeyComponent::Null)
    } else {
        Err(format!("invalid key component: {value}"))
    }
}

pub fn run_key_encoding() -> Result<(), String> {
    let fixture: KeyFixture = load_json(&fixtures_dir().join("keys.json"))?;
    for case in &fixture.cases {
        let comps: Vec<KeyComponent> = case
            .components
            .iter()
            .map(parse_key_component)
            .collect::<Result<_, _>>()?;
        let actual = match case.kind.as_str() {
            "pk" => encode_pk(&comps),
            "unique" => encode_unique_key(
                case.version
                    .ok_or_else(|| format!("{} missing version", case.name))?,
                case.constraint
                    .as_deref()
                    .ok_or_else(|| format!("{} missing constraint", case.name))?,
                &comps,
            ),
            "row_guard" => encode_row_guard_key(
                case.table
                    .as_deref()
                    .ok_or_else(|| format!("{} missing table", case.name))?,
                &encode_pk(&comps),
            ),
            other => return Err(format!("{} unknown key kind {other}", case.name)),
        };
        if actual != case.expected {
            return Err(format!(
                "{} key mismatch\n  actual:   {}\n  expected: {}",
                case.name, actual, case.expected
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration-failure conformance
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct SeedRow {
    table: String,
    row: Map<String, Value>,
}

#[derive(Debug, serde::Deserialize)]
struct MigrationFailureFixture {
    expected_error: String,
    create_schema: Schema,
    migrated_schema: Schema,
    create_migration: Migration,
    failing_migration: Migration,
    seed: Vec<SeedRow>,
}

pub fn run_migration_failure() -> Result<(), String> {
    let fixture: MigrationFailureFixture =
        load_json(&fixtures_dir().join("migration_failure.json"))?;
    let MigrationFailureFixture {
        expected_error,
        create_schema,
        migrated_schema,
        create_migration,
        failing_migration,
        seed,
    } = fixture;

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let mut db = Database::create(tmp.path(), create_schema).map_err(|e| e.to_string())?;
    mongreldb_kit::migrate(&mut db, std::slice::from_ref(&create_migration))
        .map_err(|e| e.to_string())?;

    {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        for s in &seed {
            txn.insert(&s.table, s.row.clone())
                .map_err(|e| e.to_string())?;
        }
        txn.commit().map_err(|e| e.to_string())?;
    }

    // Swap in the schema that declares the unique constraint so the backfill can
    // resolve it. The prior inserts were allowed because the constraint was
    // absent from the active schema.
    db.set_schema(migrated_schema);

    let migrations = vec![create_migration, failing_migration];
    match mongreldb_kit::migrate(&mut db, &migrations) {
        Ok(()) => Err(format!(
            "migration_failure expected error {expected_error} but migrate succeeded"
        )),
        Err(e) => {
            let code = error_code(&e);
            if code == expected_error {
                Ok(())
            } else {
                Err(format!(
                    "migration_failure error mismatch: {code} != {expected_error}"
                ))
            }
        }
    }
}

pub fn run_conformance() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let schema: Schema = load_json(&fixtures.join("schema.json"))?;
    let migrations: Vec<Migration> = load_json(&fixtures.join("migrations.json"))?;
    let inserts: Vec<Scenario> = load_json(&fixtures.join("inserts.json"))?;
    let updates: Vec<Scenario> = load_json(&fixtures.join("updates.json"))?;
    let deletes: Vec<Scenario> = load_json(&fixtures.join("deletes.json"))?;
    let queries: Vec<Scenario> = load_json(&fixtures.join("queries.json"))?;
    let expected_inserts: Map<String, Value> = load_json(&fixtures.join("expected/inserts.json"))?;
    let expected_updates: Map<String, Value> = load_json(&fixtures.join("expected/updates.json"))?;
    let expected_deletes: Map<String, Value> = load_json(&fixtures.join("expected/deletes.json"))?;
    let expected_queries: Map<String, Value> = load_json(&fixtures.join("expected/queries.json"))?;

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let mut db = Database::create(tmp.path(), schema).map_err(|e| e.to_string())?;
    mongreldb_kit::migrate(&mut db, &migrations).map_err(|e| e.to_string())?;

    for scenario in &inserts {
        run_insert(&mut db, scenario, expected_inserts.get(&scenario.name))?;
    }
    for scenario in &updates {
        run_update(&mut db, scenario, expected_updates.get(&scenario.name))?;
    }
    for scenario in &deletes {
        run_delete(&mut db, scenario, expected_deletes.get(&scenario.name))?;
    }
    for scenario in &queries {
        run_query(&mut db, scenario, expected_queries.get(&scenario.name))?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// LearnedRange index conformance — validates that a `learned_range` index can
// be declared on a numeric column and range queries return correct rows. The
// PGM zonemap is a performance optimization; result sets are identical to an
// unindexed scan, so this pins declare-ability + round-trip correctness.
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct LearnedRangeFixture {
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    scenarios: Vec<Scenario>,
}

pub fn run_learned_range() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: LearnedRangeFixture = load_json(&fixtures.join("learned_range.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/learned_range.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("learned_range fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let mut db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;
    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        run_query(&mut db, scenario, expected.get(&scenario.name))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Aggregate conformance (COUNT / COUNT(col) / COUNT(DISTINCT) / SUM/MIN/MAX/AVG)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct AggScenario {
    name: String,
    table: String,
    #[serde(default)]
    group_by: Vec<String>,
    #[serde(default)]
    order: Option<String>,
    aggregates: Vec<Aggregate>,
}

#[derive(Debug, serde::Deserialize)]
struct AggFixture {
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    scenarios: Vec<AggScenario>,
}

/// Compare two JSON values for aggregate-result ordering (text/number/null).
fn cmp_json(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Some(Value::String(x)), Some(Value::String(y))) => x.cmp(y),
        (Some(Value::Number(x)), Some(Value::Number(y))) => x
            .as_f64()
            .partial_cmp(&y.as_f64())
            .unwrap_or(Ordering::Equal),
        (None | Some(Value::Null), None | Some(Value::Null)) => Ordering::Equal,
        (None | Some(Value::Null), _) => Ordering::Less,
        (_, None | Some(Value::Null)) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

pub fn run_aggregates() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: AggFixture = load_json(&fixtures.join("aggregates.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/aggregates.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("aggregates fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;

    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        let query = AggregateQuery {
            table: scenario.table.clone(),
            filter: None,
            group_by: scenario.group_by.clone(),
            aggregates: scenario.aggregates.clone(),
            having: None,
        };
        let txn = db.begin().map_err(|e| e.to_string())?;
        let mut rows = txn.aggregate(&query).map_err(|e| e.to_string())?;
        if let Some(order) = &scenario.order {
            let (desc, col) = match order.strip_prefix('-') {
                Some(c) => (true, c.to_string()),
                None => (false, order.strip_prefix('+').unwrap_or(order).to_string()),
            };
            rows.sort_by(|a, b| {
                let ord = cmp_json(a.values.get(&col), b.values.get(&col));
                if desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
        }
        let actual = Value::Array(rows.iter().map(row_to_value).collect());
        let exp = expected
            .get(&scenario.name)
            .and_then(|e| e.get("rows"))
            .ok_or(format!("missing expected rows for {}", scenario.name))?;
        assert_eq_json(&scenario.name, &actual, exp)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Join conformance (inner / left / cross, unmatched LEFT side)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct JoinScenario {
    name: String,
    #[serde(default)]
    order: Vec<String>,
    query: JoinQuery,
}

#[derive(Debug, serde::Deserialize)]
struct JoinFixture {
    schema: Schema,
    seed: Map<String, Value>,
    scenarios: Vec<JoinScenario>,
}

/// Resolve a qualified `"table.column"` reference inside a join result row.
/// Returns `None` when the source is the unmatched (JSON `null`) side of a LEFT
/// join, which sorts before any present value.
fn join_key<'a>(row: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    let (table, col) = key.split_once('.')?;
    match row.get(table) {
        Some(Value::Object(m)) => m.get(col),
        _ => None,
    }
}

fn cmp_join(
    a: &Map<String, Value>,
    b: &Map<String, Value>,
    order: &[String],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for key in order {
        let ord = cmp_json(join_key(a, key), join_key(b, key));
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn sorted_join_rows(rows: &[Value], order: &[String]) -> Vec<Map<String, Value>> {
    let mut out: Vec<Map<String, Value>> =
        rows.iter().filter_map(|v| v.as_object().cloned()).collect();
    out.sort_by(|a, b| cmp_join(a, b, order));
    out
}

pub fn run_joins() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: JoinFixture = load_json(&fixtures.join("joins.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/joins.json"))?;

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;

    for (table, rows) in &fixture.seed {
        let rows = rows
            .as_array()
            .ok_or(format!("seed for {table} is not an array"))?;
        for row in rows {
            let row = row
                .as_object()
                .ok_or(format!("seed row for {table} is not an object"))?
                .clone();
            let mut txn = db.begin().map_err(|e| e.to_string())?;
            txn.insert(table, row).map_err(|e| e.to_string())?;
            txn.commit().map_err(|e| e.to_string())?;
        }
    }

    for scenario in &fixture.scenarios {
        let txn = db.begin().map_err(|e| e.to_string())?;
        let mut rows = txn.join(&scenario.query).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
        rows.sort_by(|a, b| cmp_join(a, b, &scenario.order));
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());

        let exp_rows = expected
            .get(&scenario.name)
            .and_then(|e| e.get("rows"))
            .and_then(|v| v.as_array())
            .ok_or(format!("missing expected rows for {}", scenario.name))?;
        let expected_val = Value::Array(
            sorted_join_rows(exp_rows, &scenario.order)
                .into_iter()
                .map(Value::Object)
                .collect(),
        );
        assert_eq_json(&scenario.name, &actual, &expected_val)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// CTE conformance (materialized common table expressions, incl. chaining)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct CteDef {
    name: String,
    table: String,
    #[serde(default)]
    filter: Option<Map<String, Value>>,
    #[serde(default)]
    columns: Option<Vec<String>>,
}

#[derive(Debug, serde::Deserialize)]
struct CteScenario {
    name: String,
    ctes: Vec<CteDef>,
    body: String,
    #[serde(default)]
    order: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct CteFixture {
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    scenarios: Vec<CteScenario>,
}

fn cte_def_to_cte(def: &CteDef) -> Result<Cte, String> {
    let filter = def.filter.as_ref().map(object_filter_to_expr).transpose()?;
    let columns = def
        .columns
        .as_ref()
        .map(|cs| cs.iter().map(|c| Expr::Column(c.clone())).collect())
        .unwrap_or_default();
    Ok(Cte {
        name: def.name.clone(),
        query: Box::new(Select {
            table: def.table.clone(),
            columns,
            filter,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }),
    })
}

fn sort_flat_rows(rows: &mut [Map<String, Value>], order: &str) {
    let desc = order.starts_with('-');
    let col = order.trim_start_matches(['+', '-']);
    rows.sort_by(|a, b| {
        let ord = cmp_json(a.get(col), b.get(col));
        if desc {
            ord.reverse()
        } else {
            ord
        }
    });
}

pub fn run_ctes() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: CteFixture = load_json(&fixtures.join("ctes.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/ctes.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("ctes fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;

    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        let ctes = scenario
            .ctes
            .iter()
            .map(cte_def_to_cte)
            .collect::<Result<Vec<_>, _>>()?;
        let body = Select {
            table: scenario.body.clone(),
            columns: Vec::new(),
            filter: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let txn = db.begin().map_err(|e| e.to_string())?;
        let mut rows: Vec<Map<String, Value>> = txn
            .select_with(&ctes, &body)
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|r| r.values)
            .collect();
        txn.commit().map_err(|e| e.to_string())?;
        if let Some(order) = &scenario.order {
            sort_flat_rows(&mut rows, order);
        }
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
        let exp = expected
            .get(&scenario.name)
            .and_then(|e| e.get("rows"))
            .ok_or(format!("missing expected rows for {}", scenario.name))?;
        assert_eq_json(&scenario.name, &actual, exp)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Subquery conformance (uncorrelated IN-subquery / EXISTS / NOT EXISTS)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct SubqueryScenario {
    name: String,
    table: String,
    filter: Map<String, Value>,
    #[serde(default)]
    order: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct SubqueryFixture {
    schema: Schema,
    seed: Map<String, Value>,
    scenarios: Vec<SubqueryScenario>,
}

/// Build a kit `Select` from a `{ table, filter?, columns? }` subselect spec,
/// mirroring the Python facade's `parse_subselect`.
fn subselect_to_select(spec: &Value) -> Result<Select, String> {
    let obj = spec.as_object().ok_or("subselect must be an object")?;
    let table = obj
        .get("table")
        .and_then(|v| v.as_str())
        .ok_or("subselect requires a table")?
        .to_string();
    let filter = match obj.get("filter") {
        Some(Value::Object(m)) => Some(object_filter_to_expr(m)?),
        _ => None,
    };
    let columns = obj
        .get("columns")
        .and_then(|v| v.as_array())
        .map(|cs| {
            cs.iter()
                .filter_map(|c| c.as_str())
                .map(|c| Expr::Column(c.to_string()))
                .collect()
        })
        .unwrap_or_default();
    Ok(Select {
        table,
        columns,
        filter,
        order_by: Vec::new(),
        limit: None,
        offset: None,
    })
}

/// Translate the single-key friendly subquery filter into a kit `Expr`:
/// `{ exists: sub }`, `{ not_exists: sub }`, or `{ col: { in_subquery: sub } }`.
fn subquery_filter_to_expr(map: &Map<String, Value>) -> Result<Expr, String> {
    let (key, val) = map.iter().next().ok_or("empty subquery filter")?;
    match key.as_str() {
        "exists" => Ok(Expr::Exists(Box::new(subselect_to_select(val)?))),
        "not_exists" => Ok(Expr::NotExists(Box::new(subselect_to_select(val)?))),
        column => {
            let (op, operand) = val
                .as_object()
                .and_then(|m| m.iter().next())
                .ok_or("column subquery predicate must be { in_subquery: ... }")?;
            if op == "in_subquery" {
                Ok(Expr::InSubquery(
                    Box::new(Expr::Column(column.to_string())),
                    Box::new(subselect_to_select(operand)?),
                ))
            } else {
                Err(format!("unsupported subquery operator {op}"))
            }
        }
    }
}

pub fn run_subqueries() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: SubqueryFixture = load_json(&fixtures.join("subqueries.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/subqueries.json"))?;

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;

    for (table, rows) in &fixture.seed {
        let rows = rows
            .as_array()
            .ok_or(format!("seed for {table} is not an array"))?;
        for row in rows {
            let row = row
                .as_object()
                .ok_or(format!("seed row for {table} is not an object"))?
                .clone();
            let mut txn = db.begin().map_err(|e| e.to_string())?;
            txn.insert(table, row).map_err(|e| e.to_string())?;
            txn.commit().map_err(|e| e.to_string())?;
        }
    }

    for scenario in &fixture.scenarios {
        let body = Select {
            table: scenario.table.clone(),
            columns: Vec::new(),
            filter: Some(subquery_filter_to_expr(&scenario.filter)?),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let txn = db.begin().map_err(|e| e.to_string())?;
        let mut rows: Vec<Map<String, Value>> = txn
            .select(&Query::Select(body))
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|r| r.values)
            .collect();
        txn.commit().map_err(|e| e.to_string())?;
        if let Some(order) = &scenario.order {
            sort_flat_rows(&mut rows, order);
        }
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
        let exp = expected
            .get(&scenario.name)
            .and_then(|e| e.get("rows"))
            .ok_or(format!("missing expected rows for {}", scenario.name))?;
        assert_eq_json(&scenario.name, &actual, exp)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// FM substring conformance (contains() pushed to FmContains on an FM index)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct ContainsScenario {
    name: String,
    table: String,
    column: String,
    needle: String,
    #[serde(default)]
    order: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ContainsFixture {
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    scenarios: Vec<ContainsScenario>,
}

pub fn run_contains() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: ContainsFixture = load_json(&fixtures.join("contains.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/contains.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("contains fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;
    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        let select = Select {
            table: scenario.table.clone(),
            columns: Vec::new(),
            filter: Some(Expr::Contains(
                Box::new(Expr::Column(scenario.column.clone())),
                scenario.needle.clone(),
            )),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let txn = db.begin().map_err(|e| e.to_string())?;
        let mut rows: Vec<Map<String, Value>> = txn
            .select(&Query::Select(select))
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|r| r.values)
            .collect();
        txn.commit().map_err(|e| e.to_string())?;
        if let Some(order) = &scenario.order {
            sort_flat_rows(&mut rows, order);
        }
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
        let exp = expected
            .get(&scenario.name)
            .and_then(|e| e.get("rows"))
            .ok_or(format!("missing expected rows for {}", scenario.name))?;
        assert_eq_json(&scenario.name, &actual, exp)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// BytesPrefix conformance (anchored `LIKE 'prefix%'` on a bitmap-indexed
// Bytes column — exact pushdown, no residual).
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct BytesPrefixScenario {
    name: String,
    table: String,
    column: String,
    prefix: String,
    #[serde(default)]
    order: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct BytesPrefixFixture {
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    scenarios: Vec<BytesPrefixScenario>,
}

pub fn run_bytes_prefix() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: BytesPrefixFixture = load_json(&fixtures.join("bytes_prefix.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/bytes_prefix.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("bytes_prefix fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;
    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        let select = Select {
            table: scenario.table.clone(),
            columns: Vec::new(),
            filter: Some(Expr::BytesPrefix(
                Box::new(Expr::Column(scenario.column.clone())),
                scenario.prefix.clone(),
            )),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let txn = db.begin().map_err(|e| e.to_string())?;
        let mut rows: Vec<Map<String, Value>> = txn
            .select(&Query::Select(select))
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|r| r.values)
            .collect();
        txn.commit().map_err(|e| e.to_string())?;
        if let Some(order) = &scenario.order {
            sort_flat_rows(&mut rows, order);
        }
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
        let exp = expected
            .get(&scenario.name)
            .and_then(|e| e.get("rows"))
            .ok_or(format!("missing expected rows for {}", scenario.name))?;
        assert_eq_json(&scenario.name, &actual, exp)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Views conformance (CREATE VIEW + SELECT FROM <view> via the SQL surface)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct ViewScenario {
    name: String,
    sql: String,
    expected_rows: Vec<Map<String, Value>>,
}

#[derive(Debug, serde::Deserialize)]
struct ViewsFixture {
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    view_sql: String,
    scenarios: Vec<ViewScenario>,
}

pub fn run_views() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: ViewsFixture = load_json(&fixtures.join("views.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("views fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;
    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    // Create the view via the SQL surface (session-scoped, persists across
    // subsequent sql() calls on the same Database handle).
    db.sql(&format!("CREATE VIEW pricey AS {}", fixture.view_sql))
        .map_err(|e| e.to_string())?;

    for scenario in &fixture.scenarios {
        let mut rows = db.sql_rows(&scenario.sql).map_err(|e| e.to_string())?;
        // Normalize: sort single-row/scalar results deterministically is
        // unnecessary (order is fixed by the scenario SQL), but normalize
        // numeric types for comparison (f64 vs int).
        for row in &mut rows {
            normalize_row_numbers(row);
        }
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
        let mut exp: Vec<Map<String, Value>> = scenario.expected_rows.clone();
        for row in &mut exp {
            normalize_row_numbers(row);
        }
        let expected = Value::Array(exp.into_iter().map(Value::Object).collect());
        assert_eq_json(&scenario.name, &actual, &expected)?;
    }
    Ok(())
}

/// Coerce JSON numbers to f64 for comparison (DataFusion may return COUNT(*) as
/// Int64 or Float64 depending on the planner; normalize so 1 == 1.0).
fn normalize_row_numbers(row: &mut Map<String, Value>) {
    for v in row.values_mut() {
        if let Value::Number(n) = v {
            if let Some(f) = n.as_f64() {
                *v = serde_json::json!(f);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ANN conformance (ann_search top-k over an Embedding column's HNSW index)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct AnnScenario {
    name: String,
    table: String,
    column: String,
    query: Vec<f32>,
    k: usize,
    expect_ids: Vec<i64>,
}

#[derive(Debug, serde::Deserialize)]
struct AnnFixture {
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    scenarios: Vec<AnnScenario>,
}

pub fn run_ann() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: AnnFixture = load_json(&fixtures.join("ann.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("ann fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;
    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        let txn = db.begin().map_err(|e| e.to_string())?;
        let rows = txn
            .ann_search(
                &scenario.table,
                &scenario.column,
                scenario.query.clone(),
                scenario.k,
            )
            .map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
        let mut ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.values.get("id").and_then(|v| v.as_i64()))
            .collect();
        ids.sort_unstable();
        let mut expect = scenario.expect_ids.clone();
        expect.sort_unstable();
        if ids != expect {
            return Err(format!(
                "{}: ann ids {:?} != expected {:?}",
                scenario.name, ids, expect
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Sparse (SPLADE) conformance (sparse_match top-k over a Sparse column)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct SparseScenario {
    name: String,
    table: String,
    column: String,
    query: Vec<(u32, f32)>,
    k: usize,
    expect_ids: Vec<i64>,
}

#[derive(Debug, serde::Deserialize)]
struct SparseFixture {
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    scenarios: Vec<SparseScenario>,
}

pub fn run_sparse() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: SparseFixture = load_json(&fixtures.join("sparse.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("sparse fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;
    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        let txn = db.begin().map_err(|e| e.to_string())?;
        let rows = txn
            .sparse_match(
                &scenario.table,
                &scenario.column,
                scenario.query.clone(),
                scenario.k,
            )
            .map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
        let mut ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.values.get("id").and_then(|v| v.as_i64()))
            .collect();
        ids.sort_unstable();
        let mut expect = scenario.expect_ids.clone();
        expect.sort_unstable();
        if ids != expect {
            return Err(format!(
                "{}: sparse ids {:?} != expected {:?}",
                scenario.name, ids, expect
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// IS NULL / IS NOT NULL pushdown conformance
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct NullScenario {
    name: String,
    table: String,
    filter: Map<String, Value>,
    #[serde(default)]
    order: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct NullFixture {
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    scenarios: Vec<NullScenario>,
}

pub fn run_null_filter() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: NullFixture = load_json(&fixtures.join("null_filter.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/null_filter.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("null_filter fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;
    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        let select = Select {
            table: scenario.table.clone(),
            columns: Vec::new(),
            filter: Some(object_filter_to_expr(&scenario.filter)?),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let txn = db.begin().map_err(|e| e.to_string())?;
        let mut rows: Vec<Map<String, Value>> = txn
            .select(&Query::Select(select))
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|r| r.values)
            .collect();
        txn.commit().map_err(|e| e.to_string())?;
        if let Some(order) = &scenario.order {
            sort_flat_rows(&mut rows, order);
        }
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
        let exp = expected
            .get(&scenario.name)
            .and_then(|e| e.get("rows"))
            .ok_or(format!("missing expected rows for {}", scenario.name))?;
        assert_eq_json(&scenario.name, &actual, exp)?;
    }
    Ok(())
}

/// Multi-segment LIKE (FmContainsAll) conformance. Reuses the generic
/// filter-select shape (`NullFixture`); the fixture declares an FM index.
pub fn run_like() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: NullFixture = load_json(&fixtures.join("like.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/like.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("like fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create(tmp.path(), fixture.schema).map_err(|e| e.to_string())?;
    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        let select = Select {
            table: scenario.table.clone(),
            columns: Vec::new(),
            filter: Some(object_filter_to_expr(&scenario.filter)?),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let txn = db.begin().map_err(|e| e.to_string())?;
        let mut rows: Vec<Map<String, Value>> = txn
            .select(&Query::Select(select))
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|r| r.values)
            .collect();
        txn.commit().map_err(|e| e.to_string())?;
        if let Some(order) = &scenario.order {
            sort_flat_rows(&mut rows, order);
        }
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
        let exp = expected
            .get(&scenario.name)
            .and_then(|e| e.get("rows"))
            .ok_or(format!("missing expected rows for {}", scenario.name))?;
        assert_eq_json(&scenario.name, &actual, exp)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Encrypted (encrypted-indexable) column conformance
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct EncryptedFixture {
    passphrase: String,
    schema: Schema,
    rows: Vec<Map<String, Value>>,
    scenarios: Vec<NullScenario>,
}

pub fn run_encrypted() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let fixture: EncryptedFixture = load_json(&fixtures.join("encrypted.json"))?;
    let expected: Map<String, Value> = load_json(&fixtures.join("expected/encrypted.json"))?;

    let table = fixture
        .schema
        .tables
        .first()
        .ok_or("encrypted fixture has no table")?
        .name
        .clone();

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let db = Database::create_encrypted(tmp.path(), fixture.schema, &fixture.passphrase)
        .map_err(|e| e.to_string())?;
    for row in &fixture.rows {
        let mut txn = db.begin().map_err(|e| e.to_string())?;
        txn.insert(&table, row.clone()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
    }

    for scenario in &fixture.scenarios {
        let select = Select {
            table: scenario.table.clone(),
            columns: Vec::new(),
            filter: Some(object_filter_to_expr(&scenario.filter)?),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let txn = db.begin().map_err(|e| e.to_string())?;
        let mut rows: Vec<Map<String, Value>> = txn
            .select(&Query::Select(select))
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|r| r.values)
            .collect();
        txn.commit().map_err(|e| e.to_string())?;
        if let Some(order) = &scenario.order {
            sort_flat_rows(&mut rows, order);
        }
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
        let exp = expected
            .get(&scenario.name)
            .and_then(|e| e.get("rows"))
            .ok_or(format!("missing expected rows for {}", scenario.name))?;
        assert_eq_json(&scenario.name, &actual, exp)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 1 DML conformance
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct StateCheck {
    table: String,
    #[serde(default)]
    filter: Option<Map<String, Value>>,
    #[serde(default)]
    order: Option<String>,
    rows: Vec<Map<String, Value>>,
}

#[derive(Debug, serde::Deserialize)]
struct Phase1DmlFixture {
    steps: Vec<Scenario>,
    #[serde(default)]
    state_checks: Vec<StateCheck>,
}

fn parse_on_conflict(value: Option<&Value>) -> Result<OnConflict, String> {
    match value {
        None => Ok(OnConflict::DoNothing),
        Some(Value::String(s)) if s == "do_nothing" => Ok(OnConflict::DoNothing),
        Some(Value::String(s)) => Err(format!("unknown on_conflict action: {s}")),
        Some(Value::Object(map)) => {
            if map.contains_key("do_nothing") {
                Ok(OnConflict::DoNothing)
            } else if let Some(patch) = map.get("do_update") {
                patch
                    .as_object()
                    .cloned()
                    .map(OnConflict::DoUpdate)
                    .ok_or_else(|| "do_update expects an object patch".to_string())
            } else {
                Err("on_conflict must be do_nothing or do_update".to_string())
            }
        }
        Some(other) => Err(format!("invalid on_conflict: {other}")),
    }
}

fn returning_cols(scenario: &Scenario) -> Vec<String> {
    scenario.returning.clone().unwrap_or_default()
}

#[derive(Debug)]
enum Phase1Error {
    Fixture(String),
    Db(KitError),
}

impl From<KitError> for Phase1Error {
    fn from(e: KitError) -> Self {
        Phase1Error::Db(e)
    }
}

fn assert_returning_column_order(
    scenario: &str,
    value: &Value,
    returning: &[String],
) -> Result<(), String> {
    let row = match value {
        Value::Object(map) => map,
        _ => {
            return Err(format!(
                "{} RETURNING row is not an object: {}",
                scenario, value
            ))
        }
    };
    let actual: Vec<&str> = row.keys().map(|s| s.as_str()).collect();
    let expected: Vec<&str> = returning.iter().map(|s| s.as_str()).collect();
    if actual != expected {
        return Err(format!(
            "{} returning column order mismatch: actual {:?} != expected {:?}",
            scenario, actual, expected
        ));
    }
    Ok(())
}

fn execute_phase1_op(
    txn: &mut mongreldb_kit::Transaction,
    scenario: &Scenario,
) -> Result<Value, Phase1Error> {
    let op = scenario
        .op
        .as_deref()
        .ok_or_else(|| Phase1Error::Fixture("step missing op".into()))?;
    match op {
        "insert_returning" => {
            let row = scenario
                .row
                .clone()
                .ok_or_else(|| Phase1Error::Fixture(format!("{} missing row", scenario.name)))?;
            Ok(txn.insert_returning(&scenario.table, row, returning_cols(scenario))?)
        }
        "upsert" => {
            let row = scenario
                .row
                .clone()
                .ok_or_else(|| Phase1Error::Fixture(format!("{} missing row", scenario.name)))?;
            let on_conflict =
                parse_on_conflict(scenario.on_conflict.as_ref()).map_err(Phase1Error::Fixture)?;
            Ok(txn.upsert(&scenario.table, row, on_conflict, returning_cols(scenario))?)
        }
        "update_where" => {
            let patch = scenario
                .patch
                .clone()
                .ok_or_else(|| Phase1Error::Fixture(format!("{} missing patch", scenario.name)))?;
            let filter = scenario
                .filter
                .as_ref()
                .map(object_filter_to_expr)
                .transpose()
                .map_err(Phase1Error::Fixture)?;
            Ok(txn
                .update_where(&scenario.table, filter, patch, returning_cols(scenario))
                .map(Value::Array)?)
        }
        "delete_where" => {
            let filter = scenario
                .filter
                .as_ref()
                .map(object_filter_to_expr)
                .transpose()
                .map_err(Phase1Error::Fixture)?;
            Ok(txn
                .delete_where(&scenario.table, filter, returning_cols(scenario))
                .map(Value::Array)?)
        }
        "truncate" => {
            txn.truncate(&scenario.table)?;
            Ok(Value::Object(Map::new()))
        }
        other => Err(Phase1Error::Fixture(format!("unknown op {other}"))),
    }
}

fn run_phase1_step(db: &mut Database, scenario: &Scenario) -> Result<(), String> {
    let expected = scenario
        .expected
        .as_ref()
        .ok_or_else(|| format!("{} missing expected", scenario.name))?;
    let mut txn = db.begin().map_err(|e| e.to_string())?;

    let actual = execute_phase1_op(&mut txn, scenario);

    match actual {
        Ok(value) => {
            txn.commit().map_err(|e| e.to_string())?;
            if let Some(err) = expected.get("error") {
                return Err(format!(
                    "{} expected error {} but succeeded",
                    scenario.name, err
                ));
            }
            if let Some(row) = expected.get("row") {
                assert_returning_column_order(&scenario.name, &value, &returning_cols(scenario))?;
                assert_eq_json(&scenario.name, &value, row)?;
            } else if let Some(rows) = expected.get("rows") {
                let actual_rows = match value {
                    Value::Array(arr) => arr,
                    _ => return Err(format!("{} expected rows but got {}", scenario.name, value)),
                };
                let expected_rows = match rows {
                    Value::Array(arr) => arr.clone(),
                    _ => return Err(format!("{} expected rows array", scenario.name)),
                };
                for (i, actual_row) in actual_rows.iter().enumerate() {
                    assert_returning_column_order(
                        &format!("{} row {}", scenario.name, i),
                        actual_row,
                        &returning_cols(scenario),
                    )?;
                }
                assert_eq_json(
                    &scenario.name,
                    &Value::Array(actual_rows),
                    &Value::Array(expected_rows),
                )?;
            } else {
                assert_eq_json(&scenario.name, &value, &Value::Object(Map::new()))?;
            }
        }
        Err(Phase1Error::Db(e)) => {
            txn.rollback();
            if let Some(err) = expected.get("error") {
                assert_eq_json(&scenario.name, &Value::String(error_code(&e)), err)?;
            } else {
                return Err(format!("{} unexpected error: {}", scenario.name, e));
            }
        }
        Err(Phase1Error::Fixture(msg)) => {
            txn.rollback();
            return Err(msg);
        }
    }
    Ok(())
}

fn run_phase1_state_checks(db: &mut Database, checks: &[StateCheck]) -> Result<(), String> {
    let txn = db.begin().map_err(|e| e.to_string())?;
    for check in checks {
        let filter = check
            .filter
            .as_ref()
            .map(object_filter_to_expr)
            .transpose()?;
        let order_by = parse_order(check.order.as_deref().unwrap_or("+id"));
        let query = Query::Select(Select {
            table: check.table.clone(),
            columns: Vec::new(),
            filter,
            order_by,
            limit: None,
            offset: None,
        });
        let rows: Vec<Map<String, Value>> = txn
            .select(&query)
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|r| r.values)
            .collect();
        let actual = Value::Array(rows.into_iter().map(Value::Object).collect());
        let expected = Value::Array(check.rows.iter().cloned().map(Value::Object).collect());
        assert_eq_json(&format!("state_check {}", check.table), &actual, &expected)?;
    }
    txn.commit().map_err(|e| e.to_string())?;
    Ok(())
}

pub fn run_phase1_dml() -> Result<(), String> {
    let fixtures = fixtures_dir();
    let schema: Schema = load_json(&fixtures.join("schema.json"))?;
    let migrations: Vec<Migration> = load_json(&fixtures.join("migrations.json"))?;
    let fixture: Phase1DmlFixture = load_json(&fixtures.join("phase1_dml.json"))?;

    let tmp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let mut db = Database::create(tmp.path(), schema).map_err(|e| e.to_string())?;
    mongreldb_kit::migrate(&mut db, &migrations).map_err(|e| e.to_string())?;

    for scenario in &fixture.steps {
        run_phase1_step(&mut db, scenario)?;
    }
    run_phase1_state_checks(&mut db, &fixture.state_checks)?;
    Ok(())
}
