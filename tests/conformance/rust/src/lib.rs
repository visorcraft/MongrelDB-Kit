use mongreldb_kit::{
    encode_pk, encode_row_guard_key, encode_unique_key, Database, Direction, Expr, KeyComponent,
    KitError, Literal, Migration, OrderBy, Query, Row, Schema, Select,
};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

#[derive(Debug, serde::Deserialize)]
struct Scenario {
    name: String,
    table: String,
    #[serde(default)]
    row: Option<Map<String, Value>>,
    #[serde(default)]
    pk: Option<Value>,
    #[serde(default)]
    patch: Option<Map<String, Value>>,
    #[serde(default)]
    filter: Option<Map<String, Value>>,
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
        KitError::Storage(_) => "STORAGE",
        KitError::Integrity(_) => "INTEGRITY",
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
