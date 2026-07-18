//! Schema/value conversion between the kit model and MongrelDB core.

use crate::error::{KitError, Result};
use mongreldb_core::constraint::{
    CheckConstraint as CoreCheckConstraint, CheckExpr, TableConstraints,
};
use mongreldb_core::memtable::Value as CoreValue;
use mongreldb_core::schema::{
    ColumnDef, ColumnFlags, DefaultExpr, IndexDef, IndexKind, IndexOptions, Schema as CoreSchema,
    TypeId,
};
use mongreldb_kit_core::schema::{
    Column, ColumnType, DefaultKind, EmbeddingSource as KitEmbeddingSource,
    IndexKind as KitIndexKind, Table as KitTable,
};
use serde_json::{Map, Value};
use std::path::PathBuf;

/// Convert a kit table to a core schema.
///
/// Engine 0.46.x accepts column-level `enum_values`, CHECK constraints, and
/// `Static` / `Now` / `Uuid` defaults natively on `create_table`. This pass
/// lowers the kit model into those engine-native shapes so the kit doesn't
/// have to re-validate them on every write.
///
/// Kept kit-side:
/// - `DefaultKind::Sequence` / `DefaultKind::CustomName` cannot cross the
///   in-process boundary; resolved kit-side at write stage time.
pub fn to_core_schema(table: &KitTable) -> Result<CoreSchema> {
    let mut next_check_id: u16 = 1;
    let mut core_checks: Vec<CoreCheckConstraint> = Vec::new();
    let columns: Vec<ColumnDef> = table
        .columns
        .iter()
        .map(|c| ColumnDef {
            id: c.id as u16,
            name: c.name.clone(),
            ty: resolve_type(c),
            flags: to_core_flags(table, c),
            default_value: kit_default_to_core(&c.default, c.storage_type),
            // `None` = application-supplied (engine default). Explicit kit
            // sources lower into the core catalog for LocalModel/GeneratedColumn.
            embedding_source: c
                .embedding_source
                .as_ref()
                .map(to_core_embedding_source),
        })
        .collect();

    for c in &table.columns {
        if let Some(variants) = &c.enum_values {
            if let Some(expr) = variants
                .iter()
                .map(|variant| {
                    CheckExpr::Eq(
                        Box::new(CheckExpr::Col(c.id as u16)),
                        Box::new(CheckExpr::Lit(CoreValue::Bytes(
                            variant.as_bytes().to_vec(),
                        ))),
                    )
                })
                .reduce(|left, right| CheckExpr::Or(Box::new(left), Box::new(right)))
            {
                let id = next_check_id;
                next_check_id = next_check_id.saturating_add(1);
                core_checks.push(CoreCheckConstraint {
                    id,
                    name: format!("{}_enum", c.name),
                    expr,
                });
            }
        }
        if let Some(pattern) = &c.regex {
            let id = next_check_id;
            next_check_id = next_check_id.saturating_add(1);
            core_checks.push(CoreCheckConstraint {
                id,
                name: format!("{}_regex", c.name),
                expr: CheckExpr::Regex {
                    col: c.id as u16,
                    pattern: pattern.clone(),
                    negated: false,
                    case_insensitive: false,
                    cached: std::sync::OnceLock::new(),
                },
            });
        }
    }

    for check in &table.check_constraints {
        let id = next_check_id;
        next_check_id = next_check_id.saturating_add(1);
        core_checks.push(CoreCheckConstraint {
            id,
            name: check.name.clone(),
            expr: lower_kit_check(&check.expr, table)?,
        });
    }
    for column in &table.columns {
        if let Some(expression) = &column.check_expr {
            let id = next_check_id;
            next_check_id = next_check_id.saturating_add(1);
            core_checks.push(CoreCheckConstraint {
                id,
                name: format!("{}_check", column.name),
                expr: lower_kit_check(expression, table)?,
            });
        }
    }

    let mut indexes: Vec<IndexDef> = Vec::new();
    for idx in &table.indexes {
        let kind = match idx.kind {
            KitIndexKind::Bitmap => IndexKind::Bitmap,
            KitIndexKind::Fm => IndexKind::FmIndex,
            KitIndexKind::Ann => IndexKind::Ann,
            KitIndexKind::Sparse => IndexKind::Sparse,
            KitIndexKind::MinHash => IndexKind::MinHash,
            KitIndexKind::LearnedRange => IndexKind::LearnedRange,
        };
        for col_name in &idx.columns {
            if let Some(col) = table.column(col_name) {
                indexes.push(IndexDef {
                    name: format!("{}_{}", idx.name, col_name),
                    column_id: col.id as u16,
                    kind,
                    predicate: None,
                    options: IndexOptions::default(),
                });
            }
        }
    }
    for uq in &table.unique_constraints {
        for col_name in &uq.columns {
            if let Some(col) = table.column(col_name) {
                indexes.push(IndexDef {
                    name: format!("uq_{}_{}", uq.name, col_name),
                    column_id: col.id as u16,
                    kind: IndexKind::Bitmap,
                    predicate: None,
                    options: IndexOptions::default(),
                });
            }
        }
    }

    Ok(CoreSchema {
        schema_id: table.id as u64,
        columns,
        indexes,
        colocation: Vec::new(),
        constraints: TableConstraints {
            uniques: Vec::new(),
            foreign_keys: Vec::new(),
            checks: core_checks,
        },
        clustered: false,
    })
}

fn lower_kit_check(expression: &str, table: &KitTable) -> Result<CheckExpr> {
    use mongreldb_kit_core::{CheckExpression, CheckOperand, CheckOperator};

    fn operand(operand: CheckOperand, table: &KitTable) -> Result<CheckExpr> {
        Ok(match operand {
            CheckOperand::Column(name) => CheckExpr::Col(
                table
                    .column(&name)
                    .ok_or_else(|| {
                        KitError::Validation(format!(
                            "check expression references unknown column {name:?}"
                        ))
                    })?
                    .id as u16,
            ),
            CheckOperand::Number(value)
                if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 =>
            {
                CheckExpr::Lit(CoreValue::Int64(value as i64))
            }
            CheckOperand::Number(value) => CheckExpr::Lit(CoreValue::Float64(value)),
            CheckOperand::String(value) => CheckExpr::Lit(CoreValue::Bytes(value.into_bytes())),
            CheckOperand::Bool(value) => CheckExpr::Lit(CoreValue::Bool(value)),
            CheckOperand::Null => CheckExpr::Lit(CoreValue::Null),
        })
    }

    fn lower(expression: CheckExpression, table: &KitTable) -> Result<CheckExpr> {
        Ok(match expression {
            CheckExpression::Compare { left, op, right } => {
                let left = Box::new(operand(left, table)?);
                let right = Box::new(operand(right, table)?);
                match op {
                    CheckOperator::Eq => CheckExpr::Eq(left, right),
                    CheckOperator::Ne => CheckExpr::Ne(left, right),
                    CheckOperator::Lt => CheckExpr::Lt(left, right),
                    CheckOperator::Le => CheckExpr::Le(left, right),
                    CheckOperator::Gt => CheckExpr::Gt(left, right),
                    CheckOperator::Ge => CheckExpr::Ge(left, right),
                }
            }
            CheckExpression::And(left, right) => CheckExpr::And(
                Box::new(lower(*left, table)?),
                Box::new(lower(*right, table)?),
            ),
            CheckExpression::Or(left, right) => CheckExpr::Or(
                Box::new(lower(*left, table)?),
                Box::new(lower(*right, table)?),
            ),
            CheckExpression::Not(expression) => {
                CheckExpr::Not(Box::new(lower(*expression, table)?))
            }
        })
    }

    let parsed = mongreldb_kit_core::parse_check(expression)
        .map_err(|error| KitError::Validation(error.0))?;
    let lowered = lower(parsed, table)?;
    lowered.validate().map_err(KitError::from)?;
    Ok(lowered)
}

fn resolve_type(col: &Column) -> TypeId {
    if let Some(variants) = &col.enum_values {
        return TypeId::Enum {
            variants: variants.to_vec().into(),
        };
    }
    match col.storage_type {
        ColumnType::Embedding => TypeId::Embedding {
            dim: col.embedding_dim.unwrap_or(0),
        },
        other => to_core_type(other),
    }
}

/// Lower kit embedding-source metadata to the engine catalog shape.
pub fn to_core_embedding_source(source: &KitEmbeddingSource) -> mongreldb_core::EmbeddingSource {
    match source {
        KitEmbeddingSource::SuppliedByApplication => {
            mongreldb_core::EmbeddingSource::SuppliedByApplication
        }
        KitEmbeddingSource::LocalModel {
            model_path,
            model_id,
        } => mongreldb_core::EmbeddingSource::LocalModel {
            model_path: PathBuf::from(model_path),
            model_id: model_id.clone(),
        },
        KitEmbeddingSource::GeneratedColumn { provider } => {
            mongreldb_core::EmbeddingSource::GeneratedColumn {
                provider: provider.clone(),
            }
        }
    }
}

fn kit_default_to_core(default: &Option<DefaultKind>, ty: ColumnType) -> Option<DefaultExpr> {
    let k = default.as_ref()?;
    match k {
        DefaultKind::Static(v) => json_to_core(v, ty).ok().map(DefaultExpr::Static),
        DefaultKind::Now => Some(DefaultExpr::Now),
        DefaultKind::Uuid => Some(DefaultExpr::Uuid),
        // Sequence / CustomName are kit-only resolution paths; leave None so
        // the kit continues to apply them at write stage time.
        DefaultKind::Sequence(_) | DefaultKind::CustomName(_) => None,
    }
}

pub(crate) fn to_core_flags(table: &KitTable, column: &Column) -> ColumnFlags {
    let mut flags = ColumnFlags::empty();
    if column.nullable {
        flags = flags.with(ColumnFlags::NULLABLE);
    }
    if table.primary_key.contains(&column.name) || column.primary_key {
        flags = flags.with(ColumnFlags::PRIMARY_KEY);
    }
    if column.encrypted {
        flags = flags.with(ColumnFlags::ENCRYPTED);
    }
    if column.encrypted_indexable {
        flags = flags.with(ColumnFlags::ENCRYPTED_INDEXABLE);
    }
    flags
}

pub(crate) fn to_core_type(ty: ColumnType) -> TypeId {
    match ty {
        ColumnType::Bool => TypeId::Bool,
        ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64 => {
            TypeId::Int64
        }
        ColumnType::Float32 | ColumnType::Float64 => TypeId::Float64,
        ColumnType::Text
        | ColumnType::Bytes
        | ColumnType::Json
        | ColumnType::Date
        | ColumnType::DateTime => TypeId::Bytes,
        ColumnType::TimestampNanos => TypeId::Int64,
        ColumnType::Date64 => TypeId::Date64,
        ColumnType::Time64 => TypeId::Time64,
        ColumnType::Interval => TypeId::Interval,
        ColumnType::Decimal128 => TypeId::Decimal128 {
            precision: 38,
            scale: 2,
        },
        ColumnType::Uuid => TypeId::Uuid,
        ColumnType::JsonNative => TypeId::Json,
        ColumnType::Array => TypeId::Array { element_type: 0 },
        // Dimension is filled from the column's `embedding_dim` in
        // `to_core_schema`; a bare type has no dimension context.
        ColumnType::Embedding => TypeId::Embedding { dim: 0 },
        // Sparse vectors are stored as bincode'd `Vec<(u32, f32)>` in a Bytes
        // column; the Sparse index reads the tokens from those bytes.
        ColumnType::Sparse => TypeId::Bytes,
    }
}

/// Convert a JSON value to a core cell value using the column type for guidance.
pub fn json_to_core(value: &Value, ty: ColumnType) -> Result<CoreValue> {
    Ok(match value {
        Value::Null => CoreValue::Null,
        Value::Bool(b) => CoreValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                CoreValue::Int64(i)
            } else {
                CoreValue::Float64(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::String(s) => CoreValue::Bytes(s.as_bytes().to_vec()),
        Value::Array(arr) => {
            if ty == ColumnType::Sparse {
                let mut terms: Vec<(u32, f32)> = Vec::with_capacity(arr.len());
                for pair in arr {
                    let p = pair
                        .as_array()
                        .ok_or_else(|| KitError::Validation("sparse expects pairs".into()))?;
                    let token =
                        p.first().and_then(|v| v.as_u64()).ok_or_else(|| {
                            KitError::Validation("sparse token must be u32".into())
                        })? as u32;
                    let weight = p.get(1).and_then(|v| v.as_f64()).ok_or_else(|| {
                        KitError::Validation("sparse weight must be number".into())
                    })? as f32;
                    terms.push((token, weight));
                }
                CoreValue::Bytes(
                    bincode::serialize(&terms).map_err(|e| KitError::Validation(e.to_string()))?,
                )
            } else if ty == ColumnType::Embedding {
                let mut vec = Vec::with_capacity(arr.len());
                for v in arr {
                    match v.as_f64() {
                        Some(f) => vec.push(f as f32),
                        None => {
                            return Err(KitError::Validation("embedding expects numbers".into()))
                        }
                    }
                }
                CoreValue::Embedding(vec)
            } else if ty == ColumnType::Bytes {
                let mut bytes = Vec::with_capacity(arr.len());
                for v in arr {
                    match v {
                        Value::Number(n) => bytes.push(n.as_i64().unwrap_or(0) as u8),
                        _ => return Err(KitError::Validation("bytes array expected".into())),
                    }
                }
                CoreValue::Bytes(bytes)
            } else {
                CoreValue::Bytes(serde_json::to_vec(value)?)
            }
        }
        Value::Object(_) => CoreValue::Bytes(serde_json::to_vec(value)?),
    })
}

/// Convert a core cell value back to JSON, guided by the column type.
pub fn core_to_json(value: &CoreValue, ty: ColumnType) -> Result<Value> {
    Ok(match (value, ty) {
        (CoreValue::Null, _) => Value::Null,
        (CoreValue::Bool(b), _) => Value::Bool(*b),
        (CoreValue::Int64(i), ColumnType::Int8) => Value::Number((*i as i8).into()),
        (CoreValue::Int64(i), ColumnType::Int16) => Value::Number((*i as i16).into()),
        (CoreValue::Int64(i), ColumnType::Int32) => Value::Number((*i as i32).into()),
        (CoreValue::Int64(i), ColumnType::Int64) => Value::Number((*i).into()),
        (CoreValue::Int64(i), ColumnType::TimestampNanos) => Value::Number((*i).into()),
        (CoreValue::Int64(i), _) => Value::Number((*i).into()),
        (CoreValue::Float64(f), ColumnType::Float32) => serde_json::to_value(*f as f32)?,
        (CoreValue::Float64(f), _) => serde_json::to_value(*f)?,
        (CoreValue::Bytes(b), ColumnType::Sparse) => {
            let terms: Vec<(u32, f32)> =
                bincode::deserialize(b).map_err(|e| KitError::Validation(e.to_string()))?;
            Value::Array(
                terms
                    .into_iter()
                    .map(|(t, w)| Value::Array(vec![Value::from(t), Value::from(w as f64)]))
                    .collect(),
            )
        }
        (CoreValue::Bytes(b), ColumnType::Bytes) => {
            Value::Array(b.iter().map(|x| Value::Number((*x).into())).collect())
        }
        (CoreValue::Bytes(b), _) => match std::str::from_utf8(b) {
            Ok(s) => Value::String(s.to_string()),
            Err(_) => Value::Array(b.iter().map(|x| Value::Number((*x).into())).collect()),
        },
        (CoreValue::Embedding(v), _) => serde_json::to_value(v)?,
        (CoreValue::Decimal(d), _) => Value::String(d.to_string()),
        (
            CoreValue::Interval {
                months,
                days,
                nanos,
            },
            _,
        ) => {
            serde_json::json!({ "months": months, "days": days, "nanos": nanos })
        }
        (CoreValue::Uuid(b), _) => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            serde_json::Value::String(hex)
        }
        (CoreValue::Json(b), _) => serde_json::from_slice(b.as_slice())
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(b).into_owned())),
    })
}

/// Build a JSON row from a core row and a kit table definition.
pub fn core_row_to_json(row: &mongreldb_core::memtable::Row, table: &KitTable) -> Result<Row> {
    let mut values = Map::new();
    for col in &table.columns {
        let v = row
            .columns
            .get(&(col.id as u16))
            .cloned()
            .unwrap_or(CoreValue::Null);
        values.insert(col.name.clone(), core_to_json(&v, col.storage_type)?);
    }
    Ok(Row {
        row_id: row.row_id.0,
        values,
    })
}

/// A kit row, identified by its internal storage row id and column values.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub row_id: u64,
    pub values: Map<String, Value>,
}

impl Row {
    /// Extract the primary-key value(s) as a JSON value.
    ///
    /// Single-column primary keys return the scalar value; composite keys return
    /// an object.
    pub fn pk(&self, table: &KitTable) -> Option<Value> {
        if table.primary_key.len() == 1 {
            self.values.get(&table.primary_key[0]).cloned()
        } else {
            let mut obj = Map::new();
            for name in &table.primary_key {
                obj.insert(
                    name.clone(),
                    self.values.get(name).cloned().unwrap_or(Value::Null),
                );
            }
            Some(Value::Object(obj))
        }
    }
}

/// Extract the primary-key value(s) from a JSON value map.
pub fn pk_value(values: &Map<String, Value>, table: &KitTable) -> Option<Value> {
    if table.primary_key.len() == 1 {
        values.get(&table.primary_key[0]).cloned()
    } else {
        let mut obj = Map::new();
        for name in &table.primary_key {
            obj.insert(
                name.clone(),
                values.get(name).cloned().unwrap_or(Value::Null),
            );
        }
        Some(Value::Object(obj))
    }
}

/// Convert a primary-key value into the column values for lookup.
pub fn pk_to_map(pk: &Value, table: &KitTable) -> Result<Map<String, Value>> {
    let mut map = Map::new();
    match pk {
        Value::Object(obj) => {
            for name in &table.primary_key {
                let v = obj
                    .get(name)
                    .cloned()
                    .ok_or_else(|| KitError::Validation(format!("missing pk column {name}")))?;
                map.insert(name.clone(), v);
            }
        }
        scalar if table.primary_key.len() == 1 => {
            map.insert(table.primary_key[0].clone(), scalar.clone());
        }
        _ => {
            return Err(KitError::Validation(
                "primary key value shape mismatch".into(),
            ))
        }
    }
    Ok(map)
}

/// Build a core cell vector from a JSON row and kit table definition.
pub fn row_to_core_cells(
    values: &Map<String, Value>,
    table: &KitTable,
) -> Result<Vec<(u16, CoreValue)>> {
    let mut cells = Vec::with_capacity(table.columns.len());
    for col in &table.columns {
        let v = values.get(&col.name).cloned().unwrap_or(Value::Null);
        cells.push((col.id as u16, json_to_core(&v, col.storage_type)?));
    }
    Ok(cells)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongreldb_core::constraint::CheckExpr;
    use mongreldb_kit_core::schema::{Column, DefaultKind, Table as KitTable};
    use serde_json::json;

    fn kit_text_column(
        id: u32,
        name: &str,
        enum_values: Option<Vec<String>>,
        regex: Option<String>,
        default: Option<DefaultKind>,
    ) -> Column {
        let mut c = Column::new(id, name, ColumnType::Text);
        c.enum_values = enum_values;
        c.regex = regex;
        c.default = default;
        c
    }

    fn envelope_table(columns: Vec<Column>) -> KitTable {
        KitTable {
            id: 1,
            name: "envelope".into(),
            columns,
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }
    }

    #[test]
    fn enum_values_lower_to_engine_enum_type() {
        let table = envelope_table(vec![
            kit_text_column(1, "id", None, None, None),
            kit_text_column(
                2,
                "role",
                Some(vec!["user".into(), "admin".into()]),
                None,
                None,
            ),
        ]);
        let core = to_core_schema(&table).unwrap();
        let role = core.columns.iter().find(|c| c.name == "role").unwrap();
        match &role.ty {
            TypeId::Enum { variants } => {
                assert_eq!(
                    variants.as_ref(),
                    &["user".to_string(), "admin".to_string()]
                )
            }
            other => panic!("expected TypeId::Enum, got {other:?}"),
        }
        assert_eq!(role.default_value, None);
        let check = &core.constraints.checks[0];
        assert_eq!(check.name, "role_enum");
        let valid = std::collections::HashMap::from([(2, CoreValue::Bytes(b"user".to_vec()))]);
        let invalid = std::collections::HashMap::from([(2, CoreValue::Bytes(b"owner".to_vec()))]);
        assert!(check.expr.satisfied(&valid));
        assert!(!check.expr.satisfied(&invalid));
    }

    #[test]
    fn regex_lower_to_engine_check_constraint() {
        let table = envelope_table(vec![
            kit_text_column(1, "id", None, None, None),
            kit_text_column(2, "slug", None, Some("^[a-z0-9-]+$".into()), None),
        ]);
        let core = to_core_schema(&table).unwrap();
        assert_eq!(core.constraints.checks.len(), 1, "{:?}", core.constraints);
        let check = &core.constraints.checks[0];
        assert_eq!(check.name, "slug_regex");
        match &check.expr {
            CheckExpr::Regex {
                col,
                pattern,
                negated,
                case_insensitive,
                ..
            } => {
                assert_eq!(*col, 2);
                assert_eq!(pattern, "^[a-z0-9-]+$");
                assert!(!*negated);
                assert!(!*case_insensitive);
            }
            other => panic!("expected CheckExpr::Regex, got {other:?}"),
        }
    }

    #[test]
    fn static_now_uuid_defaults_lower_to_engine_default_expr() {
        let mut static_col = kit_text_column(3, "label", None, None, None);
        static_col.default = Some(DefaultKind::Static(json!("draft")));
        let mut now_col = kit_text_column(4, "created", None, None, None);
        now_col.default = Some(DefaultKind::Now);
        let mut uuid_col = kit_text_column(5, "uuid", None, None, None);
        uuid_col.default = Some(DefaultKind::Uuid);
        let mut seq_col = kit_text_column(6, "seq", None, None, None);
        seq_col.default = Some(DefaultKind::Sequence("seq_users".into()));
        let mut custom_col = kit_text_column(7, "custom", None, None, None);
        custom_col.default = Some(DefaultKind::CustomName("named_fn".into()));

        let table = envelope_table(vec![
            kit_text_column(1, "id", None, None, None),
            static_col,
            now_col,
            uuid_col,
            seq_col,
            custom_col,
        ]);
        let core = to_core_schema(&table).unwrap();
        let by = |n: &str| core.columns.iter().find(|c| c.name == n).unwrap();

        assert!(matches!(
            by("label").default_value,
            Some(DefaultExpr::Static(CoreValue::Bytes(_)))
        ));
        assert!(matches!(
            by("created").default_value,
            Some(DefaultExpr::Now)
        ));
        assert!(matches!(by("uuid").default_value, Some(DefaultExpr::Uuid)));
        // Kit-only shapes stay kit-side (None = no engine default).
        assert_eq!(by("seq").default_value, None);
        assert_eq!(by("custom").default_value, None);
    }

    #[test]
    fn embedding_source_kinds_lower_to_core_catalog() {
        use mongreldb_kit_core::schema::EmbeddingSource as KitSrc;

        let mut app = Column::new(2, "app_vec", ColumnType::Embedding);
        app.embedding_dim = Some(4);
        app.embedding_source = Some(KitSrc::SuppliedByApplication);

        let mut local = Column::new(3, "local_vec", ColumnType::Embedding);
        local.embedding_dim = Some(4);
        local.embedding_source = Some(KitSrc::LocalModel {
            model_path: "/models/demo".into(),
            model_id: "demo".into(),
        });

        let mut gen = Column::new(4, "gen_vec", ColumnType::Embedding);
        gen.embedding_dim = Some(8);
        gen.embedding_source = Some(KitSrc::GeneratedColumn {
            provider: "my-provider".into(),
        });

        let mut omitted = Column::new(5, "omit_vec", ColumnType::Embedding);
        omitted.embedding_dim = Some(4);
        // embedding_source left None → application-supplied default

        let table = envelope_table(vec![
            kit_text_column(1, "id", None, None, None),
            app,
            local,
            gen,
            omitted,
        ]);
        let core = to_core_schema(&table).unwrap();
        let by = |n: &str| core.columns.iter().find(|c| c.name == n).unwrap();

        assert_eq!(
            by("app_vec").embedding_source,
            Some(mongreldb_core::EmbeddingSource::SuppliedByApplication)
        );
        assert_eq!(
            by("local_vec").embedding_source,
            Some(mongreldb_core::EmbeddingSource::LocalModel {
                model_path: PathBuf::from("/models/demo"),
                model_id: "demo".into(),
            })
        );
        assert_eq!(
            by("gen_vec").embedding_source,
            Some(mongreldb_core::EmbeddingSource::GeneratedColumn {
                provider: "my-provider".into(),
            })
        );
        assert_eq!(by("omit_vec").embedding_source, None);
    }

    #[test]
    fn table_and_column_checks_lower_to_engine() {
        let mut balance = Column::new(2, "balance", ColumnType::Int64);
        balance.check_expr = Some("balance <= 100".into());
        let mut table = envelope_table(vec![kit_text_column(1, "id", None, None, None), balance]);
        table.check_constraints = vec![mongreldb_kit_core::schema::CheckConstraint {
            name: "balance_positive".into(),
            expr: "balance > 0 AND id > 0".into(),
        }];
        let core = to_core_schema(&table).unwrap();
        assert_eq!(core.constraints.checks.len(), 2);
        let valid =
            std::collections::HashMap::from([(1, CoreValue::Int64(1)), (2, CoreValue::Int64(50))]);
        let invalid =
            std::collections::HashMap::from([(1, CoreValue::Int64(1)), (2, CoreValue::Int64(101))]);
        assert!(core
            .constraints
            .checks
            .iter()
            .all(|check| check.expr.satisfied(&valid)));
        assert!(core
            .constraints
            .checks
            .iter()
            .any(|check| !check.expr.satisfied(&invalid)));

        table.check_constraints[0].expr = "missing > 0".into();
        assert!(to_core_schema(&table).is_err());
    }
}
