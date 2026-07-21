//! Language-neutral schema model for MongrelDB Kit.
//!
//! A [`Schema`] is a collection of [`Table`]s. Each table has [`Column`]s,
//! indexes, unique constraints, foreign keys, and check constraints.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Storage/application types supported by Kit columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Text,
    Bytes,
    Json,
    Date,
    DateTime,
    TimestampNanos,
    Date64,
    Time64,
    Interval,
    Decimal128,
    /// RFC 4122 UUID (16 bytes, big-endian).
    Uuid,
    /// Native JSON value (stored as typed bytes).
    JsonNative,
    /// Variable-length array of homogeneous values.
    Array,
    /// A dense float32 vector for nearest-neighbour (ANN) search. The dimension
    /// is carried on the column as `embedding_dim`.
    Embedding,
    /// A learned-sparse (SPLADE-style) weighted token vector, stored as a
    /// `[[token_id, weight], ...]` list, for sparse retrieval.
    Sparse,
}

/// How a default value is produced when a row omits a column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefaultKind {
    /// A fixed JSON value written literally.
    Static(serde_json::Value),
    /// The current timestamp as an ISO-8601 string.
    Now,
    /// A fresh UUIDv4 string.
    Uuid,
    /// The next value from a named sequence.
    Sequence(String),
    /// A user-defined default registered by name (resolved at runtime).
    CustomName(String),
}

/// Where dense embedding values for a column originate.
///
/// Mirrors `mongreldb_core::EmbeddingSource` in a language-neutral shape
/// (string paths, serde-tagged). Omitting this on a column means
/// application-supplied vectors (the engine default). Transactional generation
/// uses a portable [`GeneratedEmbeddingSpec`] plus a process-local provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingSpecNormalization {
    #[default]
    None,
    L2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingWriteFailurePolicy {
    #[default]
    AbortWrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedEmbeddingSpec {
    pub provider_id: String,
    pub model_id: String,
    pub model_version: String,
    pub source_columns: Vec<u32>,
    pub input_template: String,
    pub dimension: u32,
    pub normalization: EmbeddingSpecNormalization,
    pub failure_policy: EmbeddingWriteFailurePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EmbeddingSource {
    /// Application writes float vectors directly (default).
    SuppliedByApplication,
    /// Local on-disk model; a provider registered under `model_id` runs inference.
    LocalModel {
        /// Filesystem path to model weights / tokenizer bundle.
        model_path: String,
        /// Stable model identity (registry key and ANN generation stamp).
        model_id: String,
    },
    /// Portable model identity resolved from node-local provider configuration.
    ConfiguredModel {
        provider_id: String,
        model_id: String,
        model_version: String,
    },
    /// Named provider registered on the process (`provider` registry key).
    GeneratedColumn {
        /// Registry key of the provider.
        provider: String,
    },
    /// Transactionally materialized embedding from source columns.
    GeneratedColumnSpec { spec: GeneratedEmbeddingSpec },
}

/// A column definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    /// Stable column identifier. IDs must be unique within a table.
    pub id: u32,
    /// Logical column name.
    pub name: String,
    /// Physical storage type.
    pub storage_type: ColumnType,
    /// Application-facing type (often the same as `storage_type`).
    pub application_type: ColumnType,
    /// Whether the column may contain `null`.
    pub nullable: bool,
    /// Whether this column is part of the primary key.
    pub primary_key: bool,
    /// Optional default value generator.
    pub default: Option<DefaultKind>,
    /// Whether the value is generated on every mutation.
    pub generated: bool,
    /// Permitted string values, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
    /// Minimum numeric value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Maximum numeric value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// Minimum string/bytes length.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_length: Option<usize>,
    /// Maximum string/bytes length.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    /// Regular expression a `text` value must match, stored as its source pattern.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    /// An optional check expression name for runtime evaluation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_expr: Option<String>,
    /// Vector dimension for an `Embedding` column (required for ANN).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_dim: Option<u32>,
    /// How embedding values are produced. Only meaningful for
    /// [`ColumnType::Embedding`]. `None` = application-supplied (engine default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_source: Option<EmbeddingSource>,
    /// Encrypt this column's page payload at rest (requires an encrypted db).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub encrypted: bool,
    /// Encrypt the column but keep it queryable via deterministic equality
    /// tokens / order-preserving encoding (requires an encrypted db).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub encrypted_indexable: bool,
}

impl Column {
    /// Convenience constructor for the common case.
    pub fn new(id: u32, name: impl Into<String>, storage_type: ColumnType) -> Self {
        Self {
            id,
            name: name.into(),
            storage_type,
            application_type: storage_type,
            nullable: false,
            primary_key: false,
            default: None,
            generated: false,
            enum_values: None,
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            regex: None,
            check_expr: None,
            embedding_dim: None,
            embedding_source: None,
            encrypted: false,
            encrypted_indexable: false,
        }
    }
}

/// The kind of secondary index the Kit declares on a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    /// Equality / `IN` acceleration (the default).
    #[default]
    Bitmap,
    /// FM-index substring search (`contains(col, needle)` pushes to `FmContains`).
    Fm,
    /// HNSW approximate-nearest-neighbour index for `Embedding` columns.
    Ann,
    /// SPLADE-style learned-sparse retrieval index for `Sparse` columns.
    Sparse,
    /// MinHash/LSH set-similarity index over a JSON-array set column
    /// (accelerates `set_similarity`).
    MinHash,
    /// Learned zonemap (PGM) index for ordered range predicates on numeric /
    /// timestamp columns. Accelerates `Range`/`RangeF64` conditions.
    LearnedRange,
}

/// ANN representation and distance metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnQuantization {
    /// One sign bit per component, ranked by Hamming distance.
    #[default]
    BinarySign,
    /// Full `f32` vectors, ranked by cosine distance.
    Dense,
    /// Product quantization: vectors split into `num_subvectors` groups, each
    /// encoded to `bits`-bit codes against trained codebooks. Ranked by ADC
    /// (asymmetric distance computation) with optional exact rerank.
    Product { num_subvectors: u16, bits: u8 },
}

/// ANN graph/structure algorithm. Orthogonal to [`AnnQuantization`]: the
/// algorithm chooses how search walks the index; the quantization chooses how
/// vectors are represented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AnnAlgorithm {
    /// Hierarchical Navigable Small World (the default).
    #[default]
    #[serde(rename = "hnsw")]
    Hnsw,
    /// DiskANN / Vamana single-layer robust-pruned graph.
    #[serde(rename = "diskann")]
    DiskAnn,
    /// Inverted file index (k-means centroids + inverted lists).
    #[serde(rename = "ivf")]
    Ivf,
}

/// An index on one or more columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    /// Index kind; defaults to `Bitmap` so pre-existing schemas deserialize
    /// unchanged.
    #[serde(default)]
    pub kind: IndexKind,
    /// ANN representation. Ignored for non-ANN indexes.
    #[serde(default)]
    pub ann_quantization: AnnQuantization,
    /// ANN graph/structure algorithm (Phase 2). Defaults to HNSW. Ignored for
    /// non-ANN indexes. Orthogonal to `ann_quantization`.
    #[serde(default)]
    pub ann_algorithm: AnnAlgorithm,
    /// Optional SQL predicate for a partial index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    /// HNSW graph degree. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_m: Option<usize>,
    /// HNSW construction search width. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_ef_construction: Option<usize>,
    /// HNSW query search width. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_ef_search: Option<usize>,
    /// DiskANN max graph degree R. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_diskann_r: Option<usize>,
    /// DiskANN build search-list size L. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_diskann_l: Option<usize>,
    /// DiskANN query beam width. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_diskann_beam_width: Option<usize>,
    /// DiskANN robust-prune alpha × 100 (120 = 1.2). Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_diskann_alpha: Option<u32>,
    /// IVF inverted-list (centroid) count. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_ivf_nlist: Option<usize>,
    /// IVF probe count at query time. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_ivf_nprobe: Option<usize>,
    /// Product-quantizer training sample cap. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_pq_training_samples: Option<usize>,
    /// Product-quantizer deterministic training seed. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_pq_seed: Option<u64>,
    /// Product-quantizer exact-rerank factor (0 disables). Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_pq_rerank_factor: Option<usize>,
    /// MinHash permutation count. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minhash_permutations: Option<usize>,
    /// MinHash LSH band count. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minhash_bands: Option<usize>,
    /// Learned-range error bound. Engine default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_range_epsilon: Option<usize>,
}

/// A uniqueness constraint over one or more columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueConstraint {
    pub name: String,
    pub columns: Vec<String>,
}

/// A foreign-key reference from child columns to parent columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub references_table: String,
    pub references_columns: Vec<String>,
    #[serde(default)]
    pub on_delete: ForeignKeyAction,
}

/// Action taken when a referenced parent row is deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignKeyAction {
    #[default]
    Restrict,
    Cascade,
    SetNull,
}

/// A named table-level check constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: String,
    pub expr: String,
}

/// A monotonic sequence allocator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sequence {
    pub name: String,
    pub next_value: i64,
}

/// A table definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    /// Stable table identifier. IDs must be unique within a schema.
    pub id: u32,
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<Index>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_keys: Vec<ForeignKey>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique_constraints: Vec<UniqueConstraint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub check_constraints: Vec<CheckConstraint>,
}

impl Table {
    /// Find a column by name.
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Whether the named column is part of the primary key.
    pub fn is_pk_column(&self, name: &str) -> bool {
        self.primary_key.iter().any(|c| c == name)
    }
}

/// Errors that can occur while constructing a [`Schema`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("duplicate table name \"{0}\"")]
    DuplicateTableName(String),
    #[error("duplicate table id {0}")]
    DuplicateTableId(u32),
    #[error("duplicate column name \"{1}\" in table \"{0}\"")]
    DuplicateColumnName(String, String),
    #[error("duplicate column id {1} in table \"{0}\"")]
    DuplicateColumnId(String, u32),
    #[error("primary key column \"{1}\" not found in table \"{0}\"")]
    MissingPrimaryKeyColumn(String, String),
    #[error("index \"{1}\" references unknown column \"{2}\" in table \"{0}\"")]
    MissingIndexColumn(String, String, String),
    #[error("unique constraint \"{1}\" references unknown column \"{2}\" in table \"{0}\"")]
    MissingUniqueColumn(String, String, String),
    #[error("foreign key \"{1}\" references unknown column \"{2}\" in table \"{0}\"")]
    MissingForeignKeyColumn(String, String, String),
    #[error("foreign key \"{1}\" references unknown table \"{2}\"")]
    MissingReferencedTable(String, String, String),
    #[error("foreign key \"{1}\" references unknown column \"{2}\" on table \"{3}\"")]
    MissingReferencedColumn(String, String, String, String),
    #[error(
        "column \"{1}\" on table \"{0}\" sets embedding_source but is not an embedding column"
    )]
    EmbeddingSourceOnNonEmbedding(String, String),
    #[error(
        "embedding column \"{1}\" on table \"{0}\" with LocalModel/GeneratedColumn source requires embedding_dim > 0"
    )]
    EmbeddingSourceMissingDim(String, String),
    #[error("generated embedding column \"{1}\" on table \"{0}\" is invalid: {2}")]
    InvalidGeneratedEmbeddingSpec(String, String, String),
}

/// A validated collection of tables.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Schema {
    pub tables: Vec<Table>,
    by_name: HashMap<String, usize>,
    by_id: HashMap<u32, usize>,
}

impl<'de> serde::Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct SchemaHelper {
            tables: Vec<Table>,
        }
        let helper = SchemaHelper::deserialize(deserializer)?;
        Schema::new(helper.tables).map_err(serde::de::Error::custom)
    }
}

/// A unique index also enforces uniqueness (guard-backed), matching SQL where a
/// UNIQUE index is a UNIQUE constraint. Synthesize a constraint for each unique
/// index unless an existing (or already-synthesized) unique constraint already
/// covers exactly the same columns. Mirrors the TypeScript kit's `table()`.
fn synthesize_unique_from_indexes(table: &mut Table) {
    let mut synthesized: Vec<UniqueConstraint> = Vec::new();
    for idx in &table.indexes {
        if !idx.unique {
            continue;
        }
        let covered = table
            .unique_constraints
            .iter()
            .chain(synthesized.iter())
            .any(|u| u.columns == idx.columns);
        if !covered {
            synthesized.push(UniqueConstraint {
                name: idx.name.clone(),
                columns: idx.columns.clone(),
            });
        }
    }
    table.unique_constraints.extend(synthesized);
}

impl Schema {
    /// Build and validate a schema from a list of tables.
    pub fn new(mut tables: Vec<Table>) -> Result<Self, SchemaError> {
        for table in &mut tables {
            synthesize_unique_from_indexes(table);
        }

        let mut by_name = HashMap::with_capacity(tables.len());
        let mut by_id = HashMap::with_capacity(tables.len());

        for (idx, table) in tables.iter().enumerate() {
            if by_name.contains_key(&table.name) {
                return Err(SchemaError::DuplicateTableName(table.name.clone()));
            }
            if by_id.contains_key(&table.id) {
                return Err(SchemaError::DuplicateTableId(table.id));
            }
            by_name.insert(table.name.clone(), idx);
            by_id.insert(table.id, idx);
        }

        for table in &tables {
            Self::validate_table(table, &by_name)?;
        }

        Ok(Self {
            tables,
            by_name,
            by_id,
        })
    }

    fn validate_table(
        table: &Table,
        table_names: &HashMap<String, usize>,
    ) -> Result<(), SchemaError> {
        let mut column_names = HashMap::with_capacity(table.columns.len());
        let mut column_ids = HashMap::with_capacity(table.columns.len());

        for col in &table.columns {
            if column_names.contains_key(&col.name) {
                return Err(SchemaError::DuplicateColumnName(
                    table.name.clone(),
                    col.name.clone(),
                ));
            }
            if column_ids.contains_key(&col.id) {
                return Err(SchemaError::DuplicateColumnId(table.name.clone(), col.id));
            }
            if col.embedding_source.is_some() && col.storage_type != ColumnType::Embedding {
                return Err(SchemaError::EmbeddingSourceOnNonEmbedding(
                    table.name.clone(),
                    col.name.clone(),
                ));
            }
            if matches!(
                col.embedding_source,
                Some(
                    EmbeddingSource::LocalModel { .. }
                        | EmbeddingSource::ConfiguredModel { .. }
                        | EmbeddingSource::GeneratedColumn { .. }
                        | EmbeddingSource::GeneratedColumnSpec { .. }
                )
            ) && col.embedding_dim.unwrap_or(0) == 0
            {
                return Err(SchemaError::EmbeddingSourceMissingDim(
                    table.name.clone(),
                    col.name.clone(),
                ));
            }
            column_names.insert(col.name.clone(), col.id);
            column_ids.insert(col.id, col.name.clone());
        }

        for col in &table.columns {
            if let Some(EmbeddingSource::ConfiguredModel {
                provider_id,
                model_id,
                model_version,
            }) = col.embedding_source.as_ref()
            {
                if provider_id.is_empty() || model_id.is_empty() || model_version.is_empty() {
                    return Err(SchemaError::InvalidGeneratedEmbeddingSpec(
                        table.name.clone(),
                        col.name.clone(),
                        "provider, model, and version are required".into(),
                    ));
                }
            }
        }

        for col in &table.columns {
            let Some(EmbeddingSource::GeneratedColumnSpec { spec }) = col.embedding_source.as_ref()
            else {
                continue;
            };
            if spec.provider_id.is_empty()
                || spec.model_id.is_empty()
                || spec.model_version.is_empty()
                || spec.source_columns.is_empty()
            {
                return Err(SchemaError::InvalidGeneratedEmbeddingSpec(
                    table.name.clone(),
                    col.name.clone(),
                    "provider, model, version, and source columns are required".into(),
                ));
            }
            if col.embedding_dim != Some(spec.dimension) {
                return Err(SchemaError::InvalidGeneratedEmbeddingSpec(
                    table.name.clone(),
                    col.name.clone(),
                    "spec dimension must match embedding_dim".into(),
                ));
            }
            let mut seen = HashSet::new();
            if spec.source_columns.iter().any(|source_id| {
                *source_id == col.id
                    || !seen.insert(*source_id)
                    || !column_ids.contains_key(source_id)
            }) {
                return Err(SchemaError::InvalidGeneratedEmbeddingSpec(
                    table.name.clone(),
                    col.name.clone(),
                    "source columns must exist, be unique, and exclude the target".into(),
                ));
            }
        }

        for pk in &table.primary_key {
            if !column_names.contains_key(pk) {
                return Err(SchemaError::MissingPrimaryKeyColumn(
                    table.name.clone(),
                    pk.clone(),
                ));
            }
        }

        for idx in &table.indexes {
            for col in &idx.columns {
                if !column_names.contains_key(col) {
                    return Err(SchemaError::MissingIndexColumn(
                        table.name.clone(),
                        idx.name.clone(),
                        col.clone(),
                    ));
                }
            }
        }

        for uq in &table.unique_constraints {
            for col in &uq.columns {
                if !column_names.contains_key(col) {
                    return Err(SchemaError::MissingUniqueColumn(
                        table.name.clone(),
                        uq.name.clone(),
                        col.clone(),
                    ));
                }
            }
        }

        for fk in &table.foreign_keys {
            for col in &fk.columns {
                if !column_names.contains_key(col) {
                    return Err(SchemaError::MissingForeignKeyColumn(
                        table.name.clone(),
                        fk.name.clone(),
                        col.clone(),
                    ));
                }
            }
            if !table_names.contains_key(&fk.references_table) {
                return Err(SchemaError::MissingReferencedTable(
                    table.name.clone(),
                    fk.name.clone(),
                    fk.references_table.clone(),
                ));
            }
        }

        Ok(())
    }

    /// Look up a table by name.
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.by_name.get(name).map(|&idx| &self.tables[idx])
    }

    /// Look up a table by stable id.
    pub fn table_by_id(&self, id: u32) -> Option<&Table> {
        self.by_id.get(&id).map(|&idx| &self.tables[idx])
    }

    /// Whether the schema contains a table with the given name.
    pub fn has_table(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }

    /// Rename a table in place, keeping the `by_name` index in sync. Returns
    /// `false` if `from` is absent or `to` is already in use (no change made).
    /// Does *not* retarget foreign keys — callers that need that should do it
    /// before/after on the tables they own.
    pub fn rename_table(&mut self, from: &str, to: &str) -> bool {
        if from == to {
            return self.has_table(from);
        }
        if !self.has_table(from) || self.has_table(to) {
            return false;
        }
        let idx = *self.by_name.get(from).unwrap();
        self.tables[idx].name = to.to_string();
        self.by_name.remove(from);
        self.by_name.insert(to.to_string(), idx);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_table(name: &str, id: u32) -> Table {
        Table {
            id,
            name: name.into(),
            columns: vec![Column::new(1, "id", ColumnType::Int64)],
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }
    }

    #[test]
    fn schema_rejects_duplicate_table_name() {
        let err = Schema::new(vec![make_table("a", 1), make_table("a", 2)]).unwrap_err();
        assert!(matches!(err, SchemaError::DuplicateTableName(n) if n == "a"));
    }

    #[test]
    fn schema_rejects_duplicate_table_id() {
        let err = Schema::new(vec![make_table("a", 1), make_table("b", 1)]).unwrap_err();
        assert!(matches!(err, SchemaError::DuplicateTableId(1)));
    }

    #[test]
    fn schema_rejects_missing_pk_column() {
        let t = Table {
            id: 1,
            name: "t".into(),
            columns: vec![Column::new(1, "x", ColumnType::Text)],
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        };
        let err = Schema::new(vec![t]).unwrap_err();
        assert!(matches!(err, SchemaError::MissingPrimaryKeyColumn(_, _)));
    }

    #[test]
    fn unique_index_synthesizes_unique_constraint() {
        let schema = Schema::new(vec![Table {
            id: 1,
            name: "users".into(),
            columns: vec![
                Column::new(1, "id", ColumnType::Int64),
                Column::new(2, "email", ColumnType::Text),
                Column::new(3, "handle", ColumnType::Text),
            ],
            primary_key: vec!["id".into()],
            indexes: vec![
                Index {
                    name: "idx_email".into(),
                    columns: vec!["email".into()],
                    unique: true,
                    kind: Default::default(),
                    ann_quantization: Default::default(),
                    ..Default::default()
                },
                // A non-unique index must NOT synthesize a constraint.
                Index {
                    name: "idx_handle".into(),
                    columns: vec!["handle".into()],
                    unique: false,
                    kind: Default::default(),
                    ann_quantization: Default::default(),
                    ..Default::default()
                },
            ],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }])
        .unwrap();
        let table = schema.table("users").unwrap();
        assert_eq!(table.unique_constraints.len(), 1);
        assert_eq!(
            table.unique_constraints[0].columns,
            vec!["email".to_string()]
        );
    }

    #[test]
    fn unique_index_does_not_duplicate_existing_constraint() {
        let schema = Schema::new(vec![Table {
            id: 1,
            name: "users".into(),
            columns: vec![
                Column::new(1, "id", ColumnType::Int64),
                Column::new(2, "email", ColumnType::Text),
            ],
            primary_key: vec!["id".into()],
            indexes: vec![Index {
                name: "idx_email".into(),
                columns: vec!["email".into()],
                unique: true,
                kind: Default::default(),
                ann_quantization: Default::default(),
                ..Default::default()
            }],
            foreign_keys: vec![],
            unique_constraints: vec![UniqueConstraint {
                name: "uq_email".into(),
                columns: vec!["email".into()],
            }],
            check_constraints: vec![],
        }])
        .unwrap();
        // The pre-existing constraint already covers `email`; no synthesis.
        let table = schema.table("users").unwrap();
        assert_eq!(table.unique_constraints.len(), 1);
        assert_eq!(table.unique_constraints[0].name, "uq_email");
    }

    #[test]
    fn schema_roundtrips_json() {
        let schema = Schema::new(vec![Table {
            id: 1,
            name: "users".into(),
            columns: vec![
                Column::new(1, "id", ColumnType::Int64),
                Column {
                    nullable: true,
                    ..Column::new(2, "email", ColumnType::Text)
                },
            ],
            primary_key: vec!["id".into()],
            indexes: vec![Index {
                name: "idx_email".into(),
                columns: vec!["email".into()],
                unique: true,
                kind: Default::default(),
                ann_quantization: Default::default(),
                ..Default::default()
            }],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![CheckConstraint {
                name: "chk_id_positive".into(),
                expr: "id > 0".into(),
            }],
        }])
        .unwrap();

        let json = serde_json::to_string(&schema).unwrap();
        let decoded: Schema = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.tables.len(), 1);
        assert_eq!(decoded.table("users").unwrap().columns.len(), 2);
    }

    #[test]
    fn dense_ann_quantization_roundtrips_and_old_json_defaults_binary() {
        let dense = Index {
            name: "idx_embedding".into(),
            columns: vec!["embedding".into()],
            unique: false,
            kind: IndexKind::Ann,
            ann_quantization: AnnQuantization::Dense,
            predicate: Some("embedding IS NOT NULL".into()),
            ann_m: Some(24),
            ann_ef_construction: Some(96),
            ann_ef_search: Some(48),
            ..Default::default()
        };
        let json = serde_json::to_value(&dense).unwrap();
        assert_eq!(json["ann_quantization"], "dense");
        assert_eq!(json["ann_m"], 24);
        assert_eq!(json["predicate"], "embedding IS NOT NULL");
        assert_eq!(
            serde_json::from_value::<Index>(json)
                .unwrap()
                .ann_quantization,
            AnnQuantization::Dense
        );

        let old: Index = serde_json::from_value(serde_json::json!({
            "name": "idx_embedding",
            "columns": ["embedding"],
            "unique": false,
            "kind": "ann"
        }))
        .unwrap();
        assert_eq!(old.ann_quantization, AnnQuantization::BinarySign);
    }

    #[test]
    fn swappable_ann_algorithm_and_product_quantization_roundtrip() {
        let diskann = Index {
            name: "idx_diskann".into(),
            columns: vec!["embedding".into()],
            unique: false,
            kind: IndexKind::Ann,
            ann_algorithm: AnnAlgorithm::DiskAnn,
            ann_quantization: AnnQuantization::Dense,
            ann_diskann_r: Some(128),
            ann_diskann_l: Some(256),
            ann_diskann_beam_width: Some(4),
            ann_diskann_alpha: Some(130),
            ..Default::default()
        };
        let json = serde_json::to_value(&diskann).unwrap();
        assert_eq!(json["ann_algorithm"], "diskann");
        assert_eq!(json["ann_diskann_r"], 128);
        assert_eq!(json["ann_diskann_alpha"], 130);
        let decoded: Index = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.ann_algorithm, AnnAlgorithm::DiskAnn);
        assert_eq!(decoded.ann_diskann_l, Some(256));

        let ivf = Index {
            name: "idx_ivf".into(),
            columns: vec!["embedding".into()],
            kind: IndexKind::Ann,
            ann_algorithm: AnnAlgorithm::Ivf,
            ann_quantization: AnnQuantization::Dense,
            ann_ivf_nlist: Some(512),
            ann_ivf_nprobe: Some(16),
            ..Default::default()
        };
        let json = serde_json::to_value(&ivf).unwrap();
        assert_eq!(json["ann_algorithm"], "ivf");
        assert_eq!(json["ann_ivf_nlist"], 512);

        let pq = Index {
            name: "idx_pq".into(),
            columns: vec!["embedding".into()],
            kind: IndexKind::Ann,
            ann_quantization: AnnQuantization::Product {
                num_subvectors: 32,
                bits: 8,
            },
            ann_pq_training_samples: Some(10_000),
            ann_pq_seed: Some(42),
            ann_pq_rerank_factor: Some(3),
            ..Default::default()
        };
        let json = serde_json::to_value(&pq).unwrap();
        assert_eq!(json["ann_quantization"]["product"]["num_subvectors"], 32);
        assert_eq!(json["ann_quantization"]["product"]["bits"], 8);
        assert_eq!(json["ann_pq_seed"], 42);
        let decoded: Index = serde_json::from_value(json).unwrap();
        assert_eq!(
            decoded.ann_quantization,
            AnnQuantization::Product {
                num_subvectors: 32,
                bits: 8
            }
        );

        // Default algorithm is HNSW and is omitted on the wire (backward compat).
        let hnsw = Index {
            name: "idx_hnsw".into(),
            columns: vec!["embedding".into()],
            kind: IndexKind::Ann,
            ann_quantization: AnnQuantization::Dense,
            ..Default::default()
        };
        let json = serde_json::to_value(&hnsw).unwrap();
        assert_eq!(json["ann_algorithm"], "hnsw");
    }

    #[test]
    fn embedding_source_roundtrips_json() {
        let mut emb = Column::new(2, "vec", ColumnType::Embedding);
        emb.embedding_dim = Some(4);
        emb.embedding_source = Some(EmbeddingSource::LocalModel {
            model_path: "/models/kit-mini".into(),
            model_id: "kit-mini".into(),
        });
        let schema = Schema::new(vec![Table {
            id: 1,
            name: "docs".into(),
            columns: vec![Column::new(1, "id", ColumnType::Int64), emb],
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }])
        .unwrap();
        let json = serde_json::to_string(&schema).unwrap();
        assert!(json.contains("local_model"));
        assert!(json.contains("kit-mini"));
        let decoded: Schema = serde_json::from_str(&json).unwrap();
        let col = decoded.table("docs").unwrap().column("vec").unwrap();
        assert_eq!(
            col.embedding_source,
            Some(EmbeddingSource::LocalModel {
                model_path: "/models/kit-mini".into(),
                model_id: "kit-mini".into(),
            })
        );
    }

    #[test]
    fn embedding_source_rejected_on_non_embedding() {
        let mut col = Column::new(2, "name", ColumnType::Text);
        col.embedding_source = Some(EmbeddingSource::SuppliedByApplication);
        let err = Schema::new(vec![Table {
            id: 1,
            name: "t".into(),
            columns: vec![Column::new(1, "id", ColumnType::Int64), col],
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }])
        .unwrap_err();
        assert!(matches!(
            err,
            SchemaError::EmbeddingSourceOnNonEmbedding(_, _)
        ));
    }

    #[test]
    fn generated_embedding_requires_dim() {
        let mut emb = Column::new(2, "vec", ColumnType::Embedding);
        emb.embedding_source = Some(EmbeddingSource::GeneratedColumn {
            provider: "local-test".into(),
        });
        let err = Schema::new(vec![Table {
            id: 1,
            name: "t".into(),
            columns: vec![Column::new(1, "id", ColumnType::Int64), emb],
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }])
        .unwrap_err();
        assert!(matches!(err, SchemaError::EmbeddingSourceMissingDim(_, _)));
    }

    #[test]
    fn generated_embedding_spec_roundtrips_and_validates_sources() {
        let mut emb = Column::new(3, "vec", ColumnType::Embedding);
        emb.embedding_dim = Some(4);
        emb.embedding_source = Some(EmbeddingSource::GeneratedColumnSpec {
            spec: GeneratedEmbeddingSpec {
                provider_id: "provider".into(),
                model_id: "model".into(),
                model_version: "1".into(),
                source_columns: vec![2],
                input_template: "{body}".into(),
                dimension: 4,
                normalization: EmbeddingSpecNormalization::None,
                failure_policy: EmbeddingWriteFailurePolicy::AbortWrite,
            },
        });
        let schema = Schema::new(vec![Table {
            id: 1,
            name: "docs".into(),
            columns: vec![
                Column::new(1, "id", ColumnType::Int64),
                Column::new(2, "body", ColumnType::Text),
                emb,
            ],
            primary_key: vec!["id".into()],
            indexes: vec![],
            foreign_keys: vec![],
            unique_constraints: vec![],
            check_constraints: vec![],
        }])
        .unwrap();
        let json = serde_json::to_string(&schema).unwrap();
        assert!(json.contains("generated_column_spec"));
        assert_eq!(serde_json::from_str::<Schema>(&json).unwrap(), schema);
    }
}
