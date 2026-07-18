//! Hybrid scored search (retrievers + fusion + optional exact rerank).
//!
//! This is the Kit-facing surface over the engine's
//! [`mongreldb_core::query::SearchRequest`] path. Kit calls **core** directly
//! (not the C ABI): the same engine path used by `mongreldb_table_search`,
//! `/kit/search`, and the NAPI binding, so behaviour stays aligned across
//! languages.
//!
//! Column references use Kit column **names**; ids are resolved against the
//! active kit schema at call time.

use crate::error::{KitError, Result};
use crate::schema::Row;
use mongreldb_core::query::{
    Condition, Fusion, NamedRetriever, Rerank, Retriever, SearchHit as CoreSearchHit,
    SearchRequest, SetMember, VectorMetric, MAX_FINAL_LIMIT, MAX_RETRIEVER_K,
    MAX_RETRIEVER_NAME_BYTES, MAX_RETRIEVER_WEIGHT,
};
use mongreldb_kit_core::schema::Table as KitTable;
use serde_json::{Map, Value};

/// One ranked component score from a named retriever.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SearchComponent {
    pub retriever_name: String,
    pub rank: usize,
    pub raw_score_kind: String,
    pub raw_score_value: f64,
    pub contribution: f64,
}

/// One hybrid-search hit with scores and row payload.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub row_id: u64,
    pub values: Map<String, Value>,
    pub fused_score: f64,
    pub final_score: f64,
    pub final_rank: usize,
    pub exact_rerank_score: Option<f32>,
    pub components: Vec<SearchComponent>,
}

impl SearchHit {
    /// View as a ordinary kit [`Row`] (drops score metadata).
    pub fn as_row(&self) -> Row {
        Row {
            row_id: self.row_id,
            values: self.values.clone(),
        }
    }
}

/// Exact-vector metric for optional rerank.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMetric {
    Cosine,
    DotProduct,
    Euclidean,
}

impl From<SearchMetric> for VectorMetric {
    fn from(m: SearchMetric) -> Self {
        match m {
            SearchMetric::Cosine => VectorMetric::Cosine,
            SearchMetric::DotProduct => VectorMetric::DotProduct,
            SearchMetric::Euclidean => VectorMetric::Euclidean,
        }
    }
}

/// A named weighted retriever (ANN / sparse / MinHash).
#[derive(Debug, Clone)]
pub enum SearchRetriever {
    Ann {
        column: String,
        name: String,
        weight: f64,
        k: usize,
        query: Vec<f32>,
    },
    Sparse {
        column: String,
        name: String,
        weight: f64,
        k: usize,
        query: Vec<(u32, f32)>,
    },
    MinHash {
        column: String,
        name: String,
        weight: f64,
        k: usize,
        members: Vec<String>,
    },
}

/// Optional exact float-vector rerank after fusion.
#[derive(Debug, Clone)]
pub struct SearchRerank {
    pub embedding_column: String,
    pub query: Vec<f32>,
    pub metric: SearchMetric,
    pub candidate_limit: usize,
    pub weight: f64,
}

/// Hybrid search request (name-based columns).
#[derive(Debug, Clone)]
pub struct SearchSpec {
    /// Hard filters applied before / during retrieval (core conditions with
    /// resolved column ids, or use helpers on [`Transaction`] to build them).
    pub must: Vec<Condition>,
    pub retrievers: Vec<SearchRetriever>,
    /// Reciprocal-rank fusion constant (default 60).
    pub fusion_constant: u32,
    pub rerank: Option<SearchRerank>,
    pub limit: usize,
    /// Optional projection by column **name**.
    pub projection: Option<Vec<String>>,
}

impl Default for SearchSpec {
    fn default() -> Self {
        Self {
            must: Vec::new(),
            retrievers: Vec::new(),
            fusion_constant: 60,
            rerank: None,
            limit: 10,
            projection: None,
        }
    }
}

pub(crate) fn resolve_column_id(table: &KitTable, name: &str) -> Result<u16> {
    table
        .column(name)
        .map(|c| c.id as u16)
        .ok_or_else(|| KitError::Validation(format!("unknown column \"{name}\"")))
}

pub(crate) fn build_core_request(table: &KitTable, spec: &SearchSpec) -> Result<SearchRequest> {
    if spec.retrievers.is_empty() {
        return Err(KitError::Validation(
            "search requires at least one retriever".into(),
        ));
    }
    if !(1..=MAX_FINAL_LIMIT).contains(&spec.limit) {
        return Err(KitError::Validation(format!(
            "search limit must be between 1 and {MAX_FINAL_LIMIT}"
        )));
    }

    let mut retrievers = Vec::with_capacity(spec.retrievers.len());
    for r in &spec.retrievers {
        retrievers.push(build_named_retriever(table, r)?);
    }

    let rerank = match &spec.rerank {
        None => None,
        Some(rr) => {
            if !(spec.limit..=MAX_RETRIEVER_K).contains(&rr.candidate_limit) {
                return Err(KitError::Validation(
                    "rerank candidate_limit is out of range".into(),
                ));
            }
            if !rr.weight.is_finite() || !(0.0..=MAX_RETRIEVER_WEIGHT).contains(&rr.weight) {
                return Err(KitError::Validation(
                    "rerank weight must be finite, non-negative, and within limit".into(),
                ));
            }
            let embedding_column = resolve_column_id(table, &rr.embedding_column)?;
            Some(Rerank::ExactVector {
                embedding_column,
                query: rr.query.clone(),
                metric: rr.metric.into(),
                candidate_limit: rr.candidate_limit,
                weight: rr.weight,
            })
        }
    };

    let projection = match &spec.projection {
        None => None,
        Some(names) => {
            let mut ids = Vec::with_capacity(names.len());
            for name in names {
                ids.push(resolve_column_id(table, name)?);
            }
            Some(ids)
        }
    };

    Ok(SearchRequest {
        must: spec.must.clone(),
        retrievers,
        fusion: Fusion::ReciprocalRank {
            constant: spec.fusion_constant.max(1),
        },
        rerank,
        limit: spec.limit,
        projection,
    })
}

fn build_named_retriever(table: &KitTable, r: &SearchRetriever) -> Result<NamedRetriever> {
    let (name, weight, retriever) = match r {
        SearchRetriever::Ann {
            column,
            name,
            weight,
            k,
            query,
        } => {
            validate_named(name, *weight, *k)?;
            if query.is_empty() {
                return Err(KitError::Validation(
                    "Ann retriever requires a non-empty embedding".into(),
                ));
            }
            let column_id = resolve_column_id(table, column)?;
            (
                name.clone(),
                *weight,
                Retriever::Ann {
                    column_id,
                    query: query.clone(),
                    k: *k,
                },
            )
        }
        SearchRetriever::Sparse {
            column,
            name,
            weight,
            k,
            query,
        } => {
            validate_named(name, *weight, *k)?;
            let column_id = resolve_column_id(table, column)?;
            (
                name.clone(),
                *weight,
                Retriever::Sparse {
                    column_id,
                    query: query.clone(),
                    k: *k,
                },
            )
        }
        SearchRetriever::MinHash {
            column,
            name,
            weight,
            k,
            members,
        } => {
            validate_named(name, *weight, *k)?;
            let column_id = resolve_column_id(table, column)?;
            let members = members
                .iter()
                .map(|s| SetMember::String(s.clone()))
                .collect();
            (
                name.clone(),
                *weight,
                Retriever::MinHash {
                    column_id,
                    members,
                    k: *k,
                },
            )
        }
    };
    Ok(NamedRetriever {
        name,
        weight,
        retriever,
    })
}

fn validate_named(name: &str, weight: f64, k: usize) -> Result<()> {
    if name.is_empty() || name.len() > MAX_RETRIEVER_NAME_BYTES {
        return Err(KitError::Validation(
            "retriever name must be non-empty and within the byte limit".into(),
        ));
    }
    if !(1..=MAX_RETRIEVER_K).contains(&k) {
        return Err(KitError::Validation(format!(
            "retriever k must be between 1 and {MAX_RETRIEVER_K}"
        )));
    }
    if !weight.is_finite() || !(0.0..=MAX_RETRIEVER_WEIGHT).contains(&weight) {
        return Err(KitError::Validation(
            "retriever weight must be finite, non-negative, and within limit".into(),
        ));
    }
    Ok(())
}

pub(crate) fn core_hit_to_kit(hit: CoreSearchHit, table: &KitTable) -> Result<SearchHit> {
    // Build a temporary core row for the existing JSON conversion path.
    let mut columns = std::collections::HashMap::new();
    for (id, value) in &hit.cells {
        columns.insert(*id, value.clone());
    }
    let core_row = mongreldb_core::memtable::Row {
        row_id: hit.row_id,
        committed_epoch: mongreldb_core::Epoch(0),
        columns,
        deleted: false,
    };
    let row = crate::schema::core_row_to_json(&core_row, table)?;
    let components = hit
        .components
        .iter()
        .map(|c| {
            let (raw_score_kind, raw_score_value) = match c.raw_score {
                mongreldb_core::query::RetrieverScore::AnnHammingDistance(d) => {
                    ("ann_hamming_distance".into(), f64::from(d))
                }
                mongreldb_core::query::RetrieverScore::SparseDotProduct(v) => {
                    ("sparse_dot_product".into(), v)
                }
                mongreldb_core::query::RetrieverScore::MinHashEstimatedJaccard(v) => {
                    ("minhash_estimated_jaccard".into(), f64::from(v))
                }
            };
            SearchComponent {
                retriever_name: c.retriever_name.to_string(),
                rank: c.rank,
                raw_score_kind,
                raw_score_value,
                contribution: c.contribution,
            }
        })
        .collect();
    Ok(SearchHit {
        row_id: hit.row_id.0,
        values: row.values,
        fused_score: hit.fused_score,
        final_score: hit.final_score,
        final_rank: hit.final_rank,
        exact_rerank_score: hit.exact_rerank_score,
        components,
    })
}
