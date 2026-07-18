//! Pluggable embedding generation (MongrelDB 0.60) — catalog fidelity,
//! process-local registry, explicit embed helper, and LocalModel plumbing.
//!
//! All tests drive public Kit APIs only (no private `inner`).

use mongreldb_kit::{
    Column, ColumnType, Database, EmbeddingSource, FixedVectorProvider, Index, IndexKind, Schema,
    Table,
};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tempfile::tempdir;

fn docs_schema(source: Option<EmbeddingSource>) -> Schema {
    let mut id = Column::new(1, "id", ColumnType::Int64);
    id.primary_key = true;
    let mut emb = Column::new(2, "embedding", ColumnType::Embedding);
    emb.embedding_dim = Some(4);
    emb.embedding_source = source;
    Schema::new(vec![Table {
        id: 1,
        name: "docs".into(),
        columns: vec![id, emb],
        primary_key: vec!["id".into()],
        indexes: vec![Index {
            name: "docs_emb_ann".into(),
            columns: vec!["embedding".into()],
            unique: false,
            kind: IndexKind::Ann,
        }],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap()
}

fn row(id: i64, emb: Vec<f32>) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("id".into(), json!(id));
    m.insert(
        "embedding".into(),
        Value::Array(emb.into_iter().map(|x| json!(x)).collect()),
    );
    m
}

#[test]
fn create_table_records_each_embedding_source_on_core_catalog() {
    let dir = tempdir().unwrap();
    let cases: Vec<(
        &str,
        Option<EmbeddingSource>,
        Option<mongreldb_kit::CoreEmbeddingSource>,
    )> = vec![
        (
            "app",
            Some(EmbeddingSource::SuppliedByApplication),
            Some(mongreldb_kit::CoreEmbeddingSource::SuppliedByApplication),
        ),
        (
            "local",
            Some(EmbeddingSource::LocalModel {
                model_path: "/models/kit-mini".into(),
                model_id: "kit-mini".into(),
            }),
            Some(mongreldb_kit::CoreEmbeddingSource::LocalModel {
                model_path: std::path::PathBuf::from("/models/kit-mini"),
                model_id: "kit-mini".into(),
            }),
        ),
        (
            "gen",
            Some(EmbeddingSource::GeneratedColumn {
                provider: "registry-prov".into(),
            }),
            Some(mongreldb_kit::CoreEmbeddingSource::GeneratedColumn {
                provider: "registry-prov".into(),
            }),
        ),
        ("omit", None, None),
    ];

    for (label, kit_src, expected_core) in cases {
        let path = dir.path().join(label);
        let db = Database::create(&path, docs_schema(kit_src)).unwrap();
        let handle = db.raw().table("docs").unwrap();
        let guard = handle.lock();
        let col = guard
            .schema()
            .columns
            .iter()
            .find(|c| c.name == "embedding")
            .expect("embedding column");
        assert_eq!(
            col.embedding_source, expected_core,
            "catalog source for case {label}"
        );
        // Omitted kit source → engine treats as application-supplied default.
        if expected_core.is_none() {
            assert!(
                col.embedding_source
                    .as_ref()
                    .map(|s| matches!(s, mongreldb_kit::CoreEmbeddingSource::SuppliedByApplication))
                    .unwrap_or(true),
                "omit means application-supplied default"
            );
        }
    }
}

#[test]
fn registry_register_list_and_embed_helper() {
    let dir = tempdir().unwrap();
    let db = Database::create(
        &dir.path().join("reg"),
        docs_schema(Some(EmbeddingSource::GeneratedColumn {
            provider: "fixed-v1".into(),
        })),
    )
    .unwrap();

    assert!(db.embedding_providers().list_ids().is_empty());

    db.register_embedding_provider(Arc::new(FixedVectorProvider {
        id: "fixed-v1".into(),
        model_id: "fixed-v1".into(),
        vector: vec![0.0, 1.0, 0.0, 0.0],
    }));
    assert_eq!(
        db.embedding_providers().list_ids(),
        vec!["fixed-v1".to_string()]
    );
    assert!(db.embedding_providers().get("fixed-v1").is_some());
    assert!(db.embedding_providers().get("missing").is_none());

    let source = EmbeddingSource::GeneratedColumn {
        provider: "fixed-v1".into(),
    };
    let vectors = db.embed_texts(&source, &["hello", "world"], 4).unwrap();
    assert_eq!(vectors.len(), 2);
    assert_eq!(vectors[0], vec![0.0, 1.0, 0.0, 0.0]);
    assert_eq!(vectors[1], vec![0.0, 1.0, 0.0, 0.0]);

    // Dimension mismatch refuses rather than inventing vectors.
    let err = db.embed_texts(&source, &["x"], 8).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("dimension") || msg.contains("mismatch"),
        "expected dim mismatch, got {msg}"
    );
}

#[test]
fn embed_texts_refuses_supplied_by_application() {
    let dir = tempdir().unwrap();
    let db = Database::create(
        &dir.path().join("refuse"),
        docs_schema(Some(EmbeddingSource::SuppliedByApplication)),
    )
    .unwrap();
    let err = db
        .embed_texts(
            &EmbeddingSource::SuppliedByApplication,
            &["should not generate"],
            4,
        )
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("supplied") || msg.contains("application"),
        "expected SuppliedByApplication refusal, got {msg}"
    );
}

#[test]
fn ordinary_insert_does_not_auto_call_providers() {
    // App-supplied path: insert with an explicit vector succeeds without any
    // registered provider (generation is never invoked on write).
    let dir = tempdir().unwrap();
    let db = Database::create(
        &dir.path().join("noauto"),
        docs_schema(Some(EmbeddingSource::SuppliedByApplication)),
    )
    .unwrap();
    assert!(db.embedding_providers().list_ids().is_empty());
    let mut txn = db.begin().unwrap();
    txn.insert("docs", row(1, vec![1.0, 0.0, 0.0, 0.0]))
        .unwrap();
    txn.commit().unwrap();
    let txn = db.begin().unwrap();
    let hits = txn
        .ann_search("docs", "embedding", vec![1.0, 0.0, 0.0, 0.0], 1)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].values.get("id"), Some(&json!(1)));
}

#[test]
fn local_model_register_embed_insert_ann_search() {
    // Non-semantic FixedVectorProvider exercises LocalModel plumbing only —
    // not a real embedding model (see docs: no invented pseudo-embeddings in
    // production paths).
    let dir = tempdir().unwrap();
    let source = EmbeddingSource::LocalModel {
        model_path: "/models/kit-mini".into(),
        model_id: "kit-mini".into(),
    };
    let db =
        Database::create(&dir.path().join("local"), docs_schema(Some(source.clone()))).unwrap();

    // Catalog recorded LocalModel.
    {
        let handle = db.raw().table("docs").unwrap();
        let guard = handle.lock();
        let col = guard
            .schema()
            .columns
            .iter()
            .find(|c| c.name == "embedding")
            .unwrap();
        assert!(matches!(
            &col.embedding_source,
            Some(mongreldb_kit::CoreEmbeddingSource::LocalModel { model_id, .. })
                if model_id == "kit-mini"
        ));
    }

    db.register_embedding_provider(Arc::new(FixedVectorProvider {
        id: "kit-mini".into(),
        model_id: "kit-mini".into(),
        vector: vec![0.25, 0.5, 0.75, 1.0],
    }));

    let vectors = db.embed_texts(&source, &["document body"], 4).unwrap();
    assert_eq!(vectors[0], vec![0.25, 0.5, 0.75, 1.0]);

    let mut txn = db.begin().unwrap();
    txn.insert("docs", row(42, vectors[0].clone())).unwrap();
    txn.commit().unwrap();

    let txn = db.begin().unwrap();
    let hits = txn
        .ann_search("docs", "embedding", vec![0.25, 0.5, 0.75, 1.0], 3)
        .unwrap();
    assert!(
        hits.iter().any(|h| h.values.get("id") == Some(&json!(42))),
        "ann_search should return the inserted LocalModel-generated row: {hits:?}"
    );
}
