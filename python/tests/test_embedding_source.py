"""Schema DSL parity for embedding_source catalog metadata."""

from mongreldb_kit import (
    Column,
    embedding,
    embedding_source_generated,
    embedding_source_local_model,
    embedding_source_supplied,
    table,
)


def test_embedding_source_helpers_and_column_dict():
    omitted = embedding("vec", 2, 4)
    assert omitted["embedding_dim"] == 4
    assert "embedding_source" not in omitted

    app = embedding(
        "app_vec", 2, 8, embedding_source=embedding_source_supplied()
    )
    assert app["embedding_source"] == {"kind": "supplied_by_application"}

    local = embedding(
        "local_vec",
        2,
        4,
        embedding_source=embedding_source_local_model("/models/kit-mini", "kit-mini"),
    )
    assert local["embedding_source"] == {
        "kind": "local_model",
        "model_path": "/models/kit-mini",
        "model_id": "kit-mini",
    }

    gen = embedding(
        "gen_vec",
        2,
        4,
        embedding_source=embedding_source_generated("my-provider"),
    )
    assert gen["embedding_source"] == {
        "kind": "generated_column",
        "provider": "my-provider",
    }

    schema = table(
        name="docs",
        id=1,
        columns=[
            {"id": 1, "name": "id", "storage_type": "int64", "primary_key": True},
            local,
        ],
        primary_key="id",
    )
    emb_col = next(c for c in schema["columns"] if c["name"] == "local_vec")
    assert emb_col["embedding_source"]["kind"] == "local_model"


def test_column_dataclass_to_dict_includes_embedding_source():
    col = Column(
        id=2,
        name="vec",
        storage_type="embedding",
        embedding_dim=4,
        embedding_source=embedding_source_local_model("/m", "m"),
    )
    d = col.to_dict()
    assert d["embedding_dim"] == 4
    assert d["embedding_source"] == {
        "kind": "local_model",
        "model_path": "/m",
        "model_id": "m",
    }
