"""Dataclass schema helpers for MongrelDB Kit."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass
class Column:
    id: int
    name: str
    storage_type: str
    application_type: Optional[str] = None
    nullable: bool = False
    primary_key: bool = False
    default: Any = None
    generated: bool = False
    enum_values: Optional[list[str]] = None
    # Dimension for storage_type == "embedding". Required for ANN indexes to
    # function (a dim of 0 makes the index non-functional). Mirrors the Rust
    # kit-core `Column.embedding_dim` and the TS kit `ColumnSpec.embeddingDim`.
    embedding_dim: Optional[int] = None
    # How embedding values are produced (kit-core EmbeddingSource JSON shape).
    # Omitted / None = application-supplied vectors (engine default).
    # Examples:
    #   {"kind": "supplied_by_application"}
    #   {"kind": "local_model", "model_path": "/models/x", "model_id": "x"}
    #   {"kind": "generated_column", "provider": "my-provider"}
    #   {"kind": "generated_column_spec", "spec": {
    #       "provider_id": "provider", "model_id": "model", "model_version": "1",
    #       "source_columns": [2], "input_template": "{body}", "dimension": 4,
    #       "normalization": "none", "failure_policy": "abort_write"}}
    embedding_source: Optional[dict[str, Any]] = None
    min: Optional[float] = None
    max: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    regex: Optional[str] = None
    check_expr: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "id": self.id,
            "name": self.name,
            "storage_type": self.storage_type,
            "application_type": self.application_type or self.storage_type,
            "nullable": self.nullable,
            "primary_key": self.primary_key,
            "generated": self.generated,
        }
        if self.default is not None:
            d["default"] = self.default
        if self.enum_values is not None:
            d["enum_values"] = self.enum_values
        if self.embedding_dim is not None:
            d["embedding_dim"] = self.embedding_dim
        if self.embedding_source is not None:
            d["embedding_source"] = self.embedding_source
        if self.min is not None:
            d["min"] = self.min
        if self.max is not None:
            d["max"] = self.max
        if self.min_length is not None:
            d["min_length"] = self.min_length
        if self.max_length is not None:
            d["max_length"] = self.max_length
        if self.regex is not None:
            d["regex"] = self.regex
        if self.check_expr is not None:
            d["check_expr"] = self.check_expr
        return d


@dataclass
class Index:
    name: str
    columns: list[str]
    unique: bool = False
    # Index kind: "bitmap", "fm", "ann", "sparse", "learned_range", "min_hash".
    # When omitted the engine picks a default per column type. Mirrors the
    # `index()` factory's `kind` kwarg and the kit-core `Index.kind`.
    kind: Optional[str] = None
    # ANN representation: "binary_sign" (default) or full-f32 "dense".
    ann_quantization: str = "binary_sign"
    predicate: Optional[str] = None
    ann_m: Optional[int] = None
    ann_ef_construction: Optional[int] = None
    ann_ef_search: Optional[int] = None
    minhash_permutations: Optional[int] = None
    minhash_bands: Optional[int] = None
    learned_range_epsilon: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"name": self.name, "columns": list(self.columns), "unique": self.unique}
        if self.kind is not None:
            d["kind"] = self.kind
        if self.kind == "ann":
            d["ann_quantization"] = self.ann_quantization
        for key in (
            "predicate",
            "ann_m",
            "ann_ef_construction",
            "ann_ef_search",
            "minhash_permutations",
            "minhash_bands",
            "learned_range_epsilon",
        ):
            value = getattr(self, key)
            if value is not None:
                d[key] = value
        return d


@dataclass
class UniqueConstraint:
    name: str
    columns: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "columns": list(self.columns)}


@dataclass
class ForeignKey:
    name: str
    columns: list[str]
    references_table: str
    references_columns: list[str]
    on_delete: str = "restrict"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "columns": list(self.columns),
            "references_table": self.references_table,
            "references_columns": list(self.references_columns),
            "on_delete": self.on_delete,
        }


@dataclass
class Table:
    id: int
    name: str
    columns: list[Column]
    primary_key: list[str]
    indexes: list[Index] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    unique_constraints: list[UniqueConstraint] = field(default_factory=list)
    check_constraints: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "primary_key": list(self.primary_key),
            "indexes": [i.to_dict() for i in self.indexes],
            "foreign_keys": [f.to_dict() for f in self.foreign_keys],
            "unique_constraints": [u.to_dict() for u in self.unique_constraints],
            "check_constraints": list(self.check_constraints),
        }
