"""Python facade for MongrelDB Kit."""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterable, Optional

from .mongreldb_kit_py import (
    ConflictError,
    DuplicateError,
    ForeignKeyError,
    IntegrityError,
    MigrationError,
    RestrictError,
    StorageError,
    TriggerValidationError,
    ValidationError,
    migrate as _migrate,
    encode_pk as _encode_pk,
    encode_unique_key as _encode_unique_key,
    encode_row_guard_key as _encode_row_guard_key,
)
from .mongreldb_kit_py import Database as _Database
from .mongreldb_kit_py import Transaction as _Transaction

from ._schema import Column, ForeignKey, Index, Table, UniqueConstraint
from .remote import RemoteDatabase, RemoteTransaction

__all__ = [
    "Database",
    "Transaction",
    "migrate",
    "table",
    "column",
    "int",
    "integer",
    "text",
    "bool_",
    "boolean",
    "float_",
    "float64",
    "json_col",
    "bytes_col",
    "timestamp",
    "datetime",
    "date",
    "index",
    "unique",
    "fk",
    "check",
    "agg",
    "count_distinct",
    "col",
    "on_eq",
    "encode_pk",
    "encode_unique_key",
    "encode_row_guard_key",
    "ConflictError",
    "DuplicateError",
    "ForeignKeyError",
    "IntegrityError",
    "MigrationError",
    "RestrictError",
    "StorageError",
    "TriggerValidationError",
    "ValidationError",
    "Column",
    "ForeignKey",
    "Index",
    "Table",
    "UniqueConstraint",
    "RemoteDatabase",
    "RemoteTransaction",
]


class Database:
    """A MongrelDB Kit database handle."""

    def __init__(self, handle: _Database) -> None:
        self._handle = handle

    @staticmethod
    def open(path: str) -> "Database":
        return Database(_Database.open(path))

    @staticmethod
    def create(path: str, schema: Any) -> "Database":
        schema_json = schema if isinstance(schema, str) else json.dumps(schema)
        return Database(_Database.create(path, schema_json))

    @staticmethod
    def open_encrypted(path: str, passphrase: str) -> "Database":
        """Open a page-encrypted database with its passphrase."""
        return Database(_Database.open_encrypted(path, passphrase))

    @staticmethod
    def create_encrypted(path: str, schema: Any, passphrase: str) -> "Database":
        """Create a page-encrypted database (AES-256-GCM). Columns flagged
        ``encrypted`` / ``encrypted_indexable`` in the schema are encrypted."""
        schema_json = schema if isinstance(schema, str) else json.dumps(schema)
        return Database(_Database.create_encrypted(path, schema_json, passphrase))

    def begin(self) -> "Transaction":
        return Transaction(self._handle.begin())

    def migrate(self, migrations: Any) -> None:
        migrations_json = migrations if isinstance(migrations, str) else json.dumps(migrations)
        _migrate(self._handle, migrations_json)

    def set_schema(self, schema: Any) -> None:
        schema_json = schema if isinstance(schema, str) else json.dumps(schema)
        self._handle.set_schema(schema_json)

    def allocate_sequence(self, name: str, count: int = 1) -> int:
        """Allocate ``count`` values from a named sequence, returning the first."""
        return self._handle.allocate_sequence(name, count)

    def table_names(self) -> list[str]:
        """Application table names, excluding reserved ``__kit_*`` tables."""
        return self._handle.table_names()

    def gc(self) -> int:
        """Reclaim orphaned runs and stale WAL/shadow files; return the count."""
        return self._handle.gc()

    def check(self) -> list[dict[str, Any]]:
        """Verify run footer checksums; return a list of integrity issues."""
        return [json.loads(s) for s in self._handle.check()]

    def doctor(self) -> list[int]:
        """Drop corrupt runs; return the ids of the runs that were dropped."""
        return self._handle.doctor()

    def rename_table(self, old_name: str, new_name: str) -> None:
        """Rename a live table.

        Fails if ``old_name`` does not exist or ``new_name`` is already in use;
        a no-op when they are equal. Names beginning with ``__kit_`` are
        reserved for internal tables. The kit schema catalog is not updated —
        call :meth:`set_schema` if you want the schema model to reflect the new
        name.
        """
        self._handle.rename_table(old_name, new_name)

    def compact_all(self) -> tuple[int, int]:
        """Compact all tables; return ``(compacted, skipped)``."""
        return self._handle.compact_all()

    def compact_table(self, name: str) -> bool:
        """Compact a single table; return ``True`` if compacted, ``False`` if skipped."""
        return self._handle.compact_table(name)

    def analyze(self) -> None:
        """Rebuild statistics/metadata for every table's indexes.

        The engine's ``ANALYZE`` equivalent. Safe to run at any time; useful
        after bulk loads so the query planner and learned indexes have fresh
        data.
        """
        self._handle.analyze()

    def vacuum(self) -> int:
        """Reclaim space across all tables: compact every sorted run, then gc.

        Returns the count of reclaimed orphaned runs/files. Equivalent to the
        engine's ``VACUUM``.
        """
        return self._handle.vacuum()

    def sql_rows(self, sql: str) -> list[dict[str, Any]]:
        """Run a SQL statement; return the result rows as a list of dicts.

        Empty for DDL/DML. Writes through SQL bypass kit-level constraints
        (defaults, enums, min/max, length, regex, triggers) — use the
        transactional API for constrained writes. The engine's own declarative
        constraints (unique, FK, check) still apply.
        """
        return self._handle.sql_rows(sql)

    def sql_arrow(self, sql: str) -> bytes:
        """Run a SQL statement; return the result as raw Arrow IPC bytes.

        Decode with ``pyarrow.ipc.open_file``. Empty for DDL/DML. The same
        bypass caveats as :meth:`sql_rows` apply.
        """
        return self._handle.sql_arrow(sql)

    def snapshot_epoch(self) -> int:
        """The current visible commit epoch (monotonically increasing version)."""
        return self._handle.snapshot_epoch()

    def create_procedure(self, procedure: Any) -> dict[str, Any]:
        procedure_json = procedure if isinstance(procedure, str) else json.dumps(procedure)
        return json.loads(self._handle.create_procedure(procedure_json))

    def replace_procedure(self, procedure: Any) -> dict[str, Any]:
        procedure_json = procedure if isinstance(procedure, str) else json.dumps(procedure)
        return json.loads(self._handle.replace_procedure(procedure_json))

    def drop_procedure(self, name: str) -> None:
        self._handle.drop_procedure(name)

    def call_procedure(self, name: str, args: Any | None = None) -> dict[str, Any]:
        args_json = "{}" if args is None else (args if isinstance(args, str) else json.dumps(args))
        return json.loads(self._handle.call_procedure(name, args_json))

    def create_trigger(self, trigger: Any) -> dict[str, Any]:
        trigger_json = trigger if isinstance(trigger, str) else json.dumps(trigger)
        return json.loads(self._handle.create_trigger(trigger_json))

    def replace_trigger(self, trigger: Any) -> dict[str, Any]:
        trigger_json = trigger if isinstance(trigger, str) else json.dumps(trigger)
        return json.loads(self._handle.replace_trigger(trigger_json))

    def drop_trigger(self, name: str) -> None:
        self._handle.drop_trigger(name)

    def triggers(self) -> list[dict[str, Any]]:
        return [json.loads(s) for s in self._handle.triggers()]

    def trigger(self, name: str) -> Optional[dict[str, Any]]:
        raw = self._handle.trigger(name)
        return None if raw is None else json.loads(raw)

    def export_tsv(self, table: str) -> str:
        """Export every visible row of ``table`` as a TSV document."""
        return self._handle.export_tsv(table)

    def import_tsv(self, table: str, text: str) -> int:
        """Import a TSV document into ``table``; return the rows inserted."""
        return self._handle.import_tsv(table, text)

    def rows_at_epoch(self, table: str, epoch: int) -> list[dict[str, Any]]:
        """Read every row of ``table`` visible at commit ``epoch`` (time-travel)."""
        return self._handle.rows_at_epoch(table, epoch)

    def approx_aggregate(
        self,
        table: str,
        agg: str,
        column: str | None = None,
        z: float = 1.96,
    ) -> dict[str, Any] | None:
        """Approximate ``count``/``sum``/``avg`` from the reservoir sample.

        Returns ``{point, ci_low, ci_high, n_population, n_sample_live,
        n_passing}`` with a ``z``-score confidence interval, or ``None`` when the
        reservoir is empty. ``column`` is required for ``sum``/``avg``.
        """
        raw = self._handle.approx_aggregate(table, agg, column, z)
        return None if raw is None else raw

    def scan_batched(
        self,
        table: str,
        batch_size: int,
        callback: "Callable[[list[dict[str, Any]]], None]",
    ) -> None:
        """Stream ``table`` in batches of at most ``batch_size`` rows.

        ``callback`` is invoked once per batch with a list of dict rows.
        """
        self._handle.scan_batched(table, batch_size, callback)

    def set_similarity(
        self,
        table: str,
        column: str,
        query: "Iterable[str]",
        k: int = 10,
    ) -> list[dict[str, Any]]:
        """Rank rows by Jaccard set-similarity to ``query`` over ``column``.

        ``column`` holds a JSON array of strings. Returns up to ``k``
        ``{"row": {...}, "similarity": float}`` dicts, highest similarity first.
        """
        return self._handle.set_similarity(table, column, list(query), k)

    def flush(self) -> None:
        """Flush all tables to durable runs (enables the incremental fast path)."""
        self._handle.flush()

    def incremental_aggregate(
        self,
        table: str,
        agg: str,
        column: str | None = None,
        filter: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Incrementally-maintained ``count``/``sum``/``min``/``max``/``avg``.

        Returns ``{value, incremental, delta_rows}``; ``value`` is always exact.
        ``column`` is required for sum/min/max/avg. An optional ``filter`` must
        translate exactly to index conditions.
        """
        return self._handle.incremental_aggregate(table, agg, column, filter)

    def explain(self, table: str, filter: dict[str, Any]) -> dict[str, Any]:
        """Explain how ``filter`` pushes down against ``table`` (diagnostic only).

        Returns ``{index_accelerated, exact, pushed_conditions}``.
        """
        return self._handle.explain(table, filter)

    def close(self) -> None:
        """Close the database handle and release underlying resources."""
        self._handle.close()

    def transaction(self, fn: Any, max_retries: int = 5) -> Any:
        """Run ``fn(txn)`` in a transaction, committing on success and retrying
        the whole callback on retryable write-write conflicts."""
        attempt = 0
        while True:
            txn = self.begin()
            try:
                result = fn(txn)
                txn.commit()
                return result
            except ConflictError:
                txn.rollback()
                if attempt >= max_retries:
                    raise
                attempt += 1
            except Exception:
                txn.rollback()
                raise

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class Transaction:
    """A kit transaction with context-manager support."""

    def __init__(self, handle: _Transaction) -> None:
        self._handle = handle
        self._closed = False

    def insert(self, table: str, row: Any) -> dict[str, Any]:
        return json.loads(self._handle.insert(table, _to_json(row)))

    def insert_many(self, table: str, rows: Iterable[Any]) -> list[dict[str, Any]]:
        """Insert many rows in this single transaction.

        Each row still passes through defaults, validation, and constraint
        checks, but the whole batch is staged in one transaction (commit once) —
        far faster than a row-at-a-time loop for bulk loads. Returns the inserted
        rows with defaults applied.
        """
        results = self._handle.insert_many(table, _to_json(list(rows)))
        return [json.loads(r) for r in results]

    def insert_returning(
        self,
        table: str,
        row: Any,
        returning: Iterable[str],
    ) -> dict[str, Any]:
        """Insert a row and return a subset of its columns (including defaults)."""
        raw = self._handle.insert_returning(
            table,
            _to_json(row),
            _to_json(list(returning)),
        )
        return json.loads(raw)

    def update(self, table: str, pk: Any, patch: Any) -> dict[str, Any]:
        return json.loads(
            self._handle.update(table, _to_json(pk), _to_json(patch))
        )

    def delete(self, table: str, pk: Any) -> None:
        self._handle.delete(table, _to_json(pk))

    def truncate(self, table: str) -> None:
        self._handle.truncate(table)

    def upsert(
        self,
        table: str,
        row: Any,
        on_conflict: Optional[Any] = None,
        returning: Optional[Iterable[str]] = None,
    ) -> dict[str, Any]:
        conflict_json = (
            json.dumps("do_nothing") if on_conflict is None else _to_json(on_conflict)
        )
        raw = self._handle.upsert(
            table,
            _to_json(row),
            conflict_json,
            _to_json(list(returning) if returning is not None else []),
        )
        return json.loads(raw)

    def update_where(
        self,
        table: str,
        *,
        set: Any,
        filter: Optional[Any] = None,
        returning: Optional[Iterable[str]] = None,
    ) -> list[dict[str, Any]]:
        rows = self._handle.update_where(
            table,
            _to_json(filter) if filter is not None else None,
            _to_json(set),
            _to_json(list(returning) if returning is not None else []),
        )
        return [json.loads(r) for r in rows]

    def delete_where(
        self,
        table: str,
        *,
        filter: Optional[Any] = None,
        returning: Optional[Iterable[str]] = None,
    ) -> list[dict[str, Any]]:
        rows = self._handle.delete_where(
            table,
            _to_json(filter) if filter is not None else None,
            _to_json(list(returning) if returning is not None else []),
        )
        return [json.loads(r) for r in rows]

    def get_by_pk(self, table: str, pk: Any) -> Optional[dict[str, Any]]:
        raw = self._handle.get_by_pk(table, _to_json(pk))
        return json.loads(raw) if raw is not None else None

    def select(
        self,
        table: str,
        filter: Optional[Any] = None,
        order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        columns: Optional[Iterable[str]] = None,
        distinct: bool = False,
        ctes: Optional[Iterable[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Run a SELECT.

        ``filter`` uses the friendly filter shape (see ``parse_filter`` in the
        binding): per-column ``{"col": {"op": value}}`` with ops ``eq``/``ne``/
        ``gt``/``gte``/``lt``/``lte``/``like``/``contains``/``bytes_prefix``/
        ``in``/``not_in``/``is_null``/``is_not_null``/``in_subquery``, plus
        top-level ``and``/``or``/``not``/``exists``/``not_exists``. ``ctes`` is a
        list of ``{"name", "table", "filter"?, ...}`` materialized before the
        body runs; the body's ``table`` may then name a CTE.
        """
        rows = self._handle.select(
            table,
            _to_json(filter) if filter is not None else None,
            order,
            limit,
            offset,
            list(columns) if columns is not None else None,
            distinct,
            _to_json(list(ctes)) if ctes is not None else None,
        )
        return [json.loads(r) for r in rows]

    def aggregate(
        self,
        table: str,
        aggregates: Iterable[Any],
        filter: Optional[Any] = None,
        group_by: Optional[Iterable[str]] = None,
        having: Optional[Any] = None,
    ) -> list[dict[str, Any]]:
        """Run an aggregate / group-by / having query.

        ``aggregates`` is a list of ``{"func", "column"?, "alias"}`` (use the
        :func:`agg` helper); ``func`` is one of ``count``/``sum``/``min``/``max``/
        ``avg``. Returns one row per group (group-key columns plus aliases).
        """
        rows = self._handle.aggregate(
            table,
            _to_json(list(aggregates)),
            _to_json(filter) if filter is not None else None,
            list(group_by) if group_by is not None else None,
            _to_json(having) if having is not None else None,
        )
        return [json.loads(r) for r in rows]

    def join(
        self,
        table: str,
        joins: Iterable[Any],
        alias: Optional[str] = None,
        filter: Optional[Any] = None,
        order_by: Optional[Iterable[Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Run a nested-loop join.

        ``joins`` is a list of ``{"kind", "table", "alias"?, "on"?}`` where
        ``kind`` is ``inner``/``left``/``cross`` and ``on`` is a join predicate
        (use :func:`on_eq`). Each result row is a dict keyed by table alias whose
        values are the matched source rows (``None`` for an unmatched LEFT side).
        """
        query: dict[str, Any] = {"table": table, "joins": list(joins)}
        if alias is not None:
            query["alias"] = alias
        if filter is not None:
            query["filter"] = filter
        if order_by is not None:
            query["order_by"] = list(order_by)
        if limit is not None:
            query["limit"] = limit
        if offset is not None:
            query["offset"] = offset
        rows = self._handle.join(_to_json(query))
        return [json.loads(r) for r in rows]

    def ann_search(
        self,
        table: str,
        column: str,
        query: Iterable[float],
        k: int,
    ) -> list[dict[str, Any]]:
        """Approximate nearest-neighbour search: return the ``k`` rows whose
        ``column`` (an embedding) is closest to ``query``, resolved by the
        column's ANN index. Results are the top-``k`` set (no distance ranking)."""
        rows = self._handle.ann_search(table, column, [float(x) for x in query], k)
        return [json.loads(r) for r in rows]

    def sparse_match(
        self,
        table: str,
        column: str,
        query: Iterable[tuple[int, float]],
        k: int,
    ) -> list[dict[str, Any]]:
        """Learned-sparse (SPLADE) retrieval: return the ``k`` rows whose
        ``column`` (a sparse token vector) best matches the weighted query
        ``(token_id, weight)`` pairs, by sparse dot product."""
        q = [(t, float(w)) for t, w in query]
        rows = self._handle.sparse_match(table, column, q, k)
        return [json.loads(r) for r in rows]

    def commit(self) -> None:
        if not self._closed:
            self._handle.commit()
            self._closed = True

    def rollback(self) -> None:
        if not self._closed:
            self._handle.rollback()
            self._closed = True

    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()


def migrate(db: Database, migrations: Any) -> None:
    db.migrate(migrations)


# ---------------------------------------------------------------------------
# Schema builder helpers
# ---------------------------------------------------------------------------


def table(
    name: str,
    id: int,
    columns: Iterable[Any],
    primary_key: str | list[str],
    indexes: Optional[Iterable[Any]] = None,
    foreign_keys: Optional[Iterable[Any]] = None,
    unique_constraints: Optional[Iterable[Any]] = None,
    check_constraints: Optional[Iterable[Any]] = None,
) -> dict[str, Any]:
    """Build a kit table dictionary."""
    cols = [_col_dict(c) for c in columns]
    return {
        "id": id,
        "name": name,
        "columns": cols,
        "primary_key": [primary_key] if isinstance(primary_key, str) else list(primary_key),
        "indexes": [_index_dict(i) for i in (indexes or [])],
        "foreign_keys": [_fk_dict(f) for f in (foreign_keys or [])],
        "unique_constraints": [_uq_dict(u) for u in (unique_constraints or [])],
        "check_constraints": list(check_constraints or []),
    }


def column(
    name: str,
    id: int,
    storage_type: str,
    *,
    application_type: Optional[str] = None,
    nullable: bool = False,
    primary_key: bool = False,
    default: Any = None,
    generated: bool = False,
    enum_values: Optional[list[str]] = None,
    min: Optional[float] = None,
    max: Optional[float] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    regex: Optional[str] = None,
    check_expr: Optional[str] = None,
) -> dict[str, Any]:
    col: dict[str, Any] = {
        "id": id,
        "name": name,
        "storage_type": storage_type,
        "application_type": application_type or storage_type,
        "nullable": nullable,
        "primary_key": primary_key,
        "generated": generated,
    }
    if default is not None:
        col["default"] = default
    if enum_values is not None:
        col["enum_values"] = enum_values
    if min is not None:
        col["min"] = min
    if max is not None:
        col["max"] = max
    if min_length is not None:
        col["min_length"] = min_length
    if max_length is not None:
        col["max_length"] = max_length
    if regex is not None:
        col["regex"] = regex
    if check_expr is not None:
        col["check_expr"] = check_expr
    return col


def int(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return column(name, id, "int64", **kwargs)


def integer(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return int(name, id, **kwargs)


def text(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return column(name, id, "text", **kwargs)


def bool_(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return column(name, id, "bool", **kwargs)


def boolean(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return bool_(name, id, **kwargs)


def float_(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return column(name, id, "float64", **kwargs)


def float64(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return float_(name, id, **kwargs)


def json_col(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return column(name, id, "json", **kwargs)


def bytes_col(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return column(name, id, "bytes", **kwargs)


def timestamp(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return column(name, id, "timestamp_nanos", **kwargs)


def datetime(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return column(name, id, "date_time", **kwargs)


def date(name: str, id: int, **kwargs: Any) -> dict[str, Any]:
    return column(name, id, "date", **kwargs)


def index(
    name: str,
    columns: str | list[str],
    unique: bool = False,
    kind: str = "bitmap",
) -> dict[str, Any]:
    """Declare a secondary index. ``kind`` is ``bitmap`` (default), ``fm``,
    ``ann``, ``sparse``, or ``minhash`` (set-similarity)."""
    aliases = {"minhash": "min_hash"}
    return {
        "name": name,
        "columns": [columns] if isinstance(columns, str) else list(columns),
        "unique": unique,
        "kind": aliases.get(kind, kind),
    }


def unique(name: str, columns: str | list[str]) -> dict[str, Any]:
    return {
        "name": name,
        "columns": [columns] if isinstance(columns, str) else list(columns),
    }


def fk(
    name: str,
    columns: str | list[str],
    references_table: str,
    references_columns: str | list[str],
    on_delete: str = "restrict",
) -> dict[str, Any]:
    return {
        "name": name,
        "columns": [columns] if isinstance(columns, str) else list(columns),
        "references_table": references_table,
        "references_columns": (
            [references_columns]
            if isinstance(references_columns, str)
            else list(references_columns)
        ),
        "on_delete": on_delete,
    }


def check(name: str, expr: str) -> dict[str, Any]:
    return {"name": name, "expr": expr}


# ---------------------------------------------------------------------------
# Query builder helpers
# ---------------------------------------------------------------------------


def agg(
    func: str,
    alias: str,
    column: Optional[str] = None,
    distinct: bool = False,
) -> dict[str, Any]:
    """Build an aggregate spec, e.g. ``agg("sum", "total_sum", "total")``.

    ``func`` is one of ``count``/``sum``/``min``/``max``/``avg``. ``column`` may
    be omitted for ``count`` (``COUNT(*)``). ``distinct=True`` gives e.g.
    ``COUNT(DISTINCT col)``; it requires a ``column`` and is a no-op for
    ``min``/``max``.
    """
    spec: dict[str, Any] = {"func": func, "alias": alias}
    if column is not None:
        spec["column"] = column
    if distinct:
        spec["distinct"] = True
    return spec


def count_distinct(alias: str, column: str) -> dict[str, Any]:
    """``COUNT(DISTINCT column) AS alias``."""
    return agg("count", alias, column, distinct=True)


def col(name: str) -> dict[str, Any]:
    """A column reference expression, e.g. ``col("u.id")``."""
    return {"column": name}


def on_eq(left: str, right: str) -> dict[str, Any]:
    """A join predicate equating two (usually qualified) columns."""
    return {"eq": [col(left), col(right)]}


# ---------------------------------------------------------------------------
# Key encoding
# ---------------------------------------------------------------------------
#
# ``components`` is a list of typed values, each one of ``{"int": <int>}``,
# ``{"text": <str>}``, or ``{"null": True}``. The encoding is byte-identical to
# the TypeScript and Rust kits, so typed components never collide (the integer
# ``1`` encodes as ``i:1`` and the text ``"1"`` as ``s:1``).


def encode_pk(components: Iterable[Any]) -> str:
    """Encode a primary key value from its typed components."""
    return _encode_pk(json.dumps(list(components)))


def encode_unique_key(version: int, constraint: str, components: Iterable[Any]) -> str:
    """Encode a unique-constraint key (``uq:<version>:<constraint>:<components>``)."""
    return _encode_unique_key(version, constraint, json.dumps(list(components)))


def encode_row_guard_key(table: str, components: Iterable[Any]) -> str:
    """Encode a row-guard key (``rg:<table>:<encoded_pk>``)."""
    return _encode_row_guard_key(table, json.dumps(list(components)))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _to_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _col_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Column):
        return value.to_dict()
    if isinstance(value, dict):
        return value
    raise TypeError(f"expected Column or dict, got {type(value).__name__}")


def _index_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Index):
        return value.to_dict()
    if isinstance(value, dict):
        return value
    raise TypeError(f"expected Index or dict, got {type(value).__name__}")


def _fk_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, ForeignKey):
        return value.to_dict()
    if isinstance(value, dict):
        return value
    raise TypeError(f"expected ForeignKey or dict, got {type(value).__name__}")


def _uq_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, UniqueConstraint):
        return value.to_dict()
    if isinstance(value, dict):
        return value
    raise TypeError(f"expected UniqueConstraint or dict, got {type(value).__name__}")
