"""Typed remote client for a running ``mongreldb-server`` daemon.

A pure-Python counterpart to the Rust ``RemoteDatabase`` (PLAN.md item #3). It
speaks the daemon's typed ``/kit/txn`` + ``/kit/schema`` endpoints and raises the
same exception hierarchy as the embedded Kit (``DuplicateError`` /
``ForeignKeyError`` / ``ValidationError`` / ``ConflictError``).

Authority is server-side: writes run inside one core transaction on the daemon,
which enforces the engine's declarative constraints atomically. Kit-specific
field validation (enum / min / max / regex / defaults) is the caller's job in
remote mode.

SQL reads return raw Arrow IPC bytes — decode with ``pyarrow.ipc.open_file``.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from .mongreldb_kit_py import (
    ConflictError,
    DuplicateError,
    ForeignKeyError,
    StorageError,
    TriggerValidationError,
    ValidationError,
)

__all__ = ["RemoteDatabase", "RemoteTransaction"]


def _map_error(status: int, body: str) -> Exception:
    try:
        env = json.loads(body)
        code = env.get("error", {}).get("code", "")
        msg = env.get("error", {}).get("message", "remote transaction rejected")
    except Exception:
        return StorageError(f"http {status}: {body}")
    if code == "UNIQUE_VIOLATION":
        return DuplicateError(msg)
    if code == "FK_VIOLATION":
        return ForeignKeyError(msg)
    if code in ("CHECK_VIOLATION", "BAD_REQUEST"):
        return ValidationError(msg)
    if code == "TRIGGER_VALIDATION":
        return TriggerValidationError(msg)
    if code == "CONFLICT":
        return ConflictError(msg)
    return StorageError(f"http {status} ({code}): {msg}")


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class RemoteDatabase:
    """A typed client bound to a ``mongreldb-server`` URL."""

    def __init__(self, url: str) -> None:
        self._base = url.rstrip("/")
        self._schemas: Dict[str, Dict[str, Any]] = {}
        self.refresh()

    # ── schema ────────────────────────────────────────────────────────────

    def refresh(self) -> None:
        """Re-fetch every table's schema metadata from the daemon."""
        data = self._get_json("/kit/schema")
        tables = data.get("tables", {}) or {}
        self._schemas = {name: _build_table(info) for name, info in tables.items()}

    def table_names(self) -> List[str]:
        return list(self._schemas)

    def table(self, name: str) -> Dict[str, Any]:
        try:
            return self._schemas[name]
        except KeyError:
            raise StorageError(f"unknown table {name!r}") from None

    def set_history_retention_epochs(self, epochs: int) -> None:
        """Set the daemon's durable MVCC history-retention window."""
        if epochs < 0:
            raise ValueError("epochs must be non-negative")
        self._put_json("/history/retention", {"history_retention_epochs": epochs})

    def history_retention_epochs(self) -> int:
        """Return the daemon's configured history-retention depth."""
        return int(self._get_json("/history/retention")["history_retention_epochs"])

    def earliest_retained_epoch(self) -> int:
        """Return the oldest epoch retained by the daemon."""
        return int(self._get_json("/history/retention")["earliest_retained_epoch"])

    # ── reads ─────────────────────────────────────────────────────────────

    def sql_arrow(self, sql: str) -> bytes:
        """Run a SQL read; return raw Arrow IPC bytes (decode with pyarrow)."""
        return self._post_bytes("/sql", {"sql": sql})

    def query(
        self,
        table: str,
        conditions: Optional[List[Dict[str, Any]]] = None,
        projection: Optional[List[int]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Native typed query (``POST /kit/query``).

        Returns rows as ``{"row_id": str, "values": {col_name: value}}`` dicts —
        the row-id-returning counterpart to SQL reads. ``conditions`` is a list
        of condition objects mirroring the daemon's variants, e.g.
        ``{"pk": {"value": 2}}`` or
        ``{"range": {"column_id": 2, "lo": 0, "hi": 100}}``.
        """
        payload: Dict[str, Any] = {"table": table}
        if conditions is not None:
            payload["conditions"] = conditions
        if projection is not None:
            payload["projection"] = projection
        if limit is not None:
            payload["limit"] = limit
        env = self._post_json("/kit/query", payload)
        info = self._schemas.get(table)
        rows = []
        for r in env.get("rows", []) or []:
            values = _decode_cells(r.get("cells", []), info)
            rows.append({"row_id": r.get("row_id"), "values": values})
        return rows

    # ── writes ────────────────────────────────────────────────────────────

    def begin(self) -> "RemoteTransaction":
        return RemoteTransaction(self)

    def create_table(self, body: Dict[str, Any]) -> int:
        """Create a constraint-bearing table over HTTP (``POST /kit/create_table``)
        and refresh the local schema cache.

        ``body`` is the full request: ``{"name": ..., "columns": [...],
        "constraints": {"uniques": [...], "foreign_keys": [...],
        "checks": [...]}}``. Returns the assigned table id.
        """
        env = self._post_json("/kit/create_table", body)
        self.refresh()
        return int(env.get("table_id", 0))

    def create_procedure(self, procedure: Dict[str, Any]) -> Dict[str, Any]:
        return self._post_json("/procedures", {"procedure": procedure})

    def drop_procedure(self, name: str) -> None:
        self._open("DELETE", f"/procedures/{name}", body=None).close()

    def call_procedure(self, name: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._post_json(f"/kit/procedures/{name}/call", {"args": args or {}})

    def triggers(self) -> List[Dict[str, Any]]:
        return list((self._get_json("/triggers").get("triggers") or []))

    def trigger(self, name: str) -> Dict[str, Any]:
        return self._get_json(f"/triggers/{name}").get("trigger")

    def create_trigger(self, trigger: Dict[str, Any]) -> Dict[str, Any]:
        return self._post_json("/triggers", {"trigger": trigger})

    def replace_trigger(self, name: str, trigger: Dict[str, Any]) -> Dict[str, Any]:
        data = json.dumps({"trigger": trigger}).encode("utf-8")
        with self._open("PUT", f"/triggers/{name}", body=data) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def drop_trigger(self, name: str) -> None:
        self._open("DELETE", f"/triggers/{name}", body=None).close()

    def create_virtual_table(self, name: str, module: str, args: Optional[List[str]] = None) -> bytes:
        arg_sql = ", ".join(args or [])
        suffix = f"({arg_sql})" if arg_sql else ""
        return self.sql_arrow(
            f"CREATE VIRTUAL TABLE {_quote_ident(name)} USING {_quote_ident(module)}{suffix}"
        )

    def drop_virtual_table(self, name: str) -> bytes:
        return self.sql_arrow(f"DROP TABLE {_quote_ident(name)}")

    # ── HTTP plumbing ─────────────────────────────────────────────────────

    def _get_json(self, path: str) -> Any:
        with self._open("GET", path, body=None) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _post_json(self, path: str, payload: Any) -> Any:
        data = json.dumps(payload).encode("utf-8")
        with self._open("POST", path, body=data) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}

    def _put_json(self, path: str, payload: Any) -> Any:
        data = json.dumps(payload).encode("utf-8")
        with self._open("PUT", path, body=data) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}

    def _post_bytes(self, path: str, payload: Any) -> bytes:
        data = json.dumps(payload).encode("utf-8")
        with self._open("POST", path, body=data) as resp:
            return resp.read()

    def _open(self, method: str, path: str, body: Optional[bytes]):
        url = self._base + path
        req = urllib.request.Request(url, data=body, method=method)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        try:
            return urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            raise _map_error(e.code, e.read().decode("utf-8", "replace")) from None
        except urllib.error.URLError as e:
            raise StorageError(f"transport error: {e}") from None


def _build_table(info: Dict[str, Any]) -> Dict[str, Any]:
    id_to_name: Dict[int, str] = {}
    name_to_id: Dict[str, int] = {}
    primary_key: Optional[int] = None
    for col in info.get("columns", []) or []:
        cid = col["id"]
        name = col["name"]
        id_to_name[cid] = name
        name_to_id[name] = cid
        if col.get("primary_key"):
            primary_key = cid
    return {"id_to_name": id_to_name, "name_to_id": name_to_id, "primary_key": primary_key}


class RemoteTransaction:
    """An in-flight typed atomic batch against the daemon."""

    def __init__(self, db: RemoteDatabase) -> None:
        self._db = db
        self._ops: List[Dict[str, Any]] = []
        self._idempotency_key: Optional[str] = None

    def with_idempotency_key(self, key: str) -> "RemoteTransaction":
        self._idempotency_key = key
        return self

    def insert(
        self, table: str, row: Dict[str, Any], returning: bool = False
    ) -> "RemoteTransaction":
        self._ops.append(
            {"put": {"table": table, "cells": _cells(self._db, table, row), "returning": returning}}
        )
        return self

    def upsert(
        self,
        table: str,
        row: Dict[str, Any],
        update: Optional[Dict[str, Any]] = None,
    ) -> "RemoteTransaction":
        update_cells = _cells(self._db, table, update) if update else None
        self._ops.append(
            {
                "upsert": {
                    "table": table,
                    "cells": _cells(self._db, table, row),
                    "update_cells": update_cells,
                    "returning": True,
                }
            }
        )
        return self

    def delete_by_pk(self, table: str, pk: Any) -> "RemoteTransaction":
        info = self._db.table(table)
        if info["primary_key"] is None:
            raise ValidationError(f"table {table!r} has no primary key")
        self._ops.append({"delete_by_pk": {"table": table, "pk": pk}})
        return self

    def commit(self) -> Dict[str, Any]:
        """Commit the batch atomically; return the daemon's typed response.

        Per-op ``row`` post-images (when ``returning`` was set) are decoded from
        ``[col_id, value, …]`` into name-keyed dicts.
        """
        req: Dict[str, Any] = {"ops": self._ops}
        if self._idempotency_key is not None:
            req["idempotency_key"] = self._idempotency_key
        resp = self._db._post_json("/kit/txn", req)
        results = resp.get("results", []) or []
        decoded = []
        for r in results:
            kind = r.get("kind")
            if kind in ("put", "upsert") and r.get("row") is not None:
                r = dict(r)
                r["row"] = _decode_row(self._db, _table_for_op(self._ops, len(decoded)), r["row"])
            decoded.append(r)
        resp["results"] = decoded
        return resp


def _cells(db: RemoteDatabase, table: str, row: Dict[str, Any]) -> List[Any]:
    info = db.table(table)
    name_to_id = info["name_to_id"]
    out: List[Any] = []
    for name, val in row.items():
        cid = name_to_id.get(name)
        if cid is None:
            raise ValidationError(f"unknown column {name!r} in table {table!r}")
        out.append(cid)
        out.append(val)
    return out


def _table_for_op(ops: List[Dict[str, Any]], index: int) -> str:
    op = ops[index]
    body = next(iter(op.values()))
    return body["table"]


def _decode_row(db: RemoteDatabase, table: str, row: List[Any]) -> Dict[str, Any]:
    info = db.table(table)
    return _decode_cells(row, info)


def _decode_cells(cells: List[Any], info: Dict[str, Any]) -> Dict[str, Any]:
    id_to_name = info["id_to_name"]
    out: Dict[str, Any] = {}
    i = 0
    while i + 1 < len(cells):
        cid = cells[i]
        name = id_to_name.get(cid)
        if name is not None:
            out[name] = cells[i + 1]
        i += 2
    return out
