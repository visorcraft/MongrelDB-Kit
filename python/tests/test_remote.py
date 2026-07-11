"""Unit tests for the pure-Python RemoteDatabase facade.

Uses an in-process stub HTTP server so no real mongreldb-server binary is
required. Requires the native extension built (`maturin develop`) because the
facade raises the shared exception classes.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from mongreldb_kit import (
    ConflictError,
    DuplicateError,
    ForeignKeyError,
    RemoteDatabase,
    StorageError,
    ValidationError,
)

SCHEMA = {
    "tables": {
        "users": {
            "columns": [
                {"id": 0, "name": "id", "primary_key": True, "nullable": False, "auto_increment": True},
                {"id": 1, "name": "email", "primary_key": False, "nullable": True, "auto_increment": False},
                {"id": 2, "name": "age", "primary_key": False, "nullable": True, "auto_increment": False},
            ],
        }
    }
}


class Stub:
    """A scriptable stub daemon. ``kit_txn`` returns the next canned response."""

    def __init__(self):
        self.kit_txn_responses = []  # list of (status, body)
        # Per-path canned POST responses: path → list of (status, body).
        self.canned = {}
        # Per-(method, path) canned error responses: (method, path) → (status, body).
        self.errors = {}
        self.requests = []
        self.history = {"history_retention_epochs": 1, "earliest_retained_epoch": 0}
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, status, body_bytes, content_type="application/json"):
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body_bytes)))
                self.end_headers()
                self.wfile.write(body_bytes)

            def do_GET(self):
                outer.requests.append(("GET", self.path, None))
                if ("GET", self.path) in outer.errors:
                    status, body = outer.errors[("GET", self.path)]
                    self._send(status, json.dumps(body).encode())
                    return
                if self.path == "/kit/schema":
                    self._send(200, json.dumps(SCHEMA).encode())
                elif self.path == "/history/retention":
                    self._send(200, json.dumps(outer.history).encode())
                else:
                    self._send(404, b"not found")

            def do_PUT(self):
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else {}
                outer.requests.append(("PUT", self.path, body))
                if ("PUT", self.path) in outer.errors:
                    status, body = outer.errors[("PUT", self.path)]
                    self._send(status, json.dumps(body).encode())
                    return
                if self.path == "/history/retention":
                    outer.history["history_retention_epochs"] = body["history_retention_epochs"]
                    self._send(200, json.dumps(outer.history).encode())
                else:
                    self._send(404, b"not found")

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else {}
                outer.requests.append(("POST", self.path, body))
                if self.path == "/kit/txn":
                    if outer.kit_txn_responses:
                        status, resp = outer.kit_txn_responses.pop(0)
                    else:
                        status, resp = 200, {"status": "committed", "epoch": 7, "results": []}
                    self._send(status, json.dumps(resp).encode())
                elif self.path in outer.canned:
                    queue = outer.canned[self.path]
                    status, resp = queue.pop(0) if queue else (200, {})
                    self._send(status, json.dumps(resp).encode())
                else:
                    self._send(404, b"not found")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=2)


@pytest.fixture
def stub():
    s = Stub()
    yield s
    s.stop()


def test_connect_loads_schema(stub):
    db = RemoteDatabase(stub.url())
    assert "users" in db.table_names()
    assert db.table("users")["primary_key"] == 0


def test_history_retention_round_trip(stub):
    db = RemoteDatabase(stub.url())
    assert db.history_retention_epochs() == 1
    assert db.earliest_retained_epoch() == 0
    db.set_history_retention_epochs(100)
    assert db.history_retention_epochs() == 100

    get_reqs = [r for r in stub.requests if r[0] == "GET" and r[1] == "/history/retention"]
    put_reqs = [r for r in stub.requests if r[0] == "PUT" and r[1] == "/history/retention"]
    assert len(put_reqs) == 1
    assert put_reqs[0][2] == {"history_retention_epochs": 100}
    assert len(get_reqs) == 3


def test_history_retention_error_propagation(stub):
    stub.errors[("GET", "/history/retention")] = (
        503,
        {"error": {"code": "STORAGE_ERROR", "message": "unavailable"}},
    )
    stub.errors[("PUT", "/history/retention")] = (
        503,
        {"error": {"code": "STORAGE_ERROR", "message": "unavailable"}},
    )
    db = RemoteDatabase(stub.url())
    with pytest.raises(StorageError):
        db.history_retention_epochs()
    with pytest.raises(StorageError):
        db.earliest_retained_epoch()
    with pytest.raises(StorageError):
        db.set_history_retention_epochs(50)


def test_insert_batch_decodes_returning_row(stub):
    stub.kit_txn_responses.append(
        (
            200,
            {
                "status": "committed",
                "epoch": 9,
                "results": [
                    {"kind": "put", "row_id": None, "auto_inc": 1, "row": [0, 1, 1, "a@x", 2, 30]}
                ],
            },
        )
    )
    db = RemoteDatabase(stub.url())
    txn = db.begin()
    txn.insert("users", {"email": "a@x", "age": 30}, returning=True)
    resp = txn.commit()
    assert resp["status"] == "committed"
    assert resp["results"][0]["row"] == {"id": 1, "email": "a@x", "age": 30}

    # The request carried flat [col_id, val, …] cells.
    posted = stub.requests[-1][2]
    assert posted["ops"][0]["put"]["cells"] == [1, "a@x", 2, 30]


def test_unknown_column_rejected_client_side(stub):
    db = RemoteDatabase(stub.url())
    txn = db.begin()
    with pytest.raises(ValidationError):
        txn.insert("users", {"nope": 1})
    # Nothing committed because commit wasn't reached.
    assert all(r[1] != "/kit/txn" for r in stub.requests)


def test_unique_violation_maps_to_duplicate(stub):
    stub.kit_txn_responses.append(
        (
            409,
            {
                "status": "aborted",
                "error": {
                    "code": "UNIQUE_VIOLATION",
                    "message": "users_email_unique violated",
                },
            },
        )
    )
    db = RemoteDatabase(stub.url())
    with pytest.raises(DuplicateError):
        db.begin().insert("users", {"email": "dup"}).commit()


def test_fk_violation_maps(stub):
    stub.kit_txn_responses.append(
        (
            409,
            {"status": "aborted", "error": {"code": "FK_VIOLATION", "message": "no parent"}},
        )
    )
    db = RemoteDatabase(stub.url())
    with pytest.raises(ForeignKeyError):
        db.begin().insert("users", {"age": 1}).commit()


def test_conflict_maps(stub):
    stub.kit_txn_responses.append(
        (
            409,
            {"status": "aborted", "error": {"code": "CONFLICT", "message": "write-write"}},
        )
    )
    db = RemoteDatabase(stub.url())
    with pytest.raises(ConflictError):
        db.begin().insert("users", {"age": 1}).commit()


def test_idempotency_key_forwarded(stub):
    stub.kit_txn_responses.append((200, {"status": "committed", "epoch": 1, "results": []}))
    db = RemoteDatabase(stub.url())
    db.begin().with_idempotency_key("k1").insert("users", {"age": 1}).commit()
    posted = stub.requests[-1][2]
    assert posted["idempotency_key"] == "k1"


def test_query_decodes_rows(stub):
    stub.canned["/kit/query"] = [
        (
            200,
            {
                "rows": [
                    {"row_id": "42", "cells": [0, 1, 1, "a@x", 2, 30]},
                ],
                "truncated": False,
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    rows = db.query("users", [{"pk": {"value": 1}}], projection=[0, 2])
    # The stub returns the full row (projection is honored server-side), so all
    # three columns decode.
    assert rows == [{"row_id": "42", "values": {"id": 1, "email": "a@x", "age": 30}}]
    posted = stub.requests[-1][2]
    assert posted["projection"] == [0, 2]
    assert posted["conditions"] == [{"pk": {"value": 1}}]


def test_create_table_forwards_body_and_returns_id(stub):
    stub.canned["/kit/create_table"] = [(200, {"table_id": 7})]
    db = RemoteDatabase(stub.url())
    tid = db.create_table(
        {
            "name": "accounts",
            "columns": [
                {"id": 0, "name": "id", "ty": "int64", "primary_key": True},
                {
                    "id": 1,
                    "name": "role",
                    "ty": "enum",
                    "enum_variants": ["user", "admin"],
                },
                {
                    "id": 2,
                    "name": "created_at",
                    "ty": "timestamp",
                    "default_expr": "now",
                },
                {"id": 3, "name": "label", "ty": "varchar", "default_value": "draft"},
                {"id": 4, "name": "count", "ty": "int64", "default_value": 7},
                {"id": 5, "name": "enabled", "ty": "bool", "default_value": True},
                {"id": 6, "name": "note", "ty": "varchar", "nullable": True, "default_value": None},
                {"id": 7, "name": "literal_now", "ty": "varchar", "default_value": "now"},
            ],
            "constraints": {
                "checks": [
                    {
                        "id": 1,
                        "name": "id_positive",
                        "expr": {"Gt": [{"Col": 0}, {"Lit": {"Int64": 0}}]},
                    }
                ]
            },
        }
    )
    assert tid == 7
    # The create POST is followed by a refresh GET; locate the POST explicitly.
    create_req = next(
        r for r in stub.requests if r[0] == "POST" and r[1] == "/kit/create_table"
    )
    posted = create_req[2]
    assert posted["name"] == "accounts"
    assert posted["columns"][0]["primary_key"] is True
    assert posted["columns"][1]["enum_variants"] == ["user", "admin"]
    assert posted["columns"][2]["default_expr"] == "now"
    assert "default_value" not in posted["columns"][2]
    assert posted["columns"][3]["default_value"] == "draft"
    assert posted["columns"][4]["default_value"] == 7
    assert posted["columns"][5]["default_value"] is True
    assert "default_value" in posted["columns"][6]
    assert posted["columns"][6]["default_value"] is None
    assert "default_expr" not in posted["columns"][6]
    assert posted["columns"][7]["default_value"] == "now"
    assert "default_expr" not in posted["columns"][7]
    assert posted["constraints"]["checks"][0]["name"] == "id_positive"
    # After create the facade refreshes /kit/schema (a GET).
    assert any(r[0] == "GET" and r[1] == "/kit/schema" for r in stub.requests)
