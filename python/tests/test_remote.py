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
        self.requests = []
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
                if self.path == "/kit/schema":
                    self._send(200, json.dumps(SCHEMA).encode())
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
