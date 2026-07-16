"""Unit tests for the pure-Python RemoteDatabase facade.

Uses an in-process stub HTTP server so no real mongreldb-server binary is
required. Requires the native extension built (`maturin develop`) because the
facade raises the shared exception classes.
"""

import copy
import json
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from mongreldb_kit import (
    CommitOutcomeError,
    ConflictError,
    DuplicateError,
    ForeignKeyError,
    RemoteDatabase,
    RemoteSqlQueryHandle,
    StorageError,
    ValidationError,
    QueryCancelledError,
    QueryTimeoutError,
    QueryOutcomeUnknownError,
    CapabilityUnsupportedError,
    ResultLimitExceededError,
    SerializationError,
)
from mongreldb_kit.remote import (
    _MalformedHttpResponse,
    _is_query_not_found,
    _is_query_status,
    _is_sql_cursor_error_envelope,
    _is_sql_error_envelope,
    _is_sql_page,
    _is_sql_write_receipt,
    _strict_json_loads,
    _validate_cancel_response,
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


def query_not_found_response(query_id):
    nullable = {
        "committed": None,
        "committed_statements": None,
        "last_commit_epoch": None,
        "last_commit_epoch_text": None,
        "first_commit_statement_index": None,
        "last_commit_statement_index": None,
        "completed_statements": None,
        "statement_index": None,
    }
    return {
        "query_id": query_id,
        "status": "unknown",
        "terminal_state": None,
        **nullable,
        "cancel_outcome": "not_found",
        "cancellation_reason": None,
        "retryable": False,
        "server_state": "not_found",
        "outcome": {**nullable, "serialization": "unknown"},
        "error": {
            "code": "QUERY_NOT_FOUND",
            "message": "query not found",
            "query_id": query_id,
            "committed": None,
            "retryable": False,
        },
    }


def sql_cursor_error_response():
    outcome = {
        "committed": False,
        "committed_statements": 0,
        "last_commit_epoch": None,
        "last_commit_epoch_text": None,
        "first_commit_statement_index": None,
        "last_commit_statement_index": None,
        "completed_statements": 0,
        "statement_index": 0,
        "serialization": "not_started",
    }
    return {
        "status": "failed_before_commit",
        "terminal_state": "failed_before_commit",
        "server_state": "failed",
        **{key: value for key, value in outcome.items() if key != "serialization"},
        "cancel_outcome": None,
        "cancellation_reason": None,
        "retryable": False,
        "outcome": outcome,
        "error": {
            "code": "SQL_CURSOR_NOT_FOUND",
            "message": "cursor missing",
            "committed": False,
            "retryable": False,
        },
    }


def sql_write_receipt(query_id, *, original_query_id=None, replayed=False, epoch=17):
    original_query_id = original_query_id or query_id
    outcome = {
        "committed": True,
        "committed_statements": 1,
        "last_commit_epoch": epoch,
        "last_commit_epoch_text": str(epoch),
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "serialization": "succeeded",
    }
    return {
        "query_id": query_id,
        "original_query_id": original_query_id,
        "status": "committed",
        "committed": True,
        "committed_statements": 1,
        "last_commit_epoch": epoch,
        "last_commit_epoch_text": str(epoch),
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "retryable": False,
        "idempotency_replayed": replayed,
        "idempotency_persisted": True,
        "idempotency_expires_at_ms": 999,
        "outcome": outcome,
        "terminal_error": None,
    }


def completed_query_status(query_id):
    outcome = {
        "committed": False,
        "committed_statements": 0,
        "last_commit_epoch": None,
        "last_commit_epoch_text": None,
        "first_commit_statement_index": None,
        "last_commit_statement_index": None,
        "completed_statements": 1,
        "statement_index": 0,
        "serialization": "succeeded",
    }
    return {
        "query_id": query_id,
        "status": "completed",
        "terminal_state": "completed",
        "state": "completed",
        "server_state": "completed",
        **{key: value for key, value in outcome.items() if key != "serialization"},
        "cancel_outcome": "already_finished",
        "cancellation_reason": "none",
        "retryable": False,
        "outcome": outcome,
        "terminal_error": None,
    }


class Stub:
    """A scriptable stub daemon. ``kit_txn`` returns the next canned response."""

    def __init__(self):
        self.kit_txn_responses = []  # list of (status, body)
        # Per-path canned POST responses: path → list of (status, body).
        self.canned = {}
        # Per-path raw POST responses: path → list of (status, bytes).
        self.raw_canned = {}
        # Per-path canned GET responses: path → list of (status, body).
        self.canned_get = {}
        # Per-(method, path) canned error responses: (method, path) → (status, body).
        self.errors = {}
        self.requests = []
        self.authorizations = []
        self.omit_query_id_header = False
        self.response_query_id_header = None
        self.pre_cancel_on_cancel = False
        self.pre_cancelled = set()
        self.history = {"history_retention_epochs": 1, "earliest_retained_epoch": 0}
        self.capabilities = {
            "sql_cancellation": {
                "version": 2,
                "client_query_ids": True,
                "cancel_endpoint": True,
                "query_status": True,
                "stream_disconnect_cancels": True,
                "pre_registration_cancel": True,
            },
            "sql_pagination": {
                "version": 1,
                "continuation_endpoint": "/sql/continue",
                "retained_snapshot": True,
                "projection_required": True,
                "byte_and_token_hints": True,
            },
            "sql_idempotency": {
                "version": 1,
                "durable_pre_execution_intent": True,
                "replay_committed_receipt": True,
                "indeterminate_never_reexecutes": True,
            },
        }
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, status, body_bytes, content_type="application/json"):
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                if self.path in ("/sql", "/sql/continue") and not outer.omit_query_id_header:
                    query_id = outer.response_query_id_header or getattr(
                        self, "request_query_id", None
                    )
                    if query_id is not None:
                        self.send_header("x-mongreldb-query-id", query_id)
                self.send_header("Content-Length", str(len(body_bytes)))
                self.end_headers()
                try:
                    self.wfile.write(body_bytes)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def do_GET(self):
                outer.authorizations.append(self.headers.get("Authorization"))
                outer.requests.append(("GET", self.path, None))
                if self.path in outer.canned_get:
                    queue = outer.canned_get[self.path]
                    status, body = queue.pop(0)
                    self._send(status, json.dumps(body).encode())
                    return
                query_id = self.path.removeprefix("/queries/")
                if query_id in outer.pre_cancelled:
                    self._send(
                        200,
                        json.dumps(
                            {
                                "query_id": query_id,
                                "status": "cancelled_before_start",
                                "terminal_state": "cancelled_before_start",
                                "state": "pre_cancelled",
                                "server_state": "pre_cancelled",
                                "committed": False,
                                "committed_statements": 0,
                                "completed_statements": 0,
                                "statement_index": 0,
                                "cancel_outcome": "pre_cancelled",
                                "cancellation_reason": "client_request",
                                "retryable": False,
                                "outcome": {
                                    "committed": False,
                                    "committed_statements": 0,
                                    "completed_statements": 0,
                                    "statement_index": 0,
                                    "serialization": "not_started",
                                },
                                "terminal_error": {
                                    "code": "QUERY_CANCELLED",
                                    "category": "cancellation",
                                },
                            }
                        ).encode(),
                    )
                    return
                if ("GET", self.path) in outer.errors:
                    status, body = outer.errors[("GET", self.path)]
                    self._send(status, json.dumps(body).encode())
                    return
                if self.path == "/kit/schema":
                    self._send(200, json.dumps(SCHEMA).encode())
                elif self.path == "/capabilities":
                    self._send(
                        200,
                        json.dumps(outer.capabilities).encode(),
                    )
                elif self.path == "/history/retention":
                    self._send(200, json.dumps(outer.history).encode())
                else:
                    self._send(404, b"not found")

            def do_PUT(self):
                outer.authorizations.append(self.headers.get("Authorization"))
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
                outer.authorizations.append(self.headers.get("Authorization"))
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else {}
                self.request_query_id = body.get("query_id") or body.get("operation_id")
                outer.requests.append(("POST", self.path, body))
                if self.path in outer.raw_canned:
                    queue = outer.raw_canned[self.path]
                    status, response = queue.pop(0)
                    self._send(status, response)
                elif self.path == "/kit/txn":
                    if outer.kit_txn_responses:
                        status, resp = outer.kit_txn_responses.pop(0)
                    else:
                        results = []
                        for op in body.get("ops", []):
                            kind, request = next(iter(op.items()))
                            if kind == "put":
                                results.append({"kind": "put", "row_id": None, "auto_inc": None, "row": None if not request.get("returning") else request["cells"]})
                            elif kind == "upsert":
                                results.append({"kind": "upsert", "action": "inserted", "auto_inc": None, "row": request["cells"] if request.get("returning") else None})
                            else:
                                results.append({"kind": "not_found"})
                        status, resp = 200, {"status": "committed", "epoch": 7, "epoch_text": "7", "results": results}
                    self._send(status, json.dumps(resp).encode())
                elif self.path == "/sql":
                    if self.path in outer.canned:
                        queue = outer.canned[self.path]
                        status, resp = queue.pop(0) if queue else (200, {})
                        self._send(status, json.dumps(resp).encode())
                    elif body.get("sql") == "SLOW_TRANSPORT":
                        time.sleep(0.2)
                        try:
                            self._send(200, b"", "application/octet-stream")
                        except BrokenPipeError:
                            pass
                    elif body.get("sql") == "TIMEOUT":
                        self._send(
                            504,
                            json.dumps(
                                {
                                    "query_id": body.get("query_id"),
                                    "status": "deadline_before_commit",
                                    "terminal_state": "deadline_before_commit",
                                    "committed": False,
                                    "committed_statements": 0,
                                    "last_commit_epoch": None,
                                    "last_commit_epoch_text": None,
                                    "first_commit_statement_index": None,
                                    "last_commit_statement_index": None,
                                    "completed_statements": 0,
                                    "statement_index": 0,
                                    "cancel_outcome": "accepted",
                                    "cancellation_reason": "deadline",
                                    "retryable": False,
                                    "server_state": "cancelled",
                                    "outcome": {
                                        "committed": False,
                                        "committed_statements": 0,
                                        "last_commit_epoch": None,
                                        "last_commit_epoch_text": None,
                                        "first_commit_statement_index": None,
                                        "last_commit_statement_index": None,
                                        "completed_statements": 0,
                                        "statement_index": 0,
                                        "serialization": "not_started",
                                    },
                                    "error": {
                                        "code": "DEADLINE_EXCEEDED",
                                        "message": "timed out",
                                        "query_id": body.get("query_id"),
                                        "committed": False,
                                        "retryable": False,
                                    }
                                }
                            ).encode(),
                        )
                    else:
                        self._send(200, b"", "application/octet-stream")
                elif self.path.endswith("/cancel"):
                    query_id = self.path.split("/")[2]
                    if outer.pre_cancel_on_cancel:
                        outer.pre_cancelled.add(query_id)
                        response = {
                            "query_id": query_id,
                            "state": "pre_cancelled",
                            "cancel_outcome": "pre_cancelled",
                            "terminal_error": {
                                "code": "QUERY_CANCELLED",
                                "category": "cancellation",
                            },
                        }
                    else:
                        response = {
                            "query_id": query_id,
                            "state": "cancellation_requested",
                            "cancel_outcome": "accepted",
                        }
                    self._send(202, json.dumps(response).encode())
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
        self.server.server_close()
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


@pytest.mark.parametrize("url", ["ftp://example.com", "example.com", "http:///"])
def test_remote_rejects_non_http_or_hostless_urls(url):
    with pytest.raises(ValueError, match="http:// or https://"):
        RemoteDatabase(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://alice:secret@example.com",
        "http://example.com?token=secret",
        "http://example.com#token",
    ],
)
def test_remote_rejects_credentials_query_and_fragment(url):
    with pytest.raises(ValueError):
        RemoteDatabase(url)


def test_remote_rejects_noncanonical_query_ids_before_route(stub):
    db = RemoteDatabase(stub.url())
    request_count = len(stub.requests)
    with pytest.raises(ValueError, match="32 hexadecimal characters"):
        db.cancel_sql("../../capabilities")
    with pytest.raises(ValueError, match="32 hexadecimal characters"):
        db.sql_arrow("SELECT 1", query_id="not-a-query-id")
    assert len(stub.requests) == request_count


def test_remote_auth_is_sent_on_capabilities_schema_and_data_routes(stub):
    bearer = RemoteDatabase(stub.url(), bearer_token="secret")
    bearer.history_retention_epochs()
    assert stub.authorizations[-3:] == ["Bearer secret"] * 3

    basic = RemoteDatabase(stub.url(), username="alice", password="s3cret")
    basic.history_retention_epochs()
    assert stub.authorizations[-3:] == ["Basic YWxpY2U6czNjcmV0"] * 3


def test_remote_repr_redacts_credentials(stub):
    for remote, secret in (
        (
            RemoteDatabase(stub.url(), bearer_token="bearer-inspection-secret"),
            "bearer-inspection-secret",
        ),
        (
            RemoteDatabase(
                stub.url(), username="alice", password="basic-inspection-secret"
            ),
            "basic-inspection-secret",
        ),
    ):
        rendered = repr(remote)
        assert secret not in rendered
        assert "Authorization" not in rendered
        assert "auth='configured'" in rendered


@pytest.mark.parametrize(
    "options",
    [
        {"bearer_token": ""},
        {"bearer_token": "secret\r\ninjected"},
        {"username": "", "password": "secret"},
        {"username": "alice:admin", "password": "secret"},
        {"username": "alice", "password": "secret\ninjected"},
        {"transport_timeout": float("nan")},
    ],
)
def test_remote_rejects_ambiguous_auth_and_timeout_options(options):
    with pytest.raises(ValueError):
        RemoteDatabase("https://example.test", **options)


def test_capabilities_reject_unknown_and_unsafe_fields(stub):
    original = copy.deepcopy(stub.capabilities)
    stub.capabilities["sql_cancellation"]["unexpected"] = True
    with pytest.raises(StorageError, match="capability response"):
        RemoteDatabase(stub.url())
    stub.capabilities = copy.deepcopy(original)
    stub.capabilities["sql_cancellation"]["version"] = 1.5
    with pytest.raises(StorageError, match="capability response"):
        RemoteDatabase(stub.url())
    stub.capabilities = copy.deepcopy(original)
    stub.capabilities["sql_cancellation"]["version"] = 2**64
    with pytest.raises(StorageError, match="capability response"):
        RemoteDatabase(stub.url())


def test_control_responses_are_size_bounded(stub):
    query_id = "11112222333344445555666677778888"
    db = RemoteDatabase(stub.url())
    stub.canned_get[f"/queries/{query_id}"] = [
        (200, {"padding": "x" * (1024 * 1024)})
    ]
    with pytest.raises(StorageError, match="exceeded 1048576 bytes"):
        db.query_status(query_id)


def test_sql_control_body_timeout_mapping_and_cancel(stub):
    db = RemoteDatabase(stub.url())
    query_id = "11112222333344445555666677778888"
    with pytest.raises(QueryTimeoutError):
        db.sql_arrow("TIMEOUT", timeout_ms=250, query_id=query_id, transport_timeout=2.0)
    request = next(r for r in stub.requests if r[0] == "POST" and r[1] == "/sql")
    assert request[2] == {
        "sql": "TIMEOUT",
        "format": "arrow",
        "query_id": query_id,
        "timeout_ms": 250,
    }
    assert db.cancel_sql(query_id)["state"] == "cancellation_requested"


def test_sql_after_commit_error_keeps_exact_code_and_terminal_metadata(stub):
    query_id = "11112222333344445555666677778888"
    stub.canned["/sql"] = [
        (
            409,
            {
                "query_id": query_id,
                "status": "cancelled_after_commit",
                "terminal_state": "cancelled_after_commit",
                "committed": True,
                "committed_statements": 1,
                "last_commit_epoch": 42,
                "last_commit_epoch_text": "42",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "error": {
                    "code": "QUERY_CANCELLED_AFTER_COMMIT",
                    "message": "cancel observed after commit",
                    "query_id": query_id,
                    "committed": True,
                    "retryable": False,
                },
                "outcome": {
                    "committed": True,
                    "committed_statements": 1,
                    "last_commit_epoch": 42,
                    "last_commit_epoch_text": "42",
                    "first_commit_statement_index": 0,
                    "last_commit_statement_index": 0,
                    "completed_statements": 1,
                    "statement_index": 0,
                    "serialization": "failed",
                },
                "cancel_outcome": "accepted",
                "cancellation_reason": "client_request",
                "retryable": False,
                "server_state": "cancelled",
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    with pytest.raises(QueryCancelledError) as caught:
        db.sql_arrow("INSERT INTO users VALUES (1)", query_id=query_id)
    assert caught.value.code == "QUERY_CANCELLED_AFTER_COMMIT"
    assert caught.value.committed is True
    assert caught.value.last_commit_epoch == 42
    assert caught.value.first_commit_statement_index == 0
    assert caught.value.last_commit_statement_index == 0
    assert caught.value.cancel_outcome == "accepted"
    assert caught.value.cancellation_reason == "client_request"
    assert caught.value.server_state == "cancelled"
    assert caught.value.retryable is False


def test_query_status_preserves_query_not_found(stub):
    query_id = "99990000111122223333444455556666"
    stub.errors[("GET", f"/queries/{query_id}")] = (
        404,
        {
            "error": {
                "code": "QUERY_NOT_FOUND",
                "message": "not retained",
                "query_id": query_id,
            },
            "committed": False,
            "committed_statements": 0,
            "completed_statements": 0,
            "statement_index": 0,
            "retryable": True,
            "server_state": "not_found",
        },
    )
    db = RemoteDatabase(stub.url())
    with pytest.raises(StorageError) as caught:
        db.query_status(query_id)
    assert caught.value.code == "QUERY_NOT_FOUND"
    assert caught.value.committed is None
    assert caught.value.committed_statements is None
    assert caught.value.completed_statements is None
    assert caught.value.statement_index is None
    assert caught.value.retryable is False
    assert caught.value.server_state == "not_found"


def test_cancel_sql_preserves_structured_conflict_body(stub):
    query_id = "aaaabbbbccccddddeeeeffff00001111"
    body = {
        "query_id": query_id,
        "state": "commit_critical",
        "server_state": "commit_critical",
        "cancel_outcome": "too_late",
        "cancellation_reason": "none",
        "committed": False,
        "retryable": False,
    }
    stub.raw_canned[f"/queries/{query_id}/cancel"] = [
        (409, json.dumps(body).encode()),
        (
            409,
            json.dumps(
                {
                    "query_id": query_id,
                    "state": "cancellation_requested",
                    "cancel_outcome": "accepted",
                    "error": {"code": "CANCEL_TOO_LATE"},
                }
            ).encode(),
        ),
        (
            200,
            json.dumps(
                {
                    "query_id": query_id,
                    "state": "finished",
                    "cancel_outcome": "already_finished",
                }
            ).encode(),
        ),
    ]
    db = RemoteDatabase(stub.url())
    assert db.cancel_sql(query_id) == body
    with pytest.raises(StorageError):
        db.cancel_sql(query_id)
    assert db.cancel_sql(query_id)["cancel_outcome"] == "already_finished"


def test_missing_sql_cancellation_uses_capability_error(stub):
    stub.capabilities.pop("sql_cancellation")
    db = RemoteDatabase(stub.url())
    with pytest.raises(CapabilityUnsupportedError):
        db.start_sql_arrow("SELECT 1")


def test_sql_pagination_request_continuation_and_auth(stub):
    stub.canned["/sql"] = [
        (
            200,
            {
                "status": "completed",
                "rows": [{"id": 1}],
                "next_cursor": "cursor-1",
                "page": {
                    "offset": 0,
                    "row_count": 1,
                    "total_rows": 2,
                    "byte_count": 10,
                    "estimated_tokens": 3,
                    "limits": {"rows": 1, "bytes": 1024, "tokens": 256},
                    "projection": ["id"],
                    "expires_at_ms": 999,
                    "snapshot": "retained_result",
                    "token_estimate": "ceil(projected_json_bytes/4)",
                },
            },
        )
    ]
    stub.canned["/sql/continue"] = [
        (
            200,
            {
                "status": "completed",
                "rows": [{"id": 2}],
                "next_cursor": None,
                "page": {
                    "offset": 1,
                    "row_count": 1,
                    "total_rows": 2,
                    "byte_count": 10,
                    "estimated_tokens": 3,
                    "limits": {"rows": 1, "bytes": 1024, "tokens": 256},
                    "projection": ["id"],
                    "expires_at_ms": 999,
                    "snapshot": "retained_result",
                    "token_estimate": "ceil(projected_json_bytes/4)",
                },
            },
        )
    ]
    db = RemoteDatabase(stub.url(), bearer_token="secret")
    first = db.sql_page(
        "SELECT id FROM users",
        projection=["id"],
        page_size_rows=1,
        query_id="1234567890abcdef1234567890abcdef",
        timeout_ms=250,
        max_page_bytes=1024,
        max_page_tokens=256,
    )
    assert first["rows"] == [{"id": 1}]
    assert db.continue_sql_page(first["next_cursor"])["rows"] == [{"id": 2}]
    sql_request = next(r for r in stub.requests if r[0] == "POST" and r[1] == "/sql")
    assert sql_request[2]["pagination"] == {
        "page_size_rows": 1,
        "projection": ["id"],
        "max_page_bytes": 1024,
        "max_page_tokens": 256,
    }
    continuation = next(
        i for i, request in enumerate(stub.requests) if request[1] == "/sql/continue"
    )
    assert stub.authorizations[continuation] == "Bearer secret"


def test_idempotent_sql_receipt_preserves_large_epoch(stub):
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    stub.canned["/sql"] = [
        (
            200,
            {
                "query_id": query_id,
                "original_query_id": query_id,
                "status": "committed",
                "committed": True,
                "committed_statements": 1,
                "last_commit_epoch": None,
                "last_commit_epoch_text": "9007199254740993",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "retryable": False,
                "idempotency_replayed": False,
                "idempotency_persisted": True,
                "idempotency_expires_at_ms": 999,
                "outcome": {
                    "committed": True,
                    "committed_statements": 1,
                    "last_commit_epoch": None,
                    "last_commit_epoch_text": "9007199254740993",
                    "first_commit_statement_index": 0,
                    "last_commit_statement_index": 0,
                    "completed_statements": 1,
                    "statement_index": 0,
                    "serialization": "succeeded",
                },
                "terminal_error": None,
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    receipt = db.execute_idempotent_sql(
        "INSERT INTO users (id) VALUES (1)",
        idempotency_key="insert-one",
        query_id=query_id,
        max_output_rows=1,
        max_output_bytes=1024,
    )
    assert receipt["last_commit_epoch"] == 9007199254740993
    assert receipt["outcome"]["last_commit_epoch"] == 9007199254740993
    request = next(r for r in stub.requests if r[0] == "POST" and r[1] == "/sql")
    assert request[2]["idempotency_key"] == "insert-one"


@pytest.mark.parametrize("header", [None, "99990000111122223333444455556666"])
def test_sql_arrow_rejects_missing_or_wrong_query_id_header(stub, header):
    query_id = "1234567890abcdef1234567890abcdef"
    stub.omit_query_id_header = header is None
    stub.response_query_id_header = header
    stub.canned_get[f"/queries/{query_id}"] = [
        (200, completed_query_status(query_id))
    ]
    with pytest.raises(SerializationError):
        RemoteDatabase(stub.url()).sql_arrow("SELECT 1", query_id=query_id)


def test_sql_page_rejects_missing_query_id_header(stub):
    query_id = "1234567890abcdef1234567890abcdef"
    stub.omit_query_id_header = True
    stub.canned["/sql"] = [(200, {})]
    stub.canned_get[f"/queries/{query_id}"] = [
        (200, completed_query_status(query_id))
    ]
    with pytest.raises(SerializationError):
        RemoteDatabase(stub.url()).sql_page(
            "SELECT id FROM users",
            projection=["id"],
            page_size_rows=1,
            query_id=query_id,
        )


def test_idempotent_sql_keeps_commit_proof_when_query_id_header_is_missing(stub):
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    stub.omit_query_id_header = True
    stub.canned["/sql"] = [(200, sql_write_receipt(query_id, epoch=29))]
    with pytest.raises(CommitOutcomeError) as caught:
        RemoteDatabase(stub.url()).execute_idempotent_sql(
            "INSERT INTO users (id) VALUES (1)",
            idempotency_key="insert-one",
            query_id=query_id,
        )
    assert caught.value.query_id == query_id
    assert caught.value.committed is True
    assert caught.value.committed_statements == 1
    assert caught.value.last_commit_epoch == 29


@pytest.mark.parametrize("replayed", [True, False])
def test_idempotent_sql_retries_once_after_restart_loses_tombstone(
    stub, monkeypatch, replayed
):
    original_query_id = "abcdefabcdefabcdefabcdefabcdefab"
    replay_query_id = "11112222333344445555666677778888"
    monkeypatch.setattr("mongreldb_kit.remote.secrets.token_hex", lambda _: replay_query_id)
    stub.pre_cancel_on_cancel = True
    stub.canned_get[f"/queries/{original_query_id}"] = [
        (404, query_not_found_response(original_query_id))
    ]
    stub.canned["/sql"] = [
        (200, {}),
        (
            200,
            {
                "query_id": replay_query_id,
                "original_query_id": original_query_id if replayed else replay_query_id,
                "status": "committed",
                "committed": True,
                "committed_statements": 1,
                "last_commit_epoch": 29,
                "last_commit_epoch_text": "29",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "retryable": False,
                "idempotency_replayed": replayed,
                "idempotency_persisted": True,
                "idempotency_expires_at_ms": 999,
                "outcome": {
                    "committed": True,
                    "committed_statements": 1,
                    "last_commit_epoch": 29,
                    "last_commit_epoch_text": "29",
                    "first_commit_statement_index": 0,
                    "last_commit_statement_index": 0,
                    "completed_statements": 1,
                    "statement_index": 0,
                    "serialization": "succeeded",
                },
                "terminal_error": None,
            },
        ),
    ]
    db = RemoteDatabase(stub.url())
    receipt = db.execute_idempotent_sql(
        "INSERT INTO users (id) VALUES (1)",
        idempotency_key="insert-one",
        query_id=original_query_id,
        timeout_ms=250,
        max_output_rows=1,
        max_output_bytes=1024,
    )
    assert receipt["query_id"] == replay_query_id
    assert receipt["original_query_id"] == (
        original_query_id if replayed else replay_query_id
    )
    assert receipt["idempotency_replayed"] is replayed
    assert receipt["last_commit_epoch"] == 29
    sql_requests = [
        body for method, path, body in stub.requests if method == "POST" and path == "/sql"
    ]
    assert len(sql_requests) == 2
    assert not any(path.endswith("/cancel") for _, path, _ in stub.requests)
    assert sql_requests[0]["query_id"] == original_query_id
    assert sql_requests[1]["query_id"] == replay_query_id
    for request in sql_requests:
        assert request["sql"] == "INSERT INTO users (id) VALUES (1)"
        assert request["idempotency_key"] == "insert-one"
        assert request["timeout_ms"] == 250
        assert request["max_output_rows"] == 1
        assert request["max_output_bytes"] == 1024


def test_idempotent_sql_refuses_second_post_after_capability_downgrade(stub):
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    initial = copy.deepcopy(stub.capabilities)
    downgraded = copy.deepcopy(initial)
    downgraded.pop("sql_idempotency")
    stub.canned_get["/capabilities"] = [(200, initial), (200, downgraded)]
    stub.canned_get[f"/queries/{query_id}"] = [
        (404, query_not_found_response(query_id))
    ]
    stub.canned["/sql"] = [(200, {})]
    with pytest.raises(CapabilityUnsupportedError):
        RemoteDatabase(stub.url()).execute_idempotent_sql(
            "INSERT INTO users (id) VALUES (1)",
            idempotency_key="insert-one",
            query_id=query_id,
        )
    assert len([request for request in stub.requests if request[1] == "/capabilities"]) == 2
    assert len([request for request in stub.requests if request[1] == "/sql"]) == 1


def test_durable_receipt_validator_rejects_conflicting_fields():
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    valid = {
        "query_id": query_id,
        "original_query_id": query_id,
        "status": "committed",
        "terminal_state": "committed",
        "server_state": "completed",
        "cancel_outcome": "already_finished",
        "cancellation_reason": "none",
        "committed": True,
        "committed_statements": 1,
        "last_commit_epoch": 17,
        "last_commit_epoch_text": "17",
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "retryable": False,
        "idempotency_replayed": False,
        "idempotency_persisted": True,
        "idempotency_expires_at_ms": 999,
        "outcome": {
            "committed": True,
            "committed_statements": 1,
            "last_commit_epoch": 17,
            "last_commit_epoch_text": "17",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "succeeded",
        },
        "terminal_error": None,
    }
    assert _is_sql_write_receipt(valid, query_id)
    invalid = []
    candidate = copy.deepcopy(valid)
    candidate["last_commit_epoch_text"] = "18"
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["outcome"]["last_commit_epoch"] = 18
    candidate["outcome"]["last_commit_epoch_text"] = "18"
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["first_commit_statement_index"] = 1
    candidate["outcome"]["first_commit_statement_index"] = 1
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["outcome"]["completed_statements"] = 0
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["outcome"]["serialization"] = ""
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["terminal_error"] = {"code": "", "category": "execution"}
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["outcome"]["serialization"] = "completed"
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["last_commit_epoch"] = None
    candidate["last_commit_epoch_text"] = "18446744073709551616"
    candidate["outcome"]["last_commit_epoch"] = None
    candidate["outcome"]["last_commit_epoch_text"] = "18446744073709551616"
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["terminal_error"] = {"code": "QUERY_FAILED", "category": "execution"}
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["unexpected"] = True
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["outcome"]["unexpected"] = True
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    del candidate["outcome"]["last_commit_epoch"]
    invalid.append(candidate)
    assert all(not _is_sql_write_receipt(candidate, query_id) for candidate in invalid)

    fresh_query_id = "11112222333344445555666677778888"
    fresh_execution = copy.deepcopy(valid)
    fresh_execution["query_id"] = fresh_query_id
    fresh_execution["original_query_id"] = fresh_query_id
    assert _is_sql_write_receipt(fresh_execution, fresh_query_id, query_id)
    fresh_execution["original_query_id"] = query_id
    assert not _is_sql_write_receipt(fresh_execution, fresh_query_id, query_id)

    replay = copy.deepcopy(valid)
    replay["query_id"] = fresh_query_id
    replay["idempotency_replayed"] = True
    assert _is_sql_write_receipt(replay, replay["query_id"], query_id)
    replay["original_query_id"] = "99990000111122223333444455556666"
    assert not _is_sql_write_receipt(replay, replay["query_id"], query_id)


def test_retained_page_validator_rejects_conflicting_metadata():
    valid = {
        "status": "completed",
        "rows": [{"id": 1}],
        "next_cursor": None,
        "page": {
            "offset": 0,
            "row_count": 1,
            "total_rows": 1,
            "byte_count": 10,
            "estimated_tokens": 3,
            "limits": {"rows": 1, "bytes": 1024, "tokens": 256},
            "projection": ["id"],
            "expires_at_ms": 999,
            "snapshot": "retained_result",
            "token_estimate": "ceil(projected_json_bytes/4)",
        },
    }
    options = {
        "page_size_rows": 1,
        "projection": ["id"],
        "max_page_bytes": 1024,
        "max_page_tokens": 256,
        "max_output_rows": 1,
        "max_output_bytes": 1024,
    }
    assert _is_sql_page(valid, options)
    invalid = []
    for path, value in (
        (("page", "row_count"), 2),
        (("page", "offset"), 1),
        (("page", "limits", "rows"), 0),
        (("page", "byte_count"), 1025),
        (("page", "projection"), ["other"]),
        (("page", "snapshot"), "live"),
        (("page", "expires_at_ms"), 2**64),
        (("next_cursor",), "unexpected"),
        (("page", "limits", "rows"), 2),
    ):
        candidate = copy.deepcopy(valid)
        target = candidate
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["page"]["total_rows"] = 2
    candidate["next_cursor"] = "cursor-1"
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["rows"] = [{"other": 1}]
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["page"]["projection"] = ["id", "id"]
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["page"]["projection"] = ["x" * 257]
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["page"]["limits"]["bytes"] = 64 * 1024 * 1024 + 1
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["page"]["total_rows"] = 2
    candidate["next_cursor"] = "😀" * 600
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["page"]["byte_count"] += 1
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["page"]["estimated_tokens"] += 1
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["rows"] = []
    candidate["page"]["row_count"] = 0
    candidate["page"]["total_rows"] = 1
    candidate["page"]["byte_count"] = 2
    candidate["page"]["estimated_tokens"] = 1
    candidate["next_cursor"] = "cursor-1"
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["unexpected"] = True
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["page"]["unexpected"] = True
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["page"]["limits"]["unexpected"] = True
    invalid.append(candidate)
    assert all(not _is_sql_page(candidate, options) for candidate in invalid)


def test_query_status_validator_rejects_cached_and_conflicting_status():
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    valid = {
        "query_id": query_id,
        "status": "committed",
        "state": "completed",
        "server_state": "completed",
        "terminal_state": "committed",
        "operation": "sql",
        "started_ms_ago": 12,
        "deadline_ms_remaining": None,
        "session_id": None,
        "committed": True,
        "committed_statements": 1,
        "last_commit_epoch": 17,
        "last_commit_epoch_text": "17",
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "cancel_outcome": "already_finished",
        "cancellation_reason": "none",
        "retryable": False,
        "outcome": {
            "committed": True,
            "committed_statements": 1,
            "last_commit_epoch": 17,
            "last_commit_epoch_text": "17",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "succeeded",
        },
        "terminal_error": None,
        "trace": {
            "queue_duration_us": 1,
            "planning_duration_us": 2,
            "execution_duration_us": 3,
            "serialization_duration_us": 4,
            "cancel_requested_phase": None,
            "cancel_observed_phase": None,
            "commit_fence_outcome": "commit_won",
        },
    }
    assert _is_query_status(valid, query_id)
    invalid = []
    for path, value in (
        (("query_id",), "11111111111111111111111111111111"),
        (("status",), "completed"),
        (("server_state",), "failed"),
        (("terminal_state",), "completed"),
        (("last_commit_epoch_text",), "18"),
        (("outcome", "last_commit_epoch_text"), "18"),
        (("outcome", "completed_statements"), 0),
    ):
        candidate = copy.deepcopy(valid)
        target = candidate
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["first_commit_statement_index"] = 1
    candidate["outcome"]["first_commit_statement_index"] = 1
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["committed_statements"] = 2
    candidate["outcome"]["committed_statements"] = 2
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["last_commit_statement_index"] = 1
    candidate["outcome"]["last_commit_statement_index"] = 1
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["statement_index"] = 2
    candidate["outcome"]["statement_index"] = 2
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["terminal_error"] = {"code": "", "category": "execution"}
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["status"] = "committed_with_error"
    candidate["state"] = "failed"
    candidate["server_state"] = "failed"
    candidate["terminal_state"] = "committed_with_error"
    candidate["terminal_error"] = {
        "code": "QUERY_CANCELLED_AFTER_COMMIT",
        "category": "execution",
    }
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["unexpected"] = True
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["outcome"]["unexpected"] = True
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    del candidate["outcome"]["last_commit_epoch"]
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["trace"]["unexpected"] = True
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["trace"]["execution_duration_us"] = -1
    invalid.append(candidate)
    assert all(not _is_query_status(candidate, query_id) for candidate in invalid)

    cancelling = copy.deepcopy(valid)
    cancelling["state"] = "cancelling"
    cancelling["server_state"] = "cancelling"
    cancelling["terminal_state"] = None
    cancelling["cancel_outcome"] = "accepted"
    cancelling["cancellation_reason"] = "deadline"
    cancelling["outcome"]["serialization"] = "in_progress"
    assert _is_query_status(cancelling, query_id)


def test_cancel_and_sql_error_envelopes_reject_crossed_metadata():
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    with pytest.raises(StorageError):
        _validate_cancel_response(
            202,
            query_id,
            {
                "query_id": "11112222333344445555666677778888",
                "state": "cancellation_requested",
                "cancel_outcome": "accepted",
            },
        )
    with pytest.raises(StorageError):
        _validate_cancel_response(
            202,
            query_id,
            {
                "query_id": query_id,
                "state": "cancellation_requested",
                "cancel_outcome": "mystery",
            },
        )
    with pytest.raises(StorageError):
        _validate_cancel_response(
            202,
            query_id,
            {"query_id": query_id, "state": "cancellation_requested"},
        )
    with pytest.raises(StorageError):
        _validate_cancel_response(
            200,
            query_id,
            {
                "query_id": query_id,
                "state": "cancellation_requested",
                "cancel_outcome": "accepted",
            },
        )
    with pytest.raises(StorageError, match="unknown fields"):
        _validate_cancel_response(
            202,
            query_id,
            {
                "query_id": query_id,
                "state": "cancellation_requested",
                "cancel_outcome": "accepted",
                "unexpected": True,
            },
        )
    not_found = {
        "query_id": query_id,
        "status": "unknown",
        "terminal_state": None,
        "committed": None,
        "committed_statements": None,
        "last_commit_epoch": None,
        "last_commit_epoch_text": None,
        "first_commit_statement_index": None,
        "last_commit_statement_index": None,
        "completed_statements": None,
        "statement_index": None,
        "cancel_outcome": "not_found",
        "cancellation_reason": None,
        "retryable": False,
        "server_state": "not_found",
        "outcome": {
            "committed": None,
            "committed_statements": None,
            "last_commit_epoch": None,
            "last_commit_epoch_text": None,
            "first_commit_statement_index": None,
            "last_commit_statement_index": None,
            "completed_statements": None,
            "statement_index": None,
            "serialization": "unknown",
        },
        "error": {
            "code": "QUERY_NOT_FOUND",
            "message": "query not found",
            "query_id": query_id,
            "committed": None,
            "retryable": False,
        },
    }
    assert _validate_cancel_response(404, query_id, not_found) == not_found
    missing_outcome = copy.deepcopy(not_found)
    del missing_outcome["outcome"]["last_commit_epoch"]
    with pytest.raises(StorageError, match="outcome last_commit_epoch is missing"):
        _validate_cancel_response(404, query_id, missing_outcome)

    error = {
        "query_id": query_id,
        "status": "cancelled_before_commit",
        "terminal_state": "cancelled_before_commit",
        "committed": False,
        "committed_statements": 0,
        "last_commit_epoch": None,
        "last_commit_epoch_text": None,
        "first_commit_statement_index": None,
        "last_commit_statement_index": None,
        "completed_statements": 0,
        "statement_index": 0,
        "retryable": False,
        "outcome": {
            "committed": False,
            "committed_statements": 0,
            "last_commit_epoch": None,
            "last_commit_epoch_text": None,
            "first_commit_statement_index": None,
            "last_commit_statement_index": None,
            "completed_statements": 0,
            "statement_index": 0,
            "serialization": "failed",
        },
        "error": {
            "code": "QUERY_CANCELLED",
            "message": "cancelled",
            "query_id": query_id,
            "committed": False,
            "retryable": False,
        },
    }
    assert _is_sql_error_envelope(error, query_id)
    unknown = copy.deepcopy(error)
    unknown["unexpected"] = True
    assert not _is_sql_error_envelope(unknown, query_id)
    unknown = copy.deepcopy(error)
    unknown["error"]["unexpected"] = True
    assert not _is_sql_error_envelope(unknown, query_id)
    missing = copy.deepcopy(error)
    del missing["outcome"]["last_commit_epoch"]
    assert not _is_sql_error_envelope(missing, query_id)
    error["error"]["code"] = "RESULT_LIMIT_EXCEEDED"
    assert not _is_sql_error_envelope(error, query_id)


def test_strict_json_rejects_duplicate_keys_at_every_depth():
    with pytest.raises(ValueError, match="duplicate JSON object key 'status'"):
        _strict_json_loads('{"status":"running","status":"completed"}')
    with pytest.raises(ValueError, match="duplicate JSON object key 'committed'"):
        _strict_json_loads(
            '{"outcome":{"committed":false,"committed":true}}'
        )
    with pytest.raises(ValueError, match="invalid JSON number NaN"):
        _strict_json_loads('{"duration":NaN}')


def test_query_not_found_requires_exact_matching_envelope():
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    valid = query_not_found_response(query_id)
    assert _is_query_not_found(valid, query_id)
    wrong = copy.deepcopy(valid)
    wrong["error"]["query_id"] = "11111111111111111111111111111111"
    assert not _is_query_not_found(wrong, query_id)
    unknown = copy.deepcopy(valid)
    unknown["result"] = None
    assert not _is_query_not_found(unknown, query_id)


def test_cursor_error_requires_exact_non_committed_outcome():
    valid = sql_cursor_error_response()
    assert _is_sql_cursor_error_envelope(valid)
    conflict = copy.deepcopy(valid)
    conflict["error"]["committed"] = True
    assert not _is_sql_cursor_error_envelope(conflict)
    unknown = copy.deepcopy(valid)
    unknown["unexpected"] = None
    assert not _is_sql_cursor_error_envelope(unknown)


def test_idempotent_recovery_rejects_wrong_query_status_without_replay(stub):
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    stub.canned["/sql"] = [(200, {})]
    stub.canned_get[f"/queries/{query_id}"] = [
        (
            200,
            {
                "query_id": "11111111111111111111111111111111",
                "status": "outcome_unknown",
                "state": "failed",
                "committed": None,
                "outcome": {"committed": None},
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    with pytest.raises(QueryOutcomeUnknownError) as caught:
        db.execute_idempotent_sql(
            "INSERT INTO users (id) VALUES (1)",
            idempotency_key="wrong-status",
            query_id=query_id,
        )
    assert caught.value.server_state == "invalid_status"
    assert len([request for request in stub.requests if request[1] == "/sql"]) == 1
    assert not any(path.endswith("/cancel") for _, path, _ in stub.requests)


def test_idempotent_recovery_rejects_malformed_not_found_without_replay(stub):
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    stub.canned["/sql"] = [(200, {})]
    stub.canned_get[f"/queries/{query_id}"] = [(404, {})]
    db = RemoteDatabase(stub.url())
    with pytest.raises(QueryOutcomeUnknownError) as caught:
        db.execute_idempotent_sql(
            "INSERT INTO users (id) VALUES (1)",
            idempotency_key="malformed-not-found",
            query_id=query_id,
        )
    assert caught.value.server_state == "invalid_status"
    assert len([request for request in stub.requests if request[1] == "/sql"]) == 1


def test_idempotent_sql_does_not_replay_indeterminate_terminal_status(stub):
    query_id = "abcdefabcdefabcdefabcdefabcdefab"
    stub.canned["/sql"] = [(200, {})]
    stub.canned_get[f"/queries/{query_id}"] = [
        (
            200,
            {
                "query_id": query_id,
                "status": "outcome_unknown",
                "state": "failed",
                "committed": None,
                "outcome": {"committed": None, "serialization": "unknown"},
                "terminal_error": {
                    "code": "QUERY_OUTCOME_UNKNOWN",
                    "category": "execution",
                },
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    with pytest.raises(QueryOutcomeUnknownError):
        db.execute_idempotent_sql(
            "INSERT INTO users (id) VALUES (1)",
            idempotency_key="insert-one",
            query_id=query_id,
        )
    assert len([r for r in stub.requests if r[0] == "POST" and r[1] == "/sql"]) == 1


def test_idempotent_sql_recovers_after_truncated_http_error(stub):
    query_id = "abababababababababababababababab"
    stub.raw_canned["/sql"] = [(500, b'{"error":')]
    stub.canned_get[f"/queries/{query_id}"] = [
        (
            200,
            {
                "query_id": query_id,
                "status": "committed",
                "terminal_state": "committed",
                "state": "completed",
                "server_state": "completed",
                "committed": True,
                "committed_statements": 1,
                "last_commit_epoch": 17,
                "last_commit_epoch_text": "17",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "cancel_outcome": "already_finished",
                "cancellation_reason": "none",
                "retryable": False,
                "outcome": {
                    "committed": True,
                    "committed_statements": 1,
                    "last_commit_epoch": 17,
                    "last_commit_epoch_text": "17",
                    "first_commit_statement_index": 0,
                    "last_commit_statement_index": 0,
                    "completed_statements": 1,
                    "statement_index": 0,
                    "serialization": "succeeded",
                },
                "terminal_error": None,
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    with pytest.raises(CommitOutcomeError) as caught:
        db.execute_idempotent_sql(
            "INSERT INTO users (id) VALUES (1)",
            idempotency_key="insert-one",
            query_id=query_id,
        )
    assert caught.value.committed is True
    assert caught.value.committed_statements == 1
    assert caught.value.last_commit_epoch == 17


def test_idempotent_sql_recovers_after_ordinary_200_body(stub):
    query_id = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
    stub.canned["/sql"] = [(200, {"status": "completed", "rows": []})]
    stub.canned_get[f"/queries/{query_id}"] = [
        (
            200,
            {
                "query_id": query_id,
                "status": "committed",
                "terminal_state": "committed",
                "state": "completed",
                "server_state": "completed",
                "committed": True,
                "committed_statements": 1,
                "last_commit_epoch": 23,
                "last_commit_epoch_text": "23",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "cancel_outcome": "already_finished",
                "cancellation_reason": "none",
                "retryable": False,
                "outcome": {
                    "committed": True,
                    "committed_statements": 1,
                    "last_commit_epoch": 23,
                    "last_commit_epoch_text": "23",
                    "first_commit_statement_index": 0,
                    "last_commit_statement_index": 0,
                    "completed_statements": 1,
                    "statement_index": 0,
                    "serialization": "succeeded",
                },
                "terminal_error": None,
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    with pytest.raises(CommitOutcomeError) as caught:
        db.execute_idempotent_sql(
            "INSERT INTO users (id) VALUES (1)",
            idempotency_key="insert-one",
            query_id=query_id,
        )
    assert caught.value.committed is True
    assert caught.value.committed_statements == 1
    assert caught.value.last_commit_epoch == 23


def test_sql_pagination_and_idempotency_validate_capability_and_inputs(stub):
    stub.capabilities.pop("sql_pagination")
    db = RemoteDatabase(stub.url())
    with pytest.raises(CapabilityUnsupportedError):
        db.sql_page("SELECT id FROM users", projection=["id"], page_size_rows=1)
    assert not any(r[1] == "/sql" for r in stub.requests)

    stub.capabilities["sql_pagination"] = {
        "version": 1,
        "continuation_endpoint": "/sql/continue",
        "retained_snapshot": True,
        "projection_required": True,
        "byte_and_token_hints": True,
    }
    current = RemoteDatabase(stub.url())
    with pytest.raises(ValueError):
        current.sql_page("SELECT id FROM users", projection=[], page_size_rows=0)
    for invalid in (True, 1.5, 2**64):
        with pytest.raises(ValueError):
            current.sql_page(
                "SELECT id FROM users", projection=["id"], page_size_rows=invalid
            )
    with pytest.raises(ValueError, match="UTF-8 bytes"):
        current.continue_sql_page("😀" * 600)
    with pytest.raises(ValueError):
        current.sql_arrow("SELECT 1", timeout_ms=float("nan"))
    with pytest.raises(ValueError):
        current.execute_idempotent_sql("INSERT INTO users VALUES (1)", idempotency_key="")
    with pytest.raises(ValueError):
        current.execute_idempotent_sql(
            "INSERT INTO users VALUES (1)", idempotency_key="key", timeout_ms=True
        )


def test_malformed_pagination_responses_are_serialization_failures(stub):
    query_id = "1234567890abcdef1234567890abcdef"
    stub.canned["/sql"] = [(200, {"status": "completed", "rows": []})]
    stub.canned_get[f"/queries/{query_id}"] = [
        (
            200,
            {
                "query_id": query_id,
                "status": "completed",
                "terminal_state": "completed",
                "state": "completed",
                "server_state": "completed",
                "committed": False,
                "committed_statements": 0,
                "last_commit_epoch": None,
                "last_commit_epoch_text": None,
                "first_commit_statement_index": None,
                "last_commit_statement_index": None,
                "completed_statements": 1,
                "statement_index": 0,
                "cancel_outcome": "already_finished",
                "cancellation_reason": "none",
                "retryable": False,
                "outcome": {
                    "committed": False,
                    "committed_statements": 0,
                    "last_commit_epoch": None,
                    "last_commit_epoch_text": None,
                    "first_commit_statement_index": None,
                    "last_commit_statement_index": None,
                    "completed_statements": 1,
                    "statement_index": 0,
                    "serialization": "succeeded",
                },
                "terminal_error": None,
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    with pytest.raises(SerializationError):
        db.sql_page(
            "SELECT id FROM users",
            projection=["id"],
            page_size_rows=1,
            query_id=query_id,
        )

    stub.canned["/sql/continue"] = [(200, {"status": "completed", "rows": []})]
    with pytest.raises(SerializationError):
        db.continue_sql_page("cursor-1")


def test_result_limit_error_keeps_durable_fields(stub):
    query_id = "99998888777766665555444433332222"
    stub.canned["/sql"] = [
        (
            413,
            {
                "query_id": query_id,
                "status": "committed_with_error",
                "terminal_state": "committed_with_error",
                "committed": True,
                "committed_statements": 2,
                "last_commit_epoch": None,
                "last_commit_epoch_text": "9007199254740993",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 1,
                "completed_statements": 3,
                "statement_index": 3,
                "cancel_outcome": "already_finished",
                "cancellation_reason": "none",
                "retryable": False,
                "server_state": "failed",
                "error": {
                    "code": "RESULT_LIMIT_EXCEEDED",
                    "message": "too large",
                    "query_id": query_id,
                    "committed": True,
                    "retryable": False,
                },
                "outcome": {
                    "committed": True,
                    "committed_statements": 2,
                    "last_commit_epoch": None,
                    "last_commit_epoch_text": "9007199254740993",
                    "first_commit_statement_index": 0,
                    "last_commit_statement_index": 1,
                    "completed_statements": 3,
                    "statement_index": 3,
                    "serialization": "failed",
                },
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    with pytest.raises(ResultLimitExceededError) as caught:
        db.sql_arrow("SELECT 1", query_id=query_id, max_output_rows=1)
    assert caught.value.committed is True
    assert caught.value.committed_statements == 2
    assert caught.value.last_commit_epoch == 9007199254740993
    assert caught.value.completed_statements == 3
    assert caught.value.statement_index == 3


def test_outcome_unknown_keeps_commit_state_unknown():
    query_id = "11112222333344445555666677778888"
    status = {
        "query_id": query_id,
        "status": "outcome_unknown",
        "state": "failed",
        "committed": None,
        "committed_statements": None,
        "completed_statements": None,
        "statement_index": None,
        "outcome": {
            "committed": None,
            "committed_statements": None,
            "completed_statements": None,
            "statement_index": None,
            "serialization": "unknown",
        },
        "terminal_error": {"code": "QUERY_OUTCOME_UNKNOWN", "category": "execution"},
    }

    with pytest.raises(QueryOutcomeUnknownError) as caught:
        RemoteDatabase._raise_if_terminal_transport_outcome(query_id, "response lost", status)
    assert caught.value.committed is None
    assert caught.value.committed_statements is None
    assert caught.value.last_commit_epoch is None
    assert caught.value.completed_statements is None
    assert caught.value.statement_index is None
    assert caught.value.retryable is False


def test_committed_serializing_status_is_immediately_decisive():
    query_id = "11112222333344445555666677778888"
    status = {
        "query_id": query_id,
        "status": "committed",
        "state": "serializing",
        "server_state": "serializing",
        "committed": True,
        "committed_statements": 1,
        "last_commit_epoch": 17,
        "last_commit_epoch_text": "17",
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "retryable": False,
        "outcome": {
            "committed": True,
            "committed_statements": 1,
            "last_commit_epoch": 17,
            "last_commit_epoch_text": "17",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "in_progress",
        },
        "terminal_error": None,
    }
    with pytest.raises(CommitOutcomeError) as caught:
        RemoteDatabase._raise_if_terminal_transport_outcome(query_id, "response lost", status)
    assert caught.value.committed is True
    assert caught.value.last_commit_epoch == 17


@pytest.mark.parametrize(
    ("state", "status_name"),
    (("failed", "failed_before_commit"), ("cancelled", "cancelled_before_commit")),
)
def test_known_terminal_failure_without_error_code_is_not_unknown(state, status_name):
    query_id = "12121212121212121212121212121212"
    status = {
        "query_id": query_id,
        "status": status_name,
        "state": state,
        "committed": False,
        "committed_statements": 0,
        "completed_statements": 0,
        "statement_index": 0,
        "retryable": False,
        "outcome": {
            "committed": False,
            "committed_statements": 0,
            "completed_statements": 0,
            "statement_index": 0,
        },
    }

    with pytest.raises(StorageError) as caught:
        RemoteDatabase._raise_if_terminal_transport_outcome(query_id, "response lost", status)
    assert not isinstance(caught.value, QueryOutcomeUnknownError)
    assert caught.value.code == "QUERY_FAILED"
    assert caught.value.query_id == query_id
    assert caught.value.committed is False
    assert caught.value.committed_statements == 0
    assert caught.value.completed_statements == 0
    assert caught.value.statement_index == 0
    assert caught.value.retryable is False
    assert caught.value.server_state == state


def test_pre_cancelled_transport_recovery_is_terminal_cancellation():
    query_id = "34343434343434343434343434343434"
    status = {
        "query_id": query_id,
        "status": "cancelled_before_commit",
        "state": "pre_cancelled",
        "committed": False,
        "committed_statements": 0,
        "completed_statements": 0,
        "statement_index": 0,
        "cancel_outcome": "pre_cancelled",
        "cancellation_reason": "client_request",
        "retryable": False,
        "outcome": {"committed": False, "committed_statements": 0},
        "terminal_error": {"code": "QUERY_CANCELLED", "category": "cancellation"},
    }

    with pytest.raises(QueryCancelledError) as caught:
        RemoteDatabase._raise_if_terminal_transport_outcome(query_id, "response lost", status)
    assert caught.value.code == "QUERY_CANCELLED"
    assert caught.value.committed is False
    assert caught.value.committed_statements == 0
    assert caught.value.cancel_outcome == "pre_cancelled"
    assert caught.value.cancellation_reason == "client_request"
    assert caught.value.server_state == "pre_cancelled"


def test_transport_timeout_is_separate_and_requests_best_effort_cancel(stub):
    db = RemoteDatabase(stub.url())
    query_id = "aaaabbbbccccddddeeeeffff00001111"
    with pytest.raises(QueryOutcomeUnknownError, match=query_id):
        db.sql_arrow(
            "SLOW_TRANSPORT",
            timeout_ms=5_000,
            query_id=query_id,
            transport_timeout=0.01,
        )
    deadline = time.time() + 1
    while time.time() < deadline and not any(
        request[1] == f"/queries/{query_id}/cancel" for request in stub.requests
    ):
        time.sleep(0.01)
    assert any(request[1] == f"/queries/{query_id}/cancel" for request in stub.requests)


def test_recovery_window_bounds_unresponsive_control_requests(monkeypatch):
    db = object.__new__(RemoteDatabase)
    db._base = "http://example.test"
    db._authorization = None
    requests = []

    def unresponsive(request, *, timeout):
        requests.append((request.get_method(), request.full_url, timeout))
        time.sleep(timeout)
        raise TimeoutError("timed out")

    monkeypatch.setattr(urllib.request, "urlopen", unresponsive)
    query_id = "ccccbbbbaaaa99998888777766665555"
    started = time.monotonic()
    with pytest.raises(QueryOutcomeUnknownError, match=query_id):
        db._raise_terminal_transport_outcome(query_id, "response lost")
    elapsed = time.monotonic() - started

    assert 1.5 <= elapsed <= 3.0
    assert any(method == "GET" for method, _, _ in requests)
    assert any(method == "POST" for method, _, _ in requests)
    assert all(0 < timeout <= 0.25 for _, _, timeout in requests)


def test_remote_background_handle_returns_query_id_and_result(stub):
    db = RemoteDatabase(stub.url())
    handle = db.start_sql_arrow(
        "SELECT 1",
        query_id="99990000111122223333444455556666",
        timeout_ms=1_000,
    )
    assert handle.id == "99990000111122223333444455556666"
    assert handle.result() == b""


def test_remote_handle_accepted_cancel_keeps_worker_durable_outcome():
    release = threading.Event()
    query_id = "ddddccccbbbbaaaa9999888877776666"

    class FakeDatabase:
        def sql_arrow(self, *args, **kwargs):
            assert release.wait(timeout=1)
            error = QueryCancelledError("cancelled after commit")
            error.query_id = query_id
            error.committed = True
            error.committed_statements = 1
            error.last_commit_epoch = 17
            error.completed_statements = 1
            error.statement_index = 1
            raise error

        def cancel_sql(self, active_query_id):
            assert active_query_id == query_id
            release.set()
            return {"state": "cancellation_requested"}

    handle = RemoteSqlQueryHandle(
        FakeDatabase(),
        "INSERT INTO items VALUES (1); SELECT * FROM large_table",
        timeout_ms=5_000,
        query_id=query_id,
        transport_timeout=None,
        max_output_rows=None,
        max_output_bytes=None,
    )
    assert handle.cancel()["state"] == "cancellation_requested"
    with pytest.raises(QueryCancelledError) as caught:
        handle.result()
    assert caught.value.committed is True
    assert caught.value.committed_statements == 1
    assert caught.value.last_commit_epoch == 17
    assert caught.value.completed_statements == 1
    assert caught.value.statement_index == 1


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
                "epoch_text": "9",
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


def test_transaction_response_validation_fails_closed(stub):
    valid = {
        "status": "committed",
        "epoch": 9,
        "epoch_text": "9",
        "results": [
            {
                "kind": "put",
                "row_id": None,
                "auto_inc": 1,
                "row": [0, 1, 1, "a@x", 2, 30],
            }
        ],
    }
    invalid = []
    candidate = copy.deepcopy(valid)
    candidate["epoch_text"] = "09"
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["results"] = []
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["results"][0]["kind"] = "upsert"
    candidate["results"][0]["action"] = "inserted"
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["results"][0].pop("row")
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["results"][0]["row"] = [0]
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["results"][0]["row"] = [99, 1]
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["results"][0]["row"] = [0, 1, 0, 2]
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["results"][0].pop("auto_inc")
    invalid.append(candidate)
    candidate = copy.deepcopy(valid)
    candidate["extra"] = True
    invalid.append(candidate)

    db = RemoteDatabase(stub.url())
    for response in invalid:
        stub.kit_txn_responses.append((200, response))
        txn = db.begin()
        txn.insert("users", {"email": "a@x", "age": 30}, returning=True)
        with pytest.raises(
            (StorageError, QueryOutcomeUnknownError, CommitOutcomeError),
            match="invalid|does not match|did not match|malformed|column",
        ):
            txn.commit()

    stub.kit_txn_responses.append(
        (
            200,
            {
                "status": "committed",
                "epoch": 10,
                "epoch_text": "10",
                "results": [
                    {
                        "kind": "upsert",
                        "action": "replaced",
                        "auto_inc": None,
                        "row": [0, 1, 1, "a@x"],
                    }
                ],
            },
        )
    )
    with pytest.raises(CommitOutcomeError, match="does not match"):
        db.begin().upsert("users", {"id": 1, "email": "a@x"}).commit()

    stub.kit_txn_responses.append(
        (
            200,
            {
                "status": "committed",
                "epoch": 11,
                "epoch_text": "11",
                "results": [{"kind": "put", "row_id": None, "auto_inc": None}],
            },
        )
    )
    with pytest.raises(CommitOutcomeError, match="does not match"):
        db.begin().delete_by_pk("users", 1).commit()


def test_transaction_success_decode_errors_preserve_commit_state(stub):
    db = RemoteDatabase(stub.url())
    stub.kit_txn_responses.append(
        (
            200,
            {
                "status": "committed",
                "epoch": 42,
                "epoch_text": "42",
                "results": [],
            },
        )
    )
    with pytest.raises(CommitOutcomeError) as committed:
        db.begin().insert("users", {"email": "a@x"}).commit()
    assert committed.value.committed is True
    assert committed.value.last_commit_epoch == 42

    stub.kit_txn_responses.append(
        (
            200,
            {
                "status": "committed",
                "epoch": 42,
                "epoch_text": "41",
                "results": [],
            },
        )
    )
    with pytest.raises(QueryOutcomeUnknownError):
        db.begin().commit()


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


def _committed_non_sql_error():
    return {
        "status": "committed",
        "committed": True,
        "epoch": 42,
        "epoch_text": "42",
        "retryable": False,
        "error": {"code": "COMMIT_OUTCOME", "message": "commit completed"},
    }


def test_transaction_commit_outcome_keeps_committed_epoch_and_retryability(stub):
    stub.kit_txn_responses.append((409, _committed_non_sql_error()))
    db = RemoteDatabase(stub.url())
    with pytest.raises(CommitOutcomeError) as caught:
        db.begin().insert("users", {"age": 1}).commit()
    assert caught.value.code == "COMMIT_OUTCOME"
    assert caught.value.committed is True
    assert caught.value.last_commit_epoch == 42
    assert caught.value.retryable is False


@pytest.mark.parametrize(
    ("path", "invoke"),
    [
        ("/procedures", lambda db: db.create_procedure({"name": "p"})),
        ("/kit/procedures/p/call", lambda db: db.call_procedure("p")),
        ("/triggers", lambda db: db.create_trigger({"name": "t"})),
    ],
)
def test_procedure_and_trigger_commit_outcomes_keep_metadata(stub, path, invoke):
    stub.canned[path] = [(409, _committed_non_sql_error())]
    db = RemoteDatabase(stub.url())
    with pytest.raises(CommitOutcomeError) as caught:
        invoke(db)
    assert caught.value.code == "COMMIT_OUTCOME"
    assert caught.value.committed is True
    assert caught.value.last_commit_epoch == 42
    assert caught.value.retryable is False


def test_procedure_call_requires_explicit_exact_commit_state(stub):
    path = "/kit/procedures/p/call"
    db = RemoteDatabase(stub.url())
    for response in (
        {
            "status": "ok",
            "committed": False,
            "epoch": None,
            "epoch_text": None,
            "result": None,
        },
        {
            "status": "ok",
            "committed": True,
            "epoch": 9,
            "epoch_text": "9",
            "result": {},
        },
    ):
        stub.canned[path] = [(200, response)]
        assert db.call_procedure("p") == response

    invalid = [
        {"status": "ok", "epoch": None, "epoch_text": None, "result": None},
        {
            "status": "ok",
            "committed": False,
            "epoch": 9,
            "epoch_text": "9",
            "result": None,
        },
        {
            "status": "ok",
            "committed": True,
            "epoch": 9,
            "epoch_text": "09",
            "result": None,
        },
        {
            "status": "ok",
            "committed": True,
            "epoch": None,
            "epoch_text": None,
            "result": None,
        },
    ]
    for response in invalid:
        stub.canned[path] = [(200, response)]
        with pytest.raises(QueryOutcomeUnknownError):
            db.call_procedure("p")


def test_malformed_non_sql_write_success_is_outcome_unknown(stub):
    stub.raw_canned["/triggers"] = [(200, b"{")]
    db = RemoteDatabase(stub.url())
    with pytest.raises(QueryOutcomeUnknownError):
        db.create_trigger({"name": "t"})


def test_create_table_refresh_failure_preserves_known_commit(stub):
    stub.canned["/kit/create_table"] = [
        (200, {"table_id": 7, "table_id_text": "7"})
    ]
    stub.canned_get["/kit/schema"] = [(200, SCHEMA), (200, {})]
    db = RemoteDatabase(stub.url())
    with pytest.raises(CommitOutcomeError) as caught:
        db.create_table({"name": "items", "columns": []})
    assert caught.value.committed is True
    assert caught.value.server_state == "invalid_response"


def test_non_sql_unknown_outcome_stays_typed_and_non_retryable(stub):
    stub.canned["/triggers"] = [
        (
            409,
            {
                "status": "outcome_unknown",
                "committed": None,
                "retryable": False,
                "error": {
                    "code": "QUERY_OUTCOME_UNKNOWN",
                    "message": "commit status unknown",
                },
            },
        )
    ]
    db = RemoteDatabase(stub.url())
    with pytest.raises(QueryOutcomeUnknownError) as caught:
        db.create_trigger({"name": "t"})
    assert caught.value.committed is None
    assert caught.value.retryable is False


def test_non_sql_commit_outcome_rejects_conflicting_exact_epoch(stub):
    response = _committed_non_sql_error()
    response["epoch"] = 41
    stub.kit_txn_responses.append((409, response))
    db = RemoteDatabase(stub.url())
    with pytest.raises(QueryOutcomeUnknownError) as caught:
        db.begin().insert("users", {"age": 1}).commit()
    assert caught.value.server_state == "invalid_outcome"
    assert caught.value.retryable is False


@pytest.mark.parametrize(
    "mutate",
    [
        lambda response: response.update({"committed": False}),
        lambda response: response.update({"committed_statements": 1}),
        lambda response: response.update({"unexpected": True}),
        lambda response: response["error"].update({"committed": False}),
    ],
)
def test_transaction_error_rejects_conflicting_or_unknown_durable_fields(stub, mutate):
    response = _committed_non_sql_error()
    mutate(response)
    stub.kit_txn_responses.append((409, response))
    db = RemoteDatabase(stub.url())
    with pytest.raises(QueryOutcomeUnknownError) as caught:
        db.begin().insert("users", {"age": 1}).commit()
    assert caught.value.committed is None
    assert caught.value.server_state == "invalid_outcome"
    assert caught.value.retryable is False


def test_idempotency_key_forwarded(stub):
    stub.kit_txn_responses.append(
        (
            200,
            {
                "status": "committed",
                "epoch": 1,
                "epoch_text": "1",
                "results": [
                    {"kind": "put", "row_id": None, "auto_inc": None, "row": None}
                ],
            },
        )
    )
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
                "next_cursor": None,
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

    stub.canned["/kit/query"] = [
        (
            200,
            {
                "rows": [],
                "truncated": False,
                "next_cursor": "cursor-without-more-rows",
            },
        )
    ]
    with pytest.raises(_MalformedHttpResponse, match="native query response fields were invalid"):
        db.query("users")


def test_create_table_forwards_body_and_returns_id(stub):
    stub.canned["/kit/create_table"] = [(200, {"table_id": 7, "table_id_text": "7"})]
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
