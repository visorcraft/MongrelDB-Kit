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
import base64
import math
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from .mongreldb_kit_py import (
    ConflictError,
    DuplicateError,
    ForeignKeyError,
    StorageError,
    TriggerValidationError,
    ValidationError,
    QueryCancelledError,
    QueryTimeoutError,
    QueryIdConflictError,
    TransactionAbortedError,
    TransportError,
    CommitOutcomeError,
    QueryOutcomeUnknownError,
    QueryRegistryFullError,
    ResultLimitExceededError,
    SerializationError,
    CapabilityUnsupportedError,
)

__all__ = [
    "CommitOutcomeError",
    "QueryOutcomeUnknownError",
    "RemoteDatabase",
    "RemoteTransaction",
    "RemoteSqlQueryHandle",
]

_SQL_RECOVERY_WINDOW = 2.0
_SQL_RECOVERY_REQUEST_TIMEOUT = 0.25
_SQL_RECOVERY_POLL_INTERVAL = 0.025
_MAX_U64 = 18_446_744_073_709_551_615
_MAX_CONTROL_JSON_RESPONSE_BYTES = 1024 * 1024
_MAX_JSON_RESPONSE_BYTES = 64 * 1024 * 1024


def _contains_http_header_control(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _strict_json_object(pairs: List[tuple[str, Any]]) -> Dict[str, Any]:
    value: Dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object key {key!r}")
        value[key] = item
    return value


def _strict_json_loads(value: str | bytes) -> Any:
    def reject_constant(constant: str) -> None:
        raise ValueError(f"invalid JSON number {constant}")

    return json.loads(
        value,
        object_pairs_hook=_strict_json_object,
        parse_constant=reject_constant,
    )


class _MalformedHttpResponse(StorageError):
    pass


def _strict_json_response(value: str | bytes, context: str) -> Any:
    try:
        return _strict_json_loads(value)
    except (UnicodeDecodeError, ValueError) as error:
        raise _MalformedHttpResponse(
            f"{context} was not valid strict JSON: {error}"
        ) from None


def _read_response(response: Any, limit: int, context: str) -> bytes:
    declared = response.headers.get("Content-Length")
    if declared is not None:
        try:
            length = int(declared)
        except ValueError:
            raise _MalformedHttpResponse(
                f"{context} had an invalid Content-Length"
            ) from None
        if length < 0 or length > limit:
            raise _MalformedHttpResponse(f"{context} exceeded {limit} bytes")
    value = response.read(limit + 1)
    if len(value) > limit:
        raise _MalformedHttpResponse(f"{context} exceeded {limit} bytes")
    return value


def _validate_sql_query_id_header(response: Any, expected_query_id: str) -> None:
    values = response.headers.get_all("x-mongreldb-query-id") or []
    if len(values) != 1 or values[0] != expected_query_id:
        raise _MalformedHttpResponse(
            "SQL response x-mongreldb-query-id does not match the request"
        )


class _RetryIdempotentSql(Exception):
    def __init__(self, outcome: QueryOutcomeUnknownError):
        super().__init__(str(outcome))
        self.outcome = outcome


def _commit_outcome_error(
    query_id: str, message: str, outcome: Dict[str, Any]
) -> CommitOutcomeError:
    error = CommitOutcomeError(message)
    error.query_id = query_id
    error.committed = outcome.get("committed")
    error.committed_statements = outcome.get("committed_statements")
    error.last_commit_epoch = None
    error.first_commit_statement_index = outcome.get("first_commit_statement_index")
    error.last_commit_statement_index = outcome.get("last_commit_statement_index")
    error.completed_statements = outcome.get("completed_statements")
    error.statement_index = outcome.get("statement_index")
    error.retryable = False
    return error


def _query_outcome_unknown(query_id: str, message: str) -> QueryOutcomeUnknownError:
    error = QueryOutcomeUnknownError(message)
    error.query_id = query_id
    error.committed = None
    error.committed_statements = None
    error.last_commit_epoch = None
    error.first_commit_statement_index = None
    error.last_commit_statement_index = None
    error.completed_statements = None
    error.statement_index = None
    error.cancel_outcome = None
    error.cancellation_reason = None
    error.server_state = None
    error.retryable = False
    return error


def _committed_txn_response_error(epoch: int, message: str) -> CommitOutcomeError:
    error = _commit_outcome_error(
        "unknown",
        message,
        {
            "committed": True,
            "committed_statements": None,
            "first_commit_statement_index": None,
            "last_commit_statement_index": None,
            "completed_statements": 0,
            "statement_index": 0,
        },
    )
    error.last_commit_epoch = epoch
    error.server_state = "invalid_response"
    return error


def _committed_write_response_error(message: str) -> CommitOutcomeError:
    error = _commit_outcome_error(
        "unknown",
        message,
        {
            "committed": True,
            "committed_statements": None,
            "first_commit_statement_index": None,
            "last_commit_statement_index": None,
            "completed_statements": 0,
            "statement_index": 0,
        },
    )
    error.server_state = "invalid_response"
    return error


def _outcome_epoch(outcome: Dict[str, Any]) -> Optional[int]:
    text = outcome.get("last_commit_epoch_text", outcome.get("epoch_text"))
    epoch = outcome.get("last_commit_epoch")
    if epoch is None:
        epoch = outcome.get("epoch")
    if text is not None:
        if not isinstance(text, str) or not text.isdigit():
            raise ValueError("invalid exact commit epoch")
        exact = int(text)
        if (
            str(exact) != text
            or exact > _MAX_U64
            or epoch is not None
            and epoch != exact
        ):
            raise ValueError("conflicting or non-canonical exact commit epoch")
        return exact
    if epoch is not None and (
        not _is_non_negative_int(epoch) or epoch > _MAX_U64
    ):
        raise ValueError("invalid numeric commit epoch")
    return epoch


def _is_non_negative_int(value: Any) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= _MAX_U64
    )


def _validate_procedure_call_response(value: Any) -> Dict[str, Any]:
    fields = {"status", "committed", "epoch", "epoch_text", "result"}
    if not isinstance(value, dict) or set(value) != fields or value.get("status") != "ok":
        raise _query_outcome_unknown("unknown", "invalid procedure call response")
    committed = value.get("committed")
    epoch = value.get("epoch")
    epoch_text = value.get("epoch_text")
    if not isinstance(committed, bool):
        raise _query_outcome_unknown("unknown", "invalid procedure call commit state")
    if committed:
        if (
            not _is_non_negative_int(epoch)
            or not isinstance(epoch_text, str)
            or not epoch_text.isdigit()
            or str(int(epoch_text)) != epoch_text
            or int(epoch_text) != epoch
        ):
            raise _query_outcome_unknown("unknown", "invalid procedure call commit epoch")
    elif epoch is not None or epoch_text is not None:
        raise _query_outcome_unknown("unknown", "non-committed procedure call contains epoch")
    return value


def _is_positive_int(value: Any) -> bool:
    return _is_non_negative_int(value) and value > 0


def _is_positive_duration(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _utf8_length(value: str) -> Optional[int]:
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        return None


def _normalize_query_id(value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 32
        or any(character not in "0123456789abcdefABCDEF" for character in value)
    ):
        raise ValueError("query_id must be exactly 32 hexadecimal characters")
    return value.lower()


def _has_only_keys(value: Dict[str, Any], allowed: tuple[str, ...]) -> bool:
    return set(value).issubset(allowed)


def _is_capabilities(value: Any) -> bool:
    if not isinstance(value, dict) or not _has_only_keys(
        value, ("sql_cancellation", "sql_idempotency", "sql_pagination")
    ):
        return False
    def booleans(name: str, fields: tuple[str, ...]) -> bool:
        if name not in value:
            return True
        entry = value[name]
        return (
            isinstance(entry, dict)
            and _has_only_keys(entry, ("version", *fields))
            and _is_non_negative_int(entry.get("version"))
            and entry["version"] <= 255
            and all(isinstance(entry.get(field), bool) for field in fields)
        )

    if not booleans(
        "sql_cancellation",
        (
            "client_query_ids",
            "cancel_endpoint",
            "query_status",
            "stream_disconnect_cancels",
            "pre_registration_cancel",
        ),
    ) or not booleans(
        "sql_idempotency",
        (
            "durable_pre_execution_intent",
            "replay_committed_receipt",
            "indeterminate_never_reexecutes",
        ),
    ):
        return False
    if "sql_pagination" not in value:
        return True
    pagination = value["sql_pagination"]
    return (
        isinstance(pagination, dict)
        and _has_only_keys(
            pagination,
            (
                "version",
                "continuation_endpoint",
                "retained_snapshot",
                "projection_required",
                "byte_and_token_hints",
            ),
        )
        and _is_non_negative_int(pagination.get("version"))
        and pagination["version"] <= 255
        and isinstance(pagination.get("continuation_endpoint"), str)
        and all(
            isinstance(pagination.get(field), bool)
            for field in (
                "retained_snapshot",
                "projection_required",
                "byte_and_token_hints",
            )
        )
    )


def _receipt_epoch(value: Dict[str, Any]) -> tuple[bool, Optional[int]]:
    numeric = value.get("last_commit_epoch")
    text = value.get("last_commit_epoch_text")
    if text is None:
        return (
            numeric is None
            or _is_non_negative_int(numeric)
            and numeric <= _MAX_U64,
            numeric,
        )
    if not isinstance(text, str) or not text.isdigit():
        return False, None
    epoch = int(text)
    if (
        str(epoch) != text
        or epoch > _MAX_U64
        or numeric is not None
        and numeric != epoch
    ):
        return False, None
    return True, epoch


def _is_sql_write_receipt(
    value: Any, query_id: str, expected_original_query_id: Optional[str] = None
) -> bool:
    if not isinstance(value, dict) or not isinstance(value.get("outcome"), dict):
        return False
    outcome = value["outcome"]
    if not _has_only_keys(
        value,
        (
            "query_id",
            "original_query_id",
            "status",
            "terminal_state",
            "detail",
            "server_state",
            "cancel_outcome",
            "cancellation_reason",
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "retryable",
            "idempotency_replayed",
            "idempotency_persisted",
            "idempotency_expires_at_ms",
            "outcome",
            "terminal_error",
        ),
    ) or not _has_only_keys(
        outcome,
        (
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "serialization",
        ),
    ):
        return False
    if not all(
        field in outcome
        for field in (
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "serialization",
        )
    ):
        return False
    optional_ints = (
        value.get("last_commit_epoch"),
        value.get("first_commit_statement_index"),
        value.get("last_commit_statement_index"),
        outcome.get("last_commit_epoch"),
        outcome.get("first_commit_statement_index"),
        outcome.get("last_commit_statement_index"),
    )
    terminal = value.get("terminal_error")
    if isinstance(terminal, dict) and not _has_only_keys(terminal, ("code", "category")):
        return False
    original_query_id = value.get("original_query_id")
    status_committed = {
        "completed": False,
        "committed": True,
        "committed_with_error": True,
        "partially_committed": True,
        "cancelled_after_commit": True,
        "deadline_after_commit": True,
    }.get(value.get("status"))
    shape_valid = (
        value.get("query_id") == query_id
        and isinstance(original_query_id, str)
        and len(original_query_id) == 32
        and all(char in "0123456789abcdefABCDEF" for char in original_query_id)
        and status_committed is not None
        and (
            "terminal_state" not in value
            or value.get("terminal_state") is None
            or value.get("terminal_state") == value.get("status")
        )
        and (
            "server_state" not in value
            or value.get("server_state") is None
            or value.get("server_state") in ("completed", "failed", "cancelled")
        )
        and (
            "cancel_outcome" not in value
            or value.get("cancel_outcome") is None
            or value.get("cancel_outcome") == "already_finished"
        )
        and (
            "cancellation_reason" not in value
            or value.get("cancellation_reason") is None
            or value.get("cancellation_reason")
            in (
                "none",
                "client_request",
                "deadline",
                "client_disconnected",
                "session_closed",
                "server_shutdown",
            )
        )
        and isinstance(value.get("committed"), bool)
        and _is_non_negative_int(value.get("committed_statements"))
        and _is_non_negative_int(value.get("completed_statements"))
        and _is_non_negative_int(value.get("statement_index"))
        and value["committed"] is outcome.get("committed")
        and value["committed_statements"] == outcome.get("committed_statements")
        and isinstance(value.get("retryable"), bool)
        and isinstance(value.get("idempotency_replayed"), bool)
        and isinstance(value.get("idempotency_persisted"), bool)
        and _is_non_negative_int(value.get("idempotency_expires_at_ms"))
        and isinstance(outcome.get("committed"), bool)
        and _is_non_negative_int(outcome.get("committed_statements"))
        and _is_non_negative_int(outcome.get("completed_statements"))
        and _is_non_negative_int(outcome.get("statement_index"))
        and outcome.get("serialization")
        in ("not_started", "in_progress", "succeeded", "failed", "unknown")
        and all(item is None or _is_non_negative_int(item) for item in optional_ints)
        and (
            terminal is None
            or isinstance(terminal, dict)
            and isinstance(terminal.get("code"), str)
            and bool(terminal["code"].strip())
            and terminal.get("category")
            in ("cancellation", "deadline", "result_limit", "serialization", "execution")
        )
    )
    if not shape_valid:
        return False
    if isinstance(terminal, dict) and (
        (terminal["category"] == "cancellation")
        != terminal["code"] in ("QUERY_CANCELLED", "QUERY_CANCELLED_AFTER_COMMIT")
        or (terminal["category"] == "deadline")
        != terminal["code"] in ("DEADLINE_EXCEEDED", "DEADLINE_AFTER_COMMIT")
        or (terminal["category"] == "result_limit")
        != (terminal["code"] == "RESULT_LIMIT_EXCEEDED")
        or (terminal["category"] == "serialization")
        != terminal["code"]
        in ("SERIALIZATION_FAILED", "SERIALIZATION_FAILED_AFTER_COMMIT")
    ):
        return False
    server_state = value.get("server_state")
    if server_state is not None:
        expected_state = {
            "completed": "completed",
            "committed": "completed",
            "committed_with_error": "failed",
            "partially_committed": "failed",
            "cancelled_after_commit": "cancelled",
            "deadline_after_commit": "cancelled",
        }.get(value["status"])
        if server_state != expected_state:
            return False
    reason = value.get("cancellation_reason")
    if reason is not None:
        if value["status"] == "cancelled_after_commit":
            reason_matches = reason not in ("none", "deadline")
        elif value["status"] == "deadline_after_commit":
            reason_matches = reason == "deadline"
        else:
            reason_matches = reason == "none"
        if not reason_matches:
            return False
    top_valid, top_epoch = _receipt_epoch(value)
    outcome_valid, outcome_epoch = _receipt_epoch(outcome)
    if not top_valid or not outcome_valid or top_epoch != outcome_epoch:
        return False
    if value["committed"] is not status_committed:
        return False
    terminal_matches = {
        "completed": terminal is None,
        "committed": terminal is None,
        "cancelled_after_commit": isinstance(terminal, dict)
        and terminal.get("code") == "QUERY_CANCELLED_AFTER_COMMIT"
        and terminal.get("category") == "cancellation",
        "deadline_after_commit": isinstance(terminal, dict)
        and terminal.get("code") == "DEADLINE_AFTER_COMMIT"
        and terminal.get("category") == "deadline",
        "committed_with_error": isinstance(terminal, dict),
        "partially_committed": isinstance(terminal, dict),
    }.get(value["status"], False)
    if not terminal_matches:
        return False
    for key in (
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
    ):
        if value.get(key) != outcome.get(key):
            return False
    first = value.get("first_commit_statement_index")
    first = outcome.get("first_commit_statement_index") if first is None else first
    last = value.get("last_commit_statement_index")
    last = outcome.get("last_commit_statement_index") if last is None else last
    if first is not None and last is not None and first > last:
        return False
    if value["committed"]:
        if (
            value["committed_statements"] == 0
            or top_epoch is None
            or value.get("last_commit_epoch_text") is None
            or outcome.get("last_commit_epoch_text") is None
            or first is None
            or last is None
        ):
            return False
    elif (
        value["committed_statements"] != 0
        or top_epoch is not None
        or first is not None
        or last is not None
    ):
        return False
    if first is not None and last is not None and (
        value["committed_statements"] > last - first + 1
        or last > value["statement_index"]
    ):
        return False
    if not (
        value["statement_index"]
        <= value["completed_statements"]
        <= value["statement_index"] + 1
    ):
        return False
    if expected_original_query_id is None:
        original_query_id_matches = (
            value["idempotency_replayed"] is True
            or original_query_id == query_id
        )
    elif value["idempotency_replayed"] is True:
        original_query_id_matches = original_query_id == expected_original_query_id
    else:
        original_query_id_matches = original_query_id == query_id
    return (
        value["idempotency_persisted"] is True
        and value["idempotency_expires_at_ms"] > 0
        and value["retryable"] is False
        and original_query_id_matches
    )


def _is_sql_page(value: Any, initial: Optional[Dict[str, Any]] = None) -> bool:
    if not isinstance(value, dict) or not isinstance(value.get("page"), dict):
        return False
    rows = value.get("rows")
    page = value["page"]
    limits = page.get("limits")
    projection = page.get("projection")
    next_cursor = value.get("next_cursor")
    if (
        not _has_only_keys(value, ("status", "rows", "next_cursor", "page"))
        or not _has_only_keys(
            page,
            (
                "offset",
                "row_count",
                "total_rows",
                "byte_count",
                "estimated_tokens",
                "limits",
                "projection",
                "expires_at_ms",
                "snapshot",
                "token_estimate",
            ),
        )
        or isinstance(limits, dict)
        and not _has_only_keys(limits, ("rows", "bytes", "tokens"))
    ):
        return False
    numbers = (
        page.get("offset"),
        page.get("row_count"),
        page.get("total_rows"),
        page.get("byte_count"),
        page.get("estimated_tokens"),
        page.get("expires_at_ms"),
    )
    shape_valid = (
        value.get("status") == "completed"
        and isinstance(rows, list)
        and all(isinstance(row, dict) for row in rows)
        and (next_cursor is None or isinstance(next_cursor, str) and bool(next_cursor))
        and all(_is_non_negative_int(number) for number in numbers)
        and page["row_count"] == len(rows)
        and page["offset"] <= page["total_rows"]
        and page["row_count"] <= page["total_rows"] - page["offset"]
        and isinstance(limits, dict)
        and all(_is_non_negative_int(limits.get(name)) for name in ("rows", "bytes", "tokens"))
        and isinstance(projection, list)
        and 1 <= len(projection) <= 128
        and all(
            isinstance(column, str)
            and bool(column)
            and column != "*"
            and _utf8_length(column) is not None
            and _utf8_length(column) <= 256
            for column in projection
        )
        and sum(_utf8_length(column) or 0 for column in projection) <= 16 * 1024
        and page.get("snapshot") == "retained_result"
        and page.get("token_estimate") == "ceil(projected_json_bytes/4)"
    )
    if not shape_valid:
        return False
    if len(set(projection)) != len(projection) or any(
        set(row) != set(projection) for row in rows
    ):
        return False
    byte_count = 2
    for index, row in enumerate(rows):
        encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        byte_count += (1 if index else 0) + len(encoded)
    if page["byte_count"] != byte_count or page["estimated_tokens"] != (byte_count + 3) // 4:
        return False
    if (
        any(limits[name] == 0 for name in ("rows", "bytes", "tokens"))
        or limits["bytes"] > _MAX_JSON_RESPONSE_BYTES
        or page["row_count"] > limits["rows"]
        or page["byte_count"] > limits["bytes"]
        or page["estimated_tokens"] > limits["tokens"]
    ):
        return False
    has_more = page["offset"] + page["row_count"] < page["total_rows"]
    cursor_bytes = _utf8_length(next_cursor) if isinstance(next_cursor, str) else None
    if (
        has_more
        and page["row_count"] == 0
        or has_more != isinstance(next_cursor, str)
        or isinstance(next_cursor, str)
        and (cursor_bytes is None or cursor_bytes > 2048)
    ):
        return False
    if page["expires_at_ms"] == 0:
        return False
    if initial is None:
        return True
    return (
        page["offset"] == 0
        and projection == initial["projection"]
        and limits["rows"] <= initial["page_size_rows"]
        and (
            initial.get("max_page_bytes") is None
            or limits["bytes"] <= initial["max_page_bytes"]
        )
        and (
            initial.get("max_page_tokens") is None
            or limits["tokens"] <= initial["max_page_tokens"]
        )
        and (
            initial.get("max_output_rows") is None
            or page["total_rows"] <= initial["max_output_rows"]
        )
        and (
            initial.get("max_output_bytes") is None
            or page["byte_count"] <= initial["max_output_bytes"]
        )
    )


_QUERY_STATES = {
    "queued",
    "planning",
    "executing",
    "streaming",
    "serializing",
    "commit_critical",
    "cancelling",
    "completed",
    "failed",
    "cancelled",
    "pre_cancelled",
    "finished",
}
_QUERY_STATUSES = {
    "running",
    "outcome_unknown",
    "completed",
    "failed_before_commit",
    "cancelled_before_commit",
    "deadline_before_commit",
    "cancelled_before_start",
    "committed",
    "committed_with_error",
    "partially_committed",
    "cancelled_after_commit",
    "deadline_after_commit",
    "finished",
}
_COMMITTED_QUERY_STATUSES = {
    "committed",
    "committed_with_error",
    "partially_committed",
    "cancelled_after_commit",
    "deadline_after_commit",
}


def _is_query_not_found(value: Any, query_id: str) -> bool:
    top_fields = {
        "query_id",
        "status",
        "terminal_state",
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "cancel_outcome",
        "cancellation_reason",
        "retryable",
        "server_state",
        "outcome",
        "error",
    }
    outcome_fields = {
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "serialization",
    }
    error_fields = {"code", "message", "query_id", "committed", "retryable"}
    if not isinstance(value, dict) or set(value) != top_fields:
        return False
    outcome = value.get("outcome")
    error = value.get("error")
    if (
        not isinstance(outcome, dict)
        or set(outcome) != outcome_fields
        or not isinstance(error, dict)
        or set(error) != error_fields
    ):
        return False
    nullable = (
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
    )
    return (
        value["query_id"] == query_id
        and value["status"] == "unknown"
        and value["terminal_state"] is None
        and value["cancel_outcome"] == "not_found"
        and value["cancellation_reason"] is None
        and value["retryable"] is False
        and value["server_state"] == "not_found"
        and all(value[field] is None and outcome[field] is None for field in nullable)
        and outcome["serialization"] == "unknown"
        and error["code"] == "QUERY_NOT_FOUND"
        and isinstance(error["message"], str)
        and bool(error["message"])
        and error["query_id"] == query_id
        and error["committed"] is None
        and error["retryable"] is False
    )


def _is_query_status(value: Any, query_id: str) -> bool:
    if not isinstance(value, dict):
        return False
    raw_outcome = value.get("outcome")
    if not isinstance(raw_outcome, dict):
        return False
    outcome = raw_outcome
    server_state = value.get("server_state", "")
    terminal = value.get("terminal_error")
    if not _has_only_keys(
        value,
        (
            "query_id",
            "status",
            "state",
            "server_state",
            "terminal_state",
            "operation",
            "started_ms_ago",
            "deadline_ms_remaining",
            "session_id",
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "cancel_outcome",
            "retryable",
            "terminal_error",
            "cancellation_reason",
            "outcome",
            "trace",
        ),
    ) or not _has_only_keys(
        outcome,
        (
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "serialization",
        ),
    ) or isinstance(terminal, dict) and not _has_only_keys(terminal, ("code", "category")):
        return False
    if not all(
        field in outcome
        for field in (
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "serialization",
        )
    ):
        return False
    trace = value.get("trace")
    if "trace" in value:
        if not isinstance(trace, dict) or not _has_only_keys(
            trace,
            (
                "queue_duration_us",
                "planning_duration_us",
                "execution_duration_us",
                "serialization_duration_us",
                "cancel_requested_phase",
                "cancel_observed_phase",
                "commit_fence_outcome",
            ),
        ):
            return False
        trace_numbers = (
            trace.get("queue_duration_us"),
            trace.get("planning_duration_us"),
            trace.get("execution_duration_us"),
            trace.get("serialization_duration_us"),
        )
        if (
            any(
                number is not None and not _is_non_negative_int(number)
                for number in trace_numbers
            )
            or any(
                phase is not None and phase not in _QUERY_STATES
                for phase in (
                    trace.get("cancel_requested_phase"),
                    trace.get("cancel_observed_phase"),
                )
            )
            or trace.get("commit_fence_outcome") is not None
            and trace.get("commit_fence_outcome")
            not in ("not_reached", "cancel_won", "commit_won")
        ):
            return False
    committed = value.get("committed")
    outcome_committed = outcome.get("committed")
    optional_ints = (
        value.get("committed_statements"),
        value.get("first_commit_statement_index"),
        value.get("last_commit_statement_index"),
        value.get("completed_statements"),
        value.get("statement_index"),
        outcome.get("committed_statements"),
        outcome.get("first_commit_statement_index"),
        outcome.get("last_commit_statement_index"),
        outcome.get("completed_statements"),
        outcome.get("statement_index"),
        value.get("last_commit_epoch"),
        outcome.get("last_commit_epoch"),
    )
    if (
        value.get("query_id") != query_id
        or value.get("detail") not in (None, "compact")
        or value.get("status") not in _QUERY_STATUSES
        or value.get("state") not in _QUERY_STATES
        or not isinstance(server_state, str)
        or server_state
        and (server_state not in _QUERY_STATES or server_state != value["state"])
        or value.get("terminal_state") is not None
        and value["terminal_state"] != value["status"]
        or value.get("started_ms_ago") is not None
        and not _is_non_negative_int(value["started_ms_ago"])
        or value.get("deadline_ms_remaining") is not None
        and not _is_non_negative_int(value["deadline_ms_remaining"])
        or value.get("session_id") is not None
        and (
            not isinstance(value["session_id"], str)
            or len(value["session_id"]) > 256
        )
        or committed is not None
        and not isinstance(committed, bool)
        or outcome_committed is not None
        and not isinstance(outcome_committed, bool)
        or not isinstance(value.get("retryable"), bool)
        or not isinstance(value.get("cancellation_reason"), str)
        or outcome.get("serialization")
        not in ("not_started", "in_progress", "succeeded", "failed", "unknown")
        or any(item is not None and not _is_non_negative_int(item) for item in optional_ints)
        or terminal is not None
        and (
            not isinstance(terminal, dict)
            or not isinstance(terminal.get("code"), str)
            or not terminal["code"].strip()
            or terminal.get("category")
            not in ("cancellation", "deadline", "result_limit", "serialization", "execution")
        )
    ):
        return False
    top_valid, top_epoch = _receipt_epoch(value)
    outcome_valid, outcome_epoch = _receipt_epoch(outcome)
    if (
        not top_valid
        or not outcome_valid
        or top_epoch != outcome_epoch
        or committed is not outcome_committed
        or value.get("committed_statements") != outcome.get("committed_statements")
        or value.get("first_commit_statement_index")
        != outcome.get("first_commit_statement_index")
        or value.get("last_commit_statement_index")
        != outcome.get("last_commit_statement_index")
        or value.get("completed_statements") != outcome.get("completed_statements")
        or value.get("statement_index") != outcome.get("statement_index")
    ):
        return False
    status = value["status"]
    state = value["state"]
    state_matches_status = {
        "running": state
        in (
            "queued",
            "planning",
            "executing",
            "streaming",
            "serializing",
            "commit_critical",
            "cancelling",
        ),
        "committed": state
        in (
            "planning",
            "executing",
            "streaming",
            "serializing",
            "commit_critical",
            "cancelling",
            "completed",
        ),
        "completed": state == "completed",
        "failed_before_commit": state == "failed",
        "committed_with_error": state == "failed",
        "partially_committed": state == "failed",
        "outcome_unknown": state == "failed",
        "cancelled_before_commit": state == "cancelled",
        "deadline_before_commit": state == "cancelled",
        "cancelled_after_commit": state == "cancelled",
        "deadline_after_commit": state == "cancelled",
        "cancelled_before_start": state == "pre_cancelled",
        "finished": state == "finished",
    }.get(status, False)
    if not state_matches_status:
        return False
    expected_terminal = (
        None
        if status == "running"
        or status == "finished"
        or status == "committed" and state != "completed"
        else status
    )
    if value.get("terminal_state") != expected_terminal:
        return False
    committed_statements = value.get("committed_statements")
    first = value.get("first_commit_statement_index")
    last = value.get("last_commit_statement_index")
    completed = value.get("completed_statements")
    statement = value.get("statement_index")
    if committed is True:
        if (
            status not in _COMMITTED_QUERY_STATUSES
            or committed_statements in (None, 0)
            or top_epoch is None
            or value.get("last_commit_epoch_text") is None
            or outcome.get("last_commit_epoch_text") is None
            or first is None
            or last is None
            or completed is None
            or statement is None
        ):
            return False
    elif committed is False:
        if (
            status in _COMMITTED_QUERY_STATUSES
            or status in ("outcome_unknown", "finished")
            or committed_statements != 0
            or top_epoch is not None
            or first is not None
            or last is not None
            or completed is None
            or statement is None
        ):
            return False
    elif (
        status not in ("outcome_unknown", "finished")
        or committed_statements is not None
        or top_epoch is not None
        or first is not None
        or last is not None
        or completed is not None
        or statement is not None
    ):
        return False
    if (
        first is not None
        and last is not None
        and committed_statements is not None
        and statement is not None
        and (
            first > last
            or committed_statements > last - first + 1
            or last > statement
        )
    ):
        return False
    if completed is not None and statement is not None and not (
        statement <= completed <= statement + 1
    ):
        return False
    expected_cancel = {
        "cancelling": "accepted",
        "commit_critical": "too_late",
        "completed": "already_finished",
        "failed": "already_finished",
        "cancelled": "already_finished",
        "finished": "already_finished",
        "pre_cancelled": "pre_cancelled",
    }.get(state)
    if value.get("cancel_outcome") != expected_cancel:
        return False
    terminal_error = value.get("terminal_error")
    terminal_code = terminal_error.get("code") if isinstance(terminal_error, dict) else None
    terminal_category = (
        terminal_error.get("category") if isinstance(terminal_error, dict) else None
    )
    if status in ("running", "completed", "committed", "finished"):
        terminal_matches = terminal_error is None
    elif status == "outcome_unknown":
        terminal_matches = (
            terminal_code == "QUERY_OUTCOME_UNKNOWN"
            and terminal_category == "execution"
        )
    elif status in ("cancelled_before_commit", "cancelled_before_start"):
        terminal_matches = (
            terminal_code == "QUERY_CANCELLED"
            and terminal_category == "cancellation"
        )
    elif status == "cancelled_after_commit":
        terminal_matches = (
            terminal_code == "QUERY_CANCELLED_AFTER_COMMIT"
            and terminal_category == "cancellation"
        )
    elif status == "deadline_before_commit":
        terminal_matches = (
            terminal_code == "DEADLINE_EXCEEDED" and terminal_category == "deadline"
        )
    elif status == "deadline_after_commit":
        terminal_matches = (
            terminal_code == "DEADLINE_AFTER_COMMIT" and terminal_category == "deadline"
        )
    else:
        terminal_matches = terminal_error is not None
    if not terminal_matches:
        return False
    if isinstance(terminal_error, dict) and (
        (terminal_category == "cancellation")
        != (terminal_code in ("QUERY_CANCELLED", "QUERY_CANCELLED_AFTER_COMMIT"))
        or (terminal_category == "deadline")
        != (terminal_code in ("DEADLINE_EXCEEDED", "DEADLINE_AFTER_COMMIT"))
    ):
        return False
    retryable = terminal_code in (
        "IDEMPOTENCY_STORE_FULL",
        "IDEMPOTENCY_STORE_UNAVAILABLE",
    )
    if value["retryable"] is not retryable:
        return False
    reason = value["cancellation_reason"]
    if reason not in (
        "none",
        "client_request",
        "deadline",
        "client_disconnected",
        "session_closed",
        "server_shutdown",
    ):
        return False
    if status in ("deadline_before_commit", "deadline_after_commit"):
        return reason == "deadline"
    if status in (
        "cancelled_before_commit",
        "cancelled_before_start",
        "cancelled_after_commit",
    ) or status in ("running", "committed") and state == "cancelling":
        return reason != "none"
    return reason == "none" or state == "commit_critical"


def _invalid_query_status(query_id: str, message: str) -> QueryOutcomeUnknownError:
    error = _query_outcome_unknown(query_id, f"invalid query status response: {message}")
    error.server_state = "invalid_status"
    return error


def _cancel_outcome(value: Any) -> Optional[str]:
    return {
        "accepted": "accepted",
        "cancellation_requested": "accepted",
        "already_cancelling": "already_cancelling",
        "cancelling": "already_cancelling",
        "too_late": "too_late",
        "commit_critical": "too_late",
        "already_finished": "already_finished",
        "finished": "already_finished",
        "not_found": "not_found",
        "pre_cancelled": "pre_cancelled",
    }.get(value)


def _validate_cancel_response(
    status: int, query_id: str, body: Any
) -> Dict[str, Any]:
    if not isinstance(body, dict) or body.get("query_id") != query_id:
        raise _MalformedHttpResponse("cancellation query_id does not match the request")
    if not _has_only_keys(
        body,
        (
            "query_id",
            "state",
            "cancel_outcome",
            "status",
            "terminal_state",
            "code",
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "cancellation_reason",
            "retryable",
            "server_state",
            "outcome",
            "error",
            "terminal_error",
        ),
    ):
        raise _MalformedHttpResponse("cancellation response has unknown fields")
    nested_outcome = body.get("outcome")
    if nested_outcome is not None and (
        not isinstance(nested_outcome, dict)
        or not _has_only_keys(
            nested_outcome,
            (
                "committed",
                "committed_statements",
                "last_commit_epoch",
                "last_commit_epoch_text",
                "first_commit_statement_index",
                "last_commit_statement_index",
                "completed_statements",
                "statement_index",
                "serialization",
            ),
        )
    ):
        raise _MalformedHttpResponse("cancellation outcome has unknown fields")
    nested_error = body.get("error")
    if nested_error is not None and (
        not isinstance(nested_error, dict)
        or not _has_only_keys(
            nested_error, ("code", "message", "query_id", "committed", "retryable")
        )
    ):
        raise _MalformedHttpResponse("cancellation error has unknown fields")
    if nested_outcome is not None:
        required = (
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "serialization",
        )
        for field in required:
            if field not in nested_outcome:
                raise _MalformedHttpResponse(
                    f"cancellation outcome {field} is missing"
                )
        outcome_epoch_valid, outcome_epoch = _receipt_epoch(nested_outcome)
        if (
            nested_outcome.get("committed") is not None
            and not isinstance(nested_outcome.get("committed"), bool)
            or any(
                nested_outcome.get(field) is not None
                and not _is_non_negative_int(nested_outcome.get(field))
                for field in (
                    "committed_statements",
                    "first_commit_statement_index",
                    "last_commit_statement_index",
                    "completed_statements",
                    "statement_index",
                )
            )
            or not outcome_epoch_valid
            or nested_outcome.get("serialization")
            not in ("not_started", "in_progress", "succeeded", "failed", "unknown")
        ):
            raise _MalformedHttpResponse("cancellation outcome fields are invalid")
    else:
        outcome_epoch = None
    if (
        "committed" in body
        and body.get("committed") is not None
        and not isinstance(body.get("committed"), bool)
    ):
        raise _MalformedHttpResponse("cancellation committed field is invalid")
    for field in (
        "committed_statements",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
    ):
        if field in body and body.get(field) is not None and not _is_non_negative_int(
            body.get(field)
        ):
            raise _MalformedHttpResponse(f"cancellation {field} is invalid")
        if (
            nested_outcome is not None
            and field in body
            and body.get(field) != nested_outcome.get(field)
        ):
            raise _MalformedHttpResponse(
                f"cancellation {field} disagrees with outcome"
            )
    if "last_commit_epoch" in body or "last_commit_epoch_text" in body:
        top_epoch_valid, top_epoch = _receipt_epoch(body)
        if not top_epoch_valid:
            raise _MalformedHttpResponse("cancellation commit epoch is invalid")
    else:
        top_epoch = None
    if nested_outcome is not None and (
        "committed" in body
        and body.get("committed") is not nested_outcome.get("committed")
        or ("last_commit_epoch" in body or "last_commit_epoch_text" in body)
        and top_epoch != outcome_epoch
    ):
        raise _MalformedHttpResponse("cancellation outcome metadata disagrees")
    if "status" in body and (
        not isinstance(body.get("status"), str)
        or body["status"] not in _QUERY_STATUSES | {"unknown"}
    ):
        raise _MalformedHttpResponse("cancellation status is invalid")
    if body.get("terminal_state") is not None and (
        not isinstance(body["terminal_state"], str)
        or body["terminal_state"] != body.get("status")
    ):
        raise _MalformedHttpResponse("cancellation terminal state is invalid")
    if "retryable" in body and not isinstance(body.get("retryable"), bool):
        raise _MalformedHttpResponse("cancellation retryable field is invalid")
    if "server_state" in body and (
        not isinstance(body.get("server_state"), str)
        or body["server_state"] not in _QUERY_STATES | {"not_found"}
    ):
        raise _MalformedHttpResponse("cancellation server state is invalid")
    if body.get("cancellation_reason") is not None and body[
        "cancellation_reason"
    ] not in (
        "none",
        "client_request",
        "deadline",
        "client_disconnected",
        "session_closed",
        "server_shutdown",
    ):
        raise _MalformedHttpResponse("cancellation reason is invalid")
    if nested_error is not None and (
        not isinstance(nested_error.get("code"), str)
        or not nested_error["code"]
        or not isinstance(nested_error.get("message"), str)
        or not nested_error["message"]
        or nested_error.get("query_id") is not None
        and not isinstance(nested_error["query_id"], str)
        or "committed" in nested_error
        and nested_error.get("committed") is not None
        and not isinstance(nested_error.get("committed"), bool)
        or "retryable" in nested_error
        and not isinstance(nested_error.get("retryable"), bool)
    ):
        raise _MalformedHttpResponse("cancellation error fields are invalid")
    terminal_error = body.get("terminal_error")
    if terminal_error is not None and (
        not isinstance(terminal_error, dict)
        or not _has_only_keys(terminal_error, ("code", "category"))
        or not isinstance(terminal_error.get("code"), str)
        or not terminal_error["code"]
        or not isinstance(terminal_error.get("category"), str)
        or not terminal_error["category"]
    ):
        raise _MalformedHttpResponse("cancellation terminal error fields are invalid")
    raw_outcome = body.get("cancel_outcome")
    raw_state = body.get("state")
    outcome = _cancel_outcome(raw_outcome)
    state = _cancel_outcome(raw_state)
    if outcome == "not_found" and raw_state is None and body.get("server_state") == "not_found":
        state = "not_found"
    if raw_outcome is None or state is None:
        raise _MalformedHttpResponse("cancellation state and outcome are required")
    if raw_outcome is not None and outcome is None:
        raise _MalformedHttpResponse("cancellation cancel_outcome is invalid")
    if raw_state is not None and state is None:
        raise _MalformedHttpResponse("cancellation state is invalid")
    if outcome != state:
        raise _MalformedHttpResponse("cancellation state and outcome disagree")
    compatible = (
        status == 202
        and outcome in ("accepted", "pre_cancelled")
        or status == 200
        and outcome in ("already_cancelling", "already_finished")
        or status == 409
        and outcome == "too_late"
        or status == 404
        and outcome == "not_found"
    )
    if not compatible:
        raise _MalformedHttpResponse("cancellation HTTP status and outcome disagree")
    if outcome == "pre_cancelled":
        if terminal_error is not None and (
            terminal_error.get("code") != "QUERY_CANCELLED"
            or terminal_error.get("category") != "cancellation"
        ):
            raise _MalformedHttpResponse(
                "cancellation terminal error disagrees with outcome"
            )
    elif terminal_error is not None:
        raise _MalformedHttpResponse(
            "cancellation terminal error disagrees with outcome"
        )
    return body


def _coded_storage(code: str, message: str) -> StorageError:
    error = StorageError(message)
    error.code = code
    return error


def _attach_query_metadata(
    error: Exception, env: Dict[str, Any], query_id: str, code: str
) -> Exception:
    outcome = env.get("outcome")
    outcome = outcome if isinstance(outcome, dict) else {}
    protocol_error = env.get("error")
    protocol_error = protocol_error if isinstance(protocol_error, dict) else {}

    def invalid(message: str) -> QueryOutcomeUnknownError:
        unknown = _query_outcome_unknown(query_id, message)
        unknown.code = "QUERY_OUTCOME_UNKNOWN"
        unknown.server_state = "invalid_outcome"
        unknown.retryable = False
        return unknown

    def exact(field: str, *sources: Dict[str, Any]) -> Any:
        values = [source[field] for source in sources if source.get(field) is not None]
        if any(value != values[0] for value in values[1:]):
            raise ValueError(f"conflicting {field}")
        return values[0] if values else None

    try:
        committed = exact("committed", env, outcome, protocol_error)
        if committed is not None and not isinstance(committed, bool):
            raise ValueError("committed is not a boolean")
        implied = {
            "committed": True,
            "aborted": False,
            "outcome_unknown": None,
        }.get(env.get("status"), committed)
        if committed is not None and implied is not None and committed is not implied:
            raise ValueError("status and committed disagree")
        committed = committed if committed is not None else implied
        outcome_known = exact("outcome_known", env, outcome)
        if outcome_known is not None and (
            not isinstance(outcome_known, bool)
            or outcome_known is not (committed is not None)
        ):
            raise ValueError("outcome_known disagrees with committed")
        committed_statements = exact("committed_statements", env, outcome)
        first_commit_statement_index = exact(
            "first_commit_statement_index", env, outcome
        )
        last_commit_statement_index = exact(
            "last_commit_statement_index", env, outcome
        )
        completed_statements = exact("completed_statements", env, outcome)
        statement_index = exact("statement_index", env, outcome)
        for name, value in (
            ("committed_statements", committed_statements),
            ("first_commit_statement_index", first_commit_statement_index),
            ("last_commit_statement_index", last_commit_statement_index),
            ("completed_statements", completed_statements),
            ("statement_index", statement_index),
        ):
            if value is not None and (
                not _is_non_negative_int(value) or value > _MAX_U64
            ):
                raise ValueError(f"invalid {name}")
        epochs = []
        for source in (env, outcome):
            epoch = _outcome_epoch(source)
            if epoch is not None:
                epochs.append(epoch)
            numeric = source.get("epoch")
            last_numeric = source.get("last_commit_epoch")
            if (
                numeric is not None
                and last_numeric is not None
                and numeric != last_numeric
            ):
                raise ValueError("conflicting commit epochs")
            text = source.get("epoch_text")
            last_text = source.get("last_commit_epoch_text")
            if text is not None and last_text is not None and text != last_text:
                raise ValueError("conflicting commit epoch text")
        if any(epoch != epochs[0] for epoch in epochs[1:]):
            raise ValueError("conflicting commit epochs")
        last_commit_epoch = epochs[0] if epochs else None
        retryable = exact("retryable", env, protocol_error)
        if retryable is not None and not isinstance(retryable, bool):
            raise ValueError("retryable is not a boolean")
        if committed is True and last_commit_epoch is None:
            raise ValueError("committed response lacks exact commit epoch")
        if committed is False and last_commit_epoch is not None:
            raise ValueError("non-committed response contains commit epoch")
        if code in (
            "COMMIT_OUTCOME",
            "QUERY_CANCELLED_AFTER_COMMIT",
            "DEADLINE_AFTER_COMMIT",
            "SERIALIZATION_FAILED_AFTER_COMMIT",
        ) and committed is not True:
            raise ValueError(f"{code} does not prove a commit")
        if code in (
            "QUERY_CANCELLED",
            "DEADLINE_EXCEEDED",
            "SERIALIZATION_FAILED",
        ) and committed is not False:
            raise ValueError(f"{code} has unknown commit outcome")
        if code == "QUERY_OUTCOME_UNKNOWN" and committed is not None:
            raise ValueError("unknown outcome claims a commit decision")
    except (TypeError, ValueError) as failure:
        return invalid(f"invalid commit outcome metadata: {failure}")
    error.code = code
    error.query_id = query_id
    error.committed = committed
    error.committed_statements = committed_statements
    error.last_commit_epoch = last_commit_epoch
    error.first_commit_statement_index = first_commit_statement_index
    error.last_commit_statement_index = last_commit_statement_index
    error.completed_statements = completed_statements
    error.statement_index = statement_index
    error.cancel_outcome = env.get("cancel_outcome")
    error.cancellation_reason = env.get("cancellation_reason")
    error.server_state = env.get("server_state")
    error.retryable = retryable if retryable is not None else False
    return error


def _is_sql_error_envelope(value: Any, query_id: str) -> bool:
    if not isinstance(value, dict):
        return False
    outcome = value.get("outcome")
    error = value.get("error")
    if not isinstance(outcome, dict) or not isinstance(error, dict):
        return False
    if not _has_only_keys(
        value,
        (
            "query_id",
            "status",
            "terminal_state",
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "cancel_outcome",
            "cancellation_reason",
            "retryable",
            "server_state",
            "outcome",
            "error",
            "max_rows",
            "max_bytes",
        ),
    ) or not _has_only_keys(
        outcome,
        (
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "serialization",
        ),
    ) or not _has_only_keys(
        error,
        (
            "code",
            "message",
            "query_id",
            "committed",
            "retryable",
            "max_rows",
            "max_bytes",
        ),
    ):
        return False
    if not all(
        field in outcome
        for field in (
            "committed",
            "committed_statements",
            "last_commit_epoch",
            "last_commit_epoch_text",
            "first_commit_statement_index",
            "last_commit_statement_index",
            "completed_statements",
            "statement_index",
            "serialization",
        )
    ):
        return False
    code = error.get("code")
    status = value.get("status")
    if (
        value.get("query_id") != query_id
        or error.get("query_id") != query_id
        or not isinstance(code, str)
        or not code.strip()
        or not isinstance(error.get("message"), str)
        or not error["message"].strip()
        or not isinstance(status, str)
        or value.get("terminal_state") != status
        or outcome.get("serialization")
        not in ("not_started", "in_progress", "succeeded", "failed", "unknown")
        or not isinstance(value.get("retryable"), bool)
        or error.get("retryable") is not value["retryable"]
    ):
        return False
    committed = value.get("committed")
    if committed is not None and not isinstance(committed, bool):
        return False
    if committed is not outcome.get("committed") or committed is not error.get("committed"):
        return False
    fields = (
        "committed_statements",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
    )
    if any(
        value.get(name) != outcome.get(name)
        or value.get(name) is not None and not _is_non_negative_int(value.get(name))
        for name in fields
    ):
        return False
    for name in ("max_rows", "max_bytes"):
        top_limit = value.get(name)
        error_limit = error.get(name)
        if (
            top_limit is not None
            and (not _is_non_negative_int(top_limit) or top_limit == 0)
            or error_limit is not None
            and (not _is_non_negative_int(error_limit) or error_limit == 0)
            or top_limit is not None
            and error_limit is not None
            and top_limit != error_limit
        ):
            return False
    top_valid, top_epoch = _receipt_epoch(value)
    outcome_valid, outcome_epoch = _receipt_epoch(outcome)
    if not top_valid or not outcome_valid or top_epoch != outcome_epoch:
        return False
    committed_statements = value.get("committed_statements")
    first = value.get("first_commit_statement_index")
    last = value.get("last_commit_statement_index")
    completed = value.get("completed_statements")
    statement = value.get("statement_index")
    unknown = code == "QUERY_OUTCOME_UNKNOWN"
    if committed is True:
        if (
            unknown
            or status not in _COMMITTED_QUERY_STATUSES
            or committed_statements in (None, 0)
            or top_epoch is None
            or value.get("last_commit_epoch_text") is None
            or outcome.get("last_commit_epoch_text") is None
            or first is None
            or last is None
            or completed is None
            or statement is None
        ):
            return False
    elif committed is False:
        if (
            unknown
            or status
            not in (
                "failed_before_commit",
                "cancelled_before_commit",
                "deadline_before_commit",
                "cancelled_before_start",
            )
            or committed_statements != 0
            or top_epoch is not None
            or first is not None
            or last is not None
            or completed is None
            or statement is None
        ):
            return False
    elif (
        not unknown
        or status != "outcome_unknown"
        or any(value.get(name) is not None for name in fields)
        or top_epoch is not None
        or value["retryable"] is not False
    ):
        return False
    if (
        first is not None
        and last is not None
        and (
            first > last
            or committed_statements > last - first + 1
            or last > statement
        )
    ):
        return False
    if completed is not None and not statement <= completed <= statement + 1:
        return False
    expected_retryable = code in (
        "QUERY_REGISTRY_FULL",
        "IDEMPOTENCY_STORE_FULL",
        "IDEMPOTENCY_STORE_UNAVAILABLE",
    )
    if value["retryable"] is not expected_retryable:
        return False
    code_matches_status = {
        "QUERY_OUTCOME_UNKNOWN": status == "outcome_unknown",
        "QUERY_CANCELLED_AFTER_COMMIT": status == "cancelled_after_commit"
        and committed is True,
        "DEADLINE_AFTER_COMMIT": status == "deadline_after_commit"
        and committed is True,
        "QUERY_CANCELLED": status
        in ("cancelled_before_commit", "cancelled_before_start"),
        "DEADLINE_EXCEEDED": status == "deadline_before_commit",
        "COMMIT_OUTCOME": committed is True,
        "SERIALIZATION_FAILED_AFTER_COMMIT": committed is True,
    }.get(code, True)
    status_matches_code = {
        "outcome_unknown": code == "QUERY_OUTCOME_UNKNOWN",
        "cancelled_after_commit": code == "QUERY_CANCELLED_AFTER_COMMIT",
        "deadline_after_commit": code == "DEADLINE_AFTER_COMMIT",
        "cancelled_before_commit": code == "QUERY_CANCELLED",
        "cancelled_before_start": code == "QUERY_CANCELLED",
        "deadline_before_commit": code == "DEADLINE_EXCEEDED",
    }.get(status, True)
    return code_matches_status and status_matches_code


def _is_sql_cursor_error_envelope(value: Any) -> bool:
    count_fields = (
        "committed_statements",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
    )
    top_fields = {
        "status",
        "terminal_state",
        "server_state",
        "committed",
        *count_fields,
        "last_commit_epoch",
        "last_commit_epoch_text",
        "cancel_outcome",
        "cancellation_reason",
        "retryable",
        "outcome",
        "error",
    }
    outcome_fields = {
        "committed",
        *count_fields,
        "last_commit_epoch",
        "last_commit_epoch_text",
        "serialization",
    }
    error_fields = {"code", "message", "committed", "retryable"}
    if not isinstance(value, dict) or set(value) != top_fields:
        return False
    outcome = value.get("outcome")
    error = value.get("error")
    if (
        not isinstance(outcome, dict)
        or set(outcome) != outcome_fields
        or not isinstance(error, dict)
        or set(error) != error_fields
    ):
        return False
    return (
        value["status"] == "failed_before_commit"
        and value["terminal_state"] == "failed_before_commit"
        and value["server_state"] == "failed"
        and value["committed"] is False
        and outcome["committed"] is False
        and value["committed_statements"] == 0
        and outcome["committed_statements"] == 0
        and value["last_commit_epoch"] is None
        and outcome["last_commit_epoch"] is None
        and value["last_commit_epoch_text"] is None
        and outcome["last_commit_epoch_text"] is None
        and value["first_commit_statement_index"] is None
        and outcome["first_commit_statement_index"] is None
        and value["last_commit_statement_index"] is None
        and outcome["last_commit_statement_index"] is None
        and value["completed_statements"] == 0
        and outcome["completed_statements"] == 0
        and value["statement_index"] == 0
        and outcome["statement_index"] == 0
        and value["cancel_outcome"] is None
        and value["cancellation_reason"] is None
        and value["retryable"] is False
        and outcome["serialization"] == "not_started"
        and isinstance(error["code"], str)
        and bool(error["code"])
        and isinstance(error["message"], str)
        and bool(error["message"])
        and error["committed"] is False
        and error["retryable"] is False
    )


def _is_txn_error_envelope(value: Any) -> bool:
    if not isinstance(value, dict) or not isinstance(value.get("error"), dict):
        return False
    error = value["error"]
    if not _has_only_keys(error, ("code", "message", "op_index")):
        return False
    if (
        not isinstance(error.get("code"), str)
        or not error["code"].strip()
        or not isinstance(error.get("message"), str)
        or error.get("op_index") is not None
        and not _is_non_negative_int(error.get("op_index"))
    ):
        return False
    status = value.get("status")
    if status == "aborted" and "committed" not in value:
        return set(value) == {"status", "error"}
    if status == "aborted":
        return (
            set(value) == {"status", "committed", "retryable", "error"}
            and value.get("committed") is False
            and isinstance(value.get("retryable"), bool)
        )
    if status == "committed":
        if set(value) not in (
            {"status", "committed", "epoch", "epoch_text", "retryable", "error"},
            {
                "status",
                "committed",
                "epoch",
                "epoch_text",
                "results",
                "retryable",
                "error",
            },
        ):
            return False
        try:
            epoch = _outcome_epoch(value)
        except (TypeError, ValueError):
            return False
        return (
            value.get("committed") is True
            and value.get("retryable") is False
            and error.get("code") == "COMMIT_OUTCOME"
            and epoch is not None
            and ("results" not in value or isinstance(value["results"], list))
        )
    if status == "outcome_unknown":
        if set(value) != {
            "status",
            "committed",
            "epoch",
            "epoch_text",
            "retryable",
            "error",
        }:
            return False
        try:
            _outcome_epoch(value)
        except (TypeError, ValueError):
            return False
        return (
            value.get("committed") is None
            and value.get("retryable") is False
            and error.get("code") == "QUERY_OUTCOME_UNKNOWN"
        )
    return False


def _map_error(
    status: int,
    body: str,
    expected_query_id: Optional[str] = None,
    expected_txn: bool = False,
    expected_cursor: bool = False,
) -> Exception:
    durable_response = False
    try:
        env = _strict_json_loads(body)
        durable_response = isinstance(env, dict) and env.get("status") in (
            "aborted",
            "committed",
            "outcome_unknown",
        )
        if expected_query_id is not None and not _is_sql_error_envelope(
            env, expected_query_id
        ):
            raise ValueError("malformed SQL error envelope")
        if expected_cursor and not _is_sql_cursor_error_envelope(env):
            raise ValueError("malformed SQL cursor error envelope")
        if (
            expected_txn
            or durable_response
        ) and not _is_txn_error_envelope(env):
            raise ValueError("malformed transaction error envelope")
        error = env.get("error")
        if not isinstance(error, dict) or not isinstance(error.get("code"), str):
            raise ValueError("missing structured error code")
        code = error["code"]
        msg = error.get("message", "remote transaction rejected")
    except Exception:
        if expected_txn or durable_response:
            unknown = _query_outcome_unknown(
                "unknown", f"HTTP {status} transaction outcome was malformed"
            )
            unknown.code = "QUERY_OUTCOME_UNKNOWN"
            unknown.server_state = "invalid_outcome"
            unknown.retryable = False
            return unknown
        return _MalformedHttpResponse(f"HTTP {status} error response was malformed")
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
    top_query_id = env.get("query_id")
    nested_query_id = env.get("error", {}).get("query_id")
    if (
        top_query_id is not None
        and nested_query_id is not None
        and top_query_id != nested_query_id
    ):
        return _attach_query_metadata(
            _query_outcome_unknown("unknown", "query_id fields disagree"),
            env,
            "unknown",
            "QUERY_OUTCOME_UNKNOWN",
        )
    query_id = nested_query_id or top_query_id or "unknown"
    outcome = env.get("outcome")
    outcome = outcome if isinstance(outcome, dict) else {}
    if code in ("QUERY_CANCELLED", "QUERY_CANCELLED_AFTER_COMMIT"):
        error = QueryCancelledError(f"query {query_id} cancelled: {msg}")
        return _attach_query_metadata(error, env, query_id, code)
    if code in ("DEADLINE_EXCEEDED", "DEADLINE_AFTER_COMMIT"):
        error = QueryTimeoutError(f"query {query_id} deadline exceeded: {msg}")
        return _attach_query_metadata(error, env, query_id, code)
    if code == "QUERY_ID_CONFLICT":
        return _attach_query_metadata(
            QueryIdConflictError(f"query id conflict: {query_id}"), env, query_id, code
        )
    if code == "TRANSACTION_ABORTED":
        return _attach_query_metadata(TransactionAbortedError(msg), env, query_id, code)
    if code == "COMMIT_OUTCOME":
        return _attach_query_metadata(
            _commit_outcome_error(query_id, msg, outcome), env, query_id, code
        )
    if code == "QUERY_OUTCOME_UNKNOWN":
        return _attach_query_metadata(
            _query_outcome_unknown(query_id, msg), env, query_id, code
        )
    if code == "QUERY_REGISTRY_FULL":
        error = QueryRegistryFullError(msg)
        return _attach_query_metadata(error, env, query_id, code)
    if code == "RESULT_LIMIT_EXCEEDED":
        error = ResultLimitExceededError(msg)
        return _attach_query_metadata(error, env, query_id, code)
    if code in ("SERIALIZATION_FAILED", "SERIALIZATION_FAILED_AFTER_COMMIT"):
        error = SerializationError(msg)
        return _attach_query_metadata(error, env, query_id, code)
    if code == "CAPABILITY_UNSUPPORTED":
        return CapabilityUnsupportedError(msg)
    if code == "QUERY_NOT_FOUND":
        mapped = _attach_query_metadata(_coded_storage(code, msg), env, query_id, code)
        mapped.committed = None
        mapped.committed_statements = None
        mapped.last_commit_epoch = None
        mapped.first_commit_statement_index = None
        mapped.last_commit_statement_index = None
        mapped.completed_statements = None
        mapped.statement_index = None
        mapped.retryable = False
        return mapped
    if code in ("CANCEL_TOO_LATE", "QUERY_ALREADY_FINISHED"):
        return _attach_query_metadata(_coded_storage(code, msg), env, query_id, code)
    return _attach_query_metadata(
        StorageError(f"http {status} ({code}): {msg}"), env, query_id, code
    )


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class RemoteDatabase:
    """A typed client bound to a ``mongreldb-server`` URL."""

    def __init__(
        self,
        url: str,
        *,
        bearer_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        transport_timeout: Optional[float] = None,
    ) -> None:
        try:
            parsed = urllib.parse.urlsplit(url)
            hostname = parsed.hostname
        except ValueError:
            raise ValueError("remote URL must be a valid http:// or https:// URL") from None
        if parsed.scheme not in ("http", "https") or hostname is None:
            raise ValueError("remote URL must use http:// or https:// and include a host")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("remote credentials must use constructor options, not the URL")
        if parsed.query or parsed.fragment:
            raise ValueError("remote URL must not include a query or fragment")
        if bearer_token is not None and (username is not None or password is not None):
            raise ValueError("choose bearer_token or username/password")
        if (username is None) != (password is None):
            raise ValueError("username and password must be provided together")
        if bearer_token is not None and (
            not isinstance(bearer_token, str)
            or not bearer_token.strip()
            or _contains_http_header_control(bearer_token)
        ):
            raise ValueError("bearer_token must not be empty")
        if username is not None and (
            not isinstance(username, str)
            or not username
            or ":" in username
            or _contains_http_header_control(username)
            or not isinstance(password, str)
            or _contains_http_header_control(password)
        ):
            raise ValueError(
                "basic-auth username must be non-empty and contain no colon"
            )
        if transport_timeout is not None and not _is_positive_duration(transport_timeout):
            raise ValueError("transport_timeout must be positive")
        self._base = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "")
        )
        if bearer_token is not None:
            self._authorization = f"Bearer {bearer_token}"
        elif username is not None and password is not None:
            encoded = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            self._authorization = f"Basic {encoded}"
        else:
            self._authorization = None
        self._transport_timeout = transport_timeout
        self._schemas: Dict[str, Dict[str, Any]] = {}
        self._capabilities = self._load_capabilities()
        self._sql_cancellation = self._capabilities.get("sql_cancellation")
        self.refresh()

    def __repr__(self) -> str:
        auth = "configured" if self._authorization is not None else "none"
        return f"RemoteDatabase(url={self._base!r}, auth={auth!r})"

    # ── schema ────────────────────────────────────────────────────────────

    def refresh(self) -> None:
        """Re-fetch every table's schema metadata from the daemon."""
        data = self._get_json("/kit/schema")
        if (
            not isinstance(data, dict)
            or set(data) != {"tables"}
            or not isinstance(data.get("tables"), dict)
        ):
            raise _MalformedHttpResponse("schema response fields were invalid")
        tables = data["tables"]
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
        if not _is_non_negative_int(epochs):
            raise ValueError("epochs must be non-negative")
        self._put_json("/history/retention", {"history_retention_epochs": epochs})

    def history_retention_epochs(self) -> int:
        """Return the daemon's configured history-retention depth."""
        return int(
            self._get_json(
                "/history/retention", _MAX_CONTROL_JSON_RESPONSE_BYTES
            )["history_retention_epochs"]
        )

    def earliest_retained_epoch(self) -> int:
        """Return the oldest epoch retained by the daemon."""
        return int(
            self._get_json(
                "/history/retention", _MAX_CONTROL_JSON_RESPONSE_BYTES
            )["earliest_retained_epoch"]
        )

    # ── reads ─────────────────────────────────────────────────────────────

    def sql_arrow(
        self,
        sql: str,
        *,
        timeout_ms: Optional[int] = None,
        query_id: Optional[str] = None,
        transport_timeout: Optional[float] = None,
        max_output_rows: Optional[int] = None,
        max_output_bytes: Optional[int] = None,
    ) -> bytes:
        """Run a SQL read; return raw Arrow IPC bytes (decode with pyarrow)."""
        if query_id is not None:
            query_id = _normalize_query_id(query_id)
        if timeout_ms is not None and not _is_positive_int(timeout_ms):
            raise ValueError("timeout_ms must be positive")
        if transport_timeout is not None and not _is_positive_duration(transport_timeout):
            raise ValueError("transport_timeout must be positive")
        if max_output_rows is not None and not _is_positive_int(max_output_rows):
            raise ValueError("max_output_rows must be positive")
        if max_output_bytes is not None and not _is_positive_int(max_output_bytes):
            raise ValueError("max_output_bytes must be positive")
        # Always retain a query ID. A response-body transport failure may occur
        # after a durable write and must be reconciled through query status.
        self._require_sql_cancellation()
        query_id = query_id or secrets.token_hex(16)
        payload = {"sql": sql, "format": "arrow"}
        for name, value in (
            ("query_id", query_id),
            ("timeout_ms", timeout_ms),
            ("max_output_rows", max_output_rows),
            ("max_output_bytes", max_output_bytes),
        ):
            if value is not None:
                payload[name] = value
        try:
            return self._post_bytes(
                "/sql",
                payload,
                timeout=transport_timeout,
                max_response_bytes=max_output_bytes,
            )
        except (TransportError, _MalformedHttpResponse) as error:
            if query_id is not None:
                self._raise_terminal_transport_outcome(query_id, str(error))
            raise

    def _raise_terminal_transport_outcome(
        self,
        query_id: str,
        message: str,
        initial_status: Optional[Dict[str, Any]] = None,
    ) -> None:
        deadline = time.monotonic() + _SQL_RECOVERY_WINDOW
        last_status = initial_status
        if last_status is None:
            last_status = self._query_status_for_recovery(query_id, deadline)
        self._raise_if_terminal_transport_outcome(query_id, message, last_status)
        cancel = self._cancel_sql_for_recovery(query_id, deadline)
        while time.monotonic() < deadline:
            last_status = self._query_status_for_recovery(query_id, deadline)
            self._raise_if_terminal_transport_outcome(query_id, message, last_status)
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(_SQL_RECOVERY_POLL_INTERVAL, remaining))
        raise _query_outcome_unknown(
            query_id,
            f"query {query_id}: {message}; cancel state={cancel.get('state', 'unknown')}",
        )

    def _raise_idempotent_transport_outcome(self, query_id: str, message: str) -> None:
        request = self._request("GET", f"/queries/{query_id}", None)
        try:
            with urllib.request.urlopen(
                request, timeout=_SQL_RECOVERY_REQUEST_TIMEOUT
            ) as response:
                raw = _read_response(
                    response,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "query status response",
                )
                status = (
                    _strict_json_response(raw, "query status response") if raw else None
                )
        except urllib.error.HTTPError as error:
            with error:
                raw = _read_response(
                    error,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "query status error response",
                )
                if error.code == 404:
                    try:
                        body = _strict_json_response(raw, "query status error response")
                    except _MalformedHttpResponse as malformed:
                        raise _invalid_query_status(query_id, str(malformed)) from None
                    if not _is_query_not_found(body, query_id):
                        raise _invalid_query_status(
                            query_id, "query-not-found response fields were inconsistent"
                        )
                    raise _RetryIdempotentSql(
                        _query_outcome_unknown(query_id, f"query {query_id} is not retained")
                    ) from None
            status = None
        except Exception:
            status = None
        if status is not None and not _is_query_status(status, query_id):
            raise _invalid_query_status(query_id, "response fields were inconsistent")
        self._raise_terminal_transport_outcome(query_id, message, status)

    @staticmethod
    def _recovery_timeout(deadline: float) -> Optional[float]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        return min(_SQL_RECOVERY_REQUEST_TIMEOUT, remaining)

    def _query_status_for_recovery(
        self, query_id: str, deadline: float
    ) -> Optional[Dict[str, Any]]:
        timeout = self._recovery_timeout(deadline)
        if timeout is None:
            return None
        request = self._request("GET", f"/queries/{query_id}", None)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = _read_response(
                    response,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "query status response",
                )
                status = (
                    _strict_json_response(raw, "query status response") if raw else None
                )
                return status if _is_query_status(status, query_id) else None
        except urllib.error.HTTPError as error:
            with error:
                _read_response(
                    error,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "query status error response",
                )
            return None
        except Exception:
            return None

    def _cancel_sql_for_recovery(self, query_id: str, deadline: float) -> Dict[str, Any]:
        timeout = self._recovery_timeout(deadline)
        if timeout is None:
            return {"state": "unknown"}
        request = self._request("POST", f"/queries/{query_id}/cancel", b"")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = _read_response(
                    response,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "SQL cancellation response",
                )
                body = _strict_json_response(raw, "SQL cancellation response")
                return _validate_cancel_response(response.status, query_id, body)
        except urllib.error.HTTPError as error:
            with error:
                raw = _read_response(
                    error,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "SQL cancellation response",
                )
            if error.code in (404, 409):
                try:
                    body = (
                        _strict_json_response(raw, "SQL cancellation response")
                        if raw
                        else {}
                    )
                except _MalformedHttpResponse:
                    body = {}
                try:
                    return _validate_cancel_response(error.code, query_id, body)
                except _MalformedHttpResponse:
                    return {"state": "unknown"}
            return {"state": "unknown"}
        except Exception:
            return {"state": "unknown"}

    @staticmethod
    def _raise_if_terminal_transport_outcome(
        query_id: str, message: str, status: Optional[Dict[str, Any]]
    ) -> None:
        state = None if status is None else status.get("server_state") or status.get("state")
        outcome = {} if status is None else status.get("outcome") or {}
        committed = None if status is None else (
            True
            if status.get("committed") is True or outcome.get("committed") is True
            else outcome.get("committed", status.get("committed"))
        )
        if status is None or committed is not True and state not in (
            "cancelled", "failed", "completed", "pre_cancelled"
        ):
            return
        terminal_error = status.get("terminal_error") or {}
        code = terminal_error.get("code")
        envelope = {
            "query_id": query_id,
            "error": {
                "code": code,
                "message": message,
                "query_id": query_id,
            },
            "committed": status.get("committed"),
            "committed_statements": status.get("committed_statements"),
            "last_commit_epoch": status.get("last_commit_epoch"),
            "last_commit_epoch_text": status.get("last_commit_epoch_text"),
            "first_commit_statement_index": status.get("first_commit_statement_index"),
            "last_commit_statement_index": status.get("last_commit_statement_index"),
            "completed_statements": status.get("completed_statements"),
            "statement_index": status.get("statement_index"),
            "outcome": outcome,
            "cancel_outcome": status.get("cancel_outcome"),
            "cancellation_reason": status.get("cancellation_reason"),
            "retryable": status.get("retryable", False),
            "server_state": status.get("server_state", status.get("state")),
        }
        if code:
            raise _map_error(500, json.dumps(envelope))
        if committed is None:
            raise _query_outcome_unknown(query_id, message)
        if committed is True:
            envelope["error"]["code"] = "COMMIT_OUTCOME"
            raise _map_error(500, json.dumps(envelope))
        if state == "completed":
            envelope["error"]["code"] = "SERIALIZATION_FAILED"
            raise _map_error(500, json.dumps(envelope))
        envelope["error"]["code"] = "QUERY_FAILED"
        raise _map_error(500, json.dumps(envelope))

    def start_sql_arrow(
        self,
        sql: str,
        *,
        timeout_ms: Optional[int] = None,
        query_id: Optional[str] = None,
        transport_timeout: Optional[float] = None,
        max_output_rows: Optional[int] = None,
        max_output_bytes: Optional[int] = None,
    ) -> "RemoteSqlQueryHandle":
        if query_id is not None:
            query_id = _normalize_query_id(query_id)
        self._require_sql_cancellation()
        return RemoteSqlQueryHandle(
            self,
            sql,
            timeout_ms=timeout_ms,
            query_id=query_id or secrets.token_hex(16),
            transport_timeout=transport_timeout,
            max_output_rows=max_output_rows,
            max_output_bytes=max_output_bytes,
        )

    def cancel_sql(self, query_id: str) -> Dict[str, Any]:
        query_id = _normalize_query_id(query_id)
        self._require_sql_cancellation()
        path = f"/queries/{query_id}/cancel"
        request = self._request("POST", path, b"")
        try:
            with urllib.request.urlopen(request, timeout=self._transport_timeout or 5.0) as response:
                raw = _read_response(
                    response,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "SQL cancellation response",
                )
                body = _strict_json_response(raw, "SQL cancellation response")
                return _validate_cancel_response(response.status, query_id, body)
        except urllib.error.HTTPError as error:
            with error:
                raw = _read_response(
                    error,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "SQL cancellation response",
                )
            if error.code in (404, 409):
                try:
                    body = (
                        _strict_json_response(raw, "SQL cancellation response")
                        if raw
                        else {}
                    )
                except _MalformedHttpResponse:
                    body = {}
                return _validate_cancel_response(error.code, query_id, body)
            raise _map_error(error.code, raw.decode("utf-8", "replace")) from None
        except (urllib.error.URLError, TimeoutError) as error:
            raise TransportError(f"query {query_id}: cancellation transport error: {error}") from None

    def query_status(self, query_id: str) -> Dict[str, Any]:
        """Return the retained server status for one SQL execution."""
        query_id = _normalize_query_id(query_id)
        self._require_sql_cancellation()
        status = self._get_json(
            f"/queries/{query_id}", _MAX_CONTROL_JSON_RESPONSE_BYTES
        )
        if not _is_query_status(status, query_id):
            raise _invalid_query_status(query_id, "response fields were inconsistent")
        return status

    def sql_page(
        self,
        sql: str,
        *,
        projection: List[str],
        page_size_rows: int,
        query_id: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        max_page_bytes: Optional[int] = None,
        max_page_tokens: Optional[int] = None,
        max_output_rows: Optional[int] = None,
        max_output_bytes: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Run one read-only SELECT and retain projected rows for pagination."""
        if query_id is not None:
            query_id = _normalize_query_id(query_id)
        self._require_sql_pagination()
        if (
            not 1 <= len(projection) <= 128
            or any(
                not isinstance(column, str)
                or not column
                or column == "*"
                or _utf8_length(column) is None
                or _utf8_length(column) > 256
                for column in projection
            )
            or len(set(projection)) != len(projection)
            or sum(_utf8_length(column) or 0 for column in projection) > 16 * 1024
            or not _is_positive_int(page_size_rows)
        ):
            raise ValueError(
                "projection must contain 1 to 128 unique explicit columns and page_size_rows must be positive"
            )
        for name, value in (
            ("timeout_ms", timeout_ms),
            ("max_page_bytes", max_page_bytes),
            ("max_page_tokens", max_page_tokens),
            ("max_output_rows", max_output_rows),
            ("max_output_bytes", max_output_bytes),
        ):
            if value is not None and not _is_positive_int(value):
                raise ValueError(f"{name} must be positive")
        active_query_id = query_id or secrets.token_hex(16)
        try:
            page = self._post_json(
                "/sql",
                {
                    "sql": sql,
                    "format": "json",
                    "query_id": active_query_id,
                    "timeout_ms": timeout_ms,
                    "max_output_rows": max_output_rows,
                    "max_output_bytes": max_output_bytes,
                    "pagination": {
                        "page_size_rows": page_size_rows,
                        "projection": projection,
                        "max_page_bytes": max_page_bytes,
                        "max_page_tokens": max_page_tokens,
                    },
                },
                require_query_id_header=True,
            )
            if not _is_sql_page(
                page,
                {
                    "page_size_rows": page_size_rows,
                    "projection": projection,
                    "max_page_bytes": max_page_bytes,
                    "max_page_tokens": max_page_tokens,
                    "max_output_rows": max_output_rows,
                    "max_output_bytes": max_output_bytes,
                },
            ):
                raise _MalformedHttpResponse("SQL pagination response was not a valid retained page")
            return page
        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            TransportError,
            _MalformedHttpResponse,
        ) as error:
            self._raise_terminal_transport_outcome(active_query_id, str(error))
            raise

    def continue_sql_page(
        self,
        cursor: str,
        *,
        query_id: Optional[str] = None,
        timeout_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Read the next page for an opaque owner-bound cursor."""
        self._require_sql_pagination()
        cursor_bytes = _utf8_length(cursor) if isinstance(cursor, str) else None
        if not cursor or cursor_bytes is None or cursor_bytes > 2048:
            raise ValueError("cursor must contain 1 to 2048 UTF-8 bytes")
        if query_id is not None:
            query_id = _normalize_query_id(query_id)
        if timeout_ms is not None and (
            isinstance(timeout_ms, bool)
            or not isinstance(timeout_ms, int)
            or timeout_ms <= 0
        ):
            raise ValueError("timeout_ms must be a positive integer")
        operation_id = query_id or secrets.token_hex(16)
        try:
            page = self._post_json(
                "/sql/continue",
                {
                    "cursor": cursor,
                    "operation_id": operation_id,
                    "timeout_ms": timeout_ms,
                },
                require_query_id_header=True,
            )
            if not _is_sql_page(page):
                raise _MalformedHttpResponse(
                    "SQL continuation response was not a valid retained page"
                )
            return page
        except (json.JSONDecodeError, UnicodeDecodeError, _MalformedHttpResponse) as error:
            outcome = {
                "committed": False,
                "committed_statements": 0,
                "last_commit_epoch": None,
                "last_commit_epoch_text": None,
                "first_commit_statement_index": None,
                "last_commit_statement_index": None,
                "completed_statements": 0,
                "statement_index": 0,
                "serialization": "failed",
            }
            envelope = {
                "query_id": operation_id,
                "status": "failed_before_commit",
                "server_state": "failed",
                **{key: value for key, value in outcome.items() if key != "serialization"},
                "cancel_outcome": "already_finished",
                "cancellation_reason": "none",
                "retryable": False,
                "outcome": outcome,
                "error": {
                    "code": "SERIALIZATION_FAILED",
                    "message": str(error),
                    "query_id": operation_id,
                    "committed": False,
                    "retryable": False,
                },
            }
            raise _attach_query_metadata(
                SerializationError(str(error)),
                envelope,
                operation_id,
                "SERIALIZATION_FAILED",
            ) from None

    def execute_idempotent_sql(
        self,
        sql: str,
        *,
        idempotency_key: str,
        query_id: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        max_output_rows: Optional[int] = None,
        max_output_bytes: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Execute one write at most once and return its durable receipt."""
        if query_id is not None:
            query_id = _normalize_query_id(query_id)
        self._require_sql_idempotency()
        key_bytes = _utf8_length(idempotency_key) if isinstance(idempotency_key, str) else None
        if not idempotency_key or key_bytes is None or key_bytes > 256:
            raise ValueError("idempotency_key must contain 1 to 256 bytes")
        for name, value in (
            ("timeout_ms", timeout_ms),
            ("max_output_rows", max_output_rows),
            ("max_output_bytes", max_output_bytes),
        ):
            if value is not None and not _is_positive_int(value):
                raise ValueError(f"{name} must be positive")
        active_query_id = query_id or secrets.token_hex(16)
        try:
            return self._execute_idempotent_sql_once(
                sql,
                idempotency_key,
                active_query_id,
                timeout_ms,
                max_output_rows,
                max_output_bytes,
                None,
            )
        except _RetryIdempotentSql:
            self._require_fresh_sql_idempotency()
            replay_query_id = secrets.token_hex(16)
            while replay_query_id == active_query_id:
                replay_query_id = secrets.token_hex(16)
            try:
                return self._execute_idempotent_sql_once(
                    sql,
                    idempotency_key,
                    replay_query_id,
                    timeout_ms,
                    max_output_rows,
                    max_output_bytes,
                    active_query_id,
                )
            except _RetryIdempotentSql as error:
                raise error.outcome from None

    def _execute_idempotent_sql_once(
        self,
        sql: str,
        idempotency_key: str,
        active_query_id: str,
        timeout_ms: Optional[int],
        max_output_rows: Optional[int],
        max_output_bytes: Optional[int],
        expected_original_query_id: Optional[str],
    ) -> Dict[str, Any]:
        try:
            receipt, query_id_header_error = self._post_json(
                "/sql",
                {
                    "sql": sql,
                    "format": "json",
                    "query_id": active_query_id,
                    "timeout_ms": timeout_ms,
                    "max_output_rows": max_output_rows,
                    "max_output_bytes": max_output_bytes,
                    "idempotency_key": idempotency_key,
                },
                capture_query_id_header=True,
            )
        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            TransportError,
            _MalformedHttpResponse,
        ) as error:
            self._raise_idempotent_transport_outcome(active_query_id, str(error))
            raise
        if not _is_sql_write_receipt(
            receipt, active_query_id, expected_original_query_id
        ):
            self._raise_idempotent_transport_outcome(
                active_query_id,
                "SQL idempotency response was not a valid durable receipt",
            )
        outcome = receipt.get("outcome")
        epoch_source = outcome if isinstance(outcome, dict) else receipt
        epoch = _outcome_epoch(epoch_source)
        receipt["committed"] = bool(
            receipt.get("committed", False)
            or (isinstance(outcome, dict) and outcome.get("committed") is True)
        )
        receipt["committed_statements"] = max(
            int(receipt.get("committed_statements", 0)),
            int(outcome.get("committed_statements", 0))
            if isinstance(outcome, dict)
            else 0,
        )
        receipt["last_commit_epoch"] = epoch
        if isinstance(outcome, dict):
            outcome["last_commit_epoch"] = epoch
            receipt["first_commit_statement_index"] = receipt.get(
                "first_commit_statement_index",
                outcome.get("first_commit_statement_index"),
            )
            receipt["last_commit_statement_index"] = receipt.get(
                "last_commit_statement_index",
                outcome.get("last_commit_statement_index"),
            )
        if query_id_header_error is not None:
            if receipt["committed"]:
                error = _commit_outcome_error(
                    active_query_id,
                    "SQL committed but x-mongreldb-query-id did not match the request",
                    receipt,
                )
                error.last_commit_epoch = epoch
                error.cancel_outcome = receipt.get("cancel_outcome")
                error.cancellation_reason = receipt.get("cancellation_reason")
                error.server_state = receipt.get("server_state")
                raise error
            self._raise_idempotent_transport_outcome(
                active_query_id, str(query_id_header_error)
            )
        return receipt

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
        expected_keys = {"truncated", "rows"}
        if isinstance(env, dict) and "next_cursor" in env:
            expected_keys.add("next_cursor")
        next_cursor = env.get("next_cursor") if isinstance(env, dict) else None
        if (
            not isinstance(env, dict)
            or set(env) != expected_keys
            or not isinstance(env.get("truncated"), bool)
            or not isinstance(env.get("rows"), list)
            or (
                next_cursor is not None
                and (
                    not isinstance(next_cursor, str)
                    or not next_cursor
                    or _utf8_length(next_cursor) > 2048
                )
            )
            or (not env.get("truncated") and next_cursor is not None)
            or any(
                not isinstance(row, dict)
                or set(row) != {"row_id", "cells"}
                or not isinstance(row.get("row_id"), str)
                or not isinstance(row.get("cells"), list)
                for row in env["rows"]
            )
        ):
            raise _MalformedHttpResponse("native query response fields were invalid")
        info = self._schemas.get(table)
        rows = []
        for row in env["rows"]:
            values = _decode_cells(row["cells"], info)
            rows.append({"row_id": row["row_id"], "values": values})
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
        if not isinstance(env, dict) or set(env) != {"table_id", "table_id_text"}:
            raise _query_outcome_unknown("unknown", "invalid table creation response")
        table_id = env.get("table_id")
        table_id_text = env.get("table_id_text")
        if (
            not _is_non_negative_int(table_id)
            or not isinstance(table_id_text, str)
            or not table_id_text.isdigit()
            or str(int(table_id_text)) != table_id_text
            or int(table_id_text) != table_id
        ):
            raise _query_outcome_unknown("unknown", "invalid table creation ID")
        try:
            self.refresh()
        except Exception as error:
            raise _committed_write_response_error(
                f"table was created but schema refresh failed: {error}"
            ) from None
        return table_id

    def create_procedure(self, procedure: Dict[str, Any]) -> Dict[str, Any]:
        return self._post_json("/procedures", {"procedure": procedure})

    def drop_procedure(self, name: str) -> None:
        try:
            self._open("DELETE", f"/procedures/{name}", body=None).close()
        except (TransportError, _MalformedHttpResponse) as error:
            raise _query_outcome_unknown("unknown", str(error)) from None

    def call_procedure(self, name: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        response = self._post_json(f"/kit/procedures/{name}/call", {"args": args or {}})
        return _validate_procedure_call_response(response)

    def triggers(self) -> List[Dict[str, Any]]:
        return list((self._get_json("/triggers").get("triggers") or []))

    def trigger(self, name: str) -> Dict[str, Any]:
        return self._get_json(f"/triggers/{name}").get("trigger")

    def create_trigger(self, trigger: Dict[str, Any]) -> Dict[str, Any]:
        return self._post_json("/triggers", {"trigger": trigger})

    def replace_trigger(self, name: str, trigger: Dict[str, Any]) -> Dict[str, Any]:
        return self._put_json(f"/triggers/{name}", {"trigger": trigger})

    def drop_trigger(self, name: str) -> None:
        try:
            self._open("DELETE", f"/triggers/{name}", body=None).close()
        except (TransportError, _MalformedHttpResponse) as error:
            raise _query_outcome_unknown("unknown", str(error)) from None

    def create_virtual_table(self, name: str, module: str, args: Optional[List[str]] = None) -> bytes:
        arg_sql = ", ".join(args or [])
        suffix = f"({arg_sql})" if arg_sql else ""
        return self.sql_arrow(
            f"CREATE VIRTUAL TABLE {_quote_ident(name)} USING {_quote_ident(module)}{suffix}"
        )

    def drop_virtual_table(self, name: str) -> bytes:
        return self.sql_arrow(f"DROP TABLE {_quote_ident(name)}")

    # ── HTTP plumbing ─────────────────────────────────────────────────────

    def _get_json(self, path: str, limit: int = _MAX_JSON_RESPONSE_BYTES) -> Any:
        with self._open("GET", path, body=None) as resp:
            return _strict_json_response(
                _read_response(resp, limit, "GET response"),
                "GET response",
            )

    def _post_json(
        self,
        path: str,
        payload: Any,
        require_query_id_header: bool = False,
        capture_query_id_header: bool = False,
    ) -> Any:
        data = json.dumps(payload).encode("utf-8")
        is_transaction = path == "/kit/txn"
        is_write = is_transaction or path in (
            "/kit/create_table",
            "/procedures",
            "/triggers",
        ) or path.startswith("/kit/procedures/")
        query_id_header_error = None
        try:
            query_id = (
                payload.get("query_id")
                if path == "/sql"
                else payload.get("operation_id")
                if path == "/sql/continue"
                else None
            )
            with self._open(
                "POST",
                path,
                body=data,
                expected_query_id=query_id,
                expected_txn=is_transaction,
                expected_cursor=path == "/sql/continue",
            ) as resp:
                if require_query_id_header and query_id is not None:
                    _validate_sql_query_id_header(resp, query_id)
                elif capture_query_id_header and query_id is not None:
                    try:
                        _validate_sql_query_id_header(resp, query_id)
                    except _MalformedHttpResponse as error:
                        query_id_header_error = error
                raw = _read_response(resp, _MAX_JSON_RESPONSE_BYTES, "POST response")
        except (
            urllib.error.URLError,
            TimeoutError,
            OSError,
            TransportError,
            _MalformedHttpResponse,
        ) as error:
            if is_write:
                raise _query_outcome_unknown(
                    "unknown", f"write response was lost or invalid: {error}"
                ) from None
            raise TransportError(f"transport error while reading response: {error}") from None
        try:
            result = _strict_json_response(raw, "POST response") if raw else {}
            return (
                (result, query_id_header_error)
                if capture_query_id_header
                else result
            )
        except _MalformedHttpResponse as error:
            if is_write:
                raise _query_outcome_unknown(
                    "unknown", f"write response was invalid: {error}"
                ) from None
            raise

    def _put_json(self, path: str, payload: Any) -> Any:
        data = json.dumps(payload).encode("utf-8")
        try:
            with self._open("PUT", path, body=data) as resp:
                raw = _read_response(resp, _MAX_JSON_RESPONSE_BYTES, "PUT response")
            return _strict_json_response(raw, "PUT response") if raw else {}
        except (
            urllib.error.URLError,
            TimeoutError,
            OSError,
            TransportError,
            _MalformedHttpResponse,
        ) as error:
            raise _query_outcome_unknown(
                "unknown", f"write response was lost or invalid: {error}"
            ) from None

    def _post_bytes(
        self,
        path: str,
        payload: Any,
        timeout: Optional[float] = None,
        max_response_bytes: Optional[int] = None,
    ) -> bytes:
        data = json.dumps(payload).encode("utf-8")
        try:
            query_id = payload.get("query_id") if path == "/sql" else None
            with self._open(
                "POST",
                path,
                body=data,
                timeout=timeout,
                expected_query_id=query_id,
            ) as resp:
                if query_id is not None:
                    _validate_sql_query_id_header(resp, query_id)
                response_limit = (
                    _MAX_JSON_RESPONSE_BYTES
                    if max_response_bytes is None
                    else min(max_response_bytes, _MAX_JSON_RESPONSE_BYTES)
                )
                return _read_response(resp, response_limit, "SQL response")
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise TransportError(f"transport error while reading response: {error}") from None

    def _open(
        self,
        method: str,
        path: str,
        body: Optional[bytes],
        timeout: Optional[float] = None,
        expected_query_id: Optional[str] = None,
        expected_txn: bool = False,
        expected_cursor: bool = False,
    ):
        req = self._request(method, path, body)
        try:
            return urllib.request.urlopen(req, timeout=timeout or self._transport_timeout)
        except urllib.error.HTTPError as e:
            with e:
                raw = _read_response(
                    e,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "HTTP error response",
                )
            raise _map_error(
                e.code,
                raw.decode("utf-8", "replace"),
                expected_query_id,
                expected_txn,
                expected_cursor,
            ) from None
        except (urllib.error.URLError, TimeoutError) as e:
            raise TransportError(f"transport error: {e}") from None

    def _request(self, method: str, path: str, body: Optional[bytes]) -> urllib.request.Request:
        request = urllib.request.Request(self._base + path, data=body, method=method)
        if body is not None:
            request.add_header("Content-Type", "application/json")
        if self._authorization is not None:
            request.add_header("Authorization", self._authorization)
        return request

    def _load_capabilities(self) -> Dict[str, Any]:
        request = self._request("GET", "/capabilities", None)
        try:
            with urllib.request.urlopen(request, timeout=self._transport_timeout) as response:
                body = _strict_json_response(
                    _read_response(
                        response,
                        _MAX_CONTROL_JSON_RESPONSE_BYTES,
                        "capability response",
                    ),
                    "capability response",
                )
                if not _is_capabilities(body):
                    raise _MalformedHttpResponse(
                        "capability response had unknown or invalid fields"
                    )
                return body
        except urllib.error.HTTPError as error:
            with error:
                raw = _read_response(
                    error,
                    _MAX_CONTROL_JSON_RESPONSE_BYTES,
                    "capability error response",
                )
            if error.code == 404:
                return {}
            raise _map_error(error.code, raw.decode("utf-8", "replace")) from None
        except (urllib.error.URLError, TimeoutError) as error:
            raise TransportError(f"capability transport error: {error}") from None

    def _require_sql_cancellation(
        self, capabilities: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        capability = (
            self._sql_cancellation
            if capabilities is None
            else capabilities.get("sql_cancellation")
        )
        if (
            capability is None
            or capability.get("version") != 2
            or capability.get("client_query_ids") is not True
            or capability.get("cancel_endpoint") is not True
            or capability.get("query_status") is not True
            or capability.get("pre_registration_cancel") is not True
        ):
            raise CapabilityUnsupportedError(
                "server does not support SQL cancellation capability version 2"
            )
        return capability

    def _require_sql_pagination(self) -> Dict[str, Any]:
        self._require_sql_cancellation()
        capability = self._capabilities.get("sql_pagination")
        if (
            not isinstance(capability, dict)
            or capability.get("version") != 1
            or capability.get("continuation_endpoint") != "/sql/continue"
            or capability.get("retained_snapshot") is not True
            or capability.get("projection_required") is not True
            or capability.get("byte_and_token_hints") is not True
        ):
            raise CapabilityUnsupportedError(
                "server does not support SQL pagination capability version 1"
            )
        return capability

    def _require_sql_idempotency(
        self, capabilities: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        self._require_sql_cancellation(capabilities)
        source = self._capabilities if capabilities is None else capabilities
        capability = source.get("sql_idempotency")
        if (
            not isinstance(capability, dict)
            or capability.get("version") != 1
            or capability.get("durable_pre_execution_intent") is not True
            or capability.get("replay_committed_receipt") is not True
            or capability.get("indeterminate_never_reexecutes") is not True
        ):
            raise CapabilityUnsupportedError(
                "server does not support durable SQL idempotency capability version 1"
            )
        return capability

    def _require_fresh_sql_idempotency(self) -> None:
        self._require_sql_idempotency(self._load_capabilities())


class RemoteSqlQueryHandle:
    """Background remote SQL request cancellable from another thread."""

    def __init__(
        self,
        database: RemoteDatabase,
        sql: str,
        *,
        timeout_ms: Optional[int],
        query_id: str,
        transport_timeout: Optional[float],
        max_output_rows: Optional[int],
        max_output_bytes: Optional[int],
    ) -> None:
        self.id = query_id
        self._database = database
        self._result: Optional[bytes] = None
        self._error: Optional[BaseException] = None

        def run() -> None:
            try:
                self._result = database.sql_arrow(
                    sql,
                    timeout_ms=timeout_ms,
                    query_id=query_id,
                    transport_timeout=transport_timeout,
                    max_output_rows=max_output_rows,
                    max_output_bytes=max_output_bytes,
                )
            except BaseException as error:
                self._error = error

        self._thread = threading.Thread(target=run, name=f"mongreldb-sql-{query_id}", daemon=True)
        self._thread.start()

    def cancel(self) -> Dict[str, Any]:
        return self._database.cancel_sql(self.id)

    def status(self) -> Dict[str, Any]:
        return self._database.query_status(self.id)

    def result(self) -> bytes:
        self._thread.join()
        if self._error is not None:
            raise self._error
        return self._result or b""


def _build_table(info: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(info, dict) or not _has_only_keys(
        info, ("schema_id", "columns", "indexes", "constraints")
    ):
        raise _MalformedHttpResponse("schema table descriptor fields were invalid")
    columns = info.get("columns")
    if not isinstance(columns, list):
        raise _MalformedHttpResponse("schema columns were invalid")
    if "schema_id" in info and not _is_non_negative_int(info["schema_id"]):
        raise _MalformedHttpResponse("schema_id was invalid")
    if "indexes" in info and not isinstance(info["indexes"], list):
        raise _MalformedHttpResponse("schema indexes were invalid")
    if "constraints" in info and not isinstance(info["constraints"], dict):
        raise _MalformedHttpResponse("schema constraints were invalid")
    id_to_name: Dict[int, str] = {}
    name_to_id: Dict[str, int] = {}
    primary_key: Optional[int] = None
    for col in columns:
        if (
            not isinstance(col, dict)
            or not _has_only_keys(
                col,
                (
                    "id",
                    "name",
                    "ty",
                    "primary_key",
                    "nullable",
                    "auto_increment",
                    "embedding_source",
                ),
            )
            or not _is_non_negative_int(col.get("id"))
            or col["id"] > 65_535
            or not isinstance(col.get("name"), str)
            or not col["name"]
            or not isinstance(col.get("primary_key"), bool)
            or "ty" in col
            and not isinstance(col["ty"], str)
            or "nullable" in col
            and not isinstance(col["nullable"], bool)
            or "auto_increment" in col
            and not isinstance(col["auto_increment"], bool)
            or "embedding_source" in col
            and col["embedding_source"] is not None
            and not isinstance(col["embedding_source"], dict)
        ):
            raise _MalformedHttpResponse("schema column descriptor fields were invalid")
        cid = col["id"]
        name = col["name"]
        if cid in id_to_name or name in name_to_id:
            raise _MalformedHttpResponse("schema column identifiers were duplicated")
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
        if not isinstance(resp, dict) or set(resp) != {
            "status",
            "epoch",
            "epoch_text",
            "results",
        }:
            raise _query_outcome_unknown("unknown", "invalid /kit/txn success response")
        epoch = resp.get("epoch")
        epoch_text = resp.get("epoch_text")
        results = resp.get("results")
        if (
            resp.get("status") != "committed"
            or not _is_non_negative_int(epoch)
            or not isinstance(epoch_text, str)
            or not epoch_text.isdigit()
            or str(int(epoch_text)) != epoch_text
            or int(epoch_text) != epoch
        ):
            raise _query_outcome_unknown(
                "unknown", "invalid /kit/txn committed response metadata"
            )
        if not isinstance(results, list) or len(results) != len(self._ops):
            raise _committed_txn_response_error(
                epoch, "committed /kit/txn result count did not match the request"
            )
        decoded = []
        for op, r in zip(self._ops, results):
            if not isinstance(r, dict):
                raise _committed_txn_response_error(
                    epoch, "invalid committed /kit/txn operation result"
                )
            request_kind, request = next(iter(op.items()))
            kind = r.get("kind")
            returning = bool(request.get("returning", False))
            keys = set(r)
            valid = (
                request_kind == "put"
                and kind == "put"
                and keys
                in (
                    {"kind", "row_id", "auto_inc"},
                    {"kind", "row_id", "auto_inc", "row"},
                )
                and "row_id" in r
                and "auto_inc" in r
                and r.get("row_id") is None
                and (
                    r.get("auto_inc") is None
                    or isinstance(r["auto_inc"], int)
                    and not isinstance(r["auto_inc"], bool)
                )
                or request_kind == "upsert"
                and kind == "upsert"
                and keys
                in (
                    {"kind", "action", "auto_inc"},
                    {"kind", "action", "auto_inc", "row"},
                )
                and "auto_inc" in r
                and r.get("action") in ("inserted", "updated", "unchanged")
                and (
                    r.get("auto_inc") is None
                    or isinstance(r["auto_inc"], int)
                    and not isinstance(r["auto_inc"], bool)
                )
                or request_kind == "delete_by_pk"
                and kind in ("deleted", "not_found")
                and keys == {"kind"}
            )
            if not valid or request_kind in ("put", "upsert") and (
                (r.get("row") is not None) is not returning
            ):
                raise _committed_txn_response_error(
                    epoch, "/kit/txn result does not match its requested operation"
                )
            if kind in ("put", "upsert") and r.get("row") is not None:
                r = dict(r)
                try:
                    r["row"] = _decode_row(self._db, request["table"], r["row"])
                except StorageError as error:
                    raise _committed_txn_response_error(epoch, str(error)) from None
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


def _decode_row(db: RemoteDatabase, table: str, row: List[Any]) -> Dict[str, Any]:
    info = db.table(table)
    return _decode_cells(row, info)


def _decode_cells(cells: List[Any], info: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(cells, list) or len(cells) % 2:
        raise StorageError("server returned malformed typed cells")
    if not isinstance(info, dict):
        raise StorageError("server returned cells for an unknown table")
    id_to_name = info["id_to_name"]
    out: Dict[str, Any] = {}
    for i in range(0, len(cells), 2):
        cid = cells[i]
        if not _is_non_negative_int(cid) or cid > 65535:
            raise StorageError("server returned an invalid column id")
        name = id_to_name.get(cid)
        if name is None:
            raise StorageError(f"server returned unknown column id {cid}")
        if name in out:
            raise StorageError(f"server returned duplicate column id {cid}")
        out[name] = cells[i + 1]
    return out
