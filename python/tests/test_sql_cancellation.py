import asyncio
import threading
import time

import pytest

from mongreldb_kit import Database, QueryCancelledError, QueryTimeoutError


def slow_database(tmp_path):
    db = Database.create(
        str(tmp_path / "sql-control"),
        {
            "tables": [],
        },
    )
    db.sql_rows("CREATE TABLE numbers (id BIGINT PRIMARY KEY)")
    values = ",".join(f"({index})" for index in range(1, 1001))
    db.sql_rows(f"INSERT INTO numbers (id) VALUES {values}")
    return db


SLOW_SQL = "SELECT sum(a.id * b.id * c.id) FROM numbers a CROSS JOIN numbers b CROSS JOIN numbers c"


def test_another_thread_cancels_and_result_releases_gil(tmp_path):
    db = slow_database(tmp_path)
    handle = db.start_sql(SLOW_SQL, timeout_ms=5_000)
    outcome = []

    def wait_for_result():
        try:
            handle.result()
        except BaseException as error:
            outcome.append(error)

    worker = threading.Thread(target=wait_for_result)
    worker.start()
    time.sleep(0.02)
    assert handle.cancel() == "accepted"
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], QueryCancelledError)
    assert outcome[0].query_id == handle.id
    assert outcome[0].committed is False
    assert outcome[0].committed_statements == 0
    assert outcome[0].last_commit_epoch is None
    assert outcome[0].first_commit_statement_index is None
    assert outcome[0].last_commit_statement_index is None
    assert outcome[0].completed_statements == 0
    assert outcome[0].statement_index == 0
    assert outcome[0].retryable is False
    assert outcome[0].code == "QUERY_CANCELLED"
    assert outcome[0].cancel_outcome is None
    assert outcome[0].cancellation_reason == "client_request"
    assert outcome[0].server_state == "cancelled"
    status = handle.status()
    assert status["terminal_error_code"] == "QUERY_CANCELLED"
    assert status["terminal_error_category"] == "cancellation"
    assert status["durable_outcome"]["committed"] is False
    assert status["durable_outcome"]["committed_statements"] == 0
    assert status["durable_outcome"]["last_commit_epoch"] is None
    assert status["durable_outcome"]["first_commit_statement_index"] is None
    assert status["durable_outcome"]["last_commit_statement_index"] is None
    assert status["server_state"] == "cancelled"
    assert status["terminal_state"] == "cancelled_before_commit"
    assert status["cancellation_reason"] == "client_request"
    assert db.sql_rows("SELECT 2 AS value") == [{"value": 2}]


def test_row_and_python_object_conversion_remain_cancellable(tmp_path):
    db = slow_database(tmp_path)
    handle = db.start_sql(
        "SELECT a.id AS left_id, b.id AS right_id "
        "FROM numbers a CROSS JOIN numbers b LIMIT 500000",
        timeout_ms=30_000,
        max_output_rows=600_000,
        max_output_bytes=128 * 1024 * 1024,
    )
    outcome = []

    def convert_result():
        try:
            handle.result()
        except BaseException as error:
            outcome.append(error)

    worker = threading.Thread(target=convert_result)
    worker.start()
    deadline = time.monotonic() + 10
    while handle.status()["server_state"] != "serializing":
        assert time.monotonic() < deadline
        time.sleep(0.001)
    assert handle.cancel() in ("accepted", "already_cancelling")
    worker.join(timeout=10)

    assert not worker.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], QueryCancelledError)
    assert handle.status()["terminal_state"] == "cancelled_before_commit"
    assert db.sql_rows("SELECT 5 AS value") == [{"value": 5}]


def test_embedded_timeout_is_typed(tmp_path):
    db = slow_database(tmp_path)
    with pytest.raises(QueryTimeoutError) as caught:
        db.sql_rows(SLOW_SQL, timeout_ms=1)
    assert caught.value.committed is False
    assert caught.value.retryable is False
    assert caught.value.code == "DEADLINE_EXCEEDED"
    assert caught.value.cancel_outcome is None
    assert caught.value.cancellation_reason == "deadline"
    assert caught.value.server_state == "cancelled"
    assert db.sql_rows("SELECT 3 AS value") == [{"value": 3}]


def test_asyncio_task_cancellation_propagates(tmp_path):
    async def run():
        db = slow_database(tmp_path)
        task = asyncio.create_task(db.sql_rows_async(SLOW_SQL, timeout_ms=5_000))
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.05)
        assert db.sql_rows("SELECT 4 AS value") == [{"value": 4}]

    asyncio.run(run())


def test_exception_codes_are_stable():
    assert QueryCancelledError.code == "QUERY_CANCELLED"
    assert QueryTimeoutError.code == "DEADLINE_EXCEEDED"
