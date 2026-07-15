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
    assert handle.cancel() is True
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], QueryCancelledError)
    assert db.sql_rows("SELECT 2 AS value") == [{"value": 2}]


def test_embedded_timeout_is_typed(tmp_path):
    db = slow_database(tmp_path)
    with pytest.raises(QueryTimeoutError):
        db.sql_rows(SLOW_SQL, timeout_ms=1)
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
