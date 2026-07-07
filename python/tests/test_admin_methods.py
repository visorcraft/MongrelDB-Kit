"""Tests for the kit's administrative/lifecycle methods: rename_table,
compact_all/compact_table, analyze, vacuum, and the SQL read surface
(sql_rows / sql_arrow).

These mirror the Rust integration tests in crates/mongreldb-kit/tests/integration.rs
and the TypeScript db.test.ts suite, pinning cross-language parity.
"""

import builtins
import json
import os
import subprocess
import sys
import tempfile

import pytest

from mongreldb_kit import Database, ValidationError, bool_, int, table, text

# `int` is re-exported by mongreldb_kit as a column builder; alias the builtin
# so isinstance(..., int) checks below resolve to the type, not the helper.
py_int = builtins.int


def tmp_db():
    tmp = tempfile.mkdtemp()
    return os.path.join(tmp, "db.kitdb")


def widgets_schema():
    return {
        "tables": [
            table(
                name="widgets",
                id=1,
                columns=[
                    int("id", 1, primary_key=True),
                    text("name", 2),
                    bool_("active", 3, default={"static": True}),
                ],
                primary_key="id",
            ),
        ]
    }


def make_db():
    path = tmp_db()
    db = Database.create(path, json.dumps(widgets_schema()))
    return db


def test_rename_table_moves_data_and_blocks_internal_names():
    db = make_db()
    with db.begin() as txn:
        txn.insert("widgets", {"id": 1, "name": "w1"})
    db.rename_table("widgets", "things")
    with db.begin() as txn:
        row = txn.get_by_pk("things", {"id": 1})
        assert row is not None
        assert row["name"] == "w1"
    # Reject __kit_-reserved names (parity with the TS kit).
    with pytest.raises(Exception):
        db.rename_table("things", "__kit_evil")
    db.close()


def test_compact_runs_without_error_and_reports_counts():
    db = make_db()
    with db.begin() as txn:
        for i in range(5):
            txn.insert("widgets", {"id": i, "name": f"w{i}"})
    # compact_all returns a (compacted, skipped) tuple.
    result = db.compact_all()
    assert isinstance(result, tuple) and len(result) == 2
    # compact_table returns a bool.
    assert isinstance(db.compact_table("widgets"), bool)
    db.close()


def test_analyze_and_vacuum_run_without_error():
    db = make_db()
    with db.begin() as txn:
        txn.insert("widgets", {"id": 1, "name": "w1"})
    db.analyze()  # ensure_indexes_complete on every table; no return
    reclaimed = db.vacuum()  # compact_all + gc
    assert isinstance(reclaimed, py_int)
    db.close()


def test_sql_rows_and_sql_arrow_read_paths():
    db = make_db()
    with db.begin() as txn:
        txn.insert("widgets", {"id": 1, "name": "alpha"})
        txn.insert("widgets", {"id": 2, "name": "beta"})
    rows = db.sql_rows("SELECT id, name FROM widgets ORDER BY id")
    assert rows == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "beta"},
    ]
    ipc = db.sql_arrow("SELECT id FROM widgets ORDER BY id")
    assert isinstance(ipc, (bytes, bytearray))
    assert len(ipc) > 0
    db.close()


def test_sql_views_persist_across_calls():
    db = make_db()
    with db.begin() as txn:
        txn.insert("widgets", {"id": 1, "name": "alpha"})
        txn.insert("widgets", {"id": 2, "name": "beta"})
    assert db.sql_rows("CREATE VIEW v AS SELECT id, name FROM widgets WHERE id >= 2") == []
    assert db.sql_rows("SELECT * FROM v ORDER BY id") == [{"id": 2, "name": "beta"}]
    db.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
