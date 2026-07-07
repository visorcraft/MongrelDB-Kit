"""Creative destruction tests — Python surface (14 tests).
Angles: stale matviews, Unicode, chained ops, error propagation,
auth edge cases, transaction rollback with SQL features.
"""

import os
import tempfile
import pytest
from mongreldb_kit import Database, table, column


def make_schema():
    return {"tables": [table("items", 1, [
        column("id", 1, "int64", primary_key=True),
        column("amount", 2, "float64"),
        column("name", 3, "text"),
    ], "id")]}


def make_db(n=5):
    d = tempfile.mkdtemp()
    db = Database.create(os.path.join(d, "db"), make_schema())
    txn = db.begin()
    for i in range(1, n + 1):
        txn.insert("items", {"id": i, "amount": float(i) * 1.5, "name": f"item_{i}"})
    txn.commit()
    return d, db


class TestStaleMatview:
    def test_matview_snapshot_after_delete(self):
        d, db = make_db(3)
        try:
            db.sql_rows("CREATE MATERIALIZED VIEW mv AS SELECT id FROM items")
            db.sql_rows("DELETE FROM items WHERE id = 1")
            rows = db.sql_rows("SELECT count(*) AS c FROM mv")
            assert rows[0]["c"] == 3, "matview should be a snapshot with 3 rows"
        finally:
            pass


class TestUnicode:
    def test_ctas_preserves_unicode(self):
        d = tempfile.mkdtemp()
        try:
            db = Database.create(os.path.join(d, "db"), make_schema())
            txn = db.begin()
            txn.insert("items", {"id": 1, "amount": 1.0, "name": "hello"})
            txn.insert("items", {"id": 2, "amount": 2.0, "name": "日本語"})
            txn.commit()
            db.sql_rows("CREATE TABLE ucopy AS SELECT id, name FROM items")
            rows = db.sql_rows("SELECT name FROM ucopy ORDER BY id")
            assert rows[1]["name"] == "日本語"
        finally:
            pass

    def test_fts_rank_unicode(self):
        d = tempfile.mkdtemp()
        try:
            db = Database.create(os.path.join(tempfile.mkdtemp(), "db"), make_schema())
            txn = db.begin()
            txn.insert("items", {"id": 1, "amount": 1.0, "name": "日本語テキスト"})
            txn.commit()
            # Should not crash.
            db.sql_rows("SELECT mongreldb_fts_rank(name, '日本語') AS score FROM items")
        finally:
            pass


class TestChainedOps:
    def test_ctas_insert_ctas_select(self):
        d, db = make_db(5)
        try:
            db.sql_rows("CREATE TABLE a AS SELECT id FROM items WHERE id <= 3")
            db.sql_rows("INSERT INTO a (id) VALUES (99)")
            db.sql_rows("CREATE TABLE b AS SELECT id FROM a WHERE id < 50")
            rows = db.sql_rows("SELECT count(*) AS c FROM b")
            assert rows[0]["c"] == 3  # ids 1,2,3 (99 excluded)
        finally:
            pass

    def test_matview_on_matview(self):
        d, db = make_db(5)
        try:
            db.sql_rows("CREATE MATERIALIZED VIEW mv1 AS SELECT id FROM items")
            db.sql_rows("CREATE MATERIALIZED VIEW mv2 AS SELECT id FROM mv1 WHERE id <= 3")
            rows = db.sql_rows("SELECT count(*) AS c FROM mv2")
            assert rows[0]["c"] == 3
        finally:
            pass


class TestErrorPropagation:
    def test_ctas_nonexistent_source(self):
        d, db = make_db(3)
        try:
            with pytest.raises(Exception):
                db.sql_rows("CREATE TABLE x AS SELECT * FROM ghost")
        finally:
            pass

    def test_select_dropped_ctas_table(self):
        d, db = make_db(3)
        try:
            db.sql_rows("CREATE TABLE temp AS SELECT id FROM items LIMIT 1")
            db.sql_rows("DROP TABLE temp")
            with pytest.raises(Exception):
                db.sql_rows("SELECT * FROM temp")
        finally:
            pass

    def test_multi_statement_error_first(self):
        d, db = make_db(3)
        try:
            with pytest.raises(Exception):
                db.sql_rows("SELECT FROM ghost; SELECT 1")
            # DB still usable.
            rows = db.sql_rows("SELECT id FROM items LIMIT 1")
            assert len(rows) == 1
        finally:
            pass


class TestAuthEdge:
    def test_enable_then_create_user_under_auth(self):
        d = tempfile.mkdtemp()
        try:
            db = Database.create(os.path.join(d, "db"), make_schema())
            db.enable_auth("admin", "pw")
            assert db.require_auth_enabled() is True
            db.create_user("user2", "pass2")
            assert "admin" in db.users()
            assert "user2" in db.users()
        finally:
            pass

    def test_disable_reenable_different_admin(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin1", "pw1")
            db.disable_auth()
            db.enable_auth("admin2", "pw2")
            db.close()
            db2 = Database.open_with_credentials(path, "admin2", "pw2")
            assert db2.require_auth_enabled() is True
        finally:
            pass

    def test_refresh_principal_after_grant(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "pw")
            db.create_user("alice", "apw")
            db2 = Database.open_with_credentials(path, "alice", "apw")
            # Alice has no permissions initially.
            # Admin grants Alice a role.
            db.create_role("r")
            db.grant_permission("r", "select:items")
            db.grant_role("alice", "r")
            # Alice refreshes — should now have Select.
            db2.refresh_principal()
            # Alice can now SELECT (this exercises the refreshed permission).
            txn = db2.begin()
            try:
                rows = txn.select("items")
                assert isinstance(rows, list)
            finally:
                txn.rollback()
        finally:
            pass


class TestLargeData:
    def test_recursive_cte_1000_rows(self):
        d, db = make_db(1)
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE r(n) AS "
                "(SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 1000) "
                "SELECT count(*) AS c FROM r"
            )
            assert rows[0]["c"] == 1000
        finally:
            pass

    def test_ctas_500_rows(self):
        d = tempfile.mkdtemp()
        try:
            db = Database.create(os.path.join(d, "db"), make_schema())
            txn = db.begin()
            for i in range(1, 501):
                txn.insert("items", {"id": i, "amount": float(i), "name": f"x{i}"})
            txn.commit()
            db.sql_rows("CREATE TABLE big AS SELECT id FROM items")
            rows = db.sql_rows("SELECT count(*) AS c FROM big")
            assert rows[0]["c"] == 500
        finally:
            pass
