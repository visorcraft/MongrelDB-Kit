"""Adversarial cross-language tests — Python surface.
Tests the full stack: Database facade → pyo3 → kit → engine.
"""

import os
import tempfile
import pytest
from mongreldb_kit import Database, table, column


def make_schema():
    return {"tables": [table("orders", 1, [
        column("id", 1, "int64", primary_key=True),
        column("amount", 2, "float64"),
        column("category", 3, "text"),
    ], "id")]}


def make_db():
    d = tempfile.mkdtemp()
    db = Database.create(os.path.join(d, "db"), make_schema())
    t = db.schema if hasattr(db, 'schema') else None
    # Seed via transaction
    txn = db.begin()
    txn.insert("orders", {"id": 1, "amount": 10.0, "category": "food"})
    txn.insert("orders", {"id": 2, "amount": 20.0, "category": "food"})
    txn.insert("orders", {"id": 3, "amount": 30.0, "category": "toys"})
    txn.insert("orders", {"id": 4, "amount": 40.0, "category": "toys"})
    txn.insert("orders", {"id": 5, "amount": 50.0, "category": "toys"})
    txn.commit()
    return d, db


# ── Recursive CTEs ─────────────────────────────────────────────────────────

class TestRecursiveCTE:
    def test_basic_sequence(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE counter(n) AS "
                "(SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 10) "
                "SELECT n FROM counter ORDER BY n"
            )
            assert len(rows) == 10
            assert rows[0]["n"] == 1
            assert rows[9]["n"] == 10
        finally:
            os.makedirs(d, exist_ok=True)  # ensure cleanup

    def test_immediate_convergence(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE r(n) AS "
                "(SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 0) "
                "SELECT n FROM r"
            )
            assert len(rows) == 1
            assert rows[0]["n"] == 1
        finally:
            pass

    def test_on_real_table(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE r(id) AS "
                "(SELECT id FROM orders WHERE id = 1 "
                "UNION ALL SELECT id + 1 FROM r WHERE id < 3) "
                "SELECT id FROM r ORDER BY id"
            )
            assert len(rows) == 3
            assert rows[0]["id"] == 1
            assert rows[2]["id"] == 3
        finally:
            pass

    def test_empty_base(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE r(n) AS "
                "(SELECT 1 WHERE 1 = 0 "
                "UNION ALL SELECT n + 1 FROM r WHERE n < 5) "
                "SELECT n FROM r"
            )
            assert len(rows) == 0
        finally:
            pass


# ── CTAS ────────────────────────────────────────────────────────────────────

class TestCTAS:
    def test_filtered_copy(self):
        d, db = make_db()
        try:
            db.sql_rows("CREATE TABLE food_orders AS SELECT id, amount FROM orders WHERE category = 'food'")
            rows = db.sql_rows("SELECT id FROM food_orders ORDER BY id")
            assert len(rows) == 2
            assert rows[0]["id"] == 1
        finally:
            pass

    def test_aggregation(self):
        d, db = make_db()
        try:
            db.sql_rows(
                "CREATE TABLE summary AS "
                "SELECT category, sum(amount) AS total FROM orders GROUP BY category"
            )
            rows = db.sql_rows("SELECT category FROM summary ORDER BY category")
            assert len(rows) == 2
        finally:
            pass

    def test_duplicate_fails(self):
        d, db = make_db()
        try:
            db.sql_rows("CREATE TABLE dup AS SELECT id FROM orders")
            with pytest.raises(Exception):
                db.sql_rows("CREATE TABLE dup AS SELECT id FROM orders")
        finally:
            pass

    def test_if_not_exists(self):
        d, db = make_db()
        try:
            db.sql_rows("CREATE TABLE IF NOT EXISTS idem AS SELECT id FROM orders")
            # Should not raise
            db.sql_rows("CREATE TABLE IF NOT EXISTS idem AS SELECT id FROM orders")
        finally:
            pass


# ── Materialized views ──────────────────────────────────────────────────────

class TestMaterializedView:
    def test_create_and_query(self):
        d, db = make_db()
        try:
            db.sql_rows("CREATE MATERIALIZED VIEW mv AS SELECT id FROM orders WHERE category = 'food'")
            rows = db.sql_rows("SELECT id FROM mv ORDER BY id")
            assert len(rows) == 2
        finally:
            pass

    def test_duplicate_fails(self):
        d, db = make_db()
        try:
            db.sql_rows("CREATE MATERIALIZED VIEW dup_mv AS SELECT id FROM orders")
            with pytest.raises(Exception):
                db.sql_rows("CREATE MATERIALIZED VIEW dup_mv AS SELECT id FROM orders")
        finally:
            pass


# ── Multi-statement ─────────────────────────────────────────────────────────

class TestMultiStatement:
    def test_last_result_returned(self):
        d, db = make_db()
        try:
            rows = db.sql_rows("SELECT 1 AS n; SELECT 2 AS n; SELECT 3 AS n")
            assert len(rows) == 1
            assert rows[0]["n"] == 3
        finally:
            pass

    def test_semicolon_in_string(self):
        d, db = make_db()
        try:
            rows = db.sql_rows("SELECT 'hello; world' AS g FROM orders LIMIT 1")
            assert len(rows) == 1
            assert ";" in rows[0]["g"]
        finally:
            pass

    def test_ctas_and_insert_batch(self):
        d, db = make_db()
        try:
            db.sql_rows(
                "CREATE TABLE batch_t AS SELECT id FROM orders; "
                "INSERT INTO batch_t (id) VALUES (99); "
                "SELECT count(*) AS cnt FROM batch_t"
            )
            rows = db.sql_rows("SELECT count(*) AS cnt FROM batch_t")
            assert rows[0]["cnt"] == 6
        finally:
            pass

    def test_trailing_semicolon(self):
        d, db = make_db()
        try:
            rows = db.sql_rows("SELECT 1 AS n;")
            assert len(rows) == 1
        finally:
            pass

    def test_only_semicolons(self):
        d, db = make_db()
        try:
            db.sql_rows(";;;")
        except Exception:
            pass  # OK if it errors, just should not crash


# ── FTS ranking ─────────────────────────────────────────────────────────────

class TestFTSRank:
    def test_food_scores_higher_than_toys(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "SELECT category, mongreldb_fts_rank(category, 'food') AS score "
                "FROM orders ORDER BY score DESC"
            )
            assert len(rows) == 5
            assert rows[0]["category"] == "food"
        finally:
            pass

    def test_no_match_zero(self):
        d, db = make_db()
        try:
            rows = db.sql_rows("SELECT mongreldb_fts_rank('hello world', 'nonexistent') AS score")
            assert len(rows) == 1
            assert float(rows[0]["score"]) == 0.0
        finally:
            pass

    def test_empty_query(self):
        d, db = make_db()
        try:
            db.sql_rows("SELECT mongreldb_fts_rank('hello', '') AS score")
        finally:
            pass


# ── Window functions ────────────────────────────────────────────────────────

class TestWindowFunctions:
    def test_row_number(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "SELECT id, category, "
                "ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn "
                "FROM orders ORDER BY id"
            )
            assert len(rows) == 5
            assert rows[0]["rn"] == 1
            assert rows[2]["rn"] == 1  # first toys row
        finally:
            pass

    def test_sum_over_partition(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "SELECT id, SUM(amount) OVER (PARTITION BY category) AS total "
                "FROM orders ORDER BY id"
            )
            assert len(rows) == 5
            assert float(rows[0]["total"]) == 30.0  # food: 10+20
            assert float(rows[2]["total"]) == 120.0  # toys: 30+40+50
        finally:
            pass


# ── Credential enforcement ──────────────────────────────────────────────────

class TestCredentialEnforcement:
    def test_create_and_reopen(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "s3cret")
            assert db.require_auth_enabled() is True

            db2 = Database.open_with_credentials(path, "admin", "s3cret")
            assert db2.require_auth_enabled() is True
        finally:
            pass

    def test_wrong_password(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            Database.create_with_credentials(path, make_schema(), "admin", "s3cret")
            with pytest.raises(Exception):
                Database.open_with_credentials(path, "admin", "WRONG")
        finally:
            pass

    def test_nonexistent_user(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            Database.create_with_credentials(path, make_schema(), "admin", "s3cret")
            with pytest.raises(Exception):
                Database.open_with_credentials(path, "ghost", "pw")
        finally:
            pass

    def test_enable_auth_then_reopen_without(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "plain")
        try:
            db = Database.create(path, make_schema())
            db.enable_auth("admin", "s3cret")
            assert db.require_auth_enabled() is True

            # Reopen without credentials should fail
            with pytest.raises(Exception):
                Database.open(path)
        finally:
            pass

    def test_disable_auth_reverts(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "pw")
            db.disable_auth()
            assert db.require_auth_enabled() is False

            # Plain open should work now
            db2 = Database.open(path)
            assert db2.require_auth_enabled() is False
        finally:
            pass

    def test_double_disable_fails(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "db")
        try:
            db = Database.create(path, make_schema())
            with pytest.raises(Exception):
                db.disable_auth()
        finally:
            pass

    def test_user_role_management_under_auth(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "admin-pw")
            db.create_user("reader", "r-pw")
            db.create_role("read_only")
            db.grant_permission("read_only", "select:orders")
            db.grant_role("reader", "read_only")

            db2 = Database.open_with_credentials(path, "reader", "r-pw")
            # reader can SELECT but the user list should show both users
            assert "admin" in db2.users()
            assert "reader" in db2.users()
        finally:
            pass


# ── Cross-feature interactions ──────────────────────────────────────────────

class TestCrossFeature:
    def test_ctas_then_recursive_cte(self):
        d, db = make_db()
        try:
            db.sql_rows("CREATE TABLE copy AS SELECT id FROM orders")
            rows = db.sql_rows(
                "WITH RECURSIVE r(n) AS "
                "(SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                "SELECT n FROM r"
            )
            assert len(rows) == 3
        finally:
            pass

    def test_multi_statement_with_recursive_cte(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "SELECT 1; "
                "WITH RECURSIVE r(n) AS "
                "(SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                "SELECT n FROM r"
            )
            assert len(rows) == 3
        finally:
            pass

    def test_matview_then_fts_rank(self):
        d, db = make_db()
        try:
            db.sql_rows("CREATE MATERIALIZED VIEW cat_mv AS SELECT DISTINCT category FROM orders")
            rows = db.sql_rows(
                "SELECT category, mongreldb_fts_rank(category, 'food') AS score "
                "FROM cat_mv ORDER BY score DESC"
            )
            assert len(rows) == 2
            assert rows[0]["category"] == "food"
        finally:
            pass
