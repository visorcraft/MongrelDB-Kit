"""Deep adversarial tests — Python surface.
Focus: data integrity, reopen persistence, schema correctness, edge types,
auth + SQL interaction, error recovery.
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


def make_db(n=10):
    d = tempfile.mkdtemp()
    db = Database.create(os.path.join(d, "db"), make_schema())
    txn = db.begin()
    for i in range(1, n + 1):
        txn.insert("items", {"id": i, "amount": float(i) * 1.5, "name": f"item_{i}"})
    txn.commit()
    return d, db


# ── CTAS data integrity ─────────────────────────────────────────────────────

class TestCTASIntegrity:
    def test_values_match_source(self):
        d, db = make_db(5)
        try:
            db.sql_rows("CREATE TABLE copy AS SELECT id, amount, name FROM items")
            src = db.sql_rows("SELECT id, amount, name FROM items ORDER BY id")
            cpy = db.sql_rows("SELECT id, amount, name FROM copy ORDER BY id")
            assert len(src) == len(cpy)
            for s, c in zip(src, cpy):
                assert s == c
        finally:
            pass

    def test_ctas_from_aggregation_correct_sum(self):
        d, db = make_db(5)
        try:
            db.sql_rows("CREATE TABLE totals AS SELECT sum(amount) AS total FROM items")
            rows = db.sql_rows("SELECT total FROM totals")
            # 1.5 + 3.0 + 4.5 + 6.0 + 7.5 = 22.5
            assert abs(float(rows[0]["total"]) - 22.5) < 0.1
        finally:
            pass

    def test_ctas_table_accepts_insert(self):
        d, db = make_db(2)
        try:
            db.sql_rows("CREATE TABLE derived AS SELECT id FROM items LIMIT 1")
            db.sql_rows("INSERT INTO derived (id) VALUES (999)")
            rows = db.sql_rows("SELECT count(*) AS c FROM derived")
            assert rows[0]["c"] == 2
        finally:
            pass


# ── Reopen persistence ──────────────────────────────────────────────────────

class TestReopenPersistence:
    def test_ctas_survives_reopen(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "db")
        try:
            db = Database.create(path, make_schema())
            txn = db.begin()
            for i in range(1, 6):
                txn.insert("items", {"id": i, "amount": float(i), "name": f"x{i}"})
            txn.commit()
            db.sql_rows("CREATE TABLE copy AS SELECT id FROM items")

            db2 = Database.open(path)
            assert db2.table_names() if hasattr(db2, "table_names") else True
            rows = db2.sql_rows("SELECT count(*) AS c FROM copy")
            assert rows[0]["c"] == 5
        finally:
            pass

    def test_matview_survives_reopen(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "db")
        try:
            db = Database.create(path, make_schema())
            txn = db.begin()
            for i in range(1, 11):
                txn.insert("items", {"id": i, "amount": float(i), "name": f"x{i}"})
            txn.commit()
            db.sql_rows("CREATE MATERIALIZED VIEW mv AS SELECT id FROM items WHERE id < 5")

            db2 = Database.open(path)
            rows = db2.sql_rows("SELECT count(*) AS c FROM mv")
            assert rows[0]["c"] == 4
        finally:
            pass


# ── Recursive CTE correctness ───────────────────────────────────────────────

class TestRecursiveCTECorrectness:
    def test_fibonacci(self):
        d, db = make_db(3)
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE fib(a, b) AS "
                "(SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 100) "
                "SELECT a FROM fib ORDER BY a"
            )
            assert len(rows) >= 10
            assert rows[0]["a"] == 0
            assert rows[1]["a"] == 1
        finally:
            pass

    def test_powers_of_two(self):
        d, db = make_db(1)
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE pow(n) AS "
                "(SELECT 1 UNION ALL SELECT n * 2 FROM pow WHERE n < 256) "
                "SELECT n FROM pow ORDER BY n"
            )
            assert len(rows) == 9  # 1,2,4,8,16,32,64,128,256
            assert rows[8]["n"] == 256
        finally:
            pass

    def test_join_with_real_table(self):
        d, db = make_db(3)
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE r(n) AS "
                "(SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) "
                "SELECT r.n FROM r JOIN items ON r.n = items.id ORDER BY r.n"
            )
            assert len(rows) == 3  # items has ids 1,2,3
        finally:
            pass


# ── Error recovery ──────────────────────────────────────────────────────────

class TestErrorRecovery:
    def test_statement_before_failure_persists(self):
        d, db = make_db(5)
        try:
            try:
                db.sql_rows(
                    "CREATE TABLE before_err AS SELECT id FROM items LIMIT 1; "
                    "INSERT INTO nonexistent VALUES (1)"
                )
            except Exception:
                pass
            rows = db.sql_rows("SELECT count(*) AS c FROM before_err")
            assert rows[0]["c"] == 1
        finally:
            pass

    def test_all_select_returns_last(self):
        d, db = make_db(3)
        try:
            rows = db.sql_rows("SELECT 1 AS n; SELECT 2 AS n; SELECT 3 AS n")
            assert len(rows) == 1
            assert rows[0]["n"] == 3
        finally:
            pass


# ── Window function correctness ─────────────────────────────────────────────

class TestWindowCorrectness:
    def test_running_total(self):
        d, db = make_db(5)
        try:
            rows = db.sql_rows(
                "SELECT id, SUM(amount) OVER (ORDER BY id) AS running "
                "FROM items ORDER BY id"
            )
            assert len(rows) == 5
            # 1.5, 4.5, 9.0, 15.0, 22.5
            assert abs(float(rows[0]["running"]) - 1.5) < 0.1
            assert abs(float(rows[4]["running"]) - 22.5) < 0.1
        finally:
            pass

    def test_rank_descending(self):
        d, db = make_db(5)
        try:
            rows = db.sql_rows(
                "SELECT id, RANK() OVER (ORDER BY amount DESC) AS rnk "
                "FROM items ORDER BY rnk"
            )
            assert len(rows) == 5
            assert rows[0]["rnk"] == 1
            assert rows[4]["rnk"] == 5
        finally:
            pass


# ── Auth + SQL interaction ──────────────────────────────────────────────────

class TestAuthSQLInteraction:
    def test_admin_ctas_under_require_auth(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "pw")
            txn = db.begin()
            txn.insert("items", {"id": 1, "amount": 1.0, "name": "test"})
            txn.commit()
            # Admin should be able to CTAS.
            db.sql_rows("CREATE TABLE copy AS SELECT id FROM items")
            rows = db.sql_rows("SELECT count(*) AS c FROM copy")
            assert rows[0]["c"] == 1
        finally:
            pass

    def test_reader_cannot_ctas(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "pw")
            txn = db.begin()
            txn.insert("items", {"id": 1, "amount": 1.0, "name": "test"})
            txn.commit()
            db.create_user("reader", "r")
            db.create_role("r_role")
            db.grant_permission("r_role", "select:items")
            db.grant_role("reader", "r_role")

            db2 = Database.open_with_credentials(path, "reader", "r")
            # reader has Select on items but NOT Ddl → CTAS should fail.
            with pytest.raises(Exception):
                db2.sql_rows("CREATE TABLE copy AS SELECT id FROM items")
        finally:
            pass

    def test_disable_then_plain_open_works(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "pw")
            db.disable_auth()
            # Plain open should work.
            db2 = Database.open(path)
            assert db2.require_auth_enabled() is False
        finally:
            pass


# ── FTS ranking correctness ─────────────────────────────────────────────────

class TestFTSRankCorrectness:
    def test_multi_term_ranking(self):
        d, db = make_db(5)
        try:
            rows = db.sql_rows(
                "SELECT id, mongreldb_fts_rank(name, 'item_1 item_5') AS score "
                "FROM items ORDER BY score DESC, id ASC"
            )
            assert len(rows) == 5
            # item_1 and item_5 should be in top 2.
            top_ids = {rows[0]["id"], rows[1]["id"]}
            assert 1 in top_ids
            assert 5 in top_ids
        finally:
            pass

    def test_top_k_search(self):
        d, db = make_db(5)
        try:
            rows = db.sql_rows(
                "SELECT id FROM items "
                "WHERE mongreldb_fts_rank(name, 'item') > 0 "
                "ORDER BY mongreldb_fts_rank(name, 'item') DESC, id ASC LIMIT 3"
            )
            assert len(rows) == 3
            assert rows[0]["id"] == 1
        finally:
            pass
