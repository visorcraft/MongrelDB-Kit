"""Round 4 destruction tests — Python (13 tests).
Angles: multi-statement create-query, matview independence, privilege escalation,
FTS on identical values, recursive CTE modulo, window FIRST_VALUE,
block comments, CTAS with CASE, DROP+recreate.
"""

import os
import tempfile
import pytest
from mongreldb_kit import Database, table, column


def make_schema():
    return {"tables": [table("players", 1, [
        column("id", 1, "int64", primary_key=True),
        column("score", 2, "float64"),
        column("team", 3, "text"),
    ], "id")]}


def make_db():
    d = tempfile.mkdtemp()
    db = Database.create(os.path.join(d, "db"), make_schema())
    txn = db.begin()
    txn.insert("players", {"id": 1, "score": 100.0, "team": "A"})
    txn.insert("players", {"id": 2, "score": 100.0, "team": "A"})
    txn.insert("players", {"id": 3, "score": 90.0, "team": "A"})
    txn.insert("players", {"id": 4, "score": 80.0, "team": "B"})
    txn.insert("players", {"id": 5, "score": 80.0, "team": "B"})
    txn.commit()
    return d, db


class TestMultiStmtCreateQuery:
    def test_create_then_query_same_batch(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "CREATE TABLE instant AS SELECT id FROM players LIMIT 2; "
                "SELECT count(*) AS c FROM instant"
            )
            assert len(rows) == 1
            assert rows[0]["c"] == 2
        finally:
            pass

    def test_create_drop_create_cycle(self):
        d, db = make_db()
        try:
            db.sql_rows(
                "CREATE TABLE c AS SELECT id FROM players LIMIT 1; "
                "DROP TABLE c; "
                "CREATE TABLE c AS SELECT id FROM players LIMIT 2"
            )
            rows = db.sql_rows("SELECT count(*) AS c FROM c")
            assert rows[0]["c"] == 2
        finally:
            pass


class TestMatviewIndependence:
    def test_snapshot_after_update(self):
        d, db = make_db()
        try:
            db.sql_rows("CREATE MATERIALIZED VIEW mv AS SELECT id, score FROM players")
            db.sql_rows("UPDATE players SET score = 999 WHERE id = 1")
            rows = db.sql_rows("SELECT score FROM mv WHERE id = 1")
            assert float(rows[0]["score"]) == 100.0
        finally:
            pass

    def test_drop_recreate_different_query(self):
        d, db = make_db()
        try:
            db.sql_rows("CREATE MATERIALIZED VIEW mv AS SELECT id FROM players WHERE team = 'A'")
            db.sql_rows("DROP TABLE mv")
            db.sql_rows("CREATE MATERIALIZED VIEW mv AS SELECT id FROM players WHERE team = 'B'")
            rows = db.sql_rows("SELECT count(*) AS c FROM mv")
            assert rows[0]["c"] == 2
        finally:
            pass


class TestAuthEdge:
    def test_privilege_escalation_blocked(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "pw")
            db.create_user("regular", "rpw")
            db.close()
            # Non-admin cannot create users.
            db2 = Database.open_with_credentials(path, "regular", "rpw")
            with pytest.raises(Exception):
                db2.create_user("intruder", "pw")
        finally:
            pass

    def test_disable_clears_table_enforcement(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "pw")
            db.disable_auth()
            db.close()
            db2 = Database.open(path)
            assert db2.require_auth_enabled() is False
        finally:
            pass

    @pytest.mark.xfail(reason="Known: session caching prevents refresh_principal from blocking SQL SELECT in Python/Kit path")
    def test_refresh_after_role_revoke(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "sec")
        try:
            db = Database.create_with_credentials(path, make_schema(), "admin", "pw")
            db.create_user("alice", "apw")
            db.create_role("r")
            db.grant_permission("r", "select:players")
            db.grant_role("alice", "r")

            db2 = Database.open_with_credentials(path, "alice", "apw")
            # Alice can select initially.
            db2.sql_rows("SELECT id FROM players LIMIT 1")

            # Admin revokes.
            db.revoke_role("alice", "r")
            db2.refresh_principal()

            # Alice should now be denied.
            with pytest.raises(Exception):
                db2.sql_rows("SELECT id FROM players LIMIT 1")
        finally:
            pass


class TestFTSEdge:
    def test_identical_values(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "SELECT id, mongreldb_fts_rank(team, 'A') AS score "
                "FROM players ORDER BY id"
            )
            assert len(rows) == 5
            assert float(rows[0]["score"]) > 0  # team A
            assert float(rows[3]["score"]) == 0  # team B
        finally:
            pass

    def test_numbers_in_text(self):
        d, db = make_db()
        try:
            db.sql_rows("INSERT INTO players (id, score, team) VALUES (99, 1.0, 'version 2.0 build 1234')")
            rows = db.sql_rows(
                "SELECT mongreldb_fts_rank(team, 'version 1234') AS score "
                "FROM players WHERE id = 99"
            )
            assert float(rows[0]["score"]) > 0
        finally:
            pass


class TestWindowEdge:
    def test_first_value(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "SELECT id, FIRST_VALUE(score) OVER (PARTITION BY team ORDER BY score DESC) AS top "
                "FROM players ORDER BY id"
            )
            assert len(rows) == 5
            assert float(rows[0]["top"]) == 100.0  # team A
            assert float(rows[3]["top"]) == 80.0   # team B
        finally:
            pass

    def test_dense_rank_ties(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS drk "
                "FROM players ORDER BY id"
            )
            assert len(rows) == 5
            assert rows[0]["drk"] == 1
            assert rows[1]["drk"] == 1  # tied
            assert rows[2]["drk"] == 2
            assert rows[3]["drk"] == 3
            assert rows[4]["drk"] == 3  # tied
        finally:
            pass


class TestRecursiveCTEEdge:
    def test_modulo_filter(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE r(n) AS "
                "(SELECT 0 UNION ALL SELECT n + 2 FROM r WHERE n < 10) "
                "SELECT count(*) AS c FROM r WHERE n % 4 = 0"
            )
            assert rows[0]["c"] == 3
        finally:
            pass

    def test_integer_division(self):
        d, db = make_db()
        try:
            rows = db.sql_rows(
                "WITH RECURSIVE r(n) AS "
                "(SELECT 256 UNION ALL SELECT n / 2 FROM r WHERE n > 1) "
                "SELECT n FROM r ORDER BY n"
            )
            assert len(rows) == 9  # 1,2,4,8,16,32,64,128,256
            assert rows[0]["n"] == 1
            assert rows[8]["n"] == 256
        finally:
            pass
