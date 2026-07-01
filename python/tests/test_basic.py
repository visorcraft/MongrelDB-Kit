import json
import os
import subprocess
import sys
import tempfile
import textwrap

import pytest

from mongreldb_kit import (
    Database,
    DuplicateError,
    ForeignKeyError,
    RestrictError,
    bool_,
    fk,
    float_,
    int,
    table,
    text,
    unique,
)


def tmp_db():
    tmp = tempfile.mkdtemp()
    return os.path.join(tmp, "db.kitdb")


def users_orders_schema():
    return {
        "tables": [
            table(
                name="users",
                id=1,
                columns=[
                    int("id", 1, primary_key=True),
                    text("email", 2),
                    text("name", 3, nullable=True),
                ],
                primary_key="id",
                unique_constraints=[unique("uq_email", "email")],
            ),
            table(
                name="orders",
                id=2,
                columns=[
                    int("id", 1, primary_key=True),
                    int("user_id", 2),
                    float_("total", 3, nullable=True),
                ],
                primary_key="id",
                foreign_keys=[
                    fk("fk_orders_user", "user_id", "users", "id", on_delete="restrict")
                ],
            ),
            table(
                name="items",
                id=3,
                columns=[
                    int("id", 1, primary_key=True),
                    int("order_id", 2),
                    text("sku", 3),
                ],
                primary_key="id",
                foreign_keys=[
                    fk("fk_items_order", "order_id", "orders", "id", on_delete="cascade")
                ],
            ),
        ]
    }


def insert_user(txn, id_, email, name=None):
    return txn.insert("users", {"id": id_, "email": email, "name": name})


def test_create_open_and_crud():
    path = tmp_db()
    schema = users_orders_schema()

    db = Database.create(path, schema)
    with db.begin() as txn:
        insert_user(txn, 1, "alice@example.com", "Alice")
        txn.commit()

    with db.begin() as txn:
        row = txn.get_by_pk("users", 1)
        assert row["email"] == "alice@example.com"
        assert row["name"] == "Alice"

    # Re-open and update.
    db2 = Database.open(path)
    with db2.begin() as txn:
        txn.update("users", 1, {"name": "Alice Smith"})
        txn.commit()

    with db2.begin() as txn:
        row = txn.get_by_pk("users", 1)
        assert row["name"] == "Alice Smith"


def test_select_filters_and_orders():
    path = tmp_db()
    db = Database.create(path, users_orders_schema())

    with db.begin() as txn:
        insert_user(txn, 1, "alice@example.com")
        insert_user(txn, 2, "bob@example.com")
        insert_user(txn, 3, "carol@example.com")
        txn.commit()

    with db.begin() as txn:
        rows = txn.select("users", filter={"id": {"gt": 1}}, order="-id", limit=2)
        assert [r["id"] for r in rows] == [3, 2]


def test_unique_constraint_violation():
    path = tmp_db()
    db = Database.create(path, users_orders_schema())

    with db.begin() as txn:
        insert_user(txn, 1, "alice@example.com")
        with pytest.raises(DuplicateError) as exc_info:
            txn.insert("users", {"id": 2, "email": "alice@example.com"})
        assert exc_info.value.code == "DUPLICATE"
        txn.rollback()


def test_foreign_key_violation():
    path = tmp_db()
    db = Database.create(path, users_orders_schema())

    with db.begin() as txn:
        insert_user(txn, 1, "alice@example.com")
        with pytest.raises(ForeignKeyError) as exc_info:
            txn.insert("orders", {"id": 1, "user_id": 99, "total": 10.0})
        assert exc_info.value.code == "FOREIGN_KEY"
        txn.rollback()


def test_insert_many_batch():
    path = tmp_db()
    db = Database.create(path, users_orders_schema())

    with db.begin() as txn:
        rows = txn.insert_many(
            "users",
            [
                {"id": 1, "email": "a@example.com"},
                {"id": 2, "email": "b@example.com"},
                {"id": 3, "email": "c@example.com"},
            ],
        )
        txn.commit()
    assert [r["id"] for r in rows] == [1, 2, 3]

    with db.begin() as txn:
        assert len(txn.select("users")) == 3

    # A duplicate PK inside a batch rejects the whole batch.
    with db.begin() as txn:
        with pytest.raises(DuplicateError):
            txn.insert_many(
                "users",
                [
                    {"id": 4, "email": "d@example.com"},
                    {"id": 1, "email": "e@example.com"},
                ],
            )
        txn.rollback()

    with db.begin() as txn:
        assert len(txn.select("users")) == 3


def test_restrict_delete_blocks():
    path = tmp_db()
    db = Database.create(path, users_orders_schema())

    with db.begin() as txn:
        insert_user(txn, 1, "alice@example.com")
        txn.insert("orders", {"id": 1, "user_id": 1, "total": 10.0})
        txn.commit()

    with db.begin() as txn:
        with pytest.raises(RestrictError):
            txn.delete("users", 1)
        txn.rollback()


def test_cascade_delete_removes_children():
    path = tmp_db()
    db = Database.create(path, users_orders_schema())

    with db.begin() as txn:
        insert_user(txn, 1, "alice@example.com")
        txn.insert("orders", {"id": 1, "user_id": 1, "total": 10.0})
        txn.insert("items", {"id": 1, "order_id": 1, "sku": "ABC"})
        txn.commit()

    with db.begin() as txn:
        txn.delete("orders", 1)
        txn.commit()

    with db.begin() as txn:
        assert txn.get_by_pk("orders", 1) is None
        assert txn.get_by_pk("items", 1) is None


def test_migrate_records_versions():
    path = tmp_db()
    db = Database.create(path, {"tables": [users_orders_schema()["tables"][0]]})

    # Expand the schema and run migrations.
    db.set_schema(users_orders_schema())
    db.migrate(
        [
            {"version": 1, "name": "init", "ops": [{"create_table": {"name": "users"}}]},
            {"version": 2, "name": "add_orders", "ops": [{"create_table": {"name": "orders"}}]},
        ]
    )

    with db.begin() as txn:
        assert txn.get_by_pk("users", 1) is None


def test_migrate_alter_column_renames_and_relaxes_nullability():
    path = tmp_db()
    widgets_v1 = table(
        name="widgets",
        id=10,
        columns=[
            int("id", 1, primary_key=True),
            text("label", 2),
        ],
        primary_key="id",
    )
    widgets_v2 = table(
        name="widgets",
        id=10,
        columns=[
            int("id", 1, primary_key=True),
            text("name", 2, nullable=True),
        ],
        primary_key="id",
    )
    db = Database.create(path, {"tables": [widgets_v1]})

    with db.begin() as txn:
        txn.insert("widgets", {"id": 1, "label": "one"})
        txn.commit()

    db.set_schema({"tables": [widgets_v2]})
    db.migrate(
        [
            {
                "version": 1,
                "name": "alter_widget_name",
                "ops": [{"alter_column": {"table": "widgets", "column": "label"}}],
            }
        ]
    )

    with db.begin() as txn:
        row = txn.get_by_pk("widgets", 1)
        assert row["name"] == "one"
        txn.insert("widgets", {"id": 2, "name": None})
        txn.commit()


def test_allocate_sequence_and_table_names():
    path = tmp_db()
    db = Database.create(path, users_orders_schema())

    # 1-based (AUTO_INCREMENT): 1, then 2, then reserve 5 from 3, then 8.
    assert db.allocate_sequence("ids") == 1
    assert db.allocate_sequence("ids") == 2
    assert db.allocate_sequence("ids", 5) == 3
    assert db.allocate_sequence("ids") == 8

    names = sorted(db.table_names())
    assert names == ["items", "orders", "users"]
    assert all(not n.startswith("__kit_") for n in names)


def test_transaction_helper_commits():
    path = tmp_db()
    db = Database.create(path, users_orders_schema())

    db.transaction(lambda txn: insert_user(txn, 1, "alice@example.com", "Alice"))

    with db.begin() as txn:
        row = txn.get_by_pk("users", 1)
        assert row["email"] == "alice@example.com"


def test_open_transaction_pins_database_and_never_hangs():
    """A transaction keeps the engine alive while it is open.

    Regression: `Transaction` borrows `Database` behind a lifetime-erasing
    transmute, so closing the handle (or finalizing it during interpreter
    shutdown) used to free the engine out from under a live transaction — a
    use-after-free that hung the process. The scenario runs in a subprocess so a
    regression fails fast on timeout instead of hanging the whole suite.
    """
    script = textwrap.dedent(
        """
        import os, tempfile
        from mongreldb_kit import Database, table, int as kint, text

        schema = {"tables": [table(
            name="t", id=1,
            columns=[kint("id", 1, primary_key=True), text("v", 2)],
            primary_key="id",
        )]}
        tmp = tempfile.mkdtemp()

        # (1) Close the handle while a read transaction is still open: the txn
        # must keep the engine alive and stay usable, then close cleanly.
        db = Database.create(os.path.join(tmp, "a"), schema)
        with db.begin() as w:
            w.insert("t", {"id": 1, "v": "a"}); w.commit()
        rtxn = db.begin()
        assert rtxn.select("t") == [{"id": 1, "v": "a"}]
        db.close()
        assert rtxn.select("t") == [{"id": 1, "v": "a"}]
        rtxn.rollback()

        # (2) Leave a transaction open and let interpreter shutdown finalize it.
        db2 = Database.create(os.path.join(tmp, "b"), schema)
        with db2.begin() as w:
            w.insert("t", {"id": 1, "v": "b"}); w.commit()
        dangling = db2.begin()
        dangling.select("t")
        db2.close()  # dangling intentionally left open across process exit
        print("ok")
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        timeout=60,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert proc.stdout.strip().endswith("ok"), proc.stdout


def test_set_schema_blocked_while_transaction_open():
    """`set_schema` needs exclusive access to the engine, so it must reject a
    database that an open transaction still borrows rather than mutating state
    out from under it. Committing/rolling back the transaction releases that pin
    immediately, so exclusive access returns without waiting for GC."""
    path = tmp_db()
    db = Database.create(path, users_orders_schema())
    txn = db.begin()
    with pytest.raises(RuntimeError, match="transaction is open"):
        db.set_schema(users_orders_schema())
    txn.rollback()
    # The (still-referenced) txn object no longer pins the engine after rollback.
    db.set_schema(users_orders_schema())
