import json
import os
import tempfile

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
