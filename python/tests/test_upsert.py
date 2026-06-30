import pytest

from mongreldb_kit import Database, ValidationError, int, table, text, unique


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "db.kitdb")


def users_schema():
    return {
        "tables": [
            table(
                name="users",
                id=1,
                columns=[
                    int(
                        "id",
                        1,
                        primary_key=True,
                        default={"sequence": "users_id_seq"},
                    ),
                    text("email", 2),
                    text("name", 3, nullable=True),
                ],
                primary_key="id",
                unique_constraints=[unique("uq_email", "email")],
            )
        ]
    }


def test_insert_returning_with_auto_increment_pk(tmp_db):
    db = Database.create(tmp_db, users_schema())

    with db.begin() as txn:
        row = txn.insert_returning(
            "users",
            {"email": "alice@example.com", "name": "Alice"},
            ["id", "email"],
        )
        txn.commit()

    assert row["id"] == 1
    assert row["email"] == "alice@example.com"


def test_upsert_do_update_set_form(tmp_db):
    db = Database.create(tmp_db, users_schema())

    with db.begin() as txn:
        inserted = txn.insert_returning(
            "users",
            {"email": "bob@example.com", "name": "Bob"},
            ["id"],
        )
        txn.commit()

    with db.begin() as txn:
        result = txn.upsert(
            "users",
            {"id": inserted["id"], "email": "bob@example.com", "name": "Bobby"},
            on_conflict={"do_update": {"set": {"name": "Bobby"}}},
            returning=["id", "name"],
        )
        txn.commit()

    assert result["name"] == "Bobby"

    with db.begin() as txn:
        row = txn.get_by_pk("users", result["id"])
        assert row["name"] == "Bobby"


def test_upsert_do_update_direct_patch(tmp_db):
    db = Database.create(tmp_db, users_schema())

    with db.begin() as txn:
        inserted = txn.insert_returning(
            "users",
            {"email": "carol@example.com", "name": "Carol"},
            ["id"],
        )
        txn.commit()

    with db.begin() as txn:
        result = txn.upsert(
            "users",
            {"id": inserted["id"], "email": "carol@example.com", "name": "Carrie"},
            on_conflict={"do_update": {"name": "Carrie"}},
            returning=["id", "name"],
        )
        txn.commit()

    assert result["name"] == "Carrie"

    with db.begin() as txn:
        row = txn.get_by_pk("users", result["id"])
        assert row["name"] == "Carrie"


def test_upsert_invalid_on_conflict_raises(tmp_db):
    db = Database.create(tmp_db, users_schema())

    with db.begin() as txn:
        with pytest.raises(ValidationError):
            txn.upsert(
                "users",
                {"id": 1, "email": "dave@example.com", "name": "Dave"},
                on_conflict={"do_update": "name"},
            )
        txn.rollback()
