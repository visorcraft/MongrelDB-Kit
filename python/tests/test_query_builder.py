import os
import tempfile

from mongreldb_kit import (
    Database,
    agg,
    fk,
    float_,
    int,
    on_eq,
    table,
    text,
    unique,
)


def tmp_db():
    tmp = tempfile.mkdtemp()
    return os.path.join(tmp, "db.kitdb")


def schema():
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
        ]
    }


def seed(db):
    with db.begin() as txn:
        txn.insert("users", {"id": 1, "email": "a@example.com", "name": "Dup"})
        txn.insert("users", {"id": 2, "email": "b@example.com", "name": "Dup"})
        txn.insert("users", {"id": 3, "email": "c@test.com", "name": "Other"})
        txn.insert("orders", {"id": 1, "user_id": 1, "total": 10.0})
        txn.insert("orders", {"id": 2, "user_id": 1, "total": 30.0})
        txn.insert("orders", {"id": 3, "user_id": 2, "total": 5.0})
        txn.commit()


def test_aggregates_group_by_and_having():
    db = Database.create(tmp_db(), schema())
    seed(db)

    with db.begin() as txn:
        # Whole table as one group.
        rows = txn.aggregate(
            "orders",
            [
                agg("count", "n"),
                agg("sum", "s", "total"),
                agg("min", "mn", "total"),
                agg("max", "mx", "total"),
                agg("avg", "av", "total"),
            ],
        )
        assert rows == [{"n": 3, "s": 45.0, "mn": 5.0, "mx": 30.0, "av": 15.0}]

        # Grouped with HAVING keeps only the user with > 1 order.
        rows = txn.aggregate(
            "orders",
            [agg("count", "n"), agg("sum", "s", "total")],
            group_by=["user_id"],
            having={"n": {"gt": 1}},
        )
        assert rows == [{"user_id": 1, "n": 2, "s": 40.0}]


def test_join_inner_and_left():
    db = Database.create(tmp_db(), schema())
    seed(db)

    with db.begin() as txn:
        inner = txn.join(
            "users",
            [{"kind": "inner", "table": "orders", "alias": "o", "on": on_eq("u.id", "o.user_id")}],
            alias="u",
        )
        assert len(inner) == 3  # user1 x2 orders, user2 x1 order
        assert all(r["o"]["user_id"] == r["u"]["id"] for r in inner)

        left = txn.join(
            "users",
            [{"kind": "left", "table": "orders", "alias": "o", "on": on_eq("u.id", "o.user_id")}],
            alias="u",
        )
        # 3 matched rows plus user3 with no order.
        assert len(left) == 4
        unmatched = [r for r in left if r["o"] is None]
        assert len(unmatched) == 1
        assert unmatched[0]["u"]["id"] == 3


def test_distinct_like_not_in_and_exists():
    db = Database.create(tmp_db(), schema())
    seed(db)

    with db.begin() as txn:
        names = txn.select("users", columns=["name"], distinct=True)
        assert names == [{"name": "Dup"}, {"name": "Other"}]

        like = txn.select("users", filter={"email": {"like": "%@example.com"}})
        assert sorted(r["id"] for r in like) == [1, 2]

        not_in = txn.select("users", filter={"id": {"not_in": [1, 2]}})
        assert [r["id"] for r in not_in] == [3]

        # id IN (SELECT user_id FROM orders WHERE total > 20)
        sub = {"id": {"in_subquery": {"table": "orders", "columns": ["user_id"], "filter": {"total": {"gt": 20}}}}}
        big = txn.select("users", filter=sub)
        assert [r["id"] for r in big] == [1]

        has_orders = txn.select(
            "users", filter={"exists": {"table": "orders", "filter": {"total": {"gt": 100}}}}
        )
        assert has_orders == []


def test_cte_materialization():
    db = Database.create(tmp_db(), schema())
    seed(db)

    with db.begin() as txn:
        rows = txn.select(
            "big_orders",
            order="id",
            ctes=[{"name": "big_orders", "table": "orders", "filter": {"total": {"gt": 20}}}],
        )
        assert [r["id"] for r in rows] == [2]
        assert rows[0]["user_id"] == 1
