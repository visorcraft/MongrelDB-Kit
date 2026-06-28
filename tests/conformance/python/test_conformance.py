import json
import os
import sys
import tempfile

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "python", "mongreldb_kit"))

import mongreldb_kit as kit

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures")


def load_json(name):
    with open(os.path.join(FIXTURES_DIR, name), encoding="utf-8") as f:
        return json.load(f)


def error_code(exc):
    if hasattr(exc, "code"):
        return exc.code
    name = type(exc).__name__
    return {
        "ValidationError": "VALIDATION",
        "DuplicateError": "DUPLICATE",
        "ForeignKeyError": "FOREIGN_KEY",
        "RestrictError": "RESTRICT",
        "MigrationError": "MIGRATION",
        "ConflictError": "CONFLICT",
        "StorageError": "STORAGE",
        "IntegrityError": "INTEGRITY",
    }.get(name, "UNKNOWN")


def _assert_error(scenario_name, expected, exc):
    assert expected.get("error"), f"{scenario_name} unexpected error: {exc}"
    assert error_code(exc) == expected["error"], (
        f"{scenario_name} error mismatch: {error_code(exc)} != {expected['error']}"
    )


def run_insert(db, scenario, expected):
    txn = db.begin()
    try:
        result = txn.insert(scenario["table"], scenario["row"])
    except Exception as exc:
        txn.rollback()
        _assert_error(scenario["name"], expected, exc)
        return
    assert "error" not in expected, f"{scenario['name']} expected error but succeeded"
    txn.commit()
    assert result == expected["row"], (
        f"{scenario['name']} row mismatch: {result} != {expected['row']}"
    )


def run_update(db, scenario, expected):
    txn = db.begin()
    try:
        result = txn.update(scenario["table"], scenario["pk"], scenario["patch"])
    except Exception as exc:
        txn.rollback()
        _assert_error(scenario["name"], expected, exc)
        return
    assert "error" not in expected, f"{scenario['name']} expected error but succeeded"
    txn.commit()
    assert result == expected["row"], (
        f"{scenario['name']} row mismatch: {result} != {expected['row']}"
    )


def run_delete(db, scenario, expected):
    txn = db.begin()
    try:
        txn.delete(scenario["table"], scenario["pk"])
    except Exception as exc:
        txn.rollback()
        _assert_error(scenario["name"], expected, exc)
        return
    assert "error" not in expected, f"{scenario['name']} expected error but succeeded"
    txn.commit()
    txn = db.begin()
    try:
        for table_name in ("users", "posts", "comments"):
            rows = txn.select(table_name, order="+id")
            assert rows == expected[table_name], (
                f"{scenario['name']}.{table_name} state mismatch: {rows} != {expected[table_name]}"
            )
    finally:
        txn.commit()


def run_query(db, scenario, expected):
    txn = db.begin()
    kwargs = {}
    if "filter" in scenario:
        kwargs["filter"] = scenario["filter"]
    if "order" in scenario:
        kwargs["order"] = scenario["order"]
    if "limit" in scenario:
        kwargs["limit"] = scenario["limit"]
    if "offset" in scenario:
        kwargs["offset"] = scenario["offset"]

    try:
        rows = txn.select(scenario["table"], **kwargs)
    finally:
        txn.commit()

    if scenario.get("count"):
        assert len(rows) == expected["count"], (
            f"{scenario['name']} count mismatch: {len(rows)} != {expected['count']}"
        )
    else:
        if scenario.get("select"):
            rows = [{k: r[k] for k in scenario["select"]} for r in rows]
        assert rows == expected["rows"], (
            f"{scenario['name']} rows mismatch: {rows} != {expected['rows']}"
        )


def test_conformance():
    schema = load_json("schema.json")
    migrations = load_json("migrations.json")
    inserts = load_json("inserts.json")
    updates = load_json("updates.json")
    deletes = load_json("deletes.json")
    queries = load_json("queries.json")
    expected = {
        "inserts": load_json("expected/inserts.json"),
        "updates": load_json("expected/updates.json"),
        "deletes": load_json("expected/deletes.json"),
        "queries": load_json("expected/queries.json"),
    }

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, schema)
        db.migrate(migrations)
        for scenario in inserts:
            run_insert(db, scenario, expected["inserts"][scenario["name"]])
        for scenario in updates:
            run_update(db, scenario, expected["updates"][scenario["name"]])
        for scenario in deletes:
            run_delete(db, scenario, expected["deletes"][scenario["name"]])
        for scenario in queries:
            run_query(db, scenario, expected["queries"][scenario["name"]])
