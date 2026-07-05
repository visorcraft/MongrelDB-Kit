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


def test_key_encoding():
    cases = load_json("keys.json")["cases"]
    for case in cases:
        kind = case["kind"]
        if kind == "pk":
            actual = kit.encode_pk(case["components"])
        elif kind == "unique":
            actual = kit.encode_unique_key(
                case["version"], case["constraint"], case["components"]
            )
        elif kind == "row_guard":
            actual = kit.encode_row_guard_key(case["table"], case["components"])
        else:
            raise AssertionError(f"unknown key kind {kind}")
        assert actual == case["expected"], (
            f"{case['name']} key mismatch: {actual} != {case['expected']}"
        )


def test_migration_failure():
    fail = load_json("migration_failure.json")
    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, fail["create_schema"])
        db.migrate([fail["create_migration"]])

        txn = db.begin()
        for seed in fail["seed"]:
            txn.insert(seed["table"], seed["row"])
        txn.commit()

        # Swap in the schema that declares the unique constraint so the backfill
        # can resolve it; the prior inserts were allowed because it was absent.
        db.set_schema(fail["migrated_schema"])

        try:
            db.migrate([fail["create_migration"], fail["failing_migration"]])
        except Exception as exc:  # noqa: BLE001 - asserting the error category
            assert error_code(exc) == fail["expected_error"], (
                f"migration failure error mismatch: {error_code(exc)} != {fail['expected_error']}"
            )
        else:
            raise AssertionError("expected the unique-backfill migration to fail")


def _normalize_on_conflict(on_conflict):
    if on_conflict is None:
        return {"do_nothing": {}}
    if on_conflict == "do_nothing":
        return {"do_nothing": True}
    if isinstance(on_conflict, dict) and "do_update" in on_conflict:
        patch = on_conflict["do_update"]
        if not isinstance(patch, dict):
            raise ValueError("do_update on_conflict must contain an object patch")
    return on_conflict


def _assert_returning_order(scenario_name, actual, returning):
    if isinstance(actual, dict):
        assert list(actual.keys()) == returning, (
            f"{scenario_name} returning column order mismatch: "
            f"{list(actual.keys())} != {returning}"
        )
    elif isinstance(actual, list):
        for i, row in enumerate(actual):
            assert list(row.keys()) == returning, (
                f"{scenario_name} row {i} returning column order mismatch: "
                f"{list(row.keys())} != {returning}"
            )


def _assert_phase1_result(scenario_name, actual, expected, returning):
    assert "error" not in expected, (
        f"{scenario_name} expected error {expected['error']} but succeeded with {actual}"
    )
    if "row" in expected:
        _assert_returning_order(scenario_name, actual, returning)
        assert actual == expected["row"], (
            f"{scenario_name} row mismatch: {actual} != {expected['row']}"
        )
    elif "rows" in expected:
        _assert_returning_order(scenario_name, actual, returning)
        assert actual == expected["rows"], (
            f"{scenario_name} rows mismatch: {actual} != {expected['rows']}"
        )
    else:
        assert actual == {} or actual is None, (
            f"{scenario_name} expected no data but got {actual}"
        )


def test_phase1_dml():
    run_phase1_dml()


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

        run_phase1_dml_with_db(db)
        db.close()


def test_aggregates():
    raw = load_json("aggregates.json")
    expected = load_json("expected/aggregates.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.aggregate(
                scenario["table"],
                scenario["aggregates"],
                group_by=scenario.get("group_by"),
            )
            txn.commit()
            order = scenario.get("order")
            if order:
                desc = order.startswith("-")
                col = order.lstrip("+-")
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(col) is None, r.get(col)),
                    reverse=desc,
                )
            exp = expected[scenario["name"]]["rows"]
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def _join_sort_key(row, order):
    """Sort key for a join result row: resolve each qualified ``table.column``
    reference, treating the unmatched (``None``) side of a LEFT join as sorting
    first."""
    key = []
    for qualified in order:
        table, col = qualified.split(".", 1)
        source = row.get(table)
        value = None if source is None else source.get(col)
        key.append((value is None, value))
    return tuple(key)


def test_joins():
    raw = load_json("joins.json")
    expected = load_json("expected/joins.json")

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for table, rows in raw["seed"].items():
            for row in rows:
                txn = db.begin()
                txn.insert(table, row)
                txn.commit()

        for scenario in raw["scenarios"]:
            query = scenario["query"]
            txn = db.begin()
            rows = txn.join(query["table"], query["joins"])
            txn.commit()
            order = scenario.get("order", [])
            rows = sorted(rows, key=lambda r: _join_sort_key(r, order))
            exp = sorted(
                expected[scenario["name"]]["rows"],
                key=lambda r: _join_sort_key(r, order),
            )
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def test_ctes():
    raw = load_json("ctes.json")
    expected = load_json("expected/ctes.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.select(scenario["body"], ctes=scenario["ctes"])
            txn.commit()
            order = scenario.get("order")
            if order:
                desc = order.startswith("-")
                col = order.lstrip("+-")
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(col) is None, r.get(col)),
                    reverse=desc,
                )
            exp = expected[scenario["name"]]["rows"]
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def test_subqueries():
    raw = load_json("subqueries.json")
    expected = load_json("expected/subqueries.json")

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for table, rows in raw["seed"].items():
            for row in rows:
                txn = db.begin()
                txn.insert(table, row)
                txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.select(scenario["table"], filter=scenario["filter"])
            txn.commit()
            order = scenario.get("order")
            if order:
                desc = order.startswith("-")
                col = order.lstrip("+-")
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(col) is None, r.get(col)),
                    reverse=desc,
                )
            exp = expected[scenario["name"]]["rows"]
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def test_contains():
    raw = load_json("contains.json")
    expected = load_json("expected/contains.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.select(
                scenario["table"],
                filter={scenario["column"]: {"contains": scenario["needle"]}},
            )
            txn.commit()
            order = scenario.get("order")
            if order:
                desc = order.startswith("-")
                col = order.lstrip("+-")
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(col) is None, r.get(col)),
                    reverse=desc,
                )
            exp = expected[scenario["name"]]["rows"]
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def test_bytes_prefix():
    raw = load_json("bytes_prefix.json")
    expected = load_json("expected/bytes_prefix.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.select(
                scenario["table"],
                filter={scenario["column"]: {"bytes_prefix": scenario["prefix"]}},
            )
            txn.commit()
            order = scenario.get("order")
            if order:
                desc = order.startswith("-")
                col = order.lstrip("+-")
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(col) is None, r.get(col)),
                    reverse=desc,
                )
            exp = expected[scenario["name"]]["rows"]
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def test_learned_range():
    raw = load_json("learned_range.json")
    expected = load_json("expected/learned_range.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.select(scenario["table"], filter=scenario.get("filter"))
            txn.commit()
            order = scenario.get("order")
            if order:
                desc = order.startswith("-")
                col = order.lstrip("+-")
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(col) is None, r.get(col)),
                    reverse=desc,
                )
            exp = expected[scenario["name"]]["rows"]
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def test_views():
    raw = load_json("views.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        # Create the view via the SQL surface; it lives in the kit's session
        # and is queryable by subsequent sql_rows() calls.
        db.sql_rows(f"CREATE VIEW pricey AS {raw['view_sql']}")

        for scenario in raw["scenarios"]:
            rows = db.sql_rows(scenario["sql"])
            # Normalize numeric types (COUNT(*) may be int or float across
            # language runtimes; compare by float value).
            norm_rows = [
                {k: float(v) if isinstance(v, (int, float)) else v for k, v in r.items()}
                for r in rows
            ]
            norm_exp = [
                {k: float(v) if isinstance(v, (int, float)) else v for k, v in r.items()}
                for r in scenario["expected_rows"]
            ]
            assert norm_rows == norm_exp, f"{scenario['name']}: {norm_rows} != {norm_exp}"
        db.close()


def test_ann():
    raw = load_json("ann.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.ann_search(
                scenario["table"], scenario["column"], scenario["query"], scenario["k"]
            )
            txn.commit()
            ids = sorted(r["id"] for r in rows)
            assert ids == sorted(scenario["expect_ids"]), f"{scenario['name']}: {ids}"
        db.close()


def test_null_filter():
    raw = load_json("null_filter.json")
    expected = load_json("expected/null_filter.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.select(scenario["table"], filter=scenario["filter"])
            txn.commit()
            order = scenario.get("order")
            if order:
                desc = order.startswith("-")
                col = order.lstrip("+-")
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(col) is None, r.get(col)),
                    reverse=desc,
                )
            exp = expected[scenario["name"]]["rows"]
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def test_like():
    raw = load_json("like.json")
    expected = load_json("expected/like.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.select(scenario["table"], filter=scenario["filter"])
            txn.commit()
            order = scenario.get("order")
            if order:
                desc = order.startswith("-")
                col = order.lstrip("+-")
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(col) is None, r.get(col)),
                    reverse=desc,
                )
            exp = expected[scenario["name"]]["rows"]
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def test_sparse():
    raw = load_json("sparse.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, raw["schema"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.sparse_match(
                scenario["table"], scenario["column"], scenario["query"], scenario["k"]
            )
            txn.commit()
            ids = sorted(r["id"] for r in rows)
            assert ids == sorted(scenario["expect_ids"]), f"{scenario['name']}: {ids}"
        db.close()


def test_encrypted():
    raw = load_json("encrypted.json")
    expected = load_json("expected/encrypted.json")
    table = raw["schema"]["tables"][0]["name"]

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create_encrypted(tmp, raw["schema"], raw["passphrase"])
        for row in raw["rows"]:
            txn = db.begin()
            txn.insert(table, row)
            txn.commit()

        for scenario in raw["scenarios"]:
            txn = db.begin()
            rows = txn.select(scenario["table"], filter=scenario["filter"])
            txn.commit()
            order = scenario.get("order")
            if order:
                desc = order.startswith("-")
                col = order.lstrip("+-")
                rows = sorted(
                    rows,
                    key=lambda r: (r.get(col) is None, r.get(col)),
                    reverse=desc,
                )
            exp = expected[scenario["name"]]["rows"]
            assert rows == exp, f"{scenario['name']}: {rows} != {exp}"
        db.close()


def run_phase1_dml_with_db(db):
    """Run the Phase 1 DML fixture against an already-open, migrated database."""
    fixture = load_json("phase1_dml.json")

    for step in fixture["steps"]:
        expected = step["expected"]
        returning = step.get("returning", [])
        txn = db.begin()
        committed = False
        try:
            op = step["op"]
            table = step["table"]
            try:
                if op == "insert_returning":
                    result = txn.insert_returning(
                        table, step["row"], returning
                    )
                elif op == "upsert":
                    result = txn.upsert(
                        table,
                        step["row"],
                        _normalize_on_conflict(step["on_conflict"]),
                        returning,
                    )
                elif op == "update_where":
                    result = txn.update_where(
                        table,
                        set=step["patch"],
                        filter=step.get("filter"),
                        returning=returning,
                    )
                elif op == "delete_where":
                    result = txn.delete_where(
                        table,
                        filter=step.get("filter"),
                        returning=returning,
                    )
                elif op == "truncate":
                    txn.truncate(table)
                    result = {}
                else:
                    raise AssertionError(f"unknown op {op}")
            except Exception as exc:  # noqa: BLE001
                txn.rollback()
                _assert_error(step["name"], expected, exc)
            else:
                _assert_phase1_result(step["name"], result, expected, returning)
                txn.commit()
                committed = True
        finally:
            if not committed:
                try:
                    txn.rollback()
                except Exception:  # noqa: BLE001
                    pass

    txn = db.begin()
    try:
        for check in fixture["state_checks"]:
            kwargs = {"order": check.get("order", "+id")}
            if "filter" in check:
                kwargs["filter"] = check["filter"]
            rows = txn.select(check["table"], **kwargs)
            assert rows == check["rows"], (
                f"state check {check['table']} mismatch: {rows} != {check['rows']}"
            )
    finally:
        txn.commit()


def run_phase1_dml():
    schema = load_json("schema.json")
    migrations = load_json("migrations.json")

    with tempfile.TemporaryDirectory() as tmp:
        db = kit.Database.create(tmp, schema)
        db.migrate(migrations)
        run_phase1_dml_with_db(db)
        db.close()
