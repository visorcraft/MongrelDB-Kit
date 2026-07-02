"""Cross-language benchmark: Python Kit.

Same workload as the Rust ``kit-perf`` runner: seed N rows, then measure
single-row insert/update/delete + bulk-ingest throughput. Run from the
python/mongreldb_kit directory after ``maturin develop``.

  python ../../crates/kit-perf/bench/py/bench.py [N]
"""

import os
import shutil
import statistics
import sys
import tempfile
import time

from mongreldb_kit import Database, float_, int, table, text


def users_schema():
    return [
        table("users", {
            "id": int().primary_key(),
            "name": text(),
            "cost": float_(),
        })
    ]


def us(seconds: float) -> str:
    if seconds >= 1:
        return f"{seconds:.2f} s"
    elif seconds >= 1e-3:
        return f"{seconds * 1e3:.2f} ms"
    else:
        return f"{seconds * 1e6:.1f} us"


def bench(n: int):
    d = tempfile.mkdtemp()
    db = Database.create(os.path.join(d, "db.kitdb"), users_schema())

    # Seed via insert_many (one transaction).
    seed = [{"id": i, "name": "City", "cost": 199.99 + i} for i in range(1, n + 1)]
    with db.begin() as txn:
        txn.insert_many("users", seed)
        txn.commit()

    inserts = []
    for i in range(7):
        start = time.perf_counter()
        with db.begin() as txn:
            txn.insert("users", {"id": n + 1 + i, "name": "CityX", "cost": 1.0})
            txn.commit()
        inserts.append(time.perf_counter() - start)

    updates = []
    for i in range(7):
        pk = i + 1
        start = time.perf_counter()
        with db.begin() as txn:
            txn.update("users", pk, {"cost": 99.0 + i})
            txn.commit()
        updates.append(time.perf_counter() - start)

    deletes = []
    for i in range(7):
        pk = n - 6 + i
        start = time.perf_counter()
        with db.begin() as txn:
            txn.delete("users", pk)
            txn.commit()
        deletes.append(time.perf_counter() - start)

    print(f"### Python Kit — N = {n}")
    print("| single_insert | single_update | delete_one |")
    print("|---|---|---|")
    print(f"| {us(statistics.median(inserts))} | {us(statistics.median(updates))} | {us(statistics.median(deletes))} |")
    print()
    shutil.rmtree(d, ignore_errors=True)


def bulk(n: int):
    d = tempfile.mkdtemp()
    db = Database.create(os.path.join(d, "db.kitdb"), users_schema())
    seed = [{"id": i, "name": "City", "cost": 199.99 + i} for i in range(1, n + 1)]
    start = time.perf_counter()
    with db.begin() as txn:
        txn.insert_many("users", seed)
        txn.commit()
    secs = time.perf_counter() - start
    print(f"### Python Kit bulk — N = {n}")
    print("| Melem/s |")
    print("|---|")
    print(f"| {n / secs / 1e6:.1f} |")
    print()
    shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
    bench(100)
    bulk(n)
