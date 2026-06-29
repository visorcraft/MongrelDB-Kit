# Python Quickstart

This guide shows how to define a schema, run migrations, and perform CRUD with `mongreldb_kit`.

## Installation

```sh
pip install mongreldb-kit
```

## Complete example

```python
import os
import tempfile

from mongreldb_kit import (
    Database,
    DuplicateError,
    ForeignKeyError,
    RestrictError,
    bool_,
    fk,
    int,
    table,
    text,
    unique,
)


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
                unique_constraints=[unique("uq_user_email", "email")],
            ),
            table(
                name="posts",
                id=2,
                columns=[
                    int("id", 1, primary_key=True),
                    int("user_id", 2),
                    text("title", 3),
                    text("body", 4, nullable=True),
                    bool_("published", 5, default={"static": False}),
                ],
                primary_key="id",
                foreign_keys=[
                    fk(
                        "fk_posts_user",
                        "user_id",
                        references_table="users",
                        references_columns="id",
                        on_delete="cascade",
                    )
                ],
            ),
        ]
    }


def tmp_db():
    return os.path.join(tempfile.mkdtemp(), "app.kitdb")


def main():
    path = tmp_db()

    # Create or open the database.
    db = Database.create(path, schema())

    # Run migrations.
    db.migrate(
        [
            {
                "version": 1,
                "name": "initial",
                "ops": [
                    {"create_table": {"name": "users"}},
                    {"create_table": {"name": "posts"}},
                ],
            }
        ]
    )

    # Insert users.
    with db.begin() as txn:
        alice = txn.insert("users", {"id": 1, "email": "alice@example.com", "name": "Alice"})
        bob = txn.insert("users", {"id": 2, "email": "bob@example.com"})
        txn.commit()

    # Insert a post.
    with db.begin() as txn:
        post = txn.insert(
            "posts",
            {"id": 1, "user_id": alice["id"], "title": "Hello Kit", "body": "First post."},
        )
        txn.commit()

    # Query posts by user, ordered by id descending.
    with db.begin() as txn:
        rows = txn.select(
            "posts",
            filter={"user_id": {"eq": 1}},
            order="-id",
            limit=10,
        )
        for row in rows:
            print(row)

    # Update the post.
    with db.begin() as txn:
        txn.update("posts", 1, {"published": True})
        txn.commit()

    # Deleting Alice cascades to her posts because of on_delete='cascade'.
    with db.begin() as txn:
        txn.delete("users", 1)
        txn.commit()


if __name__ == "__main__":
    main()
```

## Schema helpers

| Function | Purpose |
|---|---|
| `table(...)` | Build a table dictionary |
| `int(name, id, **kwargs)` / `integer(...)` | 64-bit integer column |
| `text(name, id, **kwargs)` | UTF-8 text column |
| `bool_(name, id, **kwargs)` / `boolean(...)` | Boolean column |
| `float_(name, id, **kwargs)` / `float64(...)` | 64-bit float column |
| `json_col(name, id, **kwargs)` | JSON column |
| `bytes_col(name, id, **kwargs)` | Bytes column |
| `timestamp(name, id, **kwargs)` | Timestamp column |
| `date(name, id, **kwargs)` | Date column |
| `datetime(name, id, **kwargs)` | DateTime column |
| `index(name, columns, unique=False)` | Index definition |
| `unique(name, columns)` | Unique constraint |
| `fk(name, columns, references_table, references_columns, on_delete='restrict')` | Foreign key |
| `check(name, expr)` | Table check constraint |

## Column kwargs

| Kwarg | Effect |
|---|---|
| `nullable=True` | Allow `None` values |
| `primary_key=True` | Mark as part of the primary key |
| `default=...` | Default value (see shapes below) |
| `generated=True` | Auto-generate on insert/update |
| `enum_values=[...]` | Restrict string values |
| `min=...`, `max=...` | Numeric range |
| `min_length=...`, `max_length=...` | String/bytes length |
| `regex=...` | Pattern match |
| `check_expr="..."` | Column check as a serialized expression string (e.g. `"price_cents >= 0"`) |

Default shapes mirror the cross-language `DefaultKind` JSON: `{"static": <value>}`,
`{"sequence": "<name>"}`, `{"custom_name": "<name>"}`, and the bare strings `"now"` and `"uuid"`.
A column whose default is `{"sequence": ...}` is auto-assigned a **1-based** id when the inserted row
omits it (the first row is `1`, never `0`). `check_expr` and the table-level `check(name, expr)` use
the serialized string-expression grammar — the cross-language form, not a Python callable.

## Transactions

Use the context manager for automatic commit/rollback:

```python
with db.begin() as txn:
    txn.insert("users", {"id": 1, "email": "alice@example.com"})
    # committed automatically on exit
```

Explicit control is also available:

```python
txn = db.begin()
try:
    txn.insert("users", {...})
    txn.commit()
except Exception:
    txn.rollback()
```

### Batch insert

`txn.insert_many(table, rows)` stages an iterable of rows in the open transaction and returns the
stored rows as a `list[dict]` in order. It runs the same per-row defaults, validation, sequence
ids, and guards as `insert`, but stages the whole batch so one commit writes it — far faster than
a row-at-a-time loop, and all-or-nothing on failure. A single-column primary key preloads existing
keys once so the per-row duplicate check stays O(1).

```python
with db.begin() as txn:
    rows = txn.insert_many("products", [
        {"sku": "A-1", "name": "Anvil"},
        {"sku": "B-1", "name": "Bucket"},
    ])
    # rows[0]["id"] == 1, rows[1]["id"] == 2  — sequence ids assigned in order
```

## Queries

`txn.select` accepts a friendly object filter and an order string:

```python
rows = txn.select(
    "posts",
    filter={"published": {"eq": True}, "user_id": {"gt": 0}},
    order="-placed_at",
    limit=10,
    offset=0,
    columns=["id", "title"],   # optional projection; omit for all columns
    distinct=False,
)
```

Per-column operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `like`, `contains`, `in`, `not_in`,
`is_null`, `is_not_null`, `in_subquery`. A bare value (`{"user_id": 1}`) is shorthand for `eq`.
Top-level logical keys combine column predicates: `and` / `or` (a list of filters), `not` (a
filter), and `exists` / `not_exists` (a subselect). Multiple keys at one level are AND-ed.

Order syntax:
- `"+id"` or `"id"` — ascending
- `"-id"` — descending
- `"-placed_at,+id"` — multiple columns

### Aggregates and joins

`txn.aggregate` runs group-by/having; build specs with the `agg` helper (`count`, `sum`, `min`,
`max`, `avg`):

```python
from mongreldb_kit import agg

rows = txn.aggregate(
    "orders",
    aggregates=[agg("count", "n"), agg("sum", "spent", "amount")],
    group_by=["customer_id"],
    having={"n": {"gt": 1}},
)
```

`txn.join` runs nested-loop joins; describe each join with `kind` (`inner`/`left`/`cross`) and an
`on` predicate built with `on_eq`:

```python
from mongreldb_kit import on_eq

rows = txn.join(
    "orders",
    alias="o",
    joins=[{"kind": "inner", "table": "customers", "alias": "c",
            "on": on_eq("o.customer_id", "c.id")}],
)
```

`txn.select` also takes `ctes=[{"name", "table", ...}]` to materialize common table expressions
before the body runs. Joins, aggregates, group/having, and CTEs are computed in memory.

## Migrations

```python
db.migrate([
    {"version": 1, "name": "init", "ops": [{"create_table": {"name": "users"}}]},
    {"version": 2, "name": "add_posts", "ops": [{"create_table": {"name": "posts"}}]},
])
```

## Database methods

Beyond `create` / `open` / `begin` / `migrate`, the `Database` handle exposes:

```python
db.allocate_sequence("orders_id_seq")        # next 1-based value (count=1 by default)
db.allocate_sequence("orders_id_seq", 10)    # reserve 10, returns the first
db.table_names()                             # application tables (excludes __kit_* internals)
db.set_schema(schema())                      # refresh the in-memory schema without migrating
db.transaction(lambda txn: txn.insert("users", {...}))  # commit on success, retry on conflict
```

`db.transaction(fn, max_retries=5)` runs `fn(txn)`, commits on success, rolls back on any error, and
retries the whole callback when a `ConflictError` (a retryable write-write conflict) is raised.

## Key encoding

The byte-identical key encoders used internally are exposed for tooling and tests. Components are
typed values — `{"int": n}`, `{"text": s}`, or `{"null": True}` — so the integer `1` and the text
`"1"` never collide:

```python
from mongreldb_kit import encode_pk, encode_unique_key, encode_row_guard_key

encode_pk([{"int": 1}])                               # primary-key bytes
encode_unique_key(1, "uq_user_email", [{"text": "a@example.com"}])
encode_row_guard_key("users", [{"int": 1}])
```

## Error handling

Exceptions carry a stable `code` attribute:

```python
from mongreldb_kit import DuplicateError, ForeignKeyError, RestrictError

try:
    txn.insert("users", {"id": 2, "email": "alice@example.com"})
except DuplicateError as exc:
    print(exc.code)  # DUPLICATE
```

Available exceptions: `ValidationError`, `DuplicateError`, `ForeignKeyError`, `RestrictError`, `MigrationError`, `ConflictError`, `StorageError`, `IntegrityError`.

## Running this example

Save the file as `kit_demo.py` and run:

```sh
python kit_demo.py
```

## See also

- [Query builder](./query-builder.md) — the full query model these helpers serialize.
- [Constraints](./constraints.md) and [Errors](./errors.md) — the rules and the typed failures.
- [Migrations](./migrations.md) — migration ops and the runner.
- [TypeScript](./typescript.md) · [Rust](./rust.md) — the sibling language surfaces.
