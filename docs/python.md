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
| `default={...}` | Static, now, UUID, or sequence default |
| `generated=True` | Auto-generate on insert/update |
| `enum_values=[...]` | Restrict string values |
| `min=...`, `max=...` | Numeric range |
| `min_length=...`, `max_length=...` | String/bytes length |
| `regex=...` | Pattern match |

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

## Queries

`txn.select` accepts a simple object filter and an order string:

```python
rows = txn.select(
    "posts",
    filter={"published": {"eq": True}, "user_id": {"gt": 0}},
    order="-created_at",
    limit=10,
    offset=0,
)
```

Supported filter operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`.

Order syntax:
- `"+id"` or `"id"` — ascending
- `"-id"` — descending
- `"-created_at,+id"` — multiple columns

## Migrations

```python
db.migrate([
    {"version": 1, "name": "init", "ops": [{"create_table": {"name": "users"}}]},
    {"version": 2, "name": "add_posts", "ops": [{"create_table": {"name": "posts"}}]},
])
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
