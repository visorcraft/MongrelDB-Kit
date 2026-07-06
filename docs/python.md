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

Per-column operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `like`, `contains`, `bytes_prefix`,
`in`, `not_in`, `is_null`, `is_not_null`, `in_subquery`. A bare value (`{"user_id": 1}`) is
shorthand for `eq`. `bytes_prefix` matches an anchored prefix on a bitmap-indexed Bytes column
(exact engine pushdown). Top-level logical keys combine column predicates: `and` / `or` (a list of
filters), `not` (a filter), and `exists` / `not_exists` (a subselect). Multiple keys at one level
are AND-ed.

Order syntax:
- `"+id"` or `"id"` — ascending
- `"-id"` — descending
- `"-placed_at,+id"` — multiple columns

```python
# Anchored prefix on a bitmap-indexed bytes column (exact engine pushdown).
events = txn.select("events", filter={"key": {"bytes_prefix": "user:"}}, order="+id")
```

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

## Phase 1 DML

In addition to the single-row `insert`, `update`, and `delete` methods, the transaction handle
exposes the Phase 1 DML operations: `insert_returning`, `upsert`, `update_where`, `delete_where`,
and `truncate`.

### `insert_returning`

Insert a row and project the result to a subset of columns. The `returning` list is required.

```python
with db.begin() as txn:
    row = txn.insert_returning(
        "users",
        {"id": 1, "email": "alice@example.com", "name": "Alice"},
        returning=["id", "email"],
    )
    # row == {"id": 1, "email": "alice@example.com"}
```

The keys in the returned dict appear in the same order as `returning`.

### `upsert`

`txn.upsert(table, row, on_conflict=..., returning=[...])` performs an insert with an
`ON CONFLICT` action. The conflict is detected on the primary key. `on_conflict` defaults to
`do_nothing` when omitted.

```python
with db.begin() as txn:
    alice = txn.insert("users", {"id": 1, "email": "alice@example.com", "name": "Alice"})

    # DO NOTHING — existing row is returned unchanged.
    result = txn.upsert(
        "users",
        {"id": 1, "email": "alice@example.com", "name": "Alicia"},
        on_conflict="do_nothing",
        returning=["id", "name"],
    )
    # result["name"] == "Alice"

    # DO UPDATE — merge a patch into the existing row.
    result = txn.upsert(
        "users",
        {"id": 1, "email": "alice@example.com", "name": "Alicia"},
        on_conflict={"do_update": {"set": {"name": "Alicia"}}},
        returning=["id", "name"],
    )
    # result["name"] == "Alicia"
```

The shorthand form `{"do_update": {"name": "Alicia"}}` is also accepted.

### `update_where`

Update every row matching `filter` (omit `filter` to update every row). Returns the updated rows
as a list of dicts.

```python
with db.begin() as txn:
    updated = txn.update_where(
        "posts",
        set={"published": True},
        filter={"user_id": {"eq": alice["id"]}},
        returning=["id", "title", "published"],
    )
    # updated == [{"id": 1, "title": "Hello Kit", "published": True}, ...]
```

### `delete_where`

Delete every row matching `filter` (omit `filter` to delete every row). Returns the deleted rows
as a list of dicts.

```python
with db.begin() as txn:
    removed = txn.delete_where(
        "posts",
        filter={"published": {"eq": True}},
        returning=["id"],
    )
    # removed == [{"id": 1}, ...]
```

### `truncate`

Remove every row from a table in one operation. It fails with `RestrictError` when another table
references it.

```python
with db.begin() as txn:
    txn.truncate("posts")
    txn.commit()
```

Because `posts` has a foreign key to `users`, `txn.truncate("users")` would raise `RestrictError`.

> **Return shapes.** `insert_returning` and `upsert` return a single dict; `update_where` and
`delete_where` return a list of dicts. `returning` is required for `insert_returning` and optional
for the other three; when supplied, only those columns are included and the key order matches the
list order.

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

### Embedded SQL surface and maintenance

`sql_rows` and `sql_arrow` run statements through the kit's embedded SQL session
(the engine's DataFusion frontend). The session is held for the database's
lifetime, so session-scoped objects (views, prepared statements, the result
cache) persist across calls. Maintenance helpers mirror the engine's `ANALYZE`
and `VACUUM`, and `rename_table` updates the engine, the kit schema catalog,
and any referencing foreign keys.

```python
rows = db.sql_rows("SELECT id, email FROM users ORDER BY id")  # list[dict]
ipc = db.sql_arrow("SELECT id FROM users ORDER BY id")         # raw Arrow IPC bytes
# (decode with pyarrow.ipc.open_file)

db.sql_rows("CREATE VIEW active AS SELECT id FROM users WHERE active = TRUE")
db.sql_rows("SELECT * FROM active")  # queries the view

# Convenience wrappers for views + auto-increment:
db.create_view("active", "SELECT id FROM users WHERE active = TRUE")
db.drop_view("active")
next_id = db.reserve_auto_inc("orders")  # Optional[int]

db.analyze()              # ensure_indexes_complete() on every table
reclaimed = db.vacuum()   # compact_all() + gc(); returns the reclaimed-file count

db.rename_table("widgets", "things")  # engine + schema catalog + persisted
db.compact_all(); db.compact_table("things")
```

> Writes through `sql_rows` / `sql_arrow` bypass kit-level constraints (defaults,
> enums, min/max, length, regex, triggers) — use the `Transaction` API for
> constrained writes. The engine's own declarative constraints (unique, FK,
> check) still apply.

### Storage tuning & introspection

```python
# Database-wide tunables.
db.set_spill_threshold(1_000_000)
db.set_recursive_triggers(True)
cfg = db.trigger_config()  # {recursive_triggers, max_depth, max_loop_iterations}
db.set_trigger_config({"recursive_triggers": True, "max_depth": 16, "max_loop_iterations": 5000})

# Per-table introspection (read-only).
runs = db.table_run_count("widgets")          # int — compaction target: 1
stats = db.table_page_cache_stats("widgets")  # {hits, misses, try_lock_misses, hit_rate}
memtable = db.table_memtable_len("widgets")   # int — uncommitted staged rows
```

The per-table tuning setters (compaction zstd level, result cache size, mutable-run spill bytes,
sync byte threshold, index build policy) are available from Rust via `Database::raw()` and from
the NAPI addon; the Python facade exposes the highest-value subset above.

### Advanced SQL (recursive CTEs, windows, regex, catalog, ATTACH, SAVEPOINTs)

The embedded DataFusion 54 session supports the full SQL stdlib via
`sql_rows()`:

```python
# Recursive CTE (tree traversal).
db.sql_rows("""
    WITH RECURSIVE tree AS (
        SELECT id, 0 AS depth FROM nodes WHERE parent IS NULL
        UNION ALL
        SELECT n.id, t.depth + 1 FROM nodes n JOIN tree t ON n.parent = t.id
    )
    SELECT id, depth FROM tree ORDER BY id
""")

# Window function (ranking within partitions).
db.sql_rows("""
    SELECT category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rank
    FROM orders
""")

# Regex match.
db.sql_rows("SELECT id FROM users WHERE regexp('^admin.*', name) = 1")

# Catalog introspection.
db.sql_rows("SELECT type, name FROM information_schema.tables ORDER BY name")

# Cross-database query.
db.sql_rows("ATTACH './other-data' AS other")
db.sql_rows("SELECT id FROM other_items")
db.sql_rows("DETACH other")

# Sub-transaction (SAVEPOINT).
db.sql_rows("BEGIN")
db.sql_rows("INSERT INTO logs VALUES (1, 'hello')")
db.sql_rows("SAVEPOINT sp1")
db.sql_rows("INSERT INTO logs VALUES (2, 'world')")
db.sql_rows("ROLLBACK TO sp1")  # discards 'world', keeps 'hello'
db.sql_rows("COMMIT")
```

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

Available exceptions: `ValidationError`, `DuplicateError`, `ForeignKeyError`, `RestrictError`, `TriggerValidationError`, `MigrationError`, `ConflictError`, `StorageError`, `IntegrityError`.

## Users, roles & permissions

The Kit forwards the engine's catalog-stored auth model — Argon2id-hashed
users, roles that bundle permissions, and `GRANT`/`REVOKE` table-level
access control. Permission strings use the compact form: `"all"`, `"admin"`,
`"ddl"`, or `"select:table"`, `"insert:table"`, `"update:table"`,
`"delete:table"`.

```python
from mongreldb_kit import Database

db = Database.open("./store.kitdb")

# Users
db.create_user("alice", "s3cret-pw")
db.alter_user_password("alice", "new-pw")
assert db.verify_user("alice", "new-pw") is True
db.set_user_admin("alice", True)            # admin bypasses all permission checks
assert db.users() == ["alice"]

# Roles + permissions
db.create_role("analyst")
db.grant_permission("analyst", "select:orders")
db.grant_permission("analyst", "insert:orders")
db.grant_role("alice", "analyst")
assert db.roles() == ["analyst"]

# Reverse
db.revoke_role("alice", "analyst")
db.revoke_permission("analyst", "insert:orders")
db.drop_role("analyst")
db.drop_user("alice")
```

The full model (including SQL DDL like `CREATE USER` / `GRANT` and the HTTP
daemon's Bearer + Basic auth modes) is documented in the engine
[Users, Roles & Permissions](https://github.com/visorcraft/MongrelDB/blob/master/docs/14-auth.md)
guide. The Kit CLI exposes the same operations as
[`user` and `role` subcommands](./cli.md#user--manage-catalog-users).

### Credential enforcement

A database with `require_auth` set rejects every open that does not supply
valid credentials. Use the credentialed constructors to create or open such a
database, and `enable_auth`/`disable_auth` to flip the flag in code.

```python
# Create a new database with require_auth on, bootstrapping the first admin.
db = Database.create_with_credentials(
    "./store.kitdb", schema, "alice", "s3cret-pw"
)

# Open an existing require_auth database.
db = Database.open_with_credentials("./store.kitdb", "alice", "s3cret-pw")

assert db.require_auth_enabled() is True

# Turn require_auth on for an existing credentialless database.
db.enable_auth("alice", "s3cret-pw")

# Recovery: clear require_auth (needs an open handle).
db.disable_auth()
```

```python
# Encrypted + credentialed: both layers in one call.
db = Database.create_encrypted_with_credentials(
    "./store.kitdb", schema, "passphrase", "admin", "s3cret-pw"
)

# Long-lived handles call refresh_principal after a REVOKE to pick up
# permission changes made by other handles.
db.refresh_principal()
```

The full model and recovery flow are documented in the engine
[credential enforcement guide](https://github.com/visorcraft/MongrelDB/blob/master/docs/15-credential-enforcement.md).

## Triggers and remote SQL

Embedded Python can install, replace, list, and drop engine-side triggers by
passing the same dict/JSON spec the engine stores:

```python
db.create_trigger({
    "name": "users_ai",
    "target": {"kind": "table", "name": "users"},
    "timing": "after",
    "event": "insert",
    "program": {"steps": []},
})

db.triggers()
db.trigger("users_ai")
db.drop_trigger("users_ai")
```

The pure-Python `RemoteDatabase` exposes SQL and virtual-table helpers against a
running `mongreldb-server`:

```python
arrow_ipc = remote.sql_arrow("SELECT count(*) AS n FROM users")
remote.create_virtual_table("docs_fts", "fts_docs", ["content=docs"])
remote.drop_virtual_table("docs_fts")
```

## Running this example

Save the file as `kit_demo.py` and run:

```sh
python kit_demo.py
```

## See also

- [Query builder](./query-builder.md) — the full query model these helpers serialize.
- [Triggers](./triggers.md) and [Extended SQL & virtual tables](./extended-sql-and-virtual-tables.md).
- [Constraints](./constraints.md) and [Errors](./errors.md) — the rules and the typed failures.
- [Migrations](./migrations.md) — migration ops and the runner.
- [TypeScript](./typescript.md) · [Rust](./rust.md) — the sibling language surfaces.
