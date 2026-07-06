# Internal tables

MongrelDB Kit reserves a set of `__kit_*` tables that back migrations, the schema
catalog, unique-key guards, row guards, and the migration lock. They are ordinary
MongrelDB tables, created automatically when a database is opened or created. Where
an internal table exists in more than one language implementation, its on-disk shape
is stable across TypeScript, Rust, and Python.

This page is reference material. You never write these tables directly; the kit
maintains them as a side effect of migrations and CRUD.

## They are hidden, and the prefix is reserved

The `__kit_` prefix is reserved. Application table enumeration excludes it:
`db.tableNames()` (TypeScript) and `db.table_names()` (Rust) filter out every
name starting with `__kit_`, so internal tables never appear alongside your own.
In the Rust core they are assigned reserved schema IDs just below `u64::MAX`.

Do **not** name an application table with the `__kit_` prefix, and do not write to
these tables yourself — the kit owns their contents and their invariants.

## `__kit_schema_migrations`

The migration history. One row per migration the runner has recorded.

| Column | Type | Notes |
| --- | --- | --- |
| `version` | int64 | Primary key. The migration's version number. |
| `name` | text | The migration's human-readable name. |
| `checksum` | text | Content-aware SHA-256 of the migration (see [Migrations](./migrations.md#checksums-and-drift-detection)). |
| `applied_at` | text | ISO-8601 UTC timestamp when the record was written. |
| `kit_version` | text | Kit package version that applied the migration. |
| `status` | text | `in_progress`, `applied`, or `failed`. |

The runner reads this table to compute the high-water mark (max applied version,
which drives idempotency) and to verify stored checksums against the supplied
migrations (drift detection). The CLI's `migrate status` and `doctor` read it.
The TypeScript runner moves a record through `in_progress` → `applied`/`failed`;
the Rust runner records successful migrations as `applied`.

## `__kit_schema_catalog`

A single-row snapshot of the active schema.

| Column | Type | Notes |
| --- | --- | --- |
| `schema_version` | int64 | Primary key. Always `1` — the catalog holds one current snapshot. |
| `schema_json` | text | The serialized active schema (tables, columns, constraints, indexes). |
| `checksum` | text | SHA-256 of `schema_json`. |
| `written_at` | text | ISO-8601 UTC timestamp of the last write. |

Rewritten when a database is opened/created and at the end of each migration run.
The CLI's `schema print` and `diff` read this catalog to report and compare the
stored schema.

## `__kit_sequences`

Backs named sequence allocation in the Rust storage crate and Python facade. Current
TypeScript databases use MongrelDB's native table `AUTO_INCREMENT` counters for
`sequenceDefault(...)` and do **not** create this table for new databases. Older
TypeScript databases may still contain it; the TypeScript migration runner reads it
once to seed native engine counters so upgraded databases do not hand out ids below a
legacy high-water mark.

| Column | Type | Notes |
| --- | --- | --- |
| `sequence_name` | text | Primary key. The sequence's name, e.g. `customers_id_seq`. |
| `next_value` | int64 | The next id to hand out. |
| `updated_at` | text | ISO-8601 UTC timestamp of the last allocation. |

Allocation is **1-based** (the first id is `1`, never `0`, matching SQL
`AUTO_INCREMENT`): the Rust/Python kit reads `next_value`, reserves a block of
`count` ids inside a transaction, and bumps `next_value` by `count`. See
[Defaults & sequences](./defaults.md).

## `__kit_unique_keys`

One reservation row per live unique-constraint key. This is how the kit enforces
unique and composite-unique constraints, which the storage engine does not
enforce natively.

| Column | Type | Notes |
| --- | --- | --- |
| `encoded_key` | text | Primary key. A versioned encoding of `(constraint, values)`. |
| `constraint_name` | text | The unique constraint that reserved the key. |
| `owner_table` | text | The table that owns the row holding this key. |
| `owner_pk` | text | The encoded primary key of the owning row. |
| `created_at` | text | ISO-8601 UTC timestamp of the reservation. |

There is a secondary index on `owner_table`. Two rows that would produce the same
`encoded_key` collide on this table's primary key, which is what rejects a
duplicate. Composite primary keys also reserve a guard here under a
`__pk_<table>` constraint name. Rows whose unique columns include a null are not
reserved (nulls never collide). The `add_unique` migration backfills these guards
for existing rows; `drop_unique` and `drop_table` delete the ones they own.

## `__kit_row_guards`

Optimistic-concurrency guards that keep foreign keys consistent under concurrent
writes.

| Column | Type | Notes |
| --- | --- | --- |
| `encoded_guard_key` | text | Primary key. Encodes `(table, primary_key)` of the guarded row. |
| `table_name` | text | The guarded (parent) table. |
| `primary_key` | text | The encoded primary key of the guarded parent row. |
| `version` | int64 | Bumped on every touch. |
| `updated_at` | text | ISO-8601 UTC timestamp of the last touch. |

There is a secondary index on `table_name`. When a child row references a parent
(insert, or a foreign-key backfill), the kit touches the parent's guard; a parent
delete writes the same guard key. Because both paths write the guard row, a
concurrent parent delete and child insert conflict at commit, so a child can
never be left pointing at a just-deleted parent. The `add_foreign_key` migration
backfills these guards; `drop_table` removes the ones it owns.

## `__kit_migration_locks`

A single advisory lock so two processes do not migrate the same database at once.

| Column | Type | Notes |
| --- | --- | --- |
| `lock_name` | text | Primary key. The kit uses one lock named `default`. |
| `holder` | text | The holder label (`kit`). |
| `acquired_at` | text | ISO-8601 UTC timestamp when the lock was taken. |
| `expires_at` | text | `acquired_at` plus a 5-minute TTL. |

The TypeScript runner inserts this row before applying migrations and deletes it
afterward. A live (unexpired) lock makes a second runner raise
`KitMigrationError` (`migration lock is already held`); an expired lock is
reclaimed automatically, so a crashed run does not wedge the database for more
than the TTL.

## See also

- [Migrations](./migrations.md) — how the runner reads and writes these tables.
- [Constraints](./constraints.md) — the unique and foreign-key rules the guard tables enforce.
- [Defaults & sequences](./defaults.md) — auto-increment semantics and the TypeScript/Rust/Python implementation split.
- [CLI](./cli.md) — `check` and `doctor` verify these tables exist.
