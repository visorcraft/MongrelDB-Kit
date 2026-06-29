# MongrelDB Kit Specification

MongrelDB Kit is the application-facing persistence layer for MongrelDB. It wraps the storage engine with a schema model, query builder, migration runner, and relational constraints that behave identically across TypeScript, Rust, and Python.

## Layers

The kit is organized into four layers:

1. **Core metadata and algorithms** (`mongreldb-kit-core` in Rust; shared logic in TypeScript/Python)
   - Schema model: tables, columns, indexes, unique constraints, foreign keys, checks
   - Type model and validation rules
   - Stable key encoding for primary keys, unique guards, and row guards
   - Migration planning and checksums
   - Delete planner (cascade, set null, restrict)
   - Query AST

2. **MongrelDB execution adapter** (`mongreldb-kit` crate, `@mongreldb/kit` package, Python bindings)
   - Open/create databases
   - Create/drop/evolve tables
   - Read/write rows through transactions
   - Conflict retry and error mapping

3. **Language bindings**
   - TypeScript DSL and types
   - Rust builders/structs/traits
   - Python dict/dataclass builders

4. **Developer tooling**
   - Migration CLI (`mongreldb-kit`)
   - Schema generation
   - Conformance tests
   - Fixture helpers

## Internal tables

The kit reserves tables whose names start with `__kit_`. These tables are created automatically when needed and are excluded from normal application table enumeration.

### `__kit_schema_migrations`

Records every migration that has been applied.

| Column | Type | Notes |
|---|---|---|
| `version` | int64 | Primary key; migration number |
| `name` | text | Human-readable migration name |
| `checksum` | text | Content-aware SHA-256 of the migration's canonical `version` / `name` / `ops` string |
| `applied_at` | text | ISO-8601 timestamp |
| `kit_version` | text | Kit release version |
| `status` | text | `applied`, `failed`, or `in_progress` |

### `__kit_schema_catalog`

Stores a single row describing the current application schema.

| Column | Type | Notes |
|---|---|---|
| `schema_version` | int64 | Primary key; currently always `1` |
| `schema_json` | text | JSON serialization of the schema |
| `checksum` | text | SHA-256 of `schema_json` |
| `written_at` | text | ISO-8601 timestamp |

### `__kit_sequences`

Tracks named sequence high-water marks in the Rust storage crate and Python facade. Current
TypeScript databases use native engine `AUTO_INCREMENT` counters for `sequenceDefault(...)`; the
TypeScript runner only reads `__kit_sequences` as a legacy upgrade source when the table already
exists.

| Column | Type | Notes |
|---|---|---|
| `sequence_name` | text | Primary key |
| `next_value` | int64 | Next value to allocate |
| `updated_at` | text | ISO-8601 timestamp |

### `__kit_unique_keys`

Implements unique constraints and composite-primary-key guards.

| Column | Type | Notes |
|---|---|---|
| `encoded_key` | text | Primary key; versioned encoded constraint value |
| `constraint_name` | text | Constraint that owns the key |
| `owner_table` | text | Table the guard belongs to |
| `owner_pk` | text | Encoded primary key of the owning row |
| `created_at` | text | ISO-8601 timestamp |

### `__kit_row_guards`

Prevents unsafe interleaving of parent deletes and child inserts under snapshot isolation.

| Column | Type | Notes |
|---|---|---|
| `encoded_guard_key` | text | Primary key; `rg:<table>:<encoded_pk>` |
| `table_name` | text | Guarded table |
| `primary_key` | text | Encoded primary key of the guarded row |
| `version` | int64 | Monotonically incremented on each touch |
| `updated_at` | text | ISO-8601 timestamp |

### `__kit_migration_locks`

Advisory lock so only one process runs migrations at a time.

| Column | Type | Notes |
|---|---|---|
| `lock_name` | text | Primary key; currently `default` |
| `holder` | text | Lock owner identifier |
| `acquired_at` | text | ISO-8601 timestamp |
| `expires_at` | text | Lock TTL (5 minutes by default) |

## Encoding specifications

### Primary keys

Single-column primary keys use the scalar value directly. Composite primary keys are encoded as colon-joined typed components.

Components:
- `s:<text>` — string value; `:` and `\` escaped as `\:` and `\\`
- `i:<integer>` — bigint integer
- `n:null` — explicit null marker

Examples:
```text
s:alice@example.com
s:orders:i:42
```

### Unique guard keys

Format: `uq:<version>:<constraint_name>:<component1>:<component2>...`

Example:
```text
uq:1:uq_user_email:s:alice@example.com
```

### Row guard keys

Format: `rg:<table_name>:<encoded_pk>`

Example:
```text
rg:users:s:alice@example.com
```

### Migration checksums

SHA-256 of the canonical migration content string, lowercase hex. The canonical shape is:

```text
{"version":<n>,"name":<json>,"ops":[<op>,...]}
```

The `ops` list is ordered and each operation uses a fixed key order, so TypeScript, Rust, and
Python produce the same checksum for the same logical migration. See
[Migrations](./migrations.md#checksums-and-drift-detection) for examples.

### Schema checksums

SHA-256 of the canonical JSON schema serialization, lowercase hex.

## Error codes

Each language maps the shared error categories into idiomatic types. All errors include a human-readable message; many include table, column, or constraint context.

| Category | Code/Variant | Meaning |
|---|---|---|
| Validation | `VALIDATION` / `KitValidationError` | Type, null, enum, range, length, regex, or check violation |
| Duplicate key | `DUPLICATE` / `KitDuplicateError` | Unique constraint or composite-primary-key conflict |
| Foreign key | `FOREIGN_KEY` / `KitForeignKeyError` | Referenced parent row does not exist |
| Restrict delete | `RESTRICT` / `KitRestrictError` | Delete blocked by a `restrict` foreign key |
| Migration | `MIGRATION` / `KitMigrationError` | Migration lock, failure, or unsupported operation |
| Schema drift | `SCHEMA_DRIFT` / `Integrity` | Stored schema catalog does not match code schema |
| Retryable conflict | `CONFLICT` / `KitConflictError` | Write-write conflict that may succeed after retry |
| Transaction timeout | `TIMEOUT` | Transaction exceeded its deadline |
| Storage | `STORAGE` / `KitError` | Underlying MongrelDB error |
| Integrity | `INTEGRITY` / `KitError` | Corruption or internal invariant violation |
| Unsupported query | `UNSUPPORTED` | Query shape not yet implemented by the execution path |

## Concurrency model

All constrained mutations run inside a MongrelDB transaction.

### Insert

1. Apply defaults and validate the full row.
2. Enforce foreign keys: load the parent row, reject if missing, and touch the parent row guard.
3. Stage unique guard rows for each unique constraint.
4. Stage a composite-primary-key guard if needed.
5. Write the application row and commit.

If a concurrent transaction inserted the same unique value, both transactions will attempt to write the same `__kit_unique_keys` primary key; one is rejected and can retry.

### Update

1. Load the existing row.
2. Merge the patch and apply update-time defaults.
3. Validate the full row.
4. Delete old unique guards and (if the PK changed) the old composite-PK guard.
5. Enforce foreign keys for changed FK columns.
6. Delete the old application row and insert the merged row.
7. Stage new unique/composite guards and commit.

### Delete

1. Plan all affected children recursively.
2. Reject immediately if any child has a `restrict` foreign key.
3. Apply `set null` updates, then cascade deletes, then delete the parent.
4. Clean unique guards, row guards, and the application row.
5. Commit atomically.

### Snapshot-isolation safety

MongrelDB provides snapshot isolation. Without extra care, a child insert and a parent delete could both commit because each sees a snapshot where the other has not yet written. The kit touches a `__kit_row_guards` row for the parent on every child insert and parent delete, forcing a write-write conflict so one transaction retries and observes the other.

## Supported types

| Application type | Storage type | Notes |
|---|---|---|
| `bool` | `bool` | |
| `int64` | `int64` | Rust also exposes `Int8`, `Int16`, `Int32` |
| `float64` | `float64` | Rust also exposes `Float32` |
| `text` | `bytes` | UTF-8 stored as bytes |
| `bytes` | `bytes` | Binary data |
| `json` | `bytes` | JSON text |
| `timestamp` | `bytes` / `int64` | ISO-8601 string or nanoseconds |
| `date` | `bytes` / `int32` | ISO-8601 date string or days |

The kit validates values against the application type before persistence.

## Extension points

Applications can still drop down to MongrelDB core when necessary:

- TypeScript: `db.nativeDb` exposes the underlying MongrelDB database object.
- Rust: the `Database` struct holds the core `Database` directly.
- Python: `Database._handle` holds the native PyO3 object.

These escape hatches bypass kit constraints and should be used only for operations the kit does not yet expose.
