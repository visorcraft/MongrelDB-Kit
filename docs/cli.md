# CLI

The `mongreldb-kit` binary (crate `mongreldb-kit-cli`) wraps the Rust kit for
scripting, inspection, CI checks, and code generation.

```sh
mongreldb-kit <command> [args]
```

If you are working from a checkout, run it through Cargo:

```sh
cargo run -p mongreldb-kit-cli -- <command> [args]
```

Release binaries are attached to GitHub releases:

```sh
VERSION=v0.47.1
ASSET=mongreldb-kit-linux-x64 # use mongreldb-kit-linux-arm64 on ARM64 Linux
curl -L -o /usr/local/bin/mongreldb-kit \
  "https://github.com/visorcraft/MongrelDB-Kit/releases/download/${VERSION}/${ASSET}"
chmod +x /usr/local/bin/mongreldb-kit
```

Most commands operate on either a **database directory** (a MongrelDB data
directory, e.g. `./store.kitdb`) or a **schema JSON file** - the catalog shape
the kit reads and writes, with snake_case storage-type tokens such as `int64`,
`text`, `bool`, `float64`, `json`, `bytes`, `date`, `date_time`, and
`timestamp_nanos`. The examples below use the "store" schema (`customers`,
`products`, `orders`) from [Schema DSL](./schema.md).

## Command reference

| Command | Purpose |
| --- | --- |
| `init <path>` | Create a new (empty) database directory |
| `check <path>` | Open a database and verify its internal tables exist |
| `doctor <path>` | Open a database and run an integrity check |
| `get <path> <table> <pk>` | Point-read a single row by primary key |
| `query <path> <table>` | Query rows with `--filter`, `--order`, `--limit`, `--offset`, `--columns`, `--distinct` |
| `count <path> <table>` | Count rows, optionally matching `--filter` |
| `insert <path> <table> <row>` | Insert one row (JSON object); prints it with defaults applied |
| `update <path> <table> <pk> <patch>` | Update a row by primary key with a JSON patch object |
| `delete <path> <table> <pk>` | Delete a row by primary key |
| `upsert <path> <table> <row>` | Insert a row, or update on PK conflict with `--update` |
| `truncate <path> <table>` | Remove all rows from a table |
| `schema print <path>` | Print the stored schema catalog JSON |
| `schema validate <schema.json>` | Validate a schema JSON file |
| `migrate apply <path> <migrations.json>` | Apply pending migrations |
| `migrate status <path>` | Print applied migration versions |
| `migrate plan <path> <migrations.json>` | Print which migrations would be applied |
| `migrate dry-run <path> <migrations.json>` | Alias for `plan` |
| `diff <schema.json> <path>` | Compare a code schema to the stored catalog |
| `generate migration <schema.json> --from <path>` | Generate a migration skeleton for drift |
| `generate types <schema.json> --lang <ts\|rust\|python>` | Generate typed row/insert/update definitions |
| `fixture create <path> <tables...>` | Dump selected table rows to JSON |
| `fixture load <path> <fixture.json>` | Load rows from a JSON fixture |
| `procedure install\|drop\|list\|describe\|call` | Manage stored procedures |
| `compact <path>` | Merge all tables' sorted runs into one (maintenance) |
| `analyze <path>` | Rebuild index statistics for every table (engine `ANALYZE`) |
| `vacuum <path>` | Reclaim space: compact every table, then gc (engine `VACUUM`) |
| `rename-table <path> <from> <to>` | Rename a live table (engine + kit schema catalog) |
| `sql <path> <statement>` | Run a SQL statement (read returns rows as JSON; DDL/DML returns `[]`) |
| `view create <path> <view.json>` \| `view drop <path> <name>` | Create/drop a SQL view (session-scoped; see [SQL views](./migrations.md#sql-views)) |
| `index create <path> <table> <name> --column <col> [--kind ...]` \| `index drop <path> <name>` | Create/drop a secondary index (`--kind`: `bitmap`/`fm`/`ann`/`sparse`/`brin`) |
| `user create\|drop\|passwd\|verify\|admin\|list` | Manage catalog users (Argon2id-hashed passwords) |
| `role create\|drop\|list\|grant\|revoke\|allow\|deny` | Manage roles and table-level permissions |
| `auth enable <path> --admin-user <user> --admin-password <pw>` | Enable `require_auth` on an existing credentialless database |
| `auth disable-offline <path> [--passphrase <pw>] [--yes]` | Disable `require_auth` (offline recovery) |

`mongreldb-kit --help` and `mongreldb-kit <command> --help` print the same
information at the terminal.

## init

Create an empty database directory. The directory is created with the internal
`__kit_*` tables but no application tables (its schema is established later by an
application that opens it with a `Schema`).

```sh
mongreldb-kit init ./store.kitdb
# initialized ./store.kitdb
```

To create the database with credential enforcement from the start, pass
`--require-auth` with the bootstrap admin's credentials. The admin user is
created (Argon2id-hashed, flagged admin) and the database is marked
`require_auth`, so all subsequent opens must supply credentials:

```sh
mongreldb-kit init ./store.kitdb \
  --require-auth --admin-user alice --admin-password 's3cret-pw'
# initialized ./store.kitdb (require_auth enabled, admin user 'alice')
```

See the engine
[credential enforcement guide](https://github.com/visorcraft/MongrelDB/blob/master/docs/15-credential-enforcement.md)
for the full model.

## check

Open a database and confirm the reserved internal tables are present. Useful as a
fast CI smoke check.

```sh
mongreldb-kit check ./store.kitdb
# OK: ./store.kitdb
```

## doctor

Open a database and run an integrity check: it confirms the internal tables exist
and that the migration log is readable, then reports a summary. Exits non-zero if
any check fails.

```sh
mongreldb-kit doctor ./store.kitdb
# [ok] internal tables present
# [ok] 0 applied migration(s)
# doctor: no problems found
```

## Data commands (get / query / count / insert / update / delete / upsert)

The CLI does single-row CRUD and filtered queries against a database. Rows are
JSON objects; primary keys are JSON scalars (numbers, strings, or `null`).

```sh
# Insert a row (defaults and generated columns are applied).
mongreldb-kit insert ./store.kitdb users '{"id": 1, "email": "alice@example.com"}'

# Point-read by primary key.
mongreldb-kit get ./store.kitdb users 1

# Update a row by primary key (JSON patch; only the listed columns change).
mongreldb-kit update ./store.kitdb users 1 '{"email": "alice@new.example"}'

# Upsert (insert or, on PK conflict, update with the same row).
mongreldb-kit upsert ./store.kitdb users '{"id": 1, "email": "alice@new.example"}' --update

# Delete by primary key.
mongreldb-kit delete ./store.kitdb users 1
```

`query` and `count` take a **friendly filter** (`--filter`), an order string
(`--order`), and pagination/projection flags:

```sh
# Filter: amount >= 100 AND region == "east" (multiple keys AND together).
mongreldb-kit query ./store.kitdb orders \
  --filter '{"amount":{"gte":100},"region":"east"}' \
  --order '-amount' --limit 10

# Projection + distinct.
mongreldb-kit query ./store.kitdb orders --columns 'region,status' --distinct

# Count matching rows.
mongreldb-kit count ./store.kitdb orders --filter '{"status":"shipped"}'
```

### Filter expression syntax

`--filter` takes a JSON object. Each key is a column name; the value is either a
bare scalar (shorthand for `eq`) or a single-operator object
`{"<op>": <operand>}`. Multiple keys are AND-ed.

| Operator | Operand | Meaning |
| --- | --- | --- |
| `eq` | scalar | `column = value` (also the bare-value shorthand) |
| `ne` | scalar | `column != value` |
| `gt` / `gte` / `lt` / `lte` | scalar | comparison |
| `in` / `not_in` | array | membership |
| `like` | string | SQL `LIKE` (`%`/`_` wildcards), evaluated in Rust |
| `contains` | string | case-sensitive substring |
| `bytes_prefix` | string | anchored prefix match `LIKE 'prefix%'` on a bitmap-indexed `bytes` column (exact engine pushdown) |
| `is_null` / `is_not_null` | `true` | null check |

```sh
# LIKE with wildcards.
mongreldb-kit query ./store.kitdb users --filter '{"email":{"like":"%@example.com"}}'

# Anchored prefix on a bytes column (exact pushdown when bitmap-indexed).
mongreldb-kit query ./store.kitdb events --filter '{"key":{"bytes_prefix":"user:"}}'

# Null check.
mongreldb-kit query ./store.kitdb users --filter '{"deleted_at":{"is_null":true}}'
```

## SQL, views, and indexes

`sql` runs an arbitrary SQL statement through the kit's embedded DataFusion
frontend. Reads return rows as a pretty-printed JSON array; DDL/DML
(`CREATE TABLE`, `CREATE VIEW`, `INSERT`, `ANALYZE`, `VACUUM`) return `[]`.

```sh
mongreldb-kit sql ./store.kitdb "SELECT region, count(*) AS n FROM orders GROUP BY region"
mongreldb-kit sql ./store.kitdb "VACUUM"
```

`analyze` and `vacuum` are convenience wrappers for the engine's `ANALYZE`
(rebuild index statistics) and `VACUUM` (compact + gc) maintenance commands.
`rename-table` durably renames a table and updates the kit schema catalog.

```sh
mongreldb-kit analyze ./store.kitdb            # "analyzed all tables"
mongreldb-kit vacuum ./store.kitdb             # "reclaimed N run(s)"
mongreldb-kit rename-table ./store.kitdb widgets things
```

`view create` / `view drop` and `index create` / `index drop` run the
corresponding DDL. **Views are session-scoped** - a view created via the CLI
exists only within that single CLI invocation (each invocation opens a fresh
`Database`/session). For a persistent view, define it in a migration
(`create_view` op), which re-creates it whenever the migration runs. Indexes
*are* persistent (they're written to the catalog).

```sh
# View: create from a JSON spec {"name": ..., "sql": "SELECT ..."}, then drop.
echo '{"name":"vip","sql":"SELECT id FROM users WHERE score >= 90"}' > vip.json
mongreldb-kit view create ./store.kitdb vip.json
mongreldb-kit view drop ./store.kitdb vip

# Index: create a bitmap index on users.email, or a learned-range (PGM) on
# orders.placed_at for range predicates.
mongreldb-kit index create ./store.kitdb users idx_users_email --column email --kind bitmap
mongreldb-kit index create ./store.kitdb orders idx_orders_ts --column placed_at --kind brin
mongreldb-kit index drop ./store.kitdb idx_users_email
```

Supported `--kind` values: `bitmap` (default), `fm`, `ann`, `sparse`, `brin`
(also `learned`/`learned_range`/`range` - aliases for the PGM zonemap).

## compact

Merge every table's sorted runs into a single clean run so query latency
stays flat. Tables with fewer than two runs are skipped. Safe to run at
any time - readers pin their own snapshot and are unaffected.

```sh
mongreldb-kit compact ./store.kitdb
# compacted 3 table(s), skipped 1
```

This is the recommended cron-job entry for non-daemon deployments. For
daemon deployments, the background auto-compactor (every 30s) already
handles this. See the engine's [Maintenance & Operations](https://github.com/visorcraft/MongrelDB/blob/main/docs/09-maintenance.md)
doc for details.

## schema print

Print the stored schema catalog as pretty JSON. For a freshly `init`'d database
the table list is empty:

```sh
mongreldb-kit schema print ./store.kitdb
# {
#   "tables": [],
#   "by_name": {},
#   "by_id": {}
# }
```

## schema validate

Validate a schema JSON file. It first checks for duplicate or reused stable
table/column IDs (naming the offender), then runs full structural validation
(primary keys, index and foreign-key references).

```sh
mongreldb-kit schema validate ./store-schema.json
# OK: ./store-schema.json
```

A bad schema exits non-zero with a pinpointed message, e.g.
`duplicate/reused table id 1 used by "customers" and "products"` or
`unknown variant 'timestamp', expected one of ... 'timestamp_nanos'`.

## migrate apply / status / plan / dry-run

`migrate` reads migrations from a JSON file shaped like the
[declarative migration format](./migrations.md#declarative-json--rust--cli):

```json
[
  { "version": 1, "name": "init",
    "ops": [ { "create_table": { "name": "customers" } } ] },
  { "version": 2, "name": "add_orders",
    "ops": [ { "create_table": { "name": "orders" } } ] }
]
```

The JSON op vocabulary also includes `create_trigger`/`replace_trigger`/
`drop_trigger`, `create_view`/`replace_view`/`drop_view`,
`create_virtual_table`/`drop_virtual_table`, and `raw_sql`. The CLI runner
executes all of them: schema/column ops run against the core engine, while the
SQL-backed ops (views, virtual tables, raw SQL) run through the kit's embedded
SQL session - no separate daemon or TypeScript process required. See the
[migration ops table](./migrations.md#supported-operations) for the full list.

**Plan** (and its `dry-run` alias) prints what would be applied without touching
the database:

```sh
mongreldb-kit migrate plan ./store.kitdb ./migrations.json
# pending migrations:
#   1 init
#   2 add_orders
```

**Status** lists the applied versions:

```sh
mongreldb-kit migrate status ./store.kitdb
# no migrations applied
```

**Apply** runs the pending migrations and records them:

```sh
mongreldb-kit migrate apply ./store.kitdb ./migrations.json
# applied migration 1 init
# applied migration 2 add_orders
```

> **Stored-schema caveat.** `migrate apply` resolves each op against the
> database's **stored** schema. A database created with `init` has an empty
> schema, so `create_table`/`add_*` ops cannot find their table definitions
> (`add_foreign_key` will fail with `table ... not found in schema`). In
> practice the schema is established by the application (TypeScript/Rust/Python)
> when it opens the database with its `Schema`; the CLI's `migrate` is most
> useful for inspecting (`plan`/`status`) and for driving migrations against an
> application-managed database. See [Migrations](./migrations.md).

## diff

Compare a code schema JSON against a database's stored catalog and print the
drift. Useful in CI to fail a build when code and database disagree.

```sh
mongreldb-kit diff ./store-schema.json ./store.kitdb
# + table customers
# + table products
# + table orders
```

`+`/`-` mark added/removed tables, columns, indexes, and constraints; `~` marks a
changed property (type, nullability, default, references, on-delete, columns);
`!` flags a reused stable column ID. When code and catalog match, it prints
`no drift`.

## generate migration

Emit a migration skeleton (as JSON on stdout) for the tables and columns present
in the code schema but missing from the database. Redirect it to a file and edit
as needed.

```sh
mongreldb-kit generate migration ./store-schema.json --from ./store.kitdb
# [
#   {
#     "version": 1,
#     "name": "generated",
#     "ops": [
#       { "create_table": { "name": "customers" } },
#       { "create_table": { "name": "products" } },
#       { "create_table": { "name": "orders" } }
#     ]
#   }
# ]
```

The `version` is one past the highest applied migration. The generator covers new
tables and new columns; review and extend the skeleton (constraints, drops)
before applying.

## generate types

Generate language-native type definitions from a schema JSON file. Output goes to
stdout, so redirect it to a file:

```sh
mongreldb-kit generate types ./store-schema.json --lang ts     > src/db-types.ts
mongreldb-kit generate types ./store-schema.json --lang rust   > src/db_types.rs
mongreldb-kit generate types ./store-schema.json --lang python > app/db_types.py
```

`--lang` accepts `ts` (or `typescript`), `rust` (or `rs`), and `python` (or
`py`); any other value exits non-zero
(`unsupported lang "go" (expected ts, rust, or python)`).

For every table the generator emits three shapes:

- **`<Table>Row`** - every column, with nullable columns widened (`| null`,
  `Option<…>`, `Optional[…]`).
- **`<Table>Insert`** - only the columns you must supply: columns that have a
  default or are generated are omitted, and nullable columns are optional.
- **`<Table>Update`** - every column made optional, for partial patches.

Table names become `PascalCase` (e.g. `order_items` → `OrderItemsRow`). Storage
types map to each language's idioms - `int64`/`timestamp_nanos` become `bigint`
in TypeScript, `i64` in Rust, and `int` in Python; `json` becomes `unknown` /
`serde_json::Value` / `Any`; `bytes` becomes `Uint8Array` / `Vec<u8>` / `bytes`.

For example, the TypeScript output for `customers` (a sequence PK and `created_at`
are generated, so they drop out of the insert shape):

```ts
// Generated by mongreldb-kit. Do not edit.

export interface CustomersRow {
	id: bigint;
	email: string;
	name: string;
	tier: string;
	created_at: bigint;
}

export interface CustomersInsert {
	email: string;
	name: string;
}

export interface CustomersUpdate {
	id?: bigint;
	email?: string;
	name?: string;
	tier?: string;
	created_at?: bigint;
}
```

Each file starts with a `Generated by mongreldb-kit. Do not edit.` header
(`#` for Python). Re-run the command after a schema change and commit the
regenerated file.

## fixture create / load

Move table rows in and out of a database as JSON, for seeding and test fixtures.

`fixture create` dumps the named tables to pretty JSON on stdout:

```sh
mongreldb-kit fixture create ./store.kitdb customers products > seed.json
```

The output is an object keyed by table name, each value an array of row objects:

```json
{
  "customers": [ { "id": 1, "email": "ada@example.com", "name": "Ada", "tier": "free" } ],
  "products": [ { "id": 1, "sku": "SKU-1", "name": "Widget", "price_cents": 500 } ]
}
```

`fixture load` inserts rows from such a file inside a single transaction:

```sh
mongreldb-kit fixture load ./store.kitdb ./seed.json
# loaded ./seed.json
```

> Both fixture commands require the named tables to exist in the database's
> stored schema; an unknown table exits non-zero with
> `table <name> not found in schema`. Point them at an application-managed
> database rather than a bare `init`'d one.

## truncate

Remove all rows from a table in a single transaction.

```sh
mongreldb-kit truncate <path> <table>
```

The table must already exist in the database's stored schema. The command opens the
database, runs the truncate inside a transaction, and commits.

```sh
# Assuming ./store.kitdb already has a 'customers' table:
echo '{"customers":[{"id":1,"email":"ada@example.com","name":"Ada","tier":"free"}]}' > /tmp/row.json
mongreldb-kit fixture load ./store.kitdb /tmp/row.json
mongreldb-kit fixture create ./store.kitdb customers
# {
#   "customers": [ { "id": 1, "email": "ada@example.com", "name": "Ada", "tier": "free" } ]
# }

mongreldb-kit truncate ./store.kitdb customers
# table customers truncated

mongreldb-kit fixture create ./store.kitdb customers
# {
#   "customers": []
# }
```

> **RESTRICT semantics.** `truncate` fails when another table has a foreign key referencing the
target table. Remove dependent rows first, or define the referencing foreign key with
`on_delete: cascade` and delete the parent rows instead - `truncate` itself always refuses when
references exist.

## user - manage catalog users

Catalog users have Argon2id-hashed passwords and live alongside the schema
catalog (invisible to `table_names()`). Each user can be flagged admin to
short-circuit permission checks. See the engine
[Users, Roles & Permissions](https://github.com/visorcraft/MongrelDB/blob/master/docs/14-auth.md)
guide for the full model.

```sh
# Create, verify, and list
mongreldb-kit user create ./store.kitdb alice 's3cret-pw'
mongreldb-kit user verify ./store.kitdb alice 's3cret-pw'   # prints ok / invalid (exit 1 on mismatch)
mongreldb-kit user list   ./store.kitdb                     # ["alice"]

# Change a password
mongreldb-kit user passwd ./store.kitdb alice 'new-pw'

# Grant or revoke admin (admin bypasses all permission checks)
mongreldb-kit user admin  ./store.kitdb alice true
mongreldb-kit user admin  ./store.kitdb alice false

# Drop
mongreldb-kit user drop   ./store.kitdb alice
```

## role - manage roles and permissions

Roles are named bundles of permissions; a user's effective permissions are
the union across all their roles. The permission string vocabulary is the
same as the NAPI and Python bindings:

| Permission string | Meaning |
| --- | --- |
| `all` | Every permission on every table |
| `admin` | User/role management (`CREATE USER`, `GRANT`, `CREATE ROLE`) |
| `ddl` | Schema changes (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`) |
| `select:<table>` | `SELECT` on a specific table |
| `insert:<table>` | `INSERT` on a specific table |
| `update:<table>` | `UPDATE` on a specific table |
| `delete:<table>` | `DELETE` on a specific table |

```sh
# Create a role and grant table-level permissions
mongreldb-kit role create ./store.kitdb analyst
mongreldb-kit role allow  ./store.kitdb analyst select:orders
mongreldb-kit role allow  ./store.kitdb analyst insert:orders
mongreldb-kit role allow  ./store.kitdb analyst all        # GRANT ALL

# Grant/revoke the role on users
mongreldb-kit role grant  ./store.kitdb alice   analyst
mongreldb-kit role list   ./store.kitdb                      # ["analyst"]

# Reverse
mongreldb-kit role deny   ./store.kitdb analyst insert:orders
mongreldb-kit role revoke ./store.kitdb alice   analyst
mongreldb-kit role drop   ./store.kitdb analyst
```

> The same operations are available via SQL DDL (`CREATE USER`, `GRANT`, …)
> through `mongreldb-kit sql`, which is convenient for batching auth changes
> with schema changes in a migration.

## auth enable / disable-offline - credential enforcement

`auth enable` turns on `require_auth` for an existing credentialless database:
it bootstraps the first admin user (Argon2id-hashed, admin flag set) and flips
the database's `require_auth` bit. After this, every open must supply valid
credentials or it is rejected.

```sh
mongreldb-kit auth enable ./store.kitdb --admin-user alice --admin-password 's3cret-pw'
# require_auth enabled on ./store.kitdb (admin user 'alice')
```

`auth disable-offline` is the recovery path for when you can still open the
database but want to drop the `require_auth` flag. It opens the database -
plain, or encrypted with `--passphrase` - and calls `disable_auth()` on the
open handle. It will prompt for confirmation unless `--yes` is given.

This requires either an openable database (one you can authenticate to with
`--user`/`--password`) or a known passphrase for an encrypted database. For a
`require_auth` database whose credentials are genuinely lost, there is no
openable handle to call `disable_auth()` on - that case needs direct catalog
editing, as documented in the spec (see the credential enforcement guide
below).

```sh
mongreldb-kit auth disable-offline ./store.kitdb            # prompts to confirm
mongreldb-kit auth disable-offline ./store.kitdb --yes      # skip the prompt
```

Both open the database directly (no daemon required). Once `require_auth` is
enabled, the standard open/create commands and the language bindings must pass
`--user`/`--password` (or the language equivalents) or they will fail with an
auth error. See the engine
[credential enforcement guide](https://github.com/visorcraft/MongrelDB/blob/master/docs/15-credential-enforcement.md)
for the full model and recovery flow.

## Global auth flags

Several commands accept credentials so they can open a `require_auth` database:

| Flag | Purpose |
| --- | --- |
| `--user <user>` | Catalog username to authenticate as |
| `--password <pw>` | Password for `--user` (passed literally on the command line) |
| `--password-stdin` | Read the password from stdin instead of `--password` (avoids leaking it in shell history / process lists) |

```sh
mongreldb-kit query ./store.kitdb users --user alice --password 's3cret-pw'
echo 's3cret-pw' | mongreldb-kit query ./store.kitdb users --user alice --password-stdin
```

Prefer `--password-stdin` in scripts and CI.

The same credentials can be supplied via the `MONGREL_USER` and
`MONGREL_PASSWORD` environment variables - handy for `check`, `doctor`, and
other commands where flags would be noisy, and for mounting CI secrets without
echoing them on the command line. Explicit `--user` / `--password` flags take
precedence over the environment.

```sh
MONGREL_USER=admin MONGREL_PASSWORD=s3cret-pw mongreldb-kit check ./secure.kitdb
```

## Notes

- Commands that read a database (`check`, `doctor`, `schema print`, `migrate
  status`, `diff`, `generate migration`, `fixture`) open it read-mostly and do
  not change application data except `migrate apply`, `fixture load`, and
  `truncate`.
- `schema validate`, `diff`, `generate migration`, and `generate types` read a
  schema **JSON file**, not a live database. Keep that file in sync with your
  code schema (or export it from your application).
- Non-zero exit codes signal failure (`doctor` problems, validation errors,
  unknown languages, missing tables), which makes the CLI easy to gate CI on.

## See also

- [Migrations](./migrations.md) - the migration model behind `migrate` and `generate migration`.
- [Schema DSL](./schema.md) - the schema the JSON files describe.
- [Internal tables](./internal-tables.md) - what `check` and `doctor` verify.
- [Types](./types.md) - the `Row`/`Insert`/`Update` shapes `generate types` mirrors.
- [Testing](./testing.md) - using fixtures in test setups.
