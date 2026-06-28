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

Most commands operate on either a **database directory** (a MongrelDB data
directory, e.g. `./store.kitdb`) or a **schema JSON file** — the catalog shape
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

- **`<Table>Row`** — every column, with nullable columns widened (`| null`,
  `Option<…>`, `Optional[…]`).
- **`<Table>Insert`** — only the columns you must supply: columns that have a
  default or are generated are omitted, and nullable columns are optional.
- **`<Table>Update`** — every column made optional, for partial patches.

Table names become `PascalCase` (e.g. `order_items` → `OrderItemsRow`). Storage
types map to each language's idioms — `int64`/`timestamp_nanos` become `bigint`
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

## Notes

- Commands that read a database (`check`, `doctor`, `schema print`, `migrate
  status`, `diff`, `generate migration`, `fixture`) open it read-mostly and do
  not change application data except `migrate apply` and `fixture load`.
- `schema validate`, `diff`, `generate migration`, and `generate types` read a
  schema **JSON file**, not a live database. Keep that file in sync with your
  code schema (or export it from your application).
- Non-zero exit codes signal failure (`doctor` problems, validation errors,
  unknown languages, missing tables), which makes the CLI easy to gate CI on.

## See also

- [Migrations](./migrations.md) — the migration model behind `migrate` and `generate migration`.
- [Schema DSL](./schema.md) — the schema the JSON files describe.
- [Internal tables](./internal-tables.md) — what `check` and `doctor` verify.
- [Types](./types.md) — the `Row`/`Insert`/`Update` shapes `generate types` mirrors.
- [Testing](./testing.md) — using fixtures in test setups.
