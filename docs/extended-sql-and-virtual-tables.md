# Extended SQL Functions And Virtual Tables

MongrelDB Kit keeps its typed CRUD/query builder focused on application rows,
but it now exposes two SQL-oriented surfaces for features that naturally belong
to the SQL frontend:

- **Extended SQL Functions** through small TypeScript expression helpers.
- **Virtual/external tables** through module specs that generate
  `CREATE VIRTUAL TABLE ... USING ...` SQL.

These APIs are intentionally modular: Kit does not hard-code one virtual-table
ecosystem or one function catalog into the typed builder. It provides stable
helpers for composing SQL and leaves module-specific arguments to the module.

## Running SQL

Embedded TypeScript uses the native addon's async SQL method:

```ts
const table = await db.sql('SELECT count(*) AS n FROM users');
const rows = await db.sqlRows('SELECT id, email FROM users ORDER BY id');
```

`db.sql(...)` returns an Apache Arrow table. `db.sqlRows(...)` decodes that table
to plain objects for convenience.

Remote TypeScript clients expose the same names synchronously because the native
remote client performs the HTTP call internally:

```ts
const rows = remote.sqlRows('SELECT count(*) AS n FROM users');
```

Rust and Python remote clients expose SQL at the daemon layer:

```rust
let rows = remote.sql_rows("SELECT count(*) AS n FROM users")?;
```

```python
arrow_ipc = remote.sql_arrow("SELECT count(*) AS n FROM users")
```

## Extended SQL Function helpers

TypeScript exports helper functions from the main package:

```ts
import {
  groupConcat,
  jsonExtract,
  mathFn,
  percentileCont,
  unixEpoch,
} from '@visorcraft/mongreldb-kit';

const p95 = percentileCont(events.latency_ms, 0.95).sql;
const tags = groupConcat(events.tag, '|').sql;
const city = jsonExtract(events.payload, '$.city').sql;
const hour = unixEpoch('now', 'start of hour').sql;
const score = mathFn('sqrt', events.score).sql;
```

Helpers return `{ sql: string }`. Use `.sql` when embedding them into a SQL
statement:

```ts
const rows = await db.sqlRows(`
  SELECT ${percentileCont(events.latency_ms, 0.95).sql} AS p95
  FROM events
`);
```

Available helpers:

| Helper | SQL emitted |
| --- | --- |
| `percentile(column, p)` | `percentile(column, p)` |
| `percentileCont(column, p)` | `percentile_cont(column, p)` |
| `percentileDisc(column, p)` | `percentile_disc(column, p)` |
| `groupConcat(column, separator?)` | `group_concat(column, separator)` |
| `stringAgg(column, separator)` | `string_agg(column, separator)` |
| `jsonExtract(value, path)` | `json_extract(value, path)` |
| `jsonValid(value)` | `json_valid(value)` |
| `dateTime(value?, ...modifiers)` | `datetime(value, ...)` |
| `dateOnly(value?, ...modifiers)` | `date(value, ...)` |
| `unixEpoch(value?, ...modifiers)` | `unixepoch(value, ...)` |
| `mathFn(name, ...args)` | `name(args...)` |
| `mongreldbFtsRank(text, query)` | `mongreldb_fts_rank(text, query)` |

`sqlLiteral(...)` quotes strings and scalar values. `mathFn(...)` accepts the
function name as raw SQL, so only pass trusted names.

### Full-text search ranking

The `mongreldb_fts_rank(text, query)` UDF computes a BM25-inspired relevance
score for a text column against a whitespace-tokenized query string. Use it in
`ORDER BY` to rank results by relevance:

```ts
// Rank articles by relevance to 'database performance'.
const rows = db.sqlRows(
  "SELECT id, title, mongreldb_fts_rank(content, 'database performance') AS score " +
  "FROM articles ORDER BY score DESC LIMIT 10"
);
```

### Advanced SQL features

The embedded SQL surface (via `db.sqlRows()`) supports the full SQL feature set
including recursive CTEs (`WITH RECURSIVE`), window functions (`ROW_NUMBER()`,
`SUM() OVER (...)`), `CREATE TABLE AS SELECT`, `CREATE MATERIALIZED VIEW`, and
multi-statement execution (semicolon-separated statements in one call).

## Virtual tables

Use `virtualTable(...)` to describe a module-backed table:

```ts
import { createVirtualTableSql, virtualTable } from '@visorcraft/mongreldb-kit';

const docsFts = virtualTable('docs_fts', 'fts_docs', [
  'content=docs',
  'tokenize=porter',
]);

await db.createVirtualTable(docsFts);
await db.dropVirtualTable('docs_fts');

console.log(createVirtualTableSql(docsFts));
// CREATE VIRTUAL TABLE "docs_fts" USING "fts_docs"(content=docs, tokenize=porter)
```

The table and module names are identifier-quoted. `args` are raw
module-specific SQL fragments because each virtual table module owns its own
argument grammar.

Virtual-table helpers are also available in migrations:

```ts
await migrate(db, schema, [{
  version: 3,
  name: 'add docs virtual table',
  ops: [{ kind: 'createVirtualTable', table: docsFts }],
  async up(ctx) {
    await ctx.createVirtualTable(docsFts);
  },
}]);
```

Use async migrations for virtual tables in TypeScript; `migrateSync` throws
because the SQL path is async. The Rust and Python runners execute
`create_virtual_table` / `drop_virtual_table` directly through their embedded
SQL session — no separate async context required.

Rust and Python remote helpers mirror the same operation:

```rust
remote.create_virtual_table(&VirtualTableSpec::new(
    "docs_fts",
    "fts_docs",
    ["content=docs", "tokenize=porter"],
))?;
remote.drop_virtual_table("docs_fts")?;
```

```python
remote.create_virtual_table("docs_fts", "fts_docs", ["content=docs", "tokenize=porter"])
remote.drop_virtual_table("docs_fts")
```

