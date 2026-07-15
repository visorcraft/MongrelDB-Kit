# SQL cancellation and timeouts

MongrelDB SQL cancellation is cooperative. A query checks one shared control
while queued, planning, executing, scanning, streaming, and serializing. A
deadline starts before queue admission. Cancellation before the commit fence
prevents an autocommit write. Once the commit fence wins, cancellation returns
`CANCEL_TOO_LATE` and the real commit result remains authoritative.

Use a server timeout for database work. Treat a transport timeout only as loss
of contact. Remote clients request best-effort server cancellation after a
transport failure, but cannot claim the server stopped without checking query
status.

## TypeScript

Embedded and remote databases share `SqlOptions`:

```ts
const controller = new AbortController();
const query = db.startSql('SELECT * FROM documents', {
  timeoutMs: 5_000,
  signal: controller.signal,
  queryId: '0123456789abcdef0123456789abcdef',
});

controller.abort();
await query.result;
```

`sql()` and `sqlRows()` accept the same options. They reject with
`QueryCancelledError`, `QueryTimeoutError`, `QueryIdConflictError`, or
`TransactionAbortedError`. Native execution runs off the Node event loop.
Abort listeners are removed when the Promise settles.

An already-aborted signal does not start native or remote SQL. The returned
query still rejects asynchronously with `QueryCancelledError`.

Migration SQL accepts an explicit default and a per-statement override:

```ts
await migrate(db, schema, migrations, {
  sql: { timeoutMs: 60_000 },
});

const migration = {
  version: 2,
  name: 'long view build',
  up: async (ctx) => {
    await ctx.sql('CREATE VIEW ...', { timeoutMs: 120_000 });
  },
};
```

`createVirtualTable`, `dropVirtualTable`, `createView`, `replaceView`, and
`dropView` also accept `SqlOptions` as their last argument.

## Rust

Embedded queries use `SqlOptions` and `SqlQueryHandle`:

```rust
use mongreldb_kit::{SqlOptions, QueryId};
use std::time::Duration;

let id: QueryId = "0123456789abcdef0123456789abcdef".parse()?;
let query = db.start_sql(
    "SELECT * FROM documents",
    SqlOptions {
        query_id: Some(id),
        timeout: Some(Duration::from_secs(5)),
    },
)?;
query.cancel();
let result = query.wait();
```

`sql_with_options`, `sql_rows_with_options`, and `sql_arrow_with_options`
apply the same deadline through execution and output conversion. Errors map to
`KitError::Cancelled`, `DeadlineExceeded`, `QueryConflict`,
`TransactionAborted`, `Unsupported`, or `Transport`.

With the `remote` feature, use `RemoteSqlOptions`. `timeout` is sent to the
server. `transport_timeout` only bounds the HTTP exchange:

```rust
let query = remote.start_sql_rows(
    "SELECT * FROM documents".into(),
    RemoteSqlOptions {
        timeout: Some(Duration::from_secs(5)),
        transport_timeout: Some(Duration::from_secs(8)),
        ..Default::default()
    },
)?;
query.cancel()?;
```

## Python

Embedded blocking calls release the GIL. A handle can be cancelled from
another thread:

```python
query = db.start_sql("SELECT * FROM documents", timeout_ms=5_000)
query.cancel()
rows = query.result()
```

`await db.sql_rows_async(...)` uses a worker thread. Cancelling the asyncio
task calls the native query handle before propagating `CancelledError`.

Remote Python separates server and transport timeouts:

```python
query = remote.start_sql_arrow(
    "SELECT * FROM documents",
    timeout_ms=5_000,
    transport_timeout=8.0,
)
query.cancel()
data = query.result()
```

Stable exceptions include `QueryCancelledError`, `QueryTimeoutError`,
`QueryIdConflictError`, `TransactionAbortedError`, `UnsupportedFeatureError`,
and `TransportError`.

## CLI

`mongreldb-kit sql` accepts `--timeout-ms` and `--query-id`. Press Ctrl-C once
to request cancellation of the active query. The CLI waits for the real query
outcome instead of reporting a write as rolled back after its commit fence won.

```sh
mongreldb-kit sql ./data 'SELECT * FROM documents' --timeout-ms 5000
```

## Remote compatibility

Controlled remote SQL requires `/capabilities` to advertise
`sql_cancellation.version = 1`, client query IDs, and the cancel endpoint.
Clients cache this response per connection.

| Client request | Older server without v1 | v1 server |
| --- | --- | --- |
| SQL without control options | Runs through the compatible legacy path | Runs |
| Timeout, query ID, signal, or transport timeout | Typed unsupported-feature error | Controlled query |
| Cancel or status | Typed unsupported-feature error | Uses `/queries/{id}` |

No client treats a local socket abort as confirmed server cancellation.

## Transactions, DDL, and migrations

- Autocommit writes cancelled before the commit fence leave no durable write.
- Cancellation after the fence returns the committed or failed commit outcome.
- A cancelled explicit-transaction statement restores its statement staging,
  marks the transaction aborted, and permits only `ROLLBACK`.
- Multi-statement requests report completed statement count. They are not
  automatically all-or-nothing. Use an explicit transaction for that.
- DDL uses the same commit fence.
- Give administrative migrations a deliberate longer timeout. Do not disable
  cancellation internally. Keep DDL out of short request paths.

## AI-agent use

Give every tool invocation a random 32-hex query ID. Persist it with the agent
run so operators can inspect or cancel the exact query. Set a server deadline
from the remaining tool budget. Keep the transport timeout slightly longer.
On a transport error, query status before retrying any write. Prefer native
scored Kit endpoints for expensive AI retrieval when they already expose a
deadline.

## Server operations

The daemon supports:

- `POST /sql` with `query_id` and `timeout_ms`;
- `X-MongrelDB-Query-ID` on responses;
- `GET /queries/{id}`;
- `POST /queries/{id}/cancel`;
- controlled prepared-statement prepare and execute;
- bounded active registry, SQL admission, output rows, and output bytes;
- cancellation on stream drop, session close, and graceful shutdown.

Query owners and admins may inspect or cancel. Other users receive 404. Status
contains an operation and SQL fingerprint, never raw SQL or parameters.

See the engine's SQL and daemon guides for environment variables, status
states, error envelopes, and metrics.
