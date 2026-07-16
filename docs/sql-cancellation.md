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
`QueryCancelledError`, `QueryTimeoutError`, `QueryIdConflictError`,
`TransactionAbortedError`, `ResultLimitExceededError`,
`SerializationError`, or `CommitOutcomeError`. Native execution runs off the
Node event loop. Abort listeners are removed when the Promise settles.

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
`TransactionAborted`, `ResultLimitExceeded`, `SerializationFailed`,
`CommitOutcome`, `OutcomeUnknown`, `CapabilityUnsupported`, or `Transport`.
Query errors preserve the engine's exact durable receipt: committed statement
count, last commit epoch, first and last committed statement indexes, completed
statement count, and current statement index. `KitError::query_metadata()` also
exposes optional cancel outcome, cancellation reason, retryability, and server
state.

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
`QueryIdConflictError`, `TransactionAbortedError`, `ResultLimitExceededError`,
`SerializationError`, `CommitOutcomeError`, `QueryOutcomeUnknownError`,
`CapabilityUnsupportedError`, and `TransportError`.

## CLI

`mongreldb-kit sql` accepts `--timeout-ms` and `--query-id`. Press Ctrl-C once
to request cancellation of the active query. Press it again to exit immediately.
Output conversion is staged, so cancellation before stdout writing emits no
partial result. Ctrl-C during final stdout writing exits with status 130 and
can leave a redirected consumer with a truncated stream. The CLI waits for the
real query outcome instead of reporting a write as rolled back after its commit
fence won.

```sh
mongreldb-kit sql ./data 'SELECT * FROM documents' --timeout-ms 5000
```

## Remote compatibility

Remote SQL requires `/capabilities` to advertise
`sql_cancellation.version = 2`, client query IDs, pre-registration cancellation,
query status, and the cancel endpoint. Clients assign every request a query ID
so a lost or malformed response can be reconciled with its durable receipt.
Official clients require exactly 32 hexadecimal characters and reject malformed
IDs before constructing any `/queries/{id}` route.
Clients cache capabilities per connection.

| Client request | Older server without v2 | v2 server |
| --- | --- | --- |
| Any SQL | Typed capability-unsupported error | Controlled query |
| Timeout, query ID, signal, or transport timeout | Typed capability-unsupported error | Controlled query |
| Cancel or status | Typed capability-unsupported error | Uses `/queries/{id}` |

No client treats a local socket abort as confirmed server cancellation.

## Remote authentication

Configure Bearer or Basic authentication on the remote client. The same
credential is applied to capabilities, SQL, cancellation, status, pagination,
and native Kit routes:

```ts
const remote = new RemoteDatabase(url, {
  auth: { bearerToken: process.env.MONGRELDB_TOKEN! },
});
```

```python
remote = RemoteDatabase(url, bearer_token=os.environ["MONGRELDB_TOKEN"])
```

```rust
let remote = RemoteDatabase::connect_with_options(
    url,
    RemoteOptions {
        auth: Some(RemoteAuth::Bearer(SecretString::from(token))),
        ..RemoteOptions::default()
    },
)?;
```

Basic authentication uses `{ username, password }` in TypeScript,
`username=` and `password=` in Python, and `RemoteAuth::Basic` in Rust. Never
put credentials, query parameters, or fragments in the remote URL. Remote
clients reject them before making a request.

## Durable outcomes and retry safety

Every remote terminal status and structured SQL error carries the query ID,
`committed`, committed statement count, exact last commit epoch, completed
statement count, statement index, terminal code, cancellation reason, and
retryability. TypeScript and Python execution errors also expose
`cancelOutcome`/`cancel_outcome`, `cancellationReason`/`cancellation_reason`,
and `serverState`/`server_state` without message parsing. JSON transports use
`last_commit_epoch_text`; clients parse that
decimal string into Rust `u64`, TypeScript `bigint`, or Python `int` without
losing precision.
For epochs above JavaScript's safe-integer range, the numeric JSON field must be
`null` and the decimal text field is authoritative. TypeScript rejects an unsafe
numeric epoch even when an exact text field is also present.

Embedded clients expose the engine receipt directly. They do not infer terminal
codes, commit counts, epochs, or statement indexes from the query phase.

`committed = true` is authoritative even when output serialization or transport
fails later. Do not retry such a write. `QUERY_OUTCOME_UNKNOWN` also forbids an
automatic retry. Retry only when the structured receipt explicitly proves no
commit and marks the failure retryable.

Unknown is never encoded as false. For `QUERY_OUTCOME_UNKNOWN`, commit state and
statement/epoch counters are `None` in Rust, `null` in TypeScript, and `None` in
Python. Only an explicit `false` proves no commit.

Remote clients validate successful idempotency responses before accepting a
durable receipt. Invalid JSON, ordinary row output, or inconsistent receipt
fields trigger query-status recovery. A retained terminal `failed` or
`cancelled` state with `committed = false` but no terminal code is reported as
`QUERY_FAILED`, not `QUERY_OUTCOME_UNKNOWN`.

Official clients parse control responses with duplicate-key detection and
reject unknown control fields, unsafe JavaScript numeric integers, and
inconsistent nested metadata. Malformed response bodies are never copied into
client error messages. Control JSON is capped at 1 MiB and paginated or general
JSON at 64 MiB; caller-provided SQL byte limits are enforced while reading the
response, not only after allocation.

## Remote pagination and idempotent writes

Bounded pagination is available only when `sql_pagination.version = 1` is
advertised. It accepts a read-only `SELECT`, a required projection, row/byte/token
limits, and returns an opaque owner-bound cursor. Continue with
`continue_sql_page`, `continueSqlPage`, or `continue_sql_page`. Cursors preserve
one retained snapshot, expire, and must never be decoded or modified by clients.

Durable at-most-once writes require `sql_idempotency.version = 1`. Use
`execute_idempotent_sql`, `executeIdempotentSql`, or `execute_idempotent_sql`
with a stable 1-to-256-byte key. The server persists intent before execution.
Replays return the stored receipt. An indeterminate intent is never executed
again automatically. The API accepts one write statement and rejects explicit
transactions, multi-statement SQL, and result-producing statements.

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
- bounded `pagination` requests and `POST /sql/continue`;
- durable `idempotency_key` write receipts;
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
