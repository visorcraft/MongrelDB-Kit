# Transactions

MongrelDB provides **snapshot isolation** and **atomic cross-table commits**: a transaction
reads a consistent point-in-time snapshot and either commits all of its staged writes or
none of them. MongrelDB Kit builds its constraint enforcement on top of that primitive, and
exposes it for the times you need several writes to land together.

This page covers how the Kit uses transactions for you, how to run your own multi-statement
transactions with `begin`/`commit`/`rollback` and the retrying `transaction()` helper,
conflict semantics under snapshot isolation, and why sequence ids can have gaps.

## Every constrained mutation is already a transaction

You do **not** manage transactions for single statements. Each
`insertInto` / `updateTable` / `deleteFrom` `.executeSync()` (and its async `.execute()`)
opens its own transaction, does its work, and commits:

1. apply defaults and validate the full row,
2. enforce foreign keys and stage unique / primary-key guards,
3. write the application row(s),
4. commit — and, on a write-write conflict, **retry automatically** (up to 5 attempts) with
   a small bounded backoff.

```ts
// Atomic on its own: defaults, validation, FK + unique checks, write, commit, auto-retry.
const order = db.insertInto(orders).values({ customer_id: ada.id }).executeSync();
```

A cascading delete is a single transaction too: deleting a customer removes her orders and
their items atomically (see [Constraints](./constraints.md#delete-actions)). The Kit does
not expose a way to thread several `insertInto`/`updateTable`/`deleteFrom` calls into one
shared transaction — each is atomic by itself. When you need *multiple* writes to commit
together, drop down to the native transaction API below.

## Manual transactions: `begin` / `commit` / `rollback`

`db.begin()` opens a cross-table transaction. Stage writes with `txn.put(table, cells)` and
`txn.delete(table, rowId)`, then `txn.commit()` (returns the commit epoch) or
`txn.rollback()` to discard everything staged:

```ts
const txn = db.begin();
try {
  txn.put('accounts', toCells(accounts, { id: 1n, owner: 'alice', balance_cents: 8_500n }));
  txn.delete('accounts', staleRowId);
  txn.commit();          // all-or-nothing
} catch (err) {
  txn.rollback();        // nothing staged is persisted
  throw err;
}
```

> **Native transactions stage raw rows and bypass Kit enforcement.** `txn.put` /
> `txn.delete` write cells straight to storage: they do **not** run the validators, the
> `unique` / foreign-key guards, or the cascade planner. You are responsible for supplying
> complete, valid rows — build them with `toCells(table, row)` and, if you want the Kit's
> checks, call `validateRow(table, row)` yourself before staging. `txn.put` is an upsert
> keyed by the row's storage id, so updates are expressed as `delete(oldRowId)` +
> `put(newCells)`, exactly as the Kit's own update path does.

## The retrying `transaction()` helper

For multi-write atomicity with built-in conflict retries, use the `transaction(fn, opts?)`
helper. It lives on the native database object, reached from the Kit via `db.nativeDb`:

```ts
const epoch = await db.nativeDb.transaction(
  (txn) => {
    txn.put('accounts', toCells(accounts, fromRow));
    txn.put('accounts', toCells(accounts, toRow));
  },
  { maxRetries: 5, baseDelayMs: 2 },
);
```

- `fn(txn)` may be **sync or async**; it stages ops on `txn` and must **not** call
  `commit`/`rollback` — the helper commits on success and rolls back on throw.
- It retries **only write-write conflicts**, up to `maxRetries` (default `3`) with a linear
  backoff of `baseDelayMs` (default `2` ms) per attempt; any other error propagates
  immediately after a rollback.
- It resolves to the commit epoch (`bigint`).
- **The callback must be idempotent / retry-safe.** On a conflict it runs again from
  scratch, so re-read the rows you depend on *inside* the callback and avoid side effects
  (logging, external calls, mutating outer state) that must not happen twice.

### Worked example: a money transfer

Two balance updates that must both apply or neither — with a guard against overdraft and
automatic retry on contention:

```ts
import { table, int, text, sequenceDefault, check, toCells, validateRow } from '@mongreldb/kit';
import { ConditionKind } from 'mongreldb/native.js';

const accounts = table('accounts', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('accounts_id_seq') }),
    text('owner', { nullable: false }),
    int('balance_cents', { nullable: false, min: 0 }),
  ],
  primaryKey: 'id',
  checks: [check('balance_nonneg', (row) => (row.balance_cents as bigint) >= 0n)],
});

const native = db.nativeDb;

// Read a row (with its storage rowId) so we can express the update as delete + put.
function readAccount(id: bigint) {
  const [rowJs] = native
    .table('accounts')
    .query([{ kind: ConditionKind.RangeInt, columnId: accounts.id.id, int64Lo: id, int64Hi: id }]);
  const owner = String(rowJs.cells.find((c) => c.columnId === accounts.owner.id)?.text ?? '');
  const balance = rowJs.cells.find((c) => c.columnId === accounts.balance_cents.id)?.int64 ?? 0n;
  return { rowId: rowJs.rowId, owner, balance };
}

async function transfer(fromId: bigint, toId: bigint, amount: bigint): Promise<void> {
  await native.transaction(
    (txn) => {
      // Re-read inside the callback so a retry sees fresh balances.
      const from = readAccount(fromId);
      const to = readAccount(toId);
      if (from.balance < amount) throw new Error('insufficient funds'); // not a conflict: no retry

      const fromRow = { id: fromId, owner: from.owner, balance_cents: from.balance - amount };
      const toRow = { id: toId, owner: to.owner, balance_cents: to.balance + amount };
      validateRow(accounts, fromRow); // opt back into the Kit's validators
      validateRow(accounts, toRow);

      txn.delete('accounts', from.rowId);
      txn.put('accounts', toCells(accounts, fromRow));
      txn.delete('accounts', to.rowId);
      txn.put('accounts', toCells(accounts, toRow));
    },
    { maxRetries: 5 },
  );
}

await transfer(alice.id, bob.id, 2_500n);
// Both balances move together. If `transfer` throws (e.g. insufficient funds),
// nothing is committed and both balances are unchanged.
```

## Concurrency model

MongrelDB uses **snapshot isolation**: each transaction sees a consistent snapshot taken at
its start, and readers never block writers. Two transactions that write the **same row**
conflict; the conflict is detected at **commit time**, where the loser's `commit` throws a
retryable error rather than overwriting the winner.

- The native layer raises a `ConflictError` whose message is prefixed `__CONFLICT__:`; the
  Kit's own paths raise `KitConflictError`. Both are recognised by `isRetryableConflict(err)`
  and carry the `CONFLICT` error code (see [Errors](./errors.md)).
- The `transaction()` helper and the Kit's internal mutation path both catch exactly these
  and retry; everything else aborts.
- Because the loser **retries** instead of clobbering, "last write wins" never silently
  drops an update on a contended row — the retry re-reads the winner's value first (which is
  why your callback must be re-runnable).
- The Kit also *deliberately manufactures* conflicts to keep relational integrity safe under
  snapshot isolation: a child insert and a concurrent parent delete each touch the parent's
  `__kit_row_guards` row, forcing a write-write conflict so one side retries and observes the
  other instead of both committing against stale snapshots. See
  [Internal tables](./internal-tables.md) and the [specification](./spec.md).

## Sequence gaps on rollback

Auto-increment ids come from sequences (`sequenceDefault(...)`), allocated by
`db.allocateSequenceSync(name, count?)` / `await db.allocateSequence(name, count?)`. Two
properties matter:

- **Sequences are 1-based** — the first id handed out is `1n`, never `0n`.
- **Allocation commits in its own transaction**, separately from whatever mutation requested
  it. So if an insert allocates an id and then fails — a validation error, a constraint
  violation, or a rolled-back surrounding transaction — the id is **already spent**. The next
  insert gets the following number, leaving a gap.

```ts
const a = db.insertInto(customers).values({ email: 'a@example.com', name: 'Ada' }).executeSync();
// a.id === 1n

// Fails validation (missing name) — but only AFTER id 2 was allocated.
try {
  db.insertInto(customers).values({ email: 'b@example.com', name: null as never }).executeSync();
} catch { /* KitValidationError */ }

const c = db.insertInto(customers).values({ email: 'c@example.com', name: 'Cy' }).executeSync();
// c.id === 3n — id 2 was consumed by the failed insert.
```

This is the same behavior as SQL `AUTO_INCREMENT` / `SERIAL`: sequence values are a source
of unique ids, **not** a gap-free counter. Do not treat ids as contiguous.

> The Kit's internal mutation path uses a synchronous transaction wrapper (default 5 retry
> attempts); the native `transaction()` helper defaults to 3. Both numbers are tunable via
> `opts.maxRetries` where the helper is called.

**In Rust/Python:** the same primitives exist — a `begin`/`commit`/`rollback` transaction
and a retrying transaction wrapper, with snapshot-isolation conflicts surfaced as the
binding's conflict error type. See [rust.md](./rust.md) / [python.md](./python.md).

## See also

- [Constraints](./constraints.md) — what each constrained mutation validates and enforces before it commits.
- [Defaults & sequences](./defaults.md) — sequence, now, uuid, and static defaults.
- [Errors](./errors.md) — `KitConflictError`, `isRetryableConflict`, and the `CONFLICT` code.
- [Internal tables](./internal-tables.md) — `__kit_sequences`, `__kit_row_guards`, and the guard mechanism.
- [Specification](./spec.md) — the concurrency model and snapshot-isolation safety in depth.
