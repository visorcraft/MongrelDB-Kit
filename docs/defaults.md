# Defaults & sequences

A **default** supplies a column's value when you omit it on insert. The Kit ships five default kinds
plus a `generated` shorthand, and a sequence-backed default that powers **auto-increment ids**. All
of them are declared in the column's [options](./schema.md#columnoptions):

```ts
import {
  int, text, timestamp,
  staticDefault, nowDefault, uuidDefault, sequenceDefault, customDefault,
} from '@mongreldb/kit';
```

## The default kinds

Each helper returns a `DefaultValue` you pass as the column's `default` option.

| Helper | Fills the column with |
| --- | --- |
| `staticDefault(value)` | a constant `value` (any type matching the column) |
| `nowDefault()` | the current time — ISO 8601 for `timestamp`, `YYYY-MM-DD` for `date` |
| `uuidDefault()` | a fresh random UUID string |
| `sequenceDefault(name)` | the next value of the named 1-based sequence (auto-increment) |
| `customDefault(fn)` | `fn()` evaluated at insert time |

```ts
int('id',         { primaryKey: true, default: sequenceDefault('customers_id_seq') }),
text('tier',      { enumValues: ['free', 'pro'], default: staticDefault('free') }),
timestamp('created_at', { default: nowDefault() }),
text('public_id', { default: uuidDefault() }),
text('token',     { default: customDefault(() => crypto.randomUUID()) }),
```

### The `generated` shorthand

`generated: 'uuid'` and `generated: 'now'` are convenience shorthands equivalent to `default:
uuidDefault()` and `default: nowDefault()`:

```ts
text('public_id', { generated: 'uuid' });        // same as default: uuidDefault()
timestamp('updated_at', { generated: 'now' });   // same as default: nowDefault()
```

If a column sets **both** `default` and `generated`, the explicit `default` wins. Either one makes
the column **optional on insert** (see [Types](./types.md#insertt)).

## When defaults apply

Defaults are an **insert-time** mechanism:

1. On insert, for each column you left **`undefined` or `null`**, the Kit fills in its default (or
   `generated` value).
2. **Then validation runs** on the completed row — not-null, enum, bounds, checks. So a default that
   produces an invalid value still fails validation, and a non-nullable column with no default that
   you omitted is rejected.

Passing `null` explicitly for a defaulted column triggers the default just like omitting it — there
is no way to force a defaulted column to `NULL` by passing `null`; make the column `nullable` with no
default if you want stored nulls.

```ts
// id, tier, created_at all omitted -> defaults fill them, then validation passes
const cust = db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada' }).executeSync();
cust.id;         // 1n
cust.tier;       // 'free'
cust.created_at; // '2026-…Z'
```

### Defaults and updates

`update` does **not** re-run insert defaults. Updating one column leaves `staticDefault`,
`uuidDefault`, `customDefault`, and `sequenceDefault` columns at their stored values:

```ts
db.updateTable(customers).set({ tier: 'pro' }).where(eq(customers.id, cust.id)).executeSync();
// tier -> 'pro'; id, public_id, created_at all keep their inserted values
```

The one exception is **`now`**: any column whose default is `nowDefault()` (or `generated: 'now'`)
is **refreshed to the current time on every update**, unless the patch sets it explicitly. This is
the `updated_at` pattern — but be aware it also applies to a `nowDefault()` column you named
`created_at`, so use a non-`now` strategy (e.g. set it once on insert and never default it) if you
need a value that must not move.

```ts
timestamp('updated_at', { generated: 'now' });   // refreshed on insert AND every update
```

## Auto-increment ids

`sequenceDefault(name)` gives a column SQL-style auto-increment. Make a column the primary key and
default it to a sequence:

```ts
int('id', { primaryKey: true, default: sequenceDefault('customers_id_seq') }),
```

Behavior, all verified against the engine:

- **1-based.** The first allocated id is `1`, never `0` — matching SQL `AUTO_INCREMENT` /
  `SERIAL`, so an assigned id is always truthy.
- **Backed by the `__kit_sequences` table.** Each named sequence stores its `next_value`; see
  [Internal tables](./internal-tables.md).
- **Allocated transactionally.** Allocation runs in its own committed transaction before the row is
  written, so concurrent inserts never hand out the same id. See [Transactions](./transactions.md)
  for the concurrency model.

```ts
const a = db.insertInto(customers).values({ email: 'a@x.com', name: 'A' }).executeSync();
const b = db.insertInto(customers).values({ email: 'b@x.com', name: 'B' }).executeSync();
a.id; // 1n
b.id; // 2n
```

### You may still supply an explicit id

A sequence column stays optional, not omitted — you can pass an `id` yourself (e.g. when importing
existing data). An **explicit id does not advance the sequence**, because allocation only happens for
omitted/`null` values:

```ts
db.insertInto(customers).values({ id: 1000n, email: 'imported@x.com', name: 'Imported' }).executeSync();
const next = db.insertInto(customers).values({ email: 'c@x.com', name: 'C' }).executeSync();
next.id; // 3n — the sequence kept counting from where it was, ignoring 1000n
```

Because explicit ids and the sequence counter are independent, a manual id can collide with a value
the sequence will later reach. If you mix the two, reserve a disjoint range (e.g. large ids for
imports) or pre-advance the sequence.

### Gaps are normal

Sequence allocation commits **before** row validation/constraint checks run, so a failed insert
(validation error, unique violation, missing foreign key) still consumes the id it reserved — exactly
like SQL `AUTO_INCREMENT`. Ids are unique and monotonic but **not** guaranteed gap-free; never assume
"id − 1" exists or that the count equals the max id.

## Notes

- Defaults fire for **omitted or `null`** values only; a non-null supplied value is never
  overwritten.
- Defaults run **before** validation; `generated`/`default` values are validated like any other.
- On update, only `now`/`generated: 'now'` columns are refreshed; other insert defaults are not
  reapplied.
- Auto-increment is **1-based**, transactional, may be overridden per row, and can leave gaps.

## See also

- [Schema DSL](./schema.md) — declaring `default` / `generated` on columns.
- [Types](./types.md) — why defaulted columns are optional on insert.
- [Internal tables](./internal-tables.md) — the `__kit_sequences` table that backs auto-increment.
- [Transactions](./transactions.md) — the concurrency model behind sequence allocation.
