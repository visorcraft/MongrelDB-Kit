# Defaults & sequences

A **default** supplies a column's value when you omit it on insert. The Kit ships five default kinds
plus a `generated` shorthand, and a sequence-backed default that powers **auto-increment ids**. All
of them are declared in the column's [options](./schema.md#columnoptions):

```ts
import {
  int, text, timestamp,
  staticDefault, nowDefault, uuidDefault, sequenceDefault, customDefault,
} from '@visorcraft/mongreldb-kit';
```

## The default kinds

Each helper returns a `DefaultValue` you pass as the column's `default` option.

| Helper | Fills the column with |
| --- | --- |
| `staticDefault(value)` | a constant `value` (any type matching the column) |
| `nowDefault()` | the current time - ISO 8601 for `timestamp`, `YYYY-MM-DD` for `date` |
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

### Static defaults and the `default_value` / `default_expr` distinction

`staticDefault(value)` stores a literal default directly in the engine schema:

| Declared default | Engine representation on insert |
| --- | --- |
| `staticDefault('draft')` | `defaultValue.text === 'draft'` |
| `staticDefault(7)` | `defaultValue.int64 === 7n` |
| `staticDefault(true)` | `defaultValue.boolean === true` |
| `staticDefault(null)` | `defaultValue` present with only `columnId` (no typed value field) |
| `staticDefault('now')` | `defaultValue.text === 'now'` — a literal string, not dynamic |

Dynamic defaults, by contrast, are represented as `defaultExpr` and evaluated at
insert time:

| Declared default | Engine representation |
| --- | --- |
| `nowDefault()` | `defaultExpr === 'now'`, no `defaultValue` |
| `uuidDefault()` | `defaultExpr === 'uuid'`, no `defaultValue` |
| `generated: 'now'` | same as `default: nowDefault()` |
| `generated: 'uuid'` | same as `default: uuidDefault()` |

If you need the literal string `"now"` or `"uuid"` as a stored default, use
`staticDefault('now')` or `staticDefault('uuid')`. The engine distinguishes the
literal from the dynamic expression by the presence of `defaultValue` versus
`defaultExpr`.

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
2. **Then validation runs** on the completed row - not-null, enum, bounds, checks. So a default that
   produces an invalid value still fails validation, and a non-nullable column with no default that
   you omitted is rejected.

Passing `null` explicitly for a defaulted column triggers the default just like omitting it - there
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
the `updated_at` pattern - but be aware it also applies to a `nowDefault()` column you named
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

- **1-based.** The first allocated id is `1`, never `0` - matching SQL `AUTO_INCREMENT` /
  `SERIAL`, so an assigned id is always truthy.
- **Engine-backed in TypeScript.** `sequenceDefault(...)` maps to MongrelDB's native
  `AUTO_INCREMENT` counter for the table. Current TypeScript databases do not allocate through a
  `__kit_sequences` hot row; older TypeScript databases are seeded from that legacy table during
  migration so upgraded counters do not move backward. The string name is retained in schema
  metadata and for legacy upgrade mapping, but the live TypeScript counter is per table.
- **Named sequence table in Rust/Python.** The Rust storage crate and Python facade still expose
  explicit named sequences through `allocate_sequence(...)`, backed by `__kit_sequences`.
- **Never reused.** Concurrent inserts never receive the same id. Failed inserts can leave gaps;
  see [Transactions](./transactions.md) for the concurrency model.

```ts
const a = db.insertInto(customers).values({ email: 'a@x.com', name: 'A' }).executeSync();
const b = db.insertInto(customers).values({ email: 'b@x.com', name: 'B' }).executeSync();
a.id; // 1n
b.id; // 2n
```

### You may still supply an explicit id

A sequence column stays optional, not omitted - you can pass an `id` yourself (e.g. when importing
existing data).

```ts
db.insertInto(customers).values({ id: 1000n, email: 'imported@x.com', name: 'Imported' }).executeSync();
const next = db.insertInto(customers).values({ email: 'c@x.com', name: 'C' }).executeSync();
next.id; // 1001n in TypeScript - the native counter advances past explicit ids
```

In TypeScript, the engine owns the counter and advances it past explicit integer primary keys, so a
future auto-assigned id will not collide with the imported value. In Rust/Python, named
`__kit_sequences` allocation is independent from manually supplied ids; if you mix manual ids with
`DefaultKind::Sequence` / `{"sequence": ...}`, reserve a disjoint range or pre-advance the named
sequence.

### Gaps are normal

Sequence reservation happens before the row is known to be durable. In TypeScript the engine-native
counter may advance for an attempted insert that later fails validation or constraints; in
Rust/Python the named-sequence allocation commits separately from the row write. Either way, a
failed insert can consume an id - exactly like SQL `AUTO_INCREMENT`. Ids are unique and monotonic
but **not** guaranteed gap-free; never assume "id - 1" exists or that the count equals the max id.

## Notes

- Defaults fire for **omitted or `null`** values only; a non-null supplied value is never
  overwritten.
- Defaults run **before** validation; `generated`/`default` values are validated like any other.
- On update, only `now`/`generated: 'now'` columns are refreshed; other insert defaults are not
  reapplied.
- Auto-increment is **1-based**, may be overridden per row, and can leave gaps.

## See also

- [Schema DSL](./schema.md) - declaring `default` / `generated` on columns.
- [Types](./types.md) - why defaulted columns are optional on insert.
- [Internal tables](./internal-tables.md) - reserved tables, including legacy/named sequence storage.
- [Transactions](./transactions.md) - the concurrency model behind sequence allocation.
