# Types

Your [schema declarations](./schema.md) are also the source of truth for TypeScript types. Three
generic helpers derive everything from a `table(...)` value — no codegen, no duplicate type
definitions:

| Helper | Shape | Used for |
| --- | --- | --- |
| `Row<T>` | the full row as stored/returned | query results |
| `Insert<T>` | row input, defaulted/nullable columns optional | `insertInto(...).values(...)` |
| `Update<T>` | a partial of `Row<T>` | `updateTable(...).set(...)` |

```ts
import type { Row, Insert, Update } from '@mongreldb/kit';
import { customers } from './schema';

type Customer       = Row<typeof customers>;
type NewCustomer    = Insert<typeof customers>;
type CustomerPatch  = Update<typeof customers>;
```

Note the `typeof` — you pass the **value** returned by `table()`, not a named type. The query
builder is generic over the same `T`, so results and inputs are typed without annotations.

## `Row<T>`

`Row<T>` is the materialized row: one property per column, typed by the column's storage type, with
`| null` added for nullable columns.

```ts
type Customer = Row<typeof customers>;
// {
//   id: bigint;          // int  -> int64  -> bigint
//   email: string;       // text -> string
//   name: string;
//   tier: string;        // text + enumValues stays `string`
//   created_at: string;  // timestamp -> string (ISO 8601)
// }
```

The storage-type → TS mapping (see [Schema DSL](./schema.md#columns) for the full table):

| Storage | TS |
| --- | --- |
| `int64` | `bigint` |
| `float64` | `number` |
| `bool` | `boolean` |
| `text` / `timestamp` / `date` | `string` |
| `json` | `unknown` (stored as text — supply/parse a string yourself) |
| `bytes` | inferred `unknown`; runtime value is `Uint8Array` |

A nullable column widens to a union:

```ts
text('note', { nullable: true });   // Row: note: string | null
```

The biggest gotcha is **`int64` is `bigint`**: ids and counts are `1n`, not `1`. Compare with
`bigint` literals (`row.id === 1n`).

## `Insert<T>`

`Insert<T>` describes what `values(...)` accepts. The rule:

- **Required** — non-nullable columns **without** a default.
- **Optional** — nullable columns (omit to store `null`) **and** any column with a `default` or
  `generated` value, *including an auto-increment / sequence primary key*. Optional means it may be
  omitted **or** supplied explicitly, matching SQL semantics.

```ts
type NewCustomer = Insert<typeof customers>;
// {
//   email: string;                      // required (non-null, no default)
//   name: string;                       // required
//   id?: bigint | undefined;            // optional — sequence default supplies it
//   tier?: string | undefined;          // optional — staticDefault('free')
//   created_at?: string | undefined;    // optional — nowDefault()
// }

// minimal insert — id/tier/created_at all filled by defaults:
const cust = db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada' }).executeSync();
cust.id;   // 1n   (assigned by the sequence)
cust.tier; // 'free'

// supplying a defaulted column explicitly is allowed (it stays optional, not omitted):
db.insertInto(customers).values({ id: 1000n, email: 'g@example.com', name: 'Grace' }).executeSync();
```

A nullable column is optional too; omitting it stores `null`:

```ts
text('note', { nullable: true });   // Insert: note?: string | null | undefined
```

Omitting a **required** column (e.g. `email`) is a compile error, so missing-not-null mistakes are
caught before they reach the database.

## `Update<T>`

`Update<T>` is simply `Partial<Row<T>>` — every column optional, each keeping its `Row<T>` type
(nullable columns may be set to `null`). You set only the columns you want to change.

```ts
type CustomerPatch = Update<typeof customers>;
// { id?: bigint; email?: string; name?: string; tier?: string; created_at?: string; }

const rows = db.updateTable(customers)
  .set({ tier: 'pro' })                 // Update<typeof customers>
  .where(eq(customers.id, cust.id))
  .executeSync();                       // Row<typeof customers>[]
```

`Update<T>` does not encode insert defaults — those apply on insert only. See
[Defaults & sequences](./defaults.md) for exactly which defaults (if any) are touched on update.

## How the builder returns these types

The CRUD entry points are generic over the table, so results carry the inferred types end to end —
no casts needed:

```ts
db.insertInto(customers).values(/* Insert<typeof customers> */).executeSync(); // -> Row<typeof customers>
db.selectFrom(customers).where(/* … */).executeSync();                         // -> Row<typeof customers>[]
db.updateTable(customers).set(/* Update<typeof customers> */).executeSync();   // -> Row<typeof customers>[]
db.deleteFrom(customers).where(/* … */).executeSync();                         // -> bigint (rows deleted)
```

Because `Row<T>` is derived from the same declaration you query against, renaming or retyping a
column updates inputs, results, and predicates together — a mismatch is a compile error.

## Notes

- **`bigint` everywhere for `int64`.** Insert with `quantity: 2n`, filter with `gt(orders.id, 0n)`,
  read back `bigint`. Mixing `number` and `bigint` is a TypeScript error and a runtime footgun.
- **`json` infers `unknown`** and is stored as text — pass a `JSON.stringify(...)` string and
  `JSON.parse` on read.
- **`bytes` infers `unknown`** but is a `Uint8Array` at runtime; cast on read (`row.data as
  Uint8Array`).
- **Generic helpers stay open.** `function fn<T extends TableSpec>(row: Row<T>)` works for code that
  is agnostic to a specific table.

## See also

- [Schema DSL](./schema.md) — the declarations these types are inferred from.
- [Defaults & sequences](./defaults.md) — why defaulted columns are optional on insert.
- [Query builder](./query-builder.md) — the typed CRUD surface.
- [Constraints](./constraints.md) — runtime validation that complements the static types.
