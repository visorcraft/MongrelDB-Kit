# Errors

Every failure the Kit raises is a typed subclass of `KitError`, each carrying a **stable
machine-readable `code`** and, where it helps, structured fields (`table`, `column`,
`constraint`, `pk`, `retryable`). You can branch on `instanceof` for the rich subtypes or on
`.code` for a language-neutral category — the codes are identical across TypeScript, Rust, and
Python.

```ts
import { KitDuplicateError } from '@visorcraft/mongreldb-kit';

try {
  db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada' }).executeSync();
} catch (err) {
  if (err instanceof KitDuplicateError) {
    // err.code === 'DUPLICATE', err.table === 'customers', err.constraint === '<unique name>'
  }
}
```

## Taxonomy at a glance

| Class | `code` | Extra fields | Thrown when |
| --- | --- | --- | --- |
| `KitError` (base) | `'STORAGE'` (default) | — | Storage failures and unsupported query shapes (see notes). |
| `KitValidationError` | `'VALIDATION'` | `table?`, `column?` | A value fails not-null / type / enum / range / length / regex / `check`. |
| `KitNotFoundError` | `'NOT_FOUND'` | `table`, `pk` | A delete targets a primary key that does not exist. |
| `KitDuplicateError` | `'DUPLICATE'` | `table`, `constraint` | An insert/update would violate a unique or composite-unique constraint (or a composite PK). |
| `KitForeignKeyError` | `'FOREIGN_KEY'` | `table`, `constraint` | An insert/update references a parent row that does not exist. |
| `KitRestrictError` | `'RESTRICT'` | `table`, `constraint` | A delete is blocked by child rows under an `onDelete: 'restrict'` foreign key. |
| `KitConflictError` | `'CONFLICT'` | `retryable === true` | A write-write conflict; safe to retry the whole transaction. |
| `KitTriggerValidationError` | `'TRIGGER_VALIDATION'` | — | An engine trigger rejected the write or raised a validation failure. |
| `KitMigrationError` | `'MIGRATION'` | — | A migration fails, is malformed, or the migration lock is already held. |
| `KitSchemaDriftError` | `'SCHEMA_DRIFT'` | — | An already-applied migration was edited, renamed, or removed. |
| `KitTimeoutError` | `'TIMEOUT'` | — | A transaction exceeded its time budget. |
| `KitUnsupportedError` | `'UNSUPPORTED'` | — | An operation is not supported. |

> The `code` union also reserves `'INTEGRITY'` for storage-corruption / invariant failures. It has
> no dedicated TypeScript subclass (it surfaces as a base `KitError`), but it exists as a first-class
> `IntegrityError` in the Rust and Python bindings — see [Cross-language mapping](#cross-language-mapping).

All subclasses extend `KitError`, which extends the built-in `Error`, so `err.name`, `err.message`,
and stack traces work as usual and a single `catch (err) { if (err instanceof KitError) … }` catches
every Kit failure.

## `KitError` (base)

```ts
class KitError extends Error {
  readonly code: KitErrorCode; // defaults to 'STORAGE'
}
```

The base class is what you get for low-level storage failures and for a handful of **unsupported
query shapes** the in-memory query engine rejects rather than silently mis-evaluating. These are
programming errors, not data errors, and are not meant to be branched on individually:

```ts
// Examples of messages thrown as a base KitError (code 'STORAGE'):
//   "Full table scan on \"orders\" requires an int64, float64, or primary key"
//   "An IN subquery must select exactly one column"
//   "values() must be called before execute()"
```

Catch `KitError` as your outermost net; reach for the subclasses below for everything you expect to
handle.

## `KitValidationError` — `'VALIDATION'`

Thrown during insert and update **before** anything is written, when a row fails column or table
validation: not-null, wrong runtime type, enum membership, `min`/`max`, `minLength`/`maxLength`,
`regex`, a column-level `check`, or a table-level `check`.

- `table` — the table being written.
- `column` — the offending column for **column-level** failures; `undefined` for **table-level**
  `check` failures (which span multiple columns).

```ts
import { KitValidationError } from '@visorcraft/mongreldb-kit';

try {
  // `name` is non-nullable with no default.
  db.insertInto(customers).values({ email: 'ada@example.com', name: null as any }).executeSync();
} catch (err) {
  if (err instanceof KitValidationError) {
    console.error(err.code);   // 'VALIDATION'
    console.error(err.table);  // 'customers'
    console.error(err.column); // 'name'
  }
}
```

A failing `check` predicate that returns a string uses that string as the message; returning `false`
falls back to a generated message. See [Constraints](./constraints.md) for the predicate contract.

## `KitNotFoundError` — `'NOT_FOUND'`

Thrown by the cascade/delete planner when a `deleteFrom(...)` resolves to a primary key that has no
row. Reads never throw this — `selectFrom(...)` returns `[]` and a missing-row update returns `[]`.

- `table` — the table the missing row belongs to.
- `pk` — the primary-key value that was not found (`unknown`; a `bigint`, string, or composite array).

```ts
import { KitNotFoundError } from '@visorcraft/mongreldb-kit';

try {
  db.deleteFrom(customers).where(eq(customers.id, 999n)).executeSync();
} catch (err) {
  if (err instanceof KitNotFoundError) {
    console.error(err.table); // 'customers'
    console.error(err.pk);    // 999n
  }
}
```

## `KitDuplicateError` — `'DUPLICATE'`

Thrown on insert/update when a row would collide on a `unique(...)` / composite-unique constraint,
or on a composite primary key. (Nullable unique columns are exempt: a `null` in any unique column
skips the guard, matching SQL `NULL` semantics.)

- `table` — the table being written.
- `constraint` — the unique constraint name. Composite-primary-key collisions report a synthetic
  name of the form `__pk_<table>`.

```ts
import { KitDuplicateError } from '@visorcraft/mongreldb-kit';

db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada' }).executeSync();
try {
  db.insertInto(customers).values({ email: 'ada@example.com', name: 'Ada II' }).executeSync();
} catch (err) {
  if (err instanceof KitDuplicateError) {
    console.error(err.table, err.constraint); // 'customers' '<email unique name>'
  }
}
```

## `KitForeignKeyError` — `'FOREIGN_KEY'`

Thrown on insert/update when the row's foreign-key columns point at a parent that does not exist.
A `null` in any FK column skips the check (an optional reference).

- `table` — the **child** table being written.
- `constraint` — the foreign-key name.

```ts
import { KitForeignKeyError } from '@visorcraft/mongreldb-kit';

try {
  db.insertInto(orders).values({ customer_id: 999n }).executeSync(); // no such customer
} catch (err) {
  if (err instanceof KitForeignKeyError) {
    console.error(err.table, err.constraint); // 'orders' 'orders_customer_id_fk'
  }
}
```

## `KitRestrictError` — `'RESTRICT'`

Thrown by `deleteFrom(...)` when the targeted parent still has child rows under a foreign key whose
`onDelete` is `'restrict'` (the default delete action). Compare with `'cascade'` (children deleted)
and `'set null'` (child FK columns nulled), which never raise this.

- `table` — the **child** table that still holds references (not the table you asked to delete from).
- `constraint` — the blocking foreign-key name.

```ts
import { KitRestrictError } from '@visorcraft/mongreldb-kit';

// order_items.product_id references products with onDelete: 'restrict'.
try {
  db.deleteFrom(products).where(eq(products.id, p.id)).executeSync();
} catch (err) {
  if (err instanceof KitRestrictError) {
    console.error(err.table);      // 'order_items' (the child holding the reference)
    console.error(err.constraint); // the FK name on order_items
  }
}
```

## `KitConflictError` — `'CONFLICT'` (retryable)

Represents a write-write conflict — two transactions racing on the same row, unique guard, or
sequence. It is the **only** error flagged `retryable === true`; retrying the whole transaction is
the correct response.

```ts
class KitConflictError extends KitError {
  retryable = true; // code === 'CONFLICT'
}
```

In practice you rarely construct or catch this yourself: the Kit's transaction helpers retry
conflicts automatically (bounded retries with backoff). Native MongrelDB commit conflicts arrive as
ordinary `Error`s whose message begins with `__CONFLICT__:` rather than as a `KitConflictError`, so
detect either form with the exported helper:

```ts
import { isRetryableConflict } from '@visorcraft/mongreldb-kit';

isRetryableConflict(err); // true for KitConflictError and for native "__CONFLICT__: …" messages
```

If you drive transactions manually, gate your own retry loop on `isRetryableConflict`. See
[Transactions](./transactions.md).

## `KitTriggerValidationError` — `'TRIGGER_VALIDATION'`

Thrown when an engine-side trigger rejects a write, for example through a trigger
`raise` step or a trigger validation failure. Treat it like a data validation
failure: the write did not commit, and retrying with the same row will fail the
same way.

```ts
import { KitTriggerValidationError } from '@visorcraft/mongreldb-kit';

try {
  db.insertInto(users).values(row).executeSync();
} catch (err) {
  if (err instanceof KitTriggerValidationError) {
    console.error(err.code); // 'TRIGGER_VALIDATION'
  }
}
```

## `KitMigrationError` — `'MIGRATION'`

Thrown by the migration runner when a migration body throws, when a migration is malformed (e.g. a
`sql()` step in a synchronous migration, an async `up()` under `migrateSync`, an unsupported column
default, a duplicate column/index/FK, or a missing table), or when the advisory **migration lock is
already held** by a concurrent run.

```ts
import { KitMigrationError } from '@visorcraft/mongreldb-kit';

try {
  db.migrateSync(schema, migrations);
} catch (err) {
  if (err instanceof KitMigrationError) {
    console.error('migration failed:', err.message);
  }
}
```

See [Migrations](./migrations.md) for the lifecycle and the supported operations.

## `KitSchemaDriftError` — `'SCHEMA_DRIFT'`

A specialized migration error: thrown when an **already-applied** migration no longer matches the
list you supplied — its content checksum changed, it was renamed, or it was deleted entirely.
Editing committed history would silently change what a recorded migration meant, so the runner
refuses to proceed.

```ts
import { KitSchemaDriftError } from '@visorcraft/mongreldb-kit';

try {
  db.migrateSync(schema, migrations);
} catch (err) {
  if (err instanceof KitSchemaDriftError) {
    // A migration recorded as applied was edited/renamed/removed. Restore it; add a new one instead.
  }
}
```

`KitSchemaDriftError` is **not** a subclass of `KitMigrationError` — both extend `KitError`
directly — so catch them separately if you want distinct handling.

## `KitTimeoutError` — `'TIMEOUT'`

Reserved for a transaction that exceeds its time budget (`code === 'TIMEOUT'`, default message
`"Transaction timed out"`). Treat it like a transient failure: roll back and, if appropriate, retry
with a fresh transaction.

## `KitUnsupportedError` — `'UNSUPPORTED'`

Marks an operation that is not supported (`code === 'UNSUPPORTED'`). It is part of the public
taxonomy for forward compatibility; today the TypeScript query builder reports unsupported query
shapes as a base `KitError` (see [`KitError`](#kiterror-base)) rather than this subclass, so do not
rely on `instanceof KitUnsupportedError` to catch them.

## Handling patterns

### Distinguish duplicate vs foreign-key on insert

Both can fail the same `insertInto(...)`; branch on the subtype:

```ts
import { KitDuplicateError, KitForeignKeyError } from '@visorcraft/mongreldb-kit';

function placeOrder(customerId: bigint) {
  try {
    return db.insertInto(orders).values({ customer_id: customerId }).executeSync();
  } catch (err) {
    if (err instanceof KitForeignKeyError) throw new Error('unknown customer');
    if (err instanceof KitDuplicateError) throw new Error('duplicate order');
    throw err; // rethrow anything you do not specifically handle
  }
}
```

### Map a category to an HTTP status

Branch on the stable `.code` instead of the class when you only need the category:

```ts
import { KitError } from '@visorcraft/mongreldb-kit';

function statusFor(err: unknown): number {
  if (!(err instanceof KitError)) return 500;
  switch (err.code) {
    case 'VALIDATION':  return 400;
    case 'TRIGGER_VALIDATION': return 400;
    case 'NOT_FOUND':   return 404;
    case 'DUPLICATE':
    case 'FOREIGN_KEY':
    case 'RESTRICT':    return 409;
    case 'CONFLICT':    return 409; // typically retried before reaching here
    default:            return 500;
  }
}
```

### Retry on conflict

```ts
import { isRetryableConflict } from '@visorcraft/mongreldb-kit';

function withRetry<T>(work: () => T, attempts = 5): T {
  for (let i = 0; ; i++) {
    try {
      return work();
    } catch (err) {
      if (i < attempts && isRetryableConflict(err)) continue;
      throw err;
    }
  }
}
```

The built-in transaction helpers already do this; reach for a manual loop only when you compose
several statements yourself.

## Cross-language mapping

The Kit guarantees one stable error *category* across languages; the surface shape differs:

| TypeScript class / `code` | Rust `KitError` variant | Python exception (`.code`) |
| --- | --- | --- |
| `KitValidationError` / `VALIDATION` | `KitError::Validation` | `ValidationError` (`VALIDATION`) |
| `KitDuplicateError` / `DUPLICATE` | `KitError::Duplicate` | `DuplicateError` (`DUPLICATE`) |
| `KitForeignKeyError` / `FOREIGN_KEY` | `KitError::ForeignKey` | `ForeignKeyError` (`FOREIGN_KEY`) |
| `KitRestrictError` / `RESTRICT` | `KitError::Restrict` | `RestrictError` (`RESTRICT`) |
| `KitMigrationError` / `MIGRATION` | `KitError::Migration` | `MigrationError` (`MIGRATION`) |
| `KitConflictError` / `CONFLICT` | `KitError::Conflict` | `ConflictError` (`CONFLICT`) |
| `KitTriggerValidationError` / `TRIGGER_VALIDATION` | `KitError::TriggerValidation` | `TriggerValidationError` (`TRIGGER_VALIDATION`) |
| base `KitError` / `STORAGE` | `KitError::Storage` | `StorageError` (`STORAGE`) |
| base `KitError` / `INTEGRITY` | `KitError::Integrity` | `IntegrityError` (`INTEGRITY`) |

Rust and Python expose **nine** categories. TypeScript adds four finer-grained subtypes —
`KitNotFoundError`, `KitSchemaDriftError`, `KitTimeoutError`, and `KitUnsupportedError` — that the
other two fold into the nearest shared category (a missing row surfaces as an integrity/storage
error; schema drift as a migration error; and so on). Code that switches only on the nine shared
codes behaves identically in every language.

## Notes

- `instanceof KitError` catches every Kit failure; the subclasses are for the cases you act on.
- `.code` strings are stable API — safe to log, serialize, and switch on. Class names and `.message`
  text are not contractual.
- Validation runs **before** the write, so a thrown `KitValidationError`/`KitDuplicateError`/
  `KitForeignKeyError`/`KitTriggerValidationError` leaves the database unchanged; the surrounding
  transaction is rolled back.
- `KitConflictError` is the only retryable error. Do not retry validation, duplicate, foreign-key,
  restrict, trigger validation, or migration errors — they will fail again with the same input.

## See also

- [Constraints](./constraints.md) — the rules that raise `Duplicate`, `ForeignKey`, `Restrict`, and `check`-based `Validation` errors.
- [Triggers](./triggers.md) — engine-side triggers and `TRIGGER_VALIDATION`.
- [Transactions](./transactions.md) — conflict handling and the retrying transaction helpers.
- [Migrations](./migrations.md) — what raises `Migration` and `SchemaDrift` errors.
- [Query builder](./query-builder.md) — the CRUD calls that surface these errors.
- [TypeScript](./typescript.md) · [Rust](./rust.md) · [Python](./python.md) — per-language error surfaces.
