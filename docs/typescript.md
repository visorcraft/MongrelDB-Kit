# TypeScript Quickstart

This guide shows how to define a schema, run migrations, and perform CRUD with `@mongreldb/kit`.

## Installation

```sh
npm install @mongreldb/kit mongreldb
```

`mongreldb` is a peer dependency providing the native database bindings.

## Complete example

```ts
import {
  KitDatabase,
  Schema,
  table,
  int,
  text,
  bool,
  foreignKey,
  check,
  index,
  staticDefault,
  sequenceDefault,
  eq,
  desc
} from '@mongreldb/kit';

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const users = table('users', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('users_id_seq') }),
    text('email'),
    text('name', { nullable: true })
  ],
  primaryKey: 'id',
  indexes: [index(['email'], { unique: true, name: 'uq_user_email' })]
});

const posts = table('posts', {
  columns: [
    int('id', { primaryKey: true, default: sequenceDefault('posts_id_seq') }),
    int('user_id'),
    text('title'),
    text('body', { nullable: true }),
    bool('published', { default: staticDefault(false) }),
    text('created_at', { generated: 'now' })
  ],
  primaryKey: 'id',
  foreignKeys: [
    foreignKey(['user_id'], { table: 'users', columns: ['id'] }, { onDelete: 'cascade' })
  ],
  checks: [check('title_not_empty', (row) => (row.title as string).length > 0 || 'title must not be empty')]
});

const schema = new Schema([users, posts]);

// ---------------------------------------------------------------------------
// Open or create the database and run migrations
// ---------------------------------------------------------------------------

const db = KitDatabase.openSync('./app.kitdb', schema);

db.migrateSync(schema, [
  {
    version: 1,
    name: 'initial',
    up({ ensureTable }) {
      ensureTable(users);
      ensureTable(posts);
    }
  }
]);

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

const alice = db.insertInto(users).values({ email: 'alice@example.com', name: 'Alice' }).executeSync();
const bob = db.insertInto(users).values({ email: 'bob@example.com' }).executeSync();

const post = db.insertInto(posts)
  .values({ user_id: alice.id, title: 'Hello Kit', body: 'First post.' })
  .executeSync();

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

const publishedPosts = db
  .selectFrom(posts)
  .where(eq(posts.published, false))
  .orderBy(desc(posts.created_at))
  .limit(10)
  .executeSync();

const titles = db.selectFrom(posts).select([posts.title]).executeSync();

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

db.updateTable(posts)
  .set({ published: true })
  .where(eq(posts.id, post.id))
  .executeSync();

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

// Deleting Alice cascades to her posts because of the FK onDelete action.
const deleted = db.deleteFrom(users).where(eq(users.id, alice.id)).executeSync();
console.log('deleted users:', deleted);

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

db.close();
```

## Column helpers

- `int(name, opts?)`
- `text(name, opts?)`
- `real(name, opts?)` — `float64`
- `bool(name, opts?)`
- `timestamp(name, opts?)`
- `date(name, opts?)`
- `json(name, opts?)`
- `blob(name, opts?)` — bytes

## Column options

| Option | Effect |
|---|---|
| `nullable?: boolean` | Allow `null` values |
| `primaryKey?: boolean` | Mark as part of the primary key |
| `default?: DefaultValue` | Static, now, UUID, sequence, or custom default |
| `generated?: 'uuid' \| 'now'` | Auto-generate on insert/update |
| `enumValues?: string[]` | Restrict string values |
| `min?: number`, `max?: number` | Numeric range |
| `minLength?: number`, `maxLength?: number` | String/bytes length |
| `regex?: RegExp` | Pattern match |
| `check?: (value) => boolean \| string` | Per-column custom check |

## Query builder

Select:
```ts
db.selectFrom(table)
  .where(predicate)
  .orderBy(asc(column), desc(column2))
  .limit(n)
  .offset(n)
  .select([col1, col2])
  .executeSync();
```

Insert:
```ts
db.insertInto(table).values({ ... }).executeSync();
```

Update:
```ts
db.updateTable(table).set({ ... }).where(predicate).executeSync();
```

Delete:
```ts
db.deleteFrom(table).where(predicate).executeSync();
```

## Predicates

- `eq(column, value)`
- `ne(column, value)`
- `gt(column, value)`, `gte(column, value)`, `lt(column, value)`, `lte(column, value)`
- `isNull(column)`, `isNotNull(column)`
- `inList(column, values)`
- `and(...predicates)`, `or(...predicates)`

## Migrations

Call `db.migrateSync(schema, migrations)` to apply pending migrations in version order. The runner acquires an advisory lock, records each migration in `__kit_schema_migrations`, and updates `__kit_schema_catalog`.

## Error handling

Catch typed errors by name:

```ts
import { KitDuplicateError, KitForeignKeyError, KitRestrictError, KitValidationError } from '@mongreldb/kit';

try {
  db.insertInto(users).values({ email: 'alice@example.com' }).executeSync();
} catch (err) {
  if (err instanceof KitDuplicateError) {
    console.error('duplicate email');
  }
}
```

## Running this example

Save the file as `kit-demo.ts` and run it with Node 22+:

```sh
npx tsx kit-demo.ts
```

The first run creates `./app.kitdb`. Subsequent runs open the existing database.
