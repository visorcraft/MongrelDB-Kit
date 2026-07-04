# Triggers

MongrelDB Kit exposes MongrelDB's engine-side trigger registry without adding a
separate Kit-only trigger language. A trigger is a declarative JSON spec stored
by the engine, so it fires for writes that come through Kit, native clients, SQL,
or the daemon.

Use triggers for database-owned behavior that must stay true regardless of which
client wrote the row: audit rows, lightweight denormalization, and custom
validation that belongs beside the data. Keep application workflows in
application code when they need network calls or side effects outside the
database.

## TypeScript

```ts
import {
  KitDatabase,
  Schema,
  int,
  newColumn,
  table,
  text,
  textValue,
  trigger,
} from '@visorcraft/mongreldb-kit';

const users = table('users', {
  columns: [int('id', { primaryKey: true }), text('name')],
  primaryKey: 'id',
});

const audit = table('audit', {
  columns: [int('id', { primaryKey: true }), int('user_id'), text('note')],
  primaryKey: 'id',
});

const usersAudit = trigger({
  name: 'users_ai',
  target: { kind: 'table', name: 'users' },
  timing: 'after',
  event: 'insert',
  program: {
    steps: [{
      kind: 'insert',
      table: 'audit',
      cells: [
        { column_id: audit.id.id, value: newColumn(users.id.id) },
        { column_id: audit.user_id.id, value: newColumn(users.id.id) },
        { column_id: audit.note.id, value: textValue('created') },
      ],
    }],
  },
});

const db = KitDatabase.openSync('./data', new Schema([users, audit]));
db.createTriggerSync(usersAudit);
db.insertInto(users).values({ id: 1n, name: 'Ada' }).executeSync();
```

`trigger(...)` fills the engine defaults (`version`, enabled state, empty
`update_of`, empty `target_columns`, and zero epochs). Helper constructors such
as `newColumn`, `oldColumn`, `textValue`, `int64Value`, and `nullValue` keep the
value shape readable.

The embedded TypeScript API is synchronous for trigger registry changes:

```ts
db.createTriggerSync(spec);
db.createOrReplaceTriggerSync(spec);
db.dropTriggerSync('users_ai');
db.triggers();
db.trigger('users_ai');
```

Remote clients use the same spec:

```ts
remote.createTrigger(spec);
remote.replaceTrigger('users_ai', spec);
remote.dropTrigger('users_ai');
remote.triggers();
```

## Migrations

In TypeScript migrations, use the context helpers and include a matching `ops`
entry when you want the trigger definition to affect drift checks:

```ts
db.migrateSync(schema, [{
  version: 2,
  name: 'add users audit trigger',
  ops: [{ kind: 'createTrigger', name: 'users_ai', trigger: usersAudit }],
  up(ctx) {
    ctx.createTrigger(usersAudit);
  },
}]);
```

Rust and CLI migrations execute trigger ops directly:

```json
[
  {
    "version": 2,
    "name": "add_users_audit_trigger",
    "ops": [
      { "create_trigger": { "name": "users_ai", "trigger": { "name": "users_ai" } } }
    ]
  }
]
```

The JSON above is abbreviated; use the full trigger spec you would pass to the
engine.

## Rust

Rust keeps the trigger spec as JSON so it can track the engine schema exactly:

```rust
use mongreldb_kit::{Database, TriggerSpec};
use serde_json::json;

let spec = TriggerSpec::new(json!({
    "name": "users_ai",
    "target": { "kind": "table", "name": "users" },
    "timing": "after",
    "event": "insert",
    "program": { "steps": [] }
}));

db.create_trigger(&spec)?;
db.replace_trigger(&spec)?;
db.drop_trigger("users_ai")?;
let all = db.triggers();
let one = db.trigger("users_ai");
```

## Python

Python accepts either a dict or a JSON string:

```python
db.create_trigger({
    "name": "users_ai",
    "target": {"kind": "table", "name": "users"},
    "timing": "after",
    "event": "insert",
    "program": {"steps": []},
})

db.replace_trigger({...})
db.drop_trigger("users_ai")
db.triggers()
db.trigger("users_ai")
```

## Errors

When a trigger raises or rejects a write, clients receive the stable
`TRIGGER_VALIDATION` category:

| Language | Error |
| --- | --- |
| TypeScript | `KitTriggerValidationError` |
| Rust | `KitError::TriggerValidation` |
| Python | `TriggerValidationError` |

