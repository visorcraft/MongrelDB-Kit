# Stored Procedures

MongrelDB Kit consumes MongrelDB stored procedures through the same declarative JSON procedure
spec used by the engine. TypeScript, Rust, Python, CLI, and remote clients all pass the same shape.

## TypeScript

```ts
import { procedure } from '@visorcraft/mongreldb-kit';

const readUsers = procedure({
  name: 'read_users',
  mode: 'read_only',
  body: {
    steps: [{ kind: 'native_query', id: 'read', table: 'users', conditions: [], projection: [1, 2] }],
    return_value: { kind: 'step_rows', value: 'read' }
  }
});

db.createProcedureSync(readUsers);
const result = db.callProcedureSync('read_users');
```

## Rust

Use `mongreldb_kit_core::ProcedureSpec` and pass it to `Database::create_procedure`,
`replace_procedure`, `drop_procedure`, or `call_procedure`.

## Python

`Database.create_procedure`, `replace_procedure`, `drop_procedure`, and `call_procedure` accept
plain dicts or JSON strings.

## CLI

```sh
mongreldb-kit procedure install ./data ./procedure.json
mongreldb-kit procedure call ./data read_users --args '{"status":"active"}'
mongreldb-kit procedure list ./data
mongreldb-kit procedure drop ./data read_users
```

Procedure migration ops are included in content-aware migration checksums:
`createProcedure`, `replaceProcedure`, and `dropProcedure`.
