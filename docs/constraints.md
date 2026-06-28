# Constraints

MongrelDB Kit enforces relational constraints across TypeScript, Rust, and Python using internal guard tables.

## Unique constraints

Unique constraints are stored in `__kit_unique_keys`. On insert or update the kit computes an encoded key for each unique constraint and rejects the write if another row already owns it.

### Single-column unique

```ts
const users = table('users', {
  columns: [int('id', { primaryKey: true }), text('email')],
  primaryKey: 'id',
  indexes: [index(['email'], { unique: true, name: 'uq_user_email' })]
});
```

### Composite unique

```ts
const memberships = table('memberships', {
  columns: [int('user_id'), int('group_id')],
  primaryKey: ['user_id', 'group_id'],
  indexes: [index(['user_id', 'group_id'], { unique: true, name: 'uq_membership' })]
});
```

### Nullable unique semantics

Rows with a `null` component in a unique constraint do not consume a guard key. This means multiple rows may have `null` in a nullable unique column without conflict.

## Primary keys

Primary keys are implicitly unique. Composite primary keys are enforced through the same guard mechanism as explicit unique constraints.

## Foreign keys

Foreign keys guarantee that child references point to existing parent rows. The kit also enforces delete actions when the parent is removed.

### Required foreign key

```ts
foreignKey(['user_id'], { table: 'users', columns: ['id'] })
```

A child insert fails if `user_id` does not exist in `users`.

### Nullable foreign key

```ts
text('user_id', { nullable: true })
foreignKey(['user_id'], { table: 'users', columns: ['id'] })
```

A null `user_id` skips the parent check.

### Composite foreign key

```ts
foreignKey(['a_id', 'b_id'], { table: 'parents', columns: ['a', 'b'] })
```

## Delete actions

| Action | Behavior |
|---|---|
| `restrict` (default) | Reject the parent delete if children exist |
| `cascade` | Delete child rows recursively |
| `set null` | Set child foreign-key columns to `null` (columns must be nullable) |
| `no action` | Treated as `restrict` unless configured otherwise |

### Restrict

```ts
foreignKey(['user_id'], { table: 'users', columns: ['id'] }, { onDelete: 'restrict' })
```

Deleting a user with orders raises `KitRestrictError`.

### Cascade

```ts
foreignKey(['order_id'], { table: 'orders', columns: ['id'] }, { onDelete: 'cascade' })
```

Deleting an order deletes its line items.

### Set null

```ts
text('manager_id', { nullable: true })
foreignKey(['manager_id'], { table: 'users', columns: ['id'] }, { onDelete: 'set null' })
```

Deleting a manager leaves the team rows in place but clears `manager_id`.

## Check constraints

Table-level checks run after defaults and validation.

```ts
const users = table('users', {
  columns: [int('id'), text('email')],
  primaryKey: 'id',
  checks: [
    check('email_has_at', (row) =>
      (row.email as string).includes('@') || 'email must contain @'
    )
  ]
});
```

Column-level checks are also supported:

```ts
text('handle', {
  check: (value) => (value as string).length >= 3 || 'handle too short'
})
```

## Validation rules

The kit validates every row before insert and update:

- Not-null constraints
- Type compatibility
- Enum membership
- Numeric `min` / `max`
- String/bytes `min_length` / `max_length`
- Regex pattern match
- JSON parseability for JSON columns
- Custom check functions

Validation failures raise `KitValidationError` with `table` and `column` set.

## Cross-language consistency

Constraint behavior is tested by the shared conformance suite. The same fixtures exercise inserts, updates, deletes, unique violations, foreign-key violations, and cascade/set-null/restrict actions in TypeScript, Rust, and Python.
