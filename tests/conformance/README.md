# MongrelDB Kit Conformance Suite

This directory contains a shared set of JSON fixtures and language-specific
runners that exercise the same database behaviors across the TypeScript, Rust,
and Python implementations of MongrelDB Kit.

## Fixtures

All fixtures live under `fixtures/` and are consumed by every runner:

- `schema.json` — `users`, `posts`, and `comments` tables covering `int64`,
  `text`, `bool`, and `json` types, nullable columns, unique constraints,
  check constraints, and foreign-key actions (`cascade`, `set_null`, `restrict`).
- `migrations.json` — initial migration that creates the three tables.
- `inserts.json` — rows to insert, including valid rows and rows that should
  fail validation or foreign-key checks.
- `updates.json` — patches and expected outcomes, including error cases.
- `deletes.json` — delete scenarios for cascade, set-null, and restrict.
- `queries.json` — filter, order, limit, offset, column projection, and count
  cases.
- `expected/` — expected result for every named scenario.

## Runners

### TypeScript

```sh
rtk npm ci
rtk npm test
```

The conformance test is included automatically from `packages/kit/vitest.config.ts`.

### Rust

```sh
rtk cargo test --workspace
```

The runner is the `conformance-runner` workspace member.  You can also run it
as a standalone binary:

```sh
rtk cargo run --bin conformance-runner
```

### Python

From the repo root:

```sh
cd tests/conformance/python
python3 -m venv .venv
.venv/bin/pip install pytest maturin
cd ../../../python/mongreldb_kit
../../tests/conformance/python/.venv/bin/python -m maturin develop
cd ../../tests/conformance/python
.venv/bin/pytest
```

## Notes

- The TypeScript runner normalizes the storage representation of `NULL` for
  nullable columns so the logical behavior matches the Rust/Python results.
- The Rust runner commits each mutating scenario before verifying state,
  matching the transactional isolation of the other runners.
