# Specification: Optional Credential Enforcement

| | |
|---|---|
| **Status** | Approved — design finalized, implementation pending (Phase 1 next) |
| **Spec ID** | `auth-enforcement` |
| **Affects** | `mongreldb-core`, `mongreldb-query`, `mongreldb-server`, `mongreldb-node`, `mongreldb-kit`, `mongreldb-kit-python`, `mongreldb-kit-cli` |
| **Default behavior** | Unchanged — databases remain credentialless and "create-and-go" by default |
| **Competitor analog** | SQLite (no enforcement, current default) vs. PostgreSQL (always enforced) — MongrelDB adds Postgres-style enforcement as an **opt-in** per database |

## 1. Goals & non-goals

### Goals

1. **Opt-in credential enforcement at the storage layer** — a database marked
   `require_auth` cannot be read, written, or have its schema changed without
   an authenticated `Principal`. This is stronger than the daemon's HTTP
   middleware: it applies to embedded, native (NAPI), Python, CLI, and daemon
   surfaces alike, and survives a process restart.
2. **Backward compatibility** — existing credentialless databases open
   unchanged with no migration step. A database without `require_auth` set
   behaves exactly as today (the "SQLite path").
3. **Per-call principal, not a global** — credentials are bound to a single
   open handle / session, so two callers opening the same file with different
   users get independent privileges. No ambient authority.
4. **Composable with encryption** — a database can be both encrypted-at-rest
   **and** require credentials. The two features are orthogonal: encryption
   protects the bytes on disk; auth enforcement protects the logical operations
   against anyone who can read the bytes.
5. **Fail-closed semantics** — a `require_auth` database opened without
   credentials, or with invalid credentials, or with insufficient
   permissions for an operation, returns a typed error. There is no
   "read-only fallback" or anonymous mode once enforcement is on.

### Non-goals

- **Network transport security.** TLS, Kerberos, OAuth, mTLS — out of scope.
  The daemon's existing Bearer/Basic middleware plus a reverse proxy covers
  the wire; this spec is about what happens *after* the request arrives.
- **Column-level / row-level security (RLS).** Permissions remain
  table-granular (`Select { table }`, …). RLS policies are a future spec.
- **Audit logging of every checked operation.** A future `audit_log` virtual
  table can layer on top; this spec defines the check, not the log.
- **Wildcard table permissions.** `Permission::Select { table: "*" }` continues
  to mean a table literally named `*`. A future `AllTables` variant could be
  added if users want "SELECT on every table" grants, but it is not required
  for v1 (use `Permission::All` for the same effect).
- **Changing the existing daemon HTTP auth.** The middleware stays as-is; the
  storage layer simply becomes *also* enforcing, so a daemon request that
  passes the HTTP gate but maps to an under-privileged principal will now be
  rejected by the storage layer (defense in depth).

## 2. The core problem this solves

Today, the catalog stores users, roles, and permissions, but they are
**decorative** — verified at the daemon's HTTP layer only, and never consulted
by any `Database`/`Table`/`Transaction`/`MongrelSession` method. Concretely:

- `db.check_permission(...)` exists in `mongreldb-core` (database.rs:946) but
  has **zero call sites** anywhere in the workspace.
- `db.resolve_principal(...)` is called once, by the daemon's
  `auth_middleware` (lib.rs:169), and the resulting `Principal` is injected
  into request extensions — then **never read back** by any handler.
- The CLI's `user`/`role` subcommands and the embedded `create_user` /
  `grant_permission` methods mutate the catalog but gate nothing.

So a daemon started with `--auth-users` will reject an HTTP request with no
`Authorization` header, but **any** catalog user — once past that single
HTTP check — can `drop_table`, `create_user`, `grant_permission`, read every
table, and write every table. And embedded/CLI/NAPI paths have no enforcement
at all.

This spec makes the catalog's permission data authoritative at the storage
layer, opt-in per database.

## 3. User stories

| # | As a… | I want… | So that… |
|---|---|---|---|
| U1 | app developer shipping an embedded binary | the database to stay credentialless by default | I keep SQLite-style "create and go" ergonomics |
| U2 | app developer with sensitive data | to flip one flag at create time and have every subsequent open require a valid user | I get defense-in-depth on top of encryption |
| U3 | app developer | to open the database with `(username, password)` and have my session's privileges checked on every operation | a stolen file alone can't query my data |
| U4 | multi-tenant service operator | to grant `select:orders` to one user and `insert:orders` to another, with the storage layer enforcing it | I don't have to wrap every call in my own checks |
| U5 | DBA | to bootstrap the first admin user at create time in one atomic call | I never have a window where the database requires auth but has no users |
| U6 | operator who lost the admin password | to disable enforcement offline via a documented recovery tool | I can recover without losing data |
| U7 | CLI user | `mongreldb-kit` to accept `--user`/`--password` (or fail-closed) when operating on a `require_auth` database | I can administer a credentialed database from the shell |

## 4. Design

### 4.1 The `require_auth` flag (catalog)

Add a boolean flag to the `Catalog` struct (`mongreldb-core/src/catalog.rs:51`),
following the existing `#[serde(default)]` backward-compat pattern:

```rust
pub struct Catalog {
    // …existing fields…
    /// When true, every Database/Table/Transaction/MongrelSession operation
    /// requires an authenticated Principal with sufficient permission.
    /// Defaults to false → existing credentialless databases open unchanged.
    #[serde(default)]
    pub require_auth: bool,
}
```

- **Backward compatibility:** old catalog files lack the field →
  `#[serde(default)]` deserializes to `false` → enforcement off → identical
  to today.
- **Setting the flag:** via `Database::enable_auth(&self, principal)` (which
  also writes the first user, see §4.5) or via SQL `ALTER DATABASE SET
  require_auth = true`. There is **no way to disable it from an authenticated
  session** — disabling requires the offline recovery tool (§4.7) to prevent
  an attacker who compromises one credential from weakening the policy.
- The flag is part of the catalog blob, so it inherits the catalog's
  AES-256-GCM sealing (encryption on) or SHA-256 tagging (encryption off).
  Tampering with the flag is detected as catalog corruption on open.

### 4.2 The `Principal` becomes a first-class open parameter

Today `Principal` (auth.rs:87) is a runtime-only struct built by
`resolve_principal` and discarded. To enforce, the open path must produce a
`Database` handle that carries a principal.

**New engine constructors** (`mongreldb-core/src/database.rs`):

```rust
impl Database {
    /// Open a `require_auth` database, verifying credentials up front.
    /// Returns `MongrelError::AuthRequired` if the database has
    /// `require_auth = false` (use plain `open` for those).
    /// Returns `MongrelError::InvalidCredentials` if verification fails.
    pub fn open_with_credentials(
        root: impl AsRef<Path>,
        username: &str,
        password: &str,
    ) -> Result<Self>;

    /// Open a `require_auth` encrypted database. Combines the encryption
    /// passphrase flow with credential verification.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted_with_credentials(
        root: impl AsRef<Path>,
        passphrase: &str,
        username: &str,
        password: &str,
    ) -> Result<Self>;
}
```

The `Database` struct gains an optional principal field, mirroring how
encryption stores optional `kek`/`meta_dek`:

```rust
pub struct Database {
    // …existing fields…
    /// The authenticated principal for this handle. `None` on databases
    /// opened with `require_auth = false` (the default), `Some` on
    /// credentialed opens. Checked by every enforcement point.
    principal: Option<Principal>,
}
```

- A non-credentialed open (`Database::open`) on a `require_auth = true`
  database fails with `MongrelError::AuthRequired`.
- A credentialed open on a `require_auth = false` database **also fails**
  with `MongrelError::AuthNotRequired` — callers must pick the right
  constructor for the database's mode. This avoids silent privilege
  confusion and makes the API self-documenting.
- `verify_user` runs **once** at open (Argon2id, ~50ms), then the resolved
  `Principal` is cached on the handle for the lifetime of the open. This
  keeps per-operation checks cheap (a `has_permission` call is a Vec scan,
  not a re-hash).

### 4.3 The enforcement matrix

Every public mutating or reading method on `Database`, `Table`,
`Transaction`, and `MongrelSession` consults the cached principal when
`require_auth` is true. The check is a single early-return helper:

```rust
// In Database:
fn require(&self, perm: &Permission) -> Result<()> {
    if let Some(catalog) = self.catalog.read() {
        if catalog.require_auth {
            let p = self.principal.as_ref()
                .ok_or(MongrelError::AuthRequired)?;
            if !p.has_permission(perm) {
                return Err(MongrelError::PermissionDenied {
                    required: perm.clone(),
                    principal: p.username.clone(),
                });
            }
        }
    }
    Ok(())
}
```

Mapping of operations to required permissions:

| Operation | Required permission | Call site |
|---|---|---|
| `Table::get` / `lookup_pk` / `scan_cursor` | `Select { table }` | engine.rs reads |
| `Table::query*` / `count*` / `aggregate*` | `Select { table }` | engine.rs reads |
| `Table::put` / `put_batch` (and `_returning`) | `Insert { table }` | engine.rs:1601+ |
| `Transaction::delete` / `delete_many` / `truncate` | `Delete { table }` | txn.rs:160+ |
| `Transaction::update_many` / `upsert` (update path) | `Update { table }` | txn.rs:198+ |
| `Transaction::put` (insert path) | `Insert { table }` | txn.rs:107 |
| `Database::create_table` / `drop_table` / `rename_table` / `alter_column` | `Ddl` | database.rs:3219+ |
| `Database::compact*` / `gc` / `truncate-via-txn` | `Ddl` (maintenance = schema-impacting) | database.rs:3146+ |
| `MongrelSession::run` (SQL) | Inspect the AST: SELECT→`Select`, INSERT→`Insert`, UPDATE→`Update`, DELETE→`Delete`, DDL→`Ddl`; multi-statement requires the union | mongreldb-query/src/lib.rs:1785 |
| `Database::create_user` / `drop_user` / `alter_user_password` / `set_user_admin` | `Admin` | database.rs:684+ |
| `Database::create_role` / `drop_role` / `grant_role` / `revoke_role` | `Admin` | database.rs:793+ |
| `Database::grant_permission` / `revoke_permission` | `Admin` | database.rs:881+ |
| `Database::call_procedure` | The procedure's declared required permission (defaults to `All` if undeclared) — future extension; v1 requires `Select { table }` for each table the procedure body touches, or `All` if static analysis is too coarse | database.rs:1185 |
| `Database::create_trigger` / procedures / external-table DDL | `Ddl` | database.rs:571+ |

**Notes on the matrix:**

- **`Transaction::put` ambiguity.** The current `put` is used for both
  insert and update-in-place (the kit's `update` goes through `put` after a
  read). The v1 enforcement treats `put` as `Insert { table }`; the kit's
  `update` path, which reads-then-`put`s, additionally requires
  `Update { table }`. This mirrors how the kit already distinguishes the two
  at the API layer.
- **`MongrelSession::run` is the hard case.** SQL parsing must classify the
  statement(s) before execution. DataFusion's `Statement` AST already exposes
  the kind (`Statement::Query`, `Insert`, `Update`, `Delete`, `CreateTable`,
  …), so the classifier is straightforward for the common cases. v1 supports
  the standard kinds; exotic statements (e.g. `ATTACH`, `PRAGMA`) require
  `Ddl`.
- **Admin bypass:** a principal with `is_admin = true` short-circuits every
  check (existing `Principal::has_permission` behavior). The first admin
  user created at `enable_auth` time gets this flag.
- **Procedures defaulting to `All`** is conservative; a follow-up can add a
  `required_permission` field to `ProcedureEntry` for finer control.

### 4.4 `MongrelSession` carries the principal

`MongrelSession::open(database: Arc<Database>)` (lib.rs:1387) gains a
credentialed variant. Because the session holds an `Arc<Database>`, and the
`Database` already carries its `principal` field after a credentialed open,
the session simply reads `db.principal` when classifying SQL. No new session
state is strictly required — but to keep the API explicit and avoid
accidentally building a session off a non-credentialed `Database` for a
`require_auth` catalog, add:

```rust
impl MongrelSession {
    pub fn open(database: Arc<Database>) -> Result<Self>; // existing; fails if require_auth && principal.is_none()
}
```

The kit's cached `session` field (db.rs:180) and the NAPI addon's
(db.rs:755 in mongreldb-node) are built from `core_arc()`; since the
`Arc<Database>` already carries the principal, they inherit enforcement
automatically once the session's `run` consults the AST classifier.

### 4.5 Bootstrapping: `enable_auth` at create time

The chicken-and-egg problem: a `require_auth` database needs at least one
user before it can be opened with credentials. Solved by an atomic create
method:

```rust
impl Database {
    /// Create a new database with `require_auth = true` and a single
    /// admin user. The credentials are verified on every subsequent open.
    pub fn create_with_credentials(
        root: impl AsRef<Path>,
        admin_username: &str,
        admin_password: &str,
    ) -> Result<Self>;

    #[cfg(feature = "encryption")]
    pub fn create_encrypted_with_credentials(
        root: impl AsRef<Path>,
        passphrase: &str,
        admin_username: &str,
        admin_password: &str,
    ) -> Result<Self>;
}
```

These write the catalog with `require_auth = true`, one `UserEntry` with
`is_admin = true`, and return an already-authenticated handle (the principal
is cached from the in-memory user). SQL equivalent:

```sql
CREATE DATABASE REQUIRE AUTH;
CREATE USER admin WITH PASSWORD '...';
ALTER USER admin ADMIN;
ALTER DATABASE SET require_auth = true;  -- only valid before first reopen
```

### 4.6 Per-language surface additions

Every layer gains credentialed constructors mirroring the existing encrypted
constructors — the encrypted variants are the established precedent for
"optional security feature with its own constructor." All four combinations
(plain/encrypted × unauth/credentialed) exist.

| Layer | New constructors |
|---|---|
| **Rust kit** (`crates/mongreldb-kit/src/db.rs`) | `Database::open_with_credentials(path, user, pass)`, `Database::open_encrypted_with_credentials(...)`, `Database::create_with_credentials(path, schema, admin_user, admin_pass)`, `Database::create_encrypted_with_credentials(...)` |
| **NAPI** (`mongreldb/crates/mongreldb-node/src/lib.rs`) | `Database.openWithCredentials(path, user, pass)`, `.openEncryptedWithCredentials(...)`, `.createWithCredentials(...)`, `.createEncryptedWithCredentials(...)` — add to the `MongrelModule` type (db.ts:116-126) and as `#[napi]` methods |
| **Python pyo3** (`crates/mongreldb-kit-python/src/lib.rs`) | `PyDatabase::open_with_credentials`, `::create_with_credentials`, encrypted variants — `#[staticmethod]` |
| **Python facade** (`python/.../__init__.py`) | `Database.open_with_credentials(path, user, pass)`, `Database.create_with_credentials(path, schema, admin_user, admin_pass)`, encrypted variants — thin wrappers. **Also: remove the dangling duplicate methods at lines 1022+** (pre-existing dead code from a prior incomplete edit). |
| **TypeScript kit** (`packages/kit/src/db.ts`) | Extend the existing `options?` arg on `openSync`/`open`: `{ credentials?: { username: string; password: string } }`. New `createWithCredentialsSync` / `createEncryptedWithCredentialsSync`. Update `MongrelModule` type. |
| **CLI** (`crates/mongreldb-kit-cli/src/main.rs`) | Global `--user <name>` / `--password <pw>` (or `--password-stdin`) flags on the root `Cli`; a single `open_db(path, &cli)` helper routes all 43 existing `Database::open` sites through `open_with_credentials` when flags are present, and fails-closed with `AuthRequired` when flags are absent but the catalog has `require_auth = true`. New `init --require-auth --admin-user … --admin-password …` flag on `cmd_init`. |

### 4.7 Offline recovery (disable enforcement)

Because `require_auth` cannot be disabled from an authenticated session
(§4.1), an operator who loses the admin password needs a documented escape
hatch. Two equivalent mechanisms:

1. **CLI recovery command** — operates on a *closed* database by reading the
   catalog blob, flipping `require_auth` to `false`, and writing it back:
   ```
   mongreldb-kit auth disable-offline <path>
   ```
   Requires filesystem access to the database directory (an operator with
   disk access can already read the bytes, so this grants no new power over
   raw file access — the security boundary is the file system, same as for
   encryption-at-rest). Prints a loud warning. For encrypted databases,
   requires the passphrase.

2. **Manual edit** — the catalog file format is documented; an operator can
   deserialize, flip the flag, re-serialize. Same threat-model caveat.

This mirrors how disk-level access trivially defeats encryption-at-rest too;
auth enforcement is a logical-access control, not a cryptographic one. The
docs (§7) will state this explicitly so users don't mistake
`require_auth` for full-disk encryption.

### 4.8 Conformance & tests

New conformance fixtures under `tests/conformance/fixtures/`:

- `auth_basic.json` — create-with-credentials, verify subsequent open
  requires them, exercise `Insert`/`Select`/`Update`/`Delete`/`Ddl`
  permission grants and denials across all three runners.
- `auth_encrypted.json` — the same, composed with an encryption passphrase.
- `auth_admin_bypass.json` — admin user bypasses every check.
- `auth_recovery.json` — offline disable round-trip.

The Rust/TS/Python runners each gain a `require_auth` open path. The
existing credentialless fixtures stay green unchanged (the default path).

Unit tests in `mongreldb-core`:
- Every `Database`/`Table`/`Transaction`/`MongrelSession` enforcement point
  gets a deny-test and an allow-test.
- The AST classifier for SQL gets a table-driven test for each statement
  kind.
- Backward-compat: a fixture catalog *without* `require_auth` deserializes
  to `false` and opens unauthenticated.

## 5. Error taxonomy

New error variants in `mongreldb-core` (added to the existing
`MongrelError` enum):

| Variant | When | HTTP status (daemon) |
|---|---|---|
| `AuthRequired` | A `require_auth` database was opened without credentials, or an operation ran on a handle with no principal. | 401 |
| `AuthNotRequired` | A credentialed constructor was used on a credentialless database. | 400 |
| `InvalidCredentials` | `open_with_credentials` verification failed (bad username/password). | 401 |
| `PermissionDenied { required, principal }` | An operation's required permission is not satisfied by the cached principal. | 403 |

Kit error mapping (`KitError` in `mongreldb-kit-core`) gains matching
variants; the TS kit maps these to `KitAuthRequiredError`,
`KitPermissionDeniedError`, etc., extending the existing
`KitDuplicateError` pattern. Python gains `AuthRequiredError`,
`PermissionDeniedError` alongside `DuplicateError`.

## 6. Threat model & limitations

**What this stops:**

- An attacker with read access to the database *file system path* but **not**
  the credentials cannot use the MongrelDB API to query, mutate, or
  enumerate data — even if they can copy the bytes. (For encrypted databases,
  they additionally can't decrypt the bytes without the passphrase.)
- A compromised low-privilege service account in a multi-process deployment
  cannot escalate beyond its granted permissions — the storage layer
  enforces what the daemon's HTTP layer only asserted.
- A bug in application code that forgets to call `check_permission` is
  caught by the storage layer anyway.

**What this does NOT stop:**

- An attacker with raw disk access who parses the catalog/blob format
  directly. Auth enforcement is logical, not cryptographic — same threat
  model as encryption-at-rest without a hardware key. Use full-disk
  encryption or HSM-backed keys for that layer.
- An attacker who can write to `_meta/` and flip `require_auth` to false
  (the offline recovery path). Filesystem permissions on `_meta/` are the
  boundary; document this in operations.
- Side-channel attacks, timing attacks on permission checks (the check is a
  constant-ish Vec scan, but not constant-time by design — leaking *whether
  you have a permission* is acceptable; the username in the error already
  discloses that).
- Brute-force of weak passwords. Argon2id with the current parameters
  (~50ms/verify) limits an online attacker to ~20 guesses/sec/core; for
  offline brute-force of a stolen hash, the same Argon2id cost applies. v1
  does not implement lockout/throttling — recommend fail2ban on the daemon.

## 7. Documentation plan

- **New engine doc** `docs/15-credential-enforcement.md` — the authoritative
  guide: when to enable, how to bootstrap, per-language examples, recovery,
  threat model. Cross-linked from `docs/14-auth.md` and `docs/08-daemon.md`.
- **Update `docs/14-auth.md`** — add a section "Enforcement: advisory vs.
  required" pointing to the new doc.
- **Update each language quickstart** (engine + kit) — a credentialed-open
  example block.
- **Update `docs/cli.md`** — `--user`/`--password`, `init --require-auth`,
  `auth disable-offline`.
- **Update `docs/production-checklist.md`** — a "Credential enforcement"
  row covering `_meta/` permissions and admin credential storage.

## 8. Implementation phases

The work is large; the phases are independently shippable.

### Phase 1 — Engine enforcement core (PR 1)
- `Catalog::require_auth` field + serde default.
- `Database::principal` field; `open_with_credentials` /
  `create_with_credentials` (+ encrypted variants).
- `Database::require()` helper; wire enforcement into `Database` methods
  (DDL, admin, maintenance).
- New `MongrelError` variants; unit tests for the engine-level matrix.
- **Shippable on its own:** Rust embedded users get enforcement.

### Phase 2 — Table/Transaction/SQL enforcement (PR 2)
- Wire `require()` into `Table` reads/writes (needs the table name threaded
  to the check — `Table` knows its own name via `CatalogEntry`).
- Wire into `Transaction` methods.
- `MongrelSession::run` AST classifier + per-statement checks.
- Unit tests for the full operation matrix.
- **Shippable on its own:** full enforcement for embedded Rust + SQL.

### Phase 3 — Bindings & Kit (PR 3)
- NAPI constructors + `MongrelModule` type.
- Rust kit constructors.
- Python pyo3 + facade (and clean up the dangling duplicate methods at
  `__init__.py:1022+`).
- TypeScript kit `options.credentials` + create variants.
- Kit `KitError` variants + TS/Python error classes.
- Per-language smoke tests.

### Phase 4 — CLI & daemon (PR 4)
- CLI global `--user`/`--password`, `init --require-auth`, `auth
  disable-offline`.
- Route all 43 open sites through the credentialed helper.
- Daemon: surface `PermissionDenied` as HTTP 403 (the middleware already
  does the auth; this adds defense-in-depth so a handler that maps to an
  under-privileged principal is rejected at the storage layer too).

### Phase 5 — Conformance & docs (PR 5)
- Conformance fixtures (`auth_basic`, `auth_encrypted`, `auth_admin_bypass`,
  `auth_recovery`) wired into all three runners.
- Engine `docs/15-credential-enforcement.md`; updates to 14-auth, 08-daemon,
  quickstarts, CLI doc, production checklist.

Each phase ends green (full gate) and can release independently. Phases 1–2
land the engine feature behind Rust-only APIs; Phases 3–5 propagate it.

## 9. Resolved decisions

All five design questions are locked. Implementation must conform to these
decisions; do not re-litigate them mid-implementation.

1. **Procedure-level permissions — defer to v2.** v1 requires `All` to call
   procedures on a `require_auth` database. A future `SECURITY DEFINER`-style
   marker on `ProcedureEntry` can refine this; it is explicitly out of scope
   for the initial implementation.

2. **`All` does not imply `Admin`.** Keep the current semantics: the
   four-way split (`All`, `Ddl`, `Admin`, table-level) is sufficient, and
   `Principal::is_admin` remains the sole superuser short-circuit. A role
   granted `All` can do every table/DDL operation but cannot create users
   or grant permissions — only `is_admin = true` grants that.

3. **Cache the principal once at open; re-verify explicitly.** v1 caches
   the `Principal` for the lifetime of the open handle (cheap per-op checks,
   matches the encryption-KEK model). Long-lived daemons call the new
   `Database::refresh_principal()` after a `REVOKE` to pick up the change;
   there is no automatic periodic re-verification and no per-transaction
   Argon2id cost.

4. **CLI credentials: flag, then env, then stdin — never a TTY prompt.**
   `--password <pw>` takes precedence; otherwise `MONGREL_PASSWORD`; otherwise
   `--password-stdin` reads one line from stdin. The CLI never prompts
   interactively because it is typically scripted.

5. **Token-only daemon mode is insufficient for a `require_auth` database.**
   The daemon must run with `--auth-users` (or `--auth-users` plus
   `--auth-token`) so every request resolves to a real catalog `Principal`.
   `--auth-token` alone is rejected at startup when the catalog has
   `require_auth = true`, with a clear error pointing at `--auth-users`.

## 10. Out-of-scope follow-ups (parking lot)

- Row-level security (RLS) policies.
- Column-level redaction.
- OAuth/OIDC principal sources for the daemon.
- Hardware-token (YubiKey) second factor for admin users.
- Audit log virtual table (`SELECT * FROM __audit_log`).
- Per-session statement rate limiting.
- Constant-time permission checks (currently fine for "do you have it" but
  not for "which permission did you lack").
