# Contributing to MongrelDB Kit

Thanks for taking the time to help MongrelDB Kit. This document describes how
to propose a change, what we expect from a pull request, and the coding
standards that apply to the codebase.

If anything here is unclear or out of date, open an issue or a PR.

## Code of conduct

Be kind, be specific, assume good faith. Disagree about the technical
details, not the person. Public reviews stay focused on the diff.

## How to propose a change

MongrelDB Kit uses a standard **fork → branch → pull request** workflow on
GitHub.

1. **Fork** [`visorcraft/MongrelDB-Kit`](https://github.com/visorcraft/MongrelDB-Kit)
   to your GitHub account.
2. **Clone** your fork and add the upstream remote:

   ```sh
   git clone git@github.com:<you>/MongrelDB-Kit.git
   cd MongrelDB-Kit
   git remote add upstream https://github.com/visorcraft/MongrelDB-Kit.git
   ```

3. **Branch** from `master`. Pick a descriptive, kebab-case branch name:
   `fix-migration-idempotency`, `feature/schema-diff`, `docs/conformance-guide`.

   ```sh
   git fetch upstream
   git switch -c my-change upstream/master
   ```

4. **Make focused commits.** One logical change per commit. Run the
   preflight (see below) before pushing.
5. **Open a pull request** against `master` on `visorcraft/MongrelDB-Kit`.
   Fill in the PR template:
   - **What.** One paragraph summary of the change.
   - **Why.** Bug fix? New feature? Doc fix? Link the issue if one
     exists.
   - **How to test.** The exact commands a reviewer should run.
   - **Risk.** What might break? What did you not test?

## Before you push: preflight

The Kit spans four surfaces (Rust, TypeScript, Python, CLI). Run the
preflight for every surface you touched. (Prefix shell commands with `rtk`
in this repo.)

### Rust

```sh
rtk cargo fmt --check
rtk cargo clippy --workspace --all-targets --all-features -- -D warnings
rtk cargo test --workspace
```

All three must pass with zero warnings. If a check fails, fix the root
cause - don't `--no-verify`, don't silence clippy lints with `#[allow(...)]`
unless you justify it in the PR description.

### TypeScript (`packages/kit`)

```sh
cd packages/kit && rtk npm ci && rtk npm run build && rtk npm run check && rtk npm test
```

### Python (`python/mongreldb_kit`)

```sh
cd python/mongreldb_kit && rtk maturin develop
rtk .venv/bin/pytest ../../python/tests ../../tests/conformance/python
```

### CLI

```sh
rtk cargo run -p mongreldb-kit-cli -- --help
```

### Conformance

Shared behavior changes must update `tests/conformance/fixtures/` and pass
all three runners (Rust, TypeScript, Python).

## What we look for in a review

- The change does one thing and does it well.
- Behavior changes ship with tests. New Rust API: a unit test in `src/` or
  `tests/`. New TS surface: a Vitest test in `packages/kit/src/`. New
  Python facade method: a test in `python/tests/`. Cross-surface behavior:
  a fixture in `tests/conformance/`.
- The change keeps the Kit as a thin, conformance-tested layer over
  `mongreldb-core`, `mongreldb-query`, and `mongreldb-server`. Don't
  re-implement storage, indexing, WAL, or SQL planning logic inside the Kit.
- Documentation is updated alongside the code (`docs/`, `README.md`) if the
  change affects users.
- Commits have clear messages (see below).

## Coding standards

### Rust

- **Edition / toolchain.** Rust 2021. Don't bump the MSRV casually.
- **Formatting.** `cargo fmt --all`. `rustfmt` clean, snake_case.
- **Linting.** `cargo clippy --workspace --all-targets --all-features
  -- -D warnings` must pass with no warnings.
- **Errors.** Return the project's `Result<T>` type. Use typed error
  variants. No `String`-typed errors at API boundaries.
- **Panics.** Don't panic from library code. Use `Result`. `unwrap` /
  `expect` is acceptable in tests and in standalone binaries only.
- **Dependencies.** Prefer crates that already appear in `Cargo.lock`.
  New dependencies must be MIT or Apache-2.0 licensed.

### TypeScript (`packages/kit`)

- Strict ESM (`NodeNext`), `.js` import specifiers, tabs, `camelCase`,
  `PascalCase` exports.

### Python (`python/mongreldb_kit`)

- 4-space indent, type hints, `snake_case` functions, `PascalCase` classes.

### Commit messages

- Subject line: imperative mood, ≤ 72 characters, no trailing period.
  Example: `Add schema-diff conformance fixture for rename column`.
- Body: wrap at 72 characters. Explain *why*, not *what* (the diff
  shows the what).
- Reference issues with `Fixes #123` / `Refs #123` on a final line
  when applicable.
- **Never** add AI/assistant attribution (no `Co-Authored-By`, no
  `Generated with`, no tool names).

## Issue reports

A useful bug report includes:

- MongrelDB Kit version (from `Cargo.toml` / `package.json`).
- Your OS and toolchain versions (`rustc --version`, `node --version`,
  `python --version`).
- The exact code or commands that reproduce the issue.
- The expected result and the actual result.
- Any error messages or panics (include the full backtrace with
  `RUST_BACKTRACE=1` for Rust).

Feature requests are welcome. Please describe the problem you're trying
to solve before proposing the solution.

## Releases

Two scripts - don't hand-edit versions:

- Kit's own version: `scripts/bump-version.sh NEW_VERSION`.
- Engine ref: `scripts/bump-mongreldb-version.sh NEW_VERSION`.

CI publishes `@visorcraft/mongreldb-kit` to npm and `mongreldb-kit-core` +
`mongreldb-kit` to crates.io on `v*` tag push.

## Security

If you find a vulnerability, **do not** open a public GitHub issue.
Report it privately through GitHub's private vulnerability reporting -
the repository's **Security** tab → **Report a vulnerability**. The full
policy is in [`SECURITY.md`](SECURITY.md).

## Licensing

MongrelDB Kit is dual-licensed under MIT OR Apache-2.0. By contributing,
you agree that your changes are made available under the same license.

- Do **not** paste code from other database engines or ORMs unless you
  have done a license review first.
- New third-party dependencies must be MIT or Apache-2.0 licensed.

Thanks again - looking forward to your PR.
