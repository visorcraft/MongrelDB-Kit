#!/usr/bin/env bash
# Point MongrelDB Kit at a different MongrelDB engine release: the dev-only
# [patch.crates-io] git tag (root Cargo.toml), the mongreldb-core version
# constraint in crates/mongreldb-kit/Cargo.toml, the mongreldb-core version
# constraint in tests/conformance/rust/Cargo.toml, and the mongreldb-server
# git tag used by the Rust conformance runner. Then regenerates Cargo.lock.
#
# Usage: scripts/bump-mongreldb-version.sh NEW_VERSION
# Example: scripts/bump-mongreldb-version.sh 0.19.5
#
# This does NOT change mongreldb-kit's own version -- for that, use
# scripts/bump-version.sh. Run this whenever a new MongrelDB tag ships and
# you want the kit to build against it; the tag must already exist upstream
# (the `cargo check` below fails clearly if it doesn't).
#
# tests/conformance/rust/Cargo.toml's bare `mongreldb-core = "X.Y.Z"` used to
# be left as a loose "0.19.0" range on the theory that it "already permits
# any 0.19.x". That's true within a minor version, but Cargo's default caret
# match on a 0.x version (`^0.19.0` = `>=0.19.0, <0.20.0`) stops matching the
# moment the engine crosses a minor bump -- at that point `[patch.crates-io]`
# can't satisfy this one caller (the patch source's own version is outside
# its range), so Cargo falls back to whatever's newest on crates.io within
# 0.19.x instead, landing two different mongreldb-core sources in the same
# build (`error[E0308]: mismatched types` / "multiple different versions of
# crate mongreldb_core"). Pin it to the exact version instead, same as the
# other two references, so it can never silently drift out of range again.
#
# This does NOT rebuild the sibling MongrelDB repo's Node addon that
# packages/kit/node_modules/@visorcraft/mongreldb symlinks to. After running
# this, rebuild it there (`npm run build`, release mode) before trusting
# local TypeScript/CLI test runs against the new engine version.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NEW="${1:?usage: scripts/bump-mongreldb-version.sh NEW_VERSION}"
if ! [[ "$NEW" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: '$NEW' doesn't look like semver (X.Y.Z)" >&2
  exit 1
fi

OLD="$(grep -m1 'mongreldb-core = { version' crates/mongreldb-kit/Cargo.toml \
  | sed -E 's/.*version = "([0-9.]+)".*/\1/')"
if [[ "$NEW" == "$OLD" ]]; then
  echo "error: $NEW is already the referenced MongrelDB version" >&2
  exit 1
fi
echo "Pointing Kit at MongrelDB $OLD -> $NEW"

sed -i "s/tag = \"v$OLD\"/tag = \"v$NEW\"/" Cargo.toml
sed -i "s/mongreldb-core = { version = \"$OLD\"/mongreldb-core = { version = \"$NEW\"/" \
  crates/mongreldb-kit/Cargo.toml
sed -i "s/tag = \"v$OLD\"/tag = \"v$NEW\"/" tests/conformance/rust/Cargo.toml
sed -i "s/mongreldb-core = \"[0-9.]*\"/mongreldb-core = \"$NEW\"/" \
  tests/conformance/rust/Cargo.toml

echo "Regenerating lockfile (fetches mongreldb-core from the git tag)..."
cargo check --workspace >/dev/null

# Safety net: catch any file the hardcoded list above missed.
STRAY="$(grep -rln "v$OLD\"\|\"$OLD\"" --include="*.toml" . 2>/dev/null \
  | grep -v -E "/target/|Cargo\.lock" || true)"
if [[ -n "$STRAY" ]]; then
  echo "warning: these files still mention MongrelDB $OLD -- check whether they need updating too:" >&2
  echo "$STRAY" >&2
fi

cat <<EOF

Done. This kit build now points at MongrelDB v$NEW.

Next:
  1. In the sibling MongrelDB repo, rebuild the Node addon in release mode
     (npm run build) at the checked-out v$NEW so the local
     node_modules symlink reflects it.
  2. Run the full gate before releasing (see AGENTS.md "Commands"):
       cargo test --workspace
       (cd packages/kit && npm run check && npm test)
       (cd python/mongreldb_kit && maturin develop --release)
       .venv/bin/pytest python/tests tests/conformance/python
       cargo run -p mongreldb-kit-cli -- --help
  3. If mongreldb-kit's own version also needs a bump for this release,
     run scripts/bump-version.sh NEW_KIT_VERSION next.
EOF
