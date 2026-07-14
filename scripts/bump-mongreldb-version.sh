#!/usr/bin/env bash
# Point MongrelDB Kit at a different MongrelDB engine release: Rust crates.io
# constraints, TypeScript npm peer metadata, live-test server downloads, and
# the standalone kit-perf crate. Then regenerates Cargo.lock files.
#
# Usage: scripts/bump-mongreldb-version.sh NEW_VERSION
# Example: scripts/bump-mongreldb-version.sh 0.19.5
#
# This does NOT change mongreldb-kit's own version -- for that, use
# scripts/bump-version.sh. Run this once the Rust engine crates are on
# crates.io and the native npm package is on npmjs.com.
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

sed -i '/^\[patch.crates-io\]/,/^mongreldb-query = /d' Cargo.toml
sed -i "s/mongreldb-core = { version = \"$OLD\"/mongreldb-core = { version = \"$NEW\"/" \
  crates/mongreldb-kit/Cargo.toml
sed -i "s/mongreldb-query = \"$OLD\"/mongreldb-query = \"$NEW\"/" \
  crates/mongreldb-kit/Cargo.toml
sed -i -E "s#^mongreldb-server = .*#mongreldb-server = \"$NEW\"#" tests/conformance/rust/Cargo.toml
sed -i "s/mongreldb-core = \"[0-9.]*\"/mongreldb-core = \"$NEW\"/" \
  tests/conformance/rust/Cargo.toml
sed -i "s/mongreldb-core = { version = \"$OLD\"/mongreldb-core = { version = \"$NEW\"/" \
  crates/kit-perf/Cargo.toml
sed -i '/^\[patch.crates-io\]/,/^mongreldb-query = /d' crates/kit-perf/Cargo.toml
sed -i "s/@visorcraft\/mongreldb\": \"\\^$OLD\"/@visorcraft\/mongreldb\": \"^$NEW\"/" \
  packages/kit/package.json
sed -i "s/SERVER_VERSION = 'v$OLD'/SERVER_VERSION = 'v$NEW'/" packages/kit/src/live_remote.test.ts
sed -i "s/v$OLD/v$NEW/g" python/tests/conftest.py

echo "Regenerating lockfiles from crates.io..."
cargo check --workspace >/dev/null
(cd crates/kit-perf && cargo check >/dev/null)
(cd packages/kit && npm install --package-lock-only --ignore-scripts --save-peer "@visorcraft/mongreldb@^$NEW" >/dev/null)

# Safety net: catch any file the hardcoded list above missed.
STRAY="$(grep -rln "v$OLD\|$OLD" \
  --include="Cargo.toml" --include="Cargo.lock" --include="package.json" \
  --include="package-lock.json" --include="*.ts" --include="*.py" . 2>/dev/null \
  | grep -v -E "/target/|/node_modules/" || true)"
if [[ -n "$STRAY" ]]; then
  echo "warning: these files still mention MongrelDB $OLD -- check whether they need updating too:" >&2
  echo "$STRAY" >&2
fi

cat <<EOF

Done. This kit build now points at MongrelDB $NEW.

Next:
  1. Run the full gate before releasing (see AGENTS.md "Commands"):
       cargo test --workspace
       (cd packages/kit && npm run check && npm test)
       (cd python/mongreldb_kit && maturin develop --release)
       .venv/bin/pytest python/tests tests/conformance/python
       cargo run -p mongreldb-kit-cli -- --help
  2. If mongreldb-kit's own version also needs a bump for this release,
     run scripts/bump-version.sh NEW_KIT_VERSION next.
EOF
