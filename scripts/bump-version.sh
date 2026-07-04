#!/usr/bin/env bash
# Bump MongrelDB Kit's own version everywhere: each Rust crate's version
# (mongreldb-kit-core/-kit/-kit-cli/-kit-python) plus their internal
# cross-pins on each other, the Python package's pyproject.toml, and the
# npm package.json. Then regenerates Cargo.lock and package-lock.json.
#
# Usage: scripts/bump-version.sh NEW_VERSION
# Example: scripts/bump-version.sh 0.7.4
#
# This does NOT change which MongrelDB engine version the kit points at --
# for that, use scripts/bump-mongreldb-version.sh. This script only edits
# files and regenerates lockfiles -- it does not commit, tag, or push.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NEW="${1:?usage: scripts/bump-version.sh NEW_VERSION}"
if ! [[ "$NEW" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: '$NEW' doesn't look like semver (X.Y.Z)" >&2
  exit 1
fi

OLD="$(grep -m1 '^version = ' crates/mongreldb-kit/Cargo.toml | sed -E 's/version = "(.*)"/\1/')"
if [[ "$NEW" == "$OLD" ]]; then
  echo "error: $NEW is already the current version" >&2
  exit 1
fi
echo "Bumping mongreldb-kit $OLD -> $NEW"

# All four Rust crates share one version number; this also rewrites the
# path-dependency version pins each carries on the others (they use the same
# `version = "X.Y.Z"` shape, so one pass covers both). Add new crates here.
CARGO_FILES=(
  crates/mongreldb-kit-core/Cargo.toml
  crates/mongreldb-kit/Cargo.toml
  crates/mongreldb-kit-cli/Cargo.toml
  crates/mongreldb-kit-python/Cargo.toml
)
for f in "${CARGO_FILES[@]}"; do
  sed -i "s/version = \"$OLD\"/version = \"$NEW\"/g" "$f"
done
sed -i "s/version = \"$OLD\"/version = \"$NEW\"/" python/mongreldb_kit/pyproject.toml
sed -i "s/\"version\": \"$OLD\"/\"version\": \"$NEW\"/" packages/kit/package.json
sed -i -E "s/return '[0-9]+\\.[0-9]+\\.[0-9]+';/return '$NEW';/" packages/kit/src/migrate.ts
sed -i "s/mongreldb-kit = { path = \"..\\/mongreldb-kit\", version = \"$OLD\" }/mongreldb-kit = { path = \"..\\/mongreldb-kit\", version = \"$NEW\" }/" \
  crates/kit-perf/Cargo.toml

echo "Regenerating lockfiles..."
cargo check --workspace >/dev/null
(cd crates/kit-perf && cargo check >/dev/null)
(cd packages/kit && npm install >/dev/null 2>&1)

# Safety net: catch any file the hardcoded list above missed (e.g. a new
# crate). Warns rather than fails -- Cargo.lock/target/node_modules/.venv
# always mention the old version transitively and are expected here.
STRAY="$(grep -rl "\"$OLD\"" --include="*.toml" --include="*.json" . 2>/dev/null \
  | grep -v -E "/target/|node_modules|\.venv|Cargo\.lock|package-lock\.json" || true)"
if [[ -n "$STRAY" ]]; then
  echo "warning: these files still mention $OLD -- check whether they need the bump too:" >&2
  echo "$STRAY" >&2
fi

cat <<EOF

Done. Review with 'git diff', then run the full gate before committing
(see AGENTS.md "Commands" -- Rust, TypeScript, Python, CLI, all four):
  cargo test --workspace
  (cd packages/kit && npm run check && npm test)
  (cd python/mongreldb_kit && maturin develop --release)
  .venv/bin/pytest python/tests tests/conformance/python
  cargo run -p mongreldb-kit-cli -- --help

Then, per AGENTS.md "Releases":
  git commit -am "release $NEW"
  git tag -a v$NEW -m "v$NEW — <one-line summary>"
  git push origin master && git push origin v$NEW
CI publishes to npm and crates.io (mongreldb-kit-core, then mongreldb-kit)
automatically on the tag push.
EOF
