#!/usr/bin/env bash
# mongreldb-kit-cli vs the sqlite3 CLI: single-record insert/update/delete
# latency at N=100 and N=1,000,000 rows, real process-per-invocation cost
# included (there is no daemon/warm-process mode today -- every invocation
# is Database::open(path) from cold).
#
# Setup (schema creation + bulk seed) goes through kit-perf's `seed` binary
# (Rust API, not the CLI) so only the actually-measured single-op CLI
# invocations pay CLI/process overhead, not the fixture load.
#
# Usage: scripts/bench-cli.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

KIT_CLI=target/release/mongreldb-kit
SEED=crates/kit-perf/target/release/seed
SQLITE=sqlite3

for bin in "$KIT_CLI" "$SEED"; do
  [[ -x "$bin" ]] || { echo "error: $bin not built (cargo build --release)" >&2; exit 1; }
done
command -v "$SQLITE" >/dev/null || { echo "error: sqlite3 not on PATH" >&2; exit 1; }

median_ns() {
  # Reads newline-separated nanosecond durations on stdin, prints the median.
  sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'
}

human() {
  awk -v ns="$1" 'BEGIN {
    if (ns >= 1000000000) printf "%.2f s", ns/1000000000
    else if (ns >= 1000000) printf "%.2f ms", ns/1000000
    else printf "%.1f us", ns/1000
  }'
}

time_cmd_ns() {
  local start end
  start=$(date +%s%N)
  "$@" >/dev/null 2>&1
  end=$(date +%s%N)
  echo $((end - start))
}

bench_kit() {
  local n=$1 dir
  dir=$(mktemp -d)
  "$SEED" "$dir/db" "$n" >/dev/null

  local ins upd del
  ins=$( { for i in $(seq 0 6); do
    time_cmd_ns "$KIT_CLI" insert "$dir/db" users "{\"id\":$((n+1+i)),\"name\":\"CityX\",\"cost\":1.0}"
  done; } | median_ns)

  upd=$( { for i in $(seq 0 6); do
    time_cmd_ns "$KIT_CLI" update "$dir/db" users "$((i+1))" "{\"cost\":$((99+i))}"
  done; } | median_ns)

  del=$( { for i in $(seq 0 6); do
    time_cmd_ns "$KIT_CLI" delete "$dir/db" users "$((n-6+i))"
  done; } | median_ns)

  rm -rf "$dir"
  echo "$ins $upd $del"
}

bench_sqlite() {
  local n=$1 dir db
  dir=$(mktemp -d)
  db="$dir/s.db"
  "$SQLITE" "$db" "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, cost REAL);"
  # awk generates the N-row seed script -- a bash `for` loop over 1e6
  # iterations is slow enough to distort setup time noticeably.
  awk -v n="$n" 'BEGIN {
    print "BEGIN;"
    for (i = 1; i <= n; i++) print "INSERT INTO users VALUES (" i ",\x27City\x27,199.99);"
    print "COMMIT;"
  }' | "$SQLITE" "$db"

  local ins upd del
  ins=$( { for i in $(seq 0 6); do
    time_cmd_ns "$SQLITE" "$db" "INSERT INTO users VALUES ($((n+1+i)),'CityX',1.0);"
  done; } | median_ns)

  upd=$( { for i in $(seq 0 6); do
    time_cmd_ns "$SQLITE" "$db" "UPDATE users SET cost=$((99+i)) WHERE id=$((i+1));"
  done; } | median_ns)

  del=$( { for i in $(seq 0 6); do
    time_cmd_ns "$SQLITE" "$db" "DELETE FROM users WHERE id=$((n-6+i));"
  done; } | median_ns)

  rm -rf "$dir"
  echo "$ins $upd $del"
}

echo "CLI vs SQLite CLI: single-record write latency (real process per op)"
echo
echo "Notes: every mongreldb-kit invocation is a fresh process (Database::open"
echo "from cold, no daemon today) -- this measures what a shell script or cron"
echo "job actually pays, not just the in-process op cost. Setup (schema +"
echo "bulk seed) uses kit-perf's Rust seed binary, not the CLI, so only the"
echo "measured single ops pay process-spawn cost."
echo

for n in 100 1000000; do
  echo "### N = $n rows (median of 7 real process invocations)"
  echo
  echo "| engine | single_insert_commit | single_update_commit | delete_one |"
  echo "|---|---:|---:|---:|"

  read -r ki ku kd <<<"$(bench_kit "$n")"
  echo "| mongreldb-kit CLI | $(human "$ki") | $(human "$ku") | $(human "$kd") |"

  read -r si su sd <<<"$(bench_sqlite "$n")"
  echo "| sqlite3 CLI | $(human "$si") | $(human "$su") | $(human "$sd") |"
  echo
done
