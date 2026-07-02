#!/usr/bin/env bash
# §4.1: Daemon-backed single-record write benchmark.
#
# Measures insert/update/delete latency against a *warm* mongreldb-server
# daemon (persistent HTTP connection, no process-per-op overhead) at N=100
# and N=1,000,000 rows. Runs alongside bench-cli.sh — the CLI row measures
# cold-process-per-op cost; this row measures warm-daemon-per-op cost.
#
# Usage: scripts/bench-daemon.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Paths (built fresh if absent).
ENGINE_DIR="${MONGREldb_ENGINE_DIR:-/work/repos/visorcraft/mongreldb}"
DAEMON_BIN="$ENGINE_DIR/crates/mongreldb-server/target/release/mongreldb-server"
KIT_PERF=crates/kit-perf/target/release
BENCH_DAEMON="$KIT_PERF/bench-daemon"
SEED="$KIT_PERF/seed"
PORT=8453
URL="http://127.0.0.1:$PORT"

# Build if needed.
for bin in "$DAEMON_BIN" "$BENCH_DAEMON" "$SEED"; do
  if [[ ! -x "$bin" ]]; then
    echo "building missing binaries..."
    (cd "$ENGINE_DIR/crates/mongreldb-server" && cargo build --release --bin mongreldb-server)
    (cd crates/kit-perf && cargo build --release --bin bench-daemon --bin seed)
    break
  fi
done

PID=""
cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for N in 100 1000000; do
  DB_DIR=$(mktemp -d)
  echo "=== N = $N ==="

  # Seed the database (in-process Kit API — fast, not measured).
  "$SEED" "$DB_DIR" "$N" >&2

  # Start the daemon.
  "$DAEMON_BIN" "$DB_DIR" "$PORT" >&2 &
  PID=$!
  sleep 1  # let the daemon bind

  # Run the benchmark.
  "$BENCH_DAEMON" "$URL" "$N"

  # Stop the daemon + clean up.
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  PID=""
  rm -rf "$DB_DIR"
  echo
done

echo "Done."
