//! Kit (Rust) vs SQLite: single-record insert/update/delete latency at N=100
//! and N=1,000,000 rows. Mirrors the methodology of the engine's
//! `mongreldb-perf` (median of 7 durable single-op timings), but drives every
//! op through `mongreldb_kit::Database` — begin -> insert/update/delete ->
//! commit -- the same path `mongreldb-kit-cli` and the guarded Kit APIs use,
//! including per-row validation, PK/unique/FK guard checks, and (for update)
//! delete+reinsert at the storage layer.
//!
//! Run: cargo run --release --bin compare

use mongreldb_kit::{Column, ColumnType, Database, Schema, Table};
use serde_json::{json, Map, Value};
use std::time::{Duration, Instant};

#[derive(Default, Clone)]
struct Times {
    single_insert_commit: Duration,
    single_update_commit: Duration,
    delete_one: Duration,
}

fn users_schema() -> Schema {
    Schema::new(vec![Table {
        id: 1,
        name: "users".into(),
        columns: vec![
            Column::new(1, "id", ColumnType::Int64),
            Column::new(2, "name", ColumnType::Text),
            Column::new(3, "cost", ColumnType::Float64),
        ],
        primary_key: vec!["id".into()],
        indexes: vec![],
        foreign_keys: vec![],
        unique_constraints: vec![],
        check_constraints: vec![],
    }])
    .unwrap()
}

fn row(id: i64, name: &str, cost: f64) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("id".into(), json!(id));
    m.insert("name".into(), json!(name));
    m.insert("cost".into(), json!(cost));
    m
}

fn median(mut ts: Vec<Duration>) -> Duration {
    ts.sort();
    ts[ts.len() / 2]
}

fn us(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s >= 1.0 {
        format!("{:.2} s", s)
    } else if s >= 1e-3 {
        format!("{:.2} ms", s * 1e3)
    } else {
        format!("{:.1} us", s * 1e6)
    }
}

// ── Kit (Rust) ───────────────────────────────────────────────────────────

fn kit(n: i64) -> Times {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db"), users_schema()).unwrap();

    // Seed 1..=n via insert_many (one transaction, one commit) -- still pays
    // full per-row validation/guard cost, just not N separate commits.
    let seed: Vec<Map<String, Value>> = (1..=n)
        .map(|i| row(i, "City", 199.99 + i as f64))
        .collect();
    let mut txn = db.begin().unwrap();
    txn.insert_many("users", seed).unwrap();
    txn.commit().unwrap();

    let mut t = Times::default();

    // Single-row insert + commit: fresh PKs, never touched by update/delete.
    t.single_insert_commit = median(
        (0..7)
            .map(|i| {
                let now = Instant::now();
                let mut txn = db.begin().unwrap();
                txn.insert("users", row(n + 1 + i, "CityX", 1.0)).unwrap();
                txn.commit().unwrap();
                now.elapsed()
            })
            .collect(),
    );

    // Single-row update + commit: existing rows 1..=7 (disjoint from delete's
    // tail range below). Kit's update is delete+reinsert at the storage layer.
    t.single_update_commit = median(
        (0..7)
            .map(|i| {
                let pk = i + 1;
                let now = Instant::now();
                let mut txn = db.begin().unwrap();
                let mut patch = Map::new();
                patch.insert("cost".into(), json!(99.0 + i as f64));
                txn.update("users", &json!(pk), patch).unwrap();
                txn.commit().unwrap();
                now.elapsed()
            })
            .collect(),
    );

    // Single-row delete + commit: the tail n-6..=n, guaranteed to exist and
    // disjoint from the update range above.
    t.delete_one = median(
        (0..7)
            .map(|i| {
                let pk = n - 6 + i;
                let now = Instant::now();
                let mut txn = db.begin().unwrap();
                txn.delete("users", &json!(pk)).unwrap();
                txn.commit().unwrap();
                now.elapsed()
            })
            .collect(),
    );

    t
}

// ── SQLite (rusqlite) ────────────────────────────────────────────────────

fn sqlite(n: i64) -> Times {
    use rusqlite::Connection;
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open(dir.path().join("s.db")).unwrap();
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, cost REAL)",
        [],
    )
    .unwrap();

    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn.prepare("INSERT INTO users VALUES (?,?,?)").unwrap();
        for i in 1..=n {
            stmt.execute(rusqlite::params![i, "City", 199.99 + i as f64])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();

    let mut t = Times::default();

    t.single_insert_commit = median(
        (0..7)
            .map(|i| {
                let now = Instant::now();
                conn.execute(
                    "INSERT INTO users VALUES (?,?,?)",
                    rusqlite::params![n + 1 + i, "CityX", 1.0],
                )
                .unwrap();
                now.elapsed()
            })
            .collect(),
    );

    t.single_update_commit = median(
        (0..7)
            .map(|i| {
                let pk = i + 1;
                let now = Instant::now();
                conn.execute(
                    "UPDATE users SET cost = ? WHERE id = ?",
                    rusqlite::params![99.0 + i as f64, pk],
                )
                .unwrap();
                now.elapsed()
            })
            .collect(),
    );

    t.delete_one = median(
        (0..7)
            .map(|i| {
                let pk = n - 6 + i;
                let now = Instant::now();
                conn.execute("DELETE FROM users WHERE id = ?", rusqlite::params![pk])
                    .unwrap();
                now.elapsed()
            })
            .collect(),
    );

    t
}

fn row_str(label: &str, t: &Times) -> String {
    format!(
        "| {label} | {} | {} | {} |",
        us(t.single_insert_commit),
        us(t.single_update_commit),
        us(t.delete_one)
    )
}

fn main() {
    println!("Kit (Rust) vs SQLite: single-record write latency\n");
    println!("Notes: both durable (one fsync-backed commit per op). Kit's path is");
    println!("Database::begin -> insert/update/delete -> commit (full per-row");
    println!("validation + PK/unique/FK guard checks; update is delete+reinsert at");
    println!("the storage layer, not in-place). SQLite is rusqlite, autocommit.\n");

    for &n in &[100i64, 1_000_000] {
        println!("### N = {n} rows (median of 7)\n");
        println!("| engine | single_insert_commit | single_update_commit | delete_one |");
        println!("|---|---:|---:|---:|");
        println!("{}", row_str("Kit (Rust)", &kit(n)));
        println!("{}", row_str("SQLite (rusqlite)", &sqlite(n)));
        println!();
    }
}
