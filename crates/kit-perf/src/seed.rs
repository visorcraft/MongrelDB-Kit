//! Create a Kit database at PATH with the `users` table seeded 1..=N rows.
//! Used to set up fixtures for the CLI benchmark (scripts/bench-cli.sh):
//! seeding goes through the fast insert_many path so only the CLI's own
//! single insert/update/delete invocations get timed, not the bulk load.
//!
//! Run: cargo run --release --bin seed -- PATH N

#[path = "common.rs"]
mod common;

use mongreldb_kit::Database;
use serde_json::{Map, Value};

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("usage: seed PATH N");
    let n: i64 = args
        .next()
        .expect("usage: seed PATH N")
        .parse()
        .expect("N must be an integer");

    let db = Database::create(std::path::Path::new(&path), common::users_schema()).unwrap();
    let seed: Vec<Map<String, Value>> = (1..=n)
        .map(|i| common::row(i, "City", 199.99 + i as f64))
        .collect();
    let mut txn = db.begin().unwrap();
    txn.insert_many("users", seed).unwrap();
    txn.commit().unwrap();
    println!("seeded {n} rows at {path}");
}
