//! §4.1: daemon-backed benchmark harness.
//!
//! Measures single-record insert/update/delete latency against a *warm*
//! `mongreldb-server` daemon via a persistent HTTP connection (ureq keep-alive).
//! The daemon must already be running and the table pre-seeded — use
//! `scripts/bench-daemon.sh` which handles setup/teardown.
//!
//! Run standalone: cargo run --release --bin bench-daemon -- <url> <N>

use serde_json::{json, Value};
use std::time::{Duration, Instant};

const N_OPS: usize = 7;

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

/// Send a single-op `/kit/txn` request and return the latency.
fn kit_txn(agent: &ureq::Agent, url: &str, ops: &[Value]) -> Duration {
    let body = json!({ "ops": ops });
    let start = Instant::now();
    let resp = agent
        .post(url)
        .set("Content-Type", "application/json")
        .send_string(&body.to_string())
        .unwrap_or_else(|e| panic!("kit_txn request failed: {e}"));
    let elapsed = start.elapsed();
    if resp.status() != 200 {
        let status = resp.status();
        let text = resp.into_string().unwrap_or_default();
        panic!("kit_txn returned {status}: {text}");
    }
    elapsed
}

fn bench(url: &str, n: i64) {
    let endpoint = format!("{url}/kit/txn");
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(10))
        .build();

    // Single insert + commit: fresh PKs above the seeded range.
    let insert_latencies: Vec<Duration> = (0..N_OPS as i64)
        .map(|i| {
            let id = n + 1 + i;
            kit_txn(
                &agent,
                &endpoint,
                &[json!({"put":{"table":"users","cells":[1,id,2,"CityX",3,1.0]}})],
            )
        })
        .collect();

    // Single update + commit: PK upsert on existing rows 1..=7.
    let update_latencies: Vec<Duration> = (0..N_OPS as i64)
        .map(|i| {
            let pk = i + 1;
            kit_txn(
                &agent,
                &endpoint,
                &[json!({"put":{"table":"users","cells":[1,pk,2,"City",3,99.0 + i as f64]}})],
            )
        })
        .collect();

    // Single delete + commit: tail rows n-6..=n.
    let delete_latencies: Vec<Duration> = (0..N_OPS as i64)
        .map(|i| {
            let pk = n - 6 + i;
            kit_txn(
                &agent,
                &endpoint,
                &[json!({"delete_by_pk":{"table":"users","pk":pk}})],
            )
        })
        .collect();

    println!("### Daemon (warm /kit/txn) — N = {n} (median of {N_OPS})\n");
    println!("| single_insert_commit | single_update_commit | delete_one |");
    println!("|---|---|---|");
    println!(
        "| {} | {} | {} |",
        us(median(insert_latencies)),
        us(median(update_latencies)),
        us(median(delete_latencies)),
    );
    println!();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: bench-daemon <daemon_url> <N>");
        eprintln!("example: bench-daemon http://127.0.0.1:8453 1000000");
        std::process::exit(2);
    }
    let url = &args[1]; // e.g. http://127.0.0.1:8453
    let n: i64 = args[2].parse().expect("N must be an integer");
    bench(url, n);
}
