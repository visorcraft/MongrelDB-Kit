fn main() {
    if let Err(e) = conformance_runner::run_conformance() {
        eprintln!("conformance suite failed: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = conformance_runner::run_phase1_dml() {
        eprintln!("phase 1 DML conformance failed: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = conformance_runner::run_aggregates() {
        eprintln!("aggregate conformance failed: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = conformance_runner::run_joins() {
        eprintln!("join conformance failed: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = conformance_runner::run_key_encoding() {
        eprintln!("key encoding conformance failed: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = conformance_runner::run_migration_failure() {
        eprintln!("migration failure conformance failed: {}", e);
        std::process::exit(1);
    }
    println!("conformance suite passed");
}
