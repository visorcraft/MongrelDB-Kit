fn main() {
    if let Err(e) = conformance_runner::run_conformance() {
        eprintln!("conformance suite failed: {}", e);
        std::process::exit(1);
    }
    println!("conformance suite passed");
}
