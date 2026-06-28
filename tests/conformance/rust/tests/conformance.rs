#[test]
fn shared_conformance_suite() {
    conformance_runner::run_conformance().expect("conformance suite failed");
}

#[test]
fn shared_key_encoding() {
    conformance_runner::run_key_encoding().expect("key encoding conformance failed");
}

#[test]
fn migration_failure() {
    conformance_runner::run_migration_failure().expect("migration failure conformance failed");
}
