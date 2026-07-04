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

#[test]
fn phase1_dml() {
    conformance_runner::run_phase1_dml().expect("phase 1 DML conformance failed");
}

#[test]
fn remote_typed_client() {
    conformance_runner::run_remote().expect("remote conformance failed");
}

#[test]
fn bytes_prefix() {
    conformance_runner::run_bytes_prefix().expect("bytes_prefix conformance failed");
}

#[test]
fn views() {
    conformance_runner::run_views().expect("views conformance failed");
}
