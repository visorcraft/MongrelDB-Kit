#[test]
fn shared_conformance_suite() {
    conformance_runner::run_conformance().expect("conformance suite failed");
}
