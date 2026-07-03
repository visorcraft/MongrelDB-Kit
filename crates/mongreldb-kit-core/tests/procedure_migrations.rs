use mongreldb_kit_core::{migration_checksum, MigrationOp, ProcedureSpec};
use serde_json::json;

#[test]
fn procedure_migration_checksum_changes_with_body() {
    let first = MigrationOp::CreateProcedure {
        name: "read_users".into(),
        procedure: ProcedureSpec::new(json!({"name": "read_users", "body": {"steps": []}})),
    };
    let second = MigrationOp::CreateProcedure {
        name: "read_users".into(),
        procedure: ProcedureSpec::new(json!({"name": "read_users", "body": {"steps": ["x"]}})),
    };

    assert_ne!(
        migration_checksum(1, "procedures", &[first]),
        migration_checksum(1, "procedures", &[second])
    );
}
