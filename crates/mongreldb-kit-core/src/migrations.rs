//! Migration planning and checksums.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A single schema-migration operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationOp {
    CreateTable { name: String },
    DropTable { name: String },
    AddColumn { table: String, column: String },
    DropColumn { table: String, column: String },
    AlterColumn { table: String, column: String },
    AddIndex { table: String, index: String },
    DropIndex { table: String, index: String },
    AddUnique { table: String, constraint: String },
    DropUnique { table: String, constraint: String },
    AddForeignKey { table: String, constraint: String },
    DropForeignKey { table: String, constraint: String },
    AddCheck { table: String, constraint: String },
    DropCheck { table: String, constraint: String },
    RawSql(String),
}

/// A numbered schema migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Migration {
    pub version: i64,
    pub name: String,
    pub ops: Vec<MigrationOp>,
}

/// JSON-encode a string exactly the way both `serde_json::to_string` and the
/// TypeScript `JSON.stringify` do, so the canonical content below is byte
/// identical across languages.
fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

/// Canonical, language-neutral serialization of a single migration op.
///
/// The key order is fixed (`op` first, then the op's fields) and string values
/// use standard JSON escaping, so TypeScript and Rust produce identical bytes
/// for the same logical op. This is intentionally distinct from the serde wire
/// format used for migration files; it exists only to feed the checksum.
fn canonical_op(op: &MigrationOp) -> String {
    match op {
        MigrationOp::CreateTable { name } => {
            format!(r#"{{"op":"create_table","name":{}}}"#, json_string(name))
        }
        MigrationOp::DropTable { name } => {
            format!(r#"{{"op":"drop_table","name":{}}}"#, json_string(name))
        }
        MigrationOp::AddColumn { table, column } => format!(
            r#"{{"op":"add_column","table":{},"column":{}}}"#,
            json_string(table),
            json_string(column)
        ),
        MigrationOp::DropColumn { table, column } => format!(
            r#"{{"op":"drop_column","table":{},"column":{}}}"#,
            json_string(table),
            json_string(column)
        ),
        MigrationOp::AlterColumn { table, column } => format!(
            r#"{{"op":"alter_column","table":{},"column":{}}}"#,
            json_string(table),
            json_string(column)
        ),
        MigrationOp::AddIndex { table, index } => format!(
            r#"{{"op":"add_index","table":{},"index":{}}}"#,
            json_string(table),
            json_string(index)
        ),
        MigrationOp::DropIndex { table, index } => format!(
            r#"{{"op":"drop_index","table":{},"index":{}}}"#,
            json_string(table),
            json_string(index)
        ),
        MigrationOp::AddUnique { table, constraint } => format!(
            r#"{{"op":"add_unique","table":{},"constraint":{}}}"#,
            json_string(table),
            json_string(constraint)
        ),
        MigrationOp::DropUnique { table, constraint } => format!(
            r#"{{"op":"drop_unique","table":{},"constraint":{}}}"#,
            json_string(table),
            json_string(constraint)
        ),
        MigrationOp::AddForeignKey { table, constraint } => format!(
            r#"{{"op":"add_foreign_key","table":{},"constraint":{}}}"#,
            json_string(table),
            json_string(constraint)
        ),
        MigrationOp::DropForeignKey { table, constraint } => format!(
            r#"{{"op":"drop_foreign_key","table":{},"constraint":{}}}"#,
            json_string(table),
            json_string(constraint)
        ),
        MigrationOp::AddCheck { table, constraint } => format!(
            r#"{{"op":"add_check","table":{},"constraint":{}}}"#,
            json_string(table),
            json_string(constraint)
        ),
        MigrationOp::DropCheck { table, constraint } => format!(
            r#"{{"op":"drop_check","table":{},"constraint":{}}}"#,
            json_string(table),
            json_string(constraint)
        ),
        MigrationOp::RawSql(sql) => {
            format!(r#"{{"op":"raw_sql","sql":{}}}"#, json_string(sql))
        }
    }
}

/// The canonical content string a migration's checksum is computed over.
///
/// Shape: `{"version":<n>,"name":<json>,"ops":[<op>,...]}` with no insignificant
/// whitespace. Editing a migration's body (its ordered ops) changes this string
/// and therefore its checksum, which is what lets drift detection notice tamper.
fn canonical_content(version: i64, name: &str, ops: &[MigrationOp]) -> String {
    let ops_json: Vec<String> = ops.iter().map(canonical_op).collect();
    format!(
        r#"{{"version":{},"name":{},"ops":[{}]}}"#,
        version,
        json_string(name),
        ops_json.join(",")
    )
}

/// Compute a deterministic, content-aware SHA-256 checksum for a migration.
///
/// The checksum covers the version, name, and the ordered list of ops via a
/// single canonical serialization ([`canonical_content`]) that is byte-for-byte
/// identical to the TypeScript kit (`packages/kit/src/migrate.ts`). The same
/// logical migration therefore produces the same checksum in every language,
/// and changing any op changes the checksum.
pub fn migration_checksum(version: i64, name: &str, ops: &[MigrationOp]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical_content(version, name, ops).as_bytes());
    hex::encode(hasher.finalize())
}

/// Migration convenience method.
impl Migration {
    pub fn checksum(&self) -> String {
        migration_checksum(self.version, &self.name, &self.ops)
    }
}

/// Plan the migrations that must be applied.
///
/// `applied` is the list of migrations already recorded in the database.
/// `desired` is the complete, ordered list of migrations defined by the
/// application. Returns references to the pending migrations in version order.
///
/// The function assumes `desired` is sorted by the caller; it returns a sorted
/// subset. If `applied` is empty, all desired migrations are returned.
pub fn plan_migrations<'a>(applied: &[Migration], desired: &'a [Migration]) -> Vec<&'a Migration> {
    let max_applied = applied.iter().map(|m| m.version).max().unwrap_or(i64::MIN);
    let mut pending: Vec<&'a Migration> =
        desired.iter().filter(|m| m.version > max_applied).collect();
    pending.sort_by_key(|m| m.version);
    pending
}

#[cfg(test)]
mod tests {
    use super::*;

    fn migration(version: i64, name: &str) -> Migration {
        Migration {
            version,
            name: name.into(),
            ops: vec![MigrationOp::CreateTable { name: name.into() }],
        }
    }

    #[test]
    fn checksum_is_stable_and_matches_typescript() {
        // This exact hex is also asserted by the TypeScript kit
        // (`packages/kit/src/migrate.test.ts`) for the same logical migration,
        // proving the canonical serialization is byte-identical cross-language.
        assert_eq!(
            migration_checksum(
                1,
                "init",
                &[MigrationOp::CreateTable {
                    name: "users".into()
                }]
            ),
            "fe2f521793591207bd4d8645c2631e4b7ce43e30fe7ea5691a2846c74ea71cc3"
        );
        // A multi-op migration vector (also shared with the TypeScript test).
        assert_eq!(
            migration_checksum(
                2,
                "add_email",
                &[
                    MigrationOp::AddColumn {
                        table: "users".into(),
                        column: "email".into()
                    },
                    MigrationOp::AddUnique {
                        table: "users".into(),
                        constraint: "uq_email".into()
                    }
                ]
            ),
            "5b05a0c349b9c6091e7bd6329a64e2a0e1960a1867471896458de79ca996f2d3"
        );
        // No-ops vector.
        assert_eq!(
            migration_checksum(1, "init", &[]),
            "6408373a4372a2c49859db2a4548ea43308e5ba7dd3609998ca376606cf09757"
        );
        // An alter_column op (also shared with the TypeScript test).
        assert_eq!(
            migration_checksum(
                3,
                "alter_payload_type",
                &[MigrationOp::AlterColumn {
                    table: "weather_cache".into(),
                    column: "payload_json".into()
                }]
            ),
            "eabab2122bc784d989e7b368e93f68d1ba1c08ec82ddd1aa132a94eaf6b5db66"
        );
    }

    #[test]
    fn checksum_changes_with_version_name_or_ops() {
        let base = migration_checksum(
            1,
            "init",
            &[MigrationOp::CreateTable {
                name: "users".into(),
            }],
        );
        // version
        assert_ne!(
            base,
            migration_checksum(
                2,
                "init",
                &[MigrationOp::CreateTable {
                    name: "users".into()
                }]
            )
        );
        // name
        assert_ne!(
            base,
            migration_checksum(
                1,
                "other",
                &[MigrationOp::CreateTable {
                    name: "users".into()
                }]
            )
        );
        // op content (table name changed)
        assert_ne!(
            base,
            migration_checksum(
                1,
                "init",
                &[MigrationOp::CreateTable {
                    name: "accounts".into()
                }]
            )
        );
        // op kind changed
        assert_ne!(
            base,
            migration_checksum(
                1,
                "init",
                &[MigrationOp::DropTable {
                    name: "users".into()
                }]
            )
        );
        // op count changed
        assert_ne!(base, migration_checksum(1, "init", &[]));
    }

    #[test]
    fn plan_migrations_returns_all_when_none_applied() {
        let desired = vec![migration(1, "a"), migration(2, "b")];
        let pending = plan_migrations(&[], &desired);
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].version, 1);
        assert_eq!(pending[1].version, 2);
    }

    #[test]
    fn plan_migrations_skips_applied() {
        let applied = vec![migration(1, "a")];
        let desired = vec![migration(1, "a"), migration(2, "b"), migration(3, "c")];
        let pending = plan_migrations(&applied, &desired);
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].version, 2);
        assert_eq!(pending[1].version, 3);
    }

    #[test]
    fn plan_migrations_returns_empty_when_fully_applied() {
        let migrations = vec![migration(1, "a"), migration(2, "b")];
        let pending = plan_migrations(&migrations, &migrations);
        assert!(pending.is_empty());
    }
}
