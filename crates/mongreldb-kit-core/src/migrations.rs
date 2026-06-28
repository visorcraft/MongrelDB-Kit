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

/// Compute a deterministic SHA-256 checksum for a migration.
///
/// The checksum covers `version:name` to mirror the TypeScript kit.
pub fn checksum(name: &str, version: i64) -> String {
    let input = format!("{version}:{name}");
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

/// Migration convenience method.
impl Migration {
    pub fn checksum(&self) -> String {
        checksum(&self.name, self.version)
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
pub fn plan_migrations<'a>(
    applied: &[Migration],
    desired: &'a [Migration],
) -> Vec<&'a Migration> {
    let max_applied = applied.iter().map(|m| m.version).max().unwrap_or(i64::MIN);
    let mut pending: Vec<&'a Migration> = desired
        .iter()
        .filter(|m| m.version > max_applied)
        .collect();
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
    fn checksum_is_stable() {
        assert_eq!(
            checksum("initial", 1),
            "6e9cbbbc7811d726062224b1faf7f8678e36dae90857bda4097ab3c119f8f0b6"
        );
    }

    #[test]
    fn checksum_changes_with_version_or_name() {
        let a = checksum("x", 1);
        let b = checksum("x", 2);
        let c = checksum("y", 1);
        assert_ne!(a, b);
        assert_ne!(a, c);
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
