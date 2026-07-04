use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualTableSpec {
    pub name: String,
    pub module: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

impl VirtualTableSpec {
    pub fn new(
        name: impl Into<String>,
        module: impl Into<String>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            module: module.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }

    pub fn create_sql(&self) -> String {
        let args = self.args.join(", ");
        if args.is_empty() {
            format!(
                "CREATE VIRTUAL TABLE {} USING {}",
                quote_ident(&self.name),
                quote_ident(&self.module)
            )
        } else {
            format!(
                "CREATE VIRTUAL TABLE {} USING {}({args})",
                quote_ident(&self.name),
                quote_ident(&self.module)
            )
        }
    }

    pub fn drop_sql(&self) -> String {
        format!("DROP TABLE {}", quote_ident(&self.name))
    }
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A SQL view definition (`CREATE VIEW <name> AS <select>`). Views are
/// session-scoped in the engine (not persisted to the catalog), so a view
/// created via a migration lives in the kit's long-lived SQL session for the
/// database's lifetime — mirroring how the daemon and long-lived apps use
/// MongrelDB. See `Database::sql` / `refresh_sql_session`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewSpec {
    pub name: String,
    /// The `AS <select>` body — the full `SELECT ...` statement the view
    /// resolves to. Stored verbatim; not validated here.
    pub sql: String,
}

impl ViewSpec {
    pub fn new(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql: sql.into(),
        }
    }

    /// `CREATE VIEW <name> AS <select>`. The engine's `CREATE VIEW` is
    /// effectively replace-on-write (it overwrites any existing entry), so
    /// this is also used for `ReplaceView`.
    pub fn create_sql(&self) -> String {
        format!("CREATE VIEW {} AS {}", quote_ident(&self.name), self.sql)
    }

    pub fn drop_sql(&self) -> String {
        format!("DROP VIEW IF EXISTS {}", quote_ident(&self.name))
    }
}
