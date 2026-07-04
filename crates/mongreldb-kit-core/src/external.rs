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
