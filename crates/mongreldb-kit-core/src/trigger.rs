use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerSpec {
    pub json: serde_json::Value,
}

impl TriggerSpec {
    pub fn new(json: serde_json::Value) -> Self {
        Self { json }
    }

    pub fn canonical_json(&self) -> String {
        serde_json::to_string(&self.json).unwrap_or_else(|_| "null".into())
    }
}
