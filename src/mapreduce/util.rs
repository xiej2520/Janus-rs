use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

impl KeyValue {
    pub fn new(key: String, value: String) -> Self {
        KeyValue { key, value }
    }
}
