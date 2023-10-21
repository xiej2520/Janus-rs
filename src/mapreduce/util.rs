use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

impl KeyValue {
    pub fn new(key: String, value: String) -> Self {
        KeyValue { key, value }
    }
}

pub type MapFn = fn(&str, &str) -> Vec<KeyValue>;
pub type ReduceFn = fn(&str, Vec<&str>) -> String;
