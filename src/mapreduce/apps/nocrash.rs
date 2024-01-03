use crate::util::KeyValue;

// same as crash but doesn't actually crash

pub fn map(filename: &str, contents: &str) -> Vec<KeyValue> {
    vec![
        KeyValue::new("a".to_owned(), filename.to_owned()),
        KeyValue::new("b".to_owned(), format!("{}", filename.len())),
        KeyValue::new("c".to_owned(), format!("{}", contents.len())),
        KeyValue::new("d".to_owned(), format!("{}", "xyzzy".to_owned())),
    ]
}

pub fn reduce(_key: &str, mut values: Vec<&str>) -> String {
    values.sort();
    values.join(" ")
}
