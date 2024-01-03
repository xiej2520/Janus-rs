use crate::util::KeyValue;

use crate::apps::mtiming::n_parallel;

pub fn map(_filename: &str, _contents: &str) -> Vec<KeyValue> {
    vec![
        KeyValue::new("a".to_owned(), "1".to_owned()),
        KeyValue::new("b".to_owned(), "1".to_owned()),
        KeyValue::new("c".to_owned(), "1".to_owned()),
        KeyValue::new("d".to_owned(), "1".to_owned()),
        KeyValue::new("e".to_owned(), "1".to_owned()),
        KeyValue::new("f".to_owned(), "1".to_owned()),
        KeyValue::new("g".to_owned(), "1".to_owned()),
        KeyValue::new("h".to_owned(), "1".to_owned()),
        KeyValue::new("i".to_owned(), "1".to_owned()),
        KeyValue::new("j".to_owned(), "1".to_owned()),
    ]
}

pub fn reduce(_key: &str, _values: Vec<&str>) -> String {
    let n = n_parallel("reduce");
    format!("{}", n)
}
