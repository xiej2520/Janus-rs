use crate::util::KeyValue;

// word count app

pub fn map(_filename: &str, contents: &str) -> Vec<KeyValue> {
    contents
        .split(|c: char| !char::is_ascii_alphabetic(&c))
        .filter(|&s| !s.is_empty())
        .map(|s| KeyValue::new(s.to_string(), String::from("1")))
        .collect()
}

pub fn reduce(_key: &str, values: Vec<&str>) -> String {
    values.len().to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::apps::wc::*;
    use crate::util::KeyValue;

    #[test]
    fn basic_map_reduce() {
        let res_map = map(
            "_",
            "abc def8ghi  jkl!!mn0-op \nqrstuv=\r\twxyz abc abc def",
        );
        assert_eq!(
            res_map,
            vec![
                KeyValue::new("abc".to_string(), "1".to_string()),
                KeyValue::new("def".to_string(), "1".to_string()),
                KeyValue::new("ghi".to_string(), "1".to_string()),
                KeyValue::new("jkl".to_string(), "1".to_string()),
                KeyValue::new("mn".to_string(), "1".to_string()),
                KeyValue::new("op".to_string(), "1".to_string()),
                KeyValue::new("qrstuv".to_string(), "1".to_string()),
                KeyValue::new("wxyz".to_string(), "1".to_string()),
                KeyValue::new("abc".to_string(), "1".to_string()),
                KeyValue::new("abc".to_string(), "1".to_string()),
                KeyValue::new("def".to_string(), "1".to_string()),
            ]
        );

        let grouped = res_map.iter().fold(BTreeMap::new(), |mut map, kv| {
            map.entry(kv.key.as_str())
                .or_insert(Vec::new())
                .push(kv.value.as_str());
            map
        });
        let res_red: Vec<_> = grouped
            .into_iter()
            .map(|(k, vs)| (k, reduce(k, vs)))
            .collect();
        assert_eq!(
            res_red,
            vec![
                ("abc", "3".to_string()),
                ("def", "2".to_string()),
                ("ghi", "1".to_string()),
                ("jkl", "1".to_string()),
                ("mn", "1".to_string()),
                ("op", "1".to_string()),
                ("qrstuv", "1".to_string()),
                ("wxyz", "1".to_string()),
            ]
        );
    }
}
