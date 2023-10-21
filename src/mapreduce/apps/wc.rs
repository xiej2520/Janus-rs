use crate::util::KeyValue;

pub fn map(_filename: &str, contents: &str) -> Vec<KeyValue> {
    contents
        .split(|c: char| !char::is_ascii_alphabetic(&c))
        .filter(|&s| !s.is_empty())
        .map(|s| KeyValue::new(s.to_string(), String::from("1")))
        .collect()
}

pub fn reduce(key: &str, values: Vec<&str>) -> String {
    values.len().to_string()
}

#[cfg(test)]
mod tests {
    use crate::mapreduce::{
        apps::wc::{map, reduce},
        KeyValue,
    };
    #[test]
    fn basic_map_reduce() {
        let mapres = map("_", "abc def8ghi  jkl!!mn0-op \nqrstuv=\r\twxyz");
        assert_eq!(
            mapres,
            vec![
                KeyValue::new("abc".to_string(), "1".to_string()),
                KeyValue::new("def".to_string(), "1".to_string()),
                KeyValue::new("ghi".to_string(), "1".to_string()),
                KeyValue::new("jkl".to_string(), "1".to_string()),
                KeyValue::new("mn".to_string(), "1".to_string()),
                KeyValue::new("op".to_string(), "1".to_string()),
                KeyValue::new("qrstuv".to_string(), "1".to_string()),
                KeyValue::new("wxyz".to_string(), "1".to_string()),
            ]
        );

        let redres = reduce("_", mapres.iter().map(|kv| kv.value.as_str()).collect());
        assert_eq!(redres, "8");
    }
}
