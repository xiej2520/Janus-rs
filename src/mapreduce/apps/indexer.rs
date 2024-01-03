use crate::util::KeyValue;

use std::collections::HashSet;

// indexing application

pub fn map(filename: &str, contents: &str) -> Vec<KeyValue> {
    let unique: HashSet<&str> = contents
        .split(|c: char| !char::is_ascii_alphabetic(&c))
        .filter(|&s| !s.is_empty())
        .collect();

    unique
        .iter()
        .map(|&s| KeyValue::new(s.to_owned(), filename.to_owned()))
        .collect()
}

pub fn reduce(_key: &str, mut values: Vec<&str>) -> String {
    values.sort();
    format!("{} {}", values.len(), values.join(","))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::apps::indexer::*;
    use crate::util::KeyValue;

    #[test]
    fn basic_map_reduce() {
        let mut res_map_1 = map(
            "file1",
            "abc def8ghi  jkl!!mn0-op \nqrstuv=\r\twxyz abc abc def",
        );
        res_map_1.sort();
        let mut expected_1 = vec![
            KeyValue::new("abc".to_string(), "file1".to_string()),
            KeyValue::new("def".to_string(), "file1".to_string()),
            KeyValue::new("ghi".to_string(), "file1".to_string()),
            KeyValue::new("jkl".to_string(), "file1".to_string()),
            KeyValue::new("mn".to_string(), "file1".to_string()),
            KeyValue::new("op".to_string(), "file1".to_string()),
            KeyValue::new("qrstuv".to_string(), "file1".to_string()),
            KeyValue::new("wxyz".to_string(), "file1".to_string()),
        ];
        expected_1.sort();

        assert_eq!(res_map_1, expected_1);

        let mut res_map_2 = map("file2", "abc def8!!mn0-op wxyz");
        res_map_2.sort();
        let mut expected_2 = vec![
            KeyValue::new("abc".to_string(), "file2".to_string()),
            KeyValue::new("def".to_string(), "file2".to_string()),
            KeyValue::new("mn".to_string(), "file2".to_string()),
            KeyValue::new("op".to_string(), "file2".to_string()),
            KeyValue::new("wxyz".to_string(), "file2".to_string()),
        ];
        expected_2.sort();

        assert_eq!(res_map_2, expected_2);

        let binding = vec![res_map_1, res_map_2];
        let res_map: Vec<_> = binding.iter().flatten().collect();

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
                ("abc", "2 file1,file2".to_string()),
                ("def", "2 file1,file2".to_string()),
                ("ghi", "1 file1".to_string()),
                ("jkl", "1 file1".to_string()),
                ("mn", "2 file1,file2".to_string()),
                ("op", "2 file1,file2".to_string()),
                ("qrstuv", "1 file1".to_string()),
                ("wxyz", "2 file1,file2".to_string()),
            ]
        );
    }
}
