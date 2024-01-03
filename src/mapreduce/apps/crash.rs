use crate::util::KeyValue;

use rand::Rng;

use std::thread::sleep;
use std::time::Duration;

// sometimes crashes and sometimes takes a long time
// test mapreduce's ability to recover

fn maybe_crash() {
    let mut rng = rand::thread_rng();
    if rng.gen_range(0..1000) < 330 {
        panic!("Worker crashed!");
    } else if rng.gen_range(0..1000) < 660 {
        let ms = rng.gen_range(0..10_000);
        sleep(Duration::from_millis(ms));
    }
}

pub fn map(filename: &str, contents: &str) -> Vec<KeyValue> {
    maybe_crash();

    vec![
        KeyValue::new("a".to_owned(), filename.to_owned()),
        KeyValue::new("b".to_owned(), format!("{}", filename.len())),
        KeyValue::new("c".to_owned(), format!("{}", contents.len())),
        KeyValue::new("d".to_owned(), format!("{}", "xyzzy".to_owned())),
    ]
}

pub fn reduce(_key: &str, mut values: Vec<&str>) -> String {
    maybe_crash();

    values.sort();
    values.join(" ")
}
