use crate::util::KeyValue;

use rand::Rng;

use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

// counts the number of times map/reduce tasks are run, to test whether jobs
// are assigned multiple times even when there is no failure

// global variable...
static JOB_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn map(_filename: &str, _contents: &str) -> Vec<KeyValue> {
    let pid = std::process::id();

    let file_name = format!("mr-worker-jobcount-{}-{}", pid, JOB_COUNT.load(SeqCst));
    JOB_COUNT.fetch_add(1, SeqCst);

    fs::write(file_name.as_str(), [b'x']).expect("Failed to write jobcount file");

    std::thread::sleep(Duration::from_millis(
        2000 + rand::thread_rng().gen_range(0..3000),
    ));

    vec![KeyValue::new("a".to_owned(), "x".to_owned())]
}

pub fn reduce(_key: &str, _values: Vec<&str>) -> String {
    let files = fs::read_dir(".").unwrap();

    let mut invocations = 0;
    for file in files {
        let path = file.unwrap().path();
        let name = path.file_name().unwrap();
        if name.to_str().unwrap().starts_with("mr-worker-jobcount") {
            invocations += 1;
        }
    }
    format!("{}", invocations)
}
