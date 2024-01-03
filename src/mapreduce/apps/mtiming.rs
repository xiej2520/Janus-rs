use crate::util::KeyValue;

use core::time;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

// tests that workers execute map tasks in parallel

pub fn n_parallel(phase: &str) -> i32 {
    // create a file so that other workers will see that we're running at the
    // same time as them.
    let pid = std::process::id();

    let my_file_name = format!("mr-worker-{}-{}", phase, pid);
    fs::write(my_file_name.as_str(), [b'x']).expect("Failed to write n_parallel file");

    let files = fs::read_dir(".").unwrap();
    let phase_pat = format!("mr-worker-{}-", phase);

    let mut res = 0;

    for file in files {
        let path = file.unwrap().path();
        let name = path.file_name().unwrap();
        if let Some(name) = name.to_str() {
            // check if file has prefix and convert suffix to number
            if let Some(xpid) = name.strip_prefix(phase_pat.as_str()) {
                if let Ok(xpid) = xpid.parse::<usize>() {
                    // check if process exists by kill signal 0
                    const CHECK_EXISTS: usize = 0;
                    const OK: usize = 0;
                    if unsafe { syscalls::raw_syscall!(syscalls::Sysno::kill, xpid, CHECK_EXISTS) }
                        == OK
                    {
                        res += 1
                    }
                }
            }
        }
    }

    std::thread::sleep(time::Duration::from_secs(1));
    fs::remove_file(my_file_name).unwrap();

    res
}

pub fn map(_filename: &str, _contents: &str) -> Vec<KeyValue> {
    let t0 = SystemTime::now();
    let d = t0.duration_since(UNIX_EPOCH).unwrap();
    let ts = d.as_secs_f64() + d.as_nanos() as f64 / 1e9;

    let pid = std::process::id();

    let n = n_parallel("map");

    vec![
        KeyValue::new(format!("times-{}", pid), format!("{:.1}", ts)),
        KeyValue::new(format!("parallel-{}", pid), format!("{}", n)),
    ]
}

pub fn reduce(_key: &str, mut values: Vec<&str>) -> String {
    values.sort();
    values.join(" ")
}
