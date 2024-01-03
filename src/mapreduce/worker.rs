use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::io::Write;

mod apps;
mod util;

use apps::{get_app, MapFn, ReduceFn};
use util::KeyValue;

use mapreduce::task_requester_client::TaskRequesterClient;
use mapreduce::{TaskReply, TaskType, WorkerDoneNotif, WorkerTaskRequest};

fn ihash(key: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish().try_into().unwrap()
}

pub mod mapreduce {
    tonic::include_proto!("mapreduce");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().len() != 2 {
        eprintln!("Usage: mrworker mapreduce_app");
        return Err("1".into());
    }
    let (map_fn, reduce_fn) = get_app(&std::env::args().nth(1).unwrap());

    worker(map_fn, reduce_fn).await;

    Ok(())
}

// impl here
async fn worker(map_fn: MapFn, reduce_fn: ReduceFn) {
    let mut client = TaskRequesterClient::connect("http://[::1]:50051")
        .await
        .unwrap();

    // loop until coordinator sends finished task
    loop {
        let request = tonic::Request::new(WorkerTaskRequest {});
        let response = client.task_request(request).await.unwrap();

        //dbg!(response.get_ref());

        let TaskReply {
            task_type,
            file_name,
            file_nums,
            task_num,
            n_reduce,
        } = response.into_inner();

        //dbg!(&file_name, &file_nums, task_num, task_type, n_reduce);

        match TaskType::try_from(task_type).expect("Unexpected task type") {
            TaskType::Map => {
                // execute map operation
                let mut file = std::fs::File::open(file_name.as_str()).unwrap();
                let mut contents = String::new();
                file.read_to_string(&mut contents).unwrap();

                let res_map = map_fn(file_name.as_str(), contents.as_str());

                let res_part = partition(res_map, n_reduce as usize);

                if let Err(e) = write_to_files(format!("mr-out-{}-", task_num), res_part) {
                    eprintln!("Task {} failed to write to files:\n{}", task_num, e);
                };

                let request = tonic::Request::new(WorkerDoneNotif {
                    task_type,
                    task_num,
                    file_name,
                });
                let _response = client.task_done(request).await.unwrap();
            }
            TaskType::Reduce => {
                // execute reduce operation

                // read all files with the reduce task number
                let mut parsed_kv = vec![];
                for &i in &file_nums {
                    let mut file =
                        std::fs::File::open(format!("mr-out-{}-{}", i, task_num)).unwrap();
                    let mut contents = String::new();
                    file.read_to_string(&mut contents).unwrap();
                    parsed_kv.extend(serde_json::from_str::<Vec<KeyValue>>(&contents).unwrap());
                }

                // group values by key
                let grouped = parsed_kv.iter().fold(HashMap::new(), |mut map, kv| {
                    map.entry(kv.key.as_str())
                        .or_insert(Vec::new())
                        .push(kv.value.as_str());
                    map
                });

                // run reduce and write to output file
                let mut outf = File::create(format!("mr-out-{}", task_num)).unwrap();
                for (key, values) in grouped.into_iter() {
                    let output = reduce_fn(key, values);
                    writeln!(outf, "{} {}", key, output).unwrap();
                }

                let request = tonic::Request::new(WorkerDoneNotif {
                    task_type,
                    task_num,
                    file_name,
                });
                let _response = client.task_done(request).await.unwrap();

                // delete temporary map files (for script to not see them?)
                for i in file_nums {
                    let file_name = format!("mr-out-{}-{}", i, task_num);
                    std::fs::remove_file(file_name.as_str())
                        .expect(format!("Failed to delete file {}", file_name).as_str());
                }
            }
            TaskType::Finished => {
                // exit
                break;
            }
        }
    }
    eprintln!("Worker exited");
}

fn partition(keyvals: Vec<KeyValue>, n: usize) -> Vec<Vec<KeyValue>> {
    let mut res = Vec::with_capacity(n);
    for _ in 0..n {
        res.push(Vec::with_capacity(keyvals.len() / n));
    }

    for keyval in keyvals {
        res[ihash(keyval.key.as_str()) % n].push(keyval);
    }

    res
}

fn write_to_files(base_file_name: String, partition: Vec<Vec<KeyValue>>) -> std::io::Result<()> {
    for (i, part) in partition.iter().enumerate() {
        let file_name = base_file_name.clone() + &i.to_string();
        std::fs::write(&file_name, serde_json::to_string(part)?)?;
    }
    Ok(())
}
