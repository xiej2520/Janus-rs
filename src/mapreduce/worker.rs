use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::io::Read;

mod util;

use mapreduce::greeter_client::GreeterClient;
use mapreduce::{HelloReply, HelloRequest};

use util::{KeyValue, MapFn, ReduceFn};

use mapreduce::task_requester_client::TaskRequesterClient;
use mapreduce::WorkerTaskRequest;

fn ihash(key: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish().try_into().unwrap()
}

pub mod mapreduce {
    tonic::include_proto!("mapreduce");
}

mod apps;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().len() != 2 {
        eprintln!("Usage: mrworker [mapreduce_app]");
        return Err("1".into());
    }
    let (map_fn, reduce_fn) = match std::env::args().nth(1).unwrap().as_str() {
        "wc" => (apps::wc::map, apps::wc::reduce),
        other => {
            eprintln!("Invalid app {}, ending.", other);
            panic!()
        }
    };

    worker(map_fn, reduce_fn).await;

    let mut client = GreeterClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(HelloRequest {
        name: "Tonic".into(),
    });

    let response = client.say_hello(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}

// impl here
async fn worker(map_fn: MapFn, reduce_fn: ReduceFn) {
    let mut client = TaskRequesterClient::connect("http://[::1]:50051")
        .await
        .unwrap();

    let request = tonic::Request::new(WorkerTaskRequest {});
    let response = client.task_request(request).await.unwrap();

    println!("RESPONSE={:?}", response);

    let (file_name, task_num, task, n_reduce) = (
        response.get_ref().file_name.clone(),
        response.get_ref().task_num,
        response.get_ref().task.clone(),
        response.get_ref().n_reduce as usize,
    );

    let mut file = std::fs::File::open(file_name.as_str()).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    if task == "map" {
        let res_map = map_fn(file_name.as_str(), contents.as_str());
        //println!("{:?}", res);

        let res_part = partition(res_map, n_reduce);

        write_to_files(format!("out/mr-out-{}-", task_num).into(), res_part);
    } else if task == "reduce" {
        let parsed_kv: Vec<KeyValue> = serde_json::from_str(&contents).unwrap();

        let grouped = parsed_kv.iter().fold(HashMap::new(), |mut map, kv| {
            map.entry(kv.key.as_str())
                .or_insert(Vec::new())
                .push(kv.value.as_str());
            map
        });

        for (key, values) in grouped.into_iter() {
            reduce_fn(key, values);
        }
    }
}

fn partition(keyvals: Vec<KeyValue>, n: usize) -> Vec<Vec<KeyValue>> {
    let mut res = Vec::with_capacity(n);
    for _ in 0..n {
        res.push(Vec::with_capacity(keyvals.len() / n));
    }

    for keyval in keyvals {
        res[ihash(&keyval.key.as_str()) % n].push(keyval);
    }

    res
}

fn write_to_files(base_file_name: String, partition: Vec<Vec<KeyValue>>) {
    for (i, part) in partition.iter().enumerate() {
        std::fs::write(
            base_file_name.clone() + &i.to_string(),
            serde_json::to_string(part).unwrap(),
        )
        .unwrap();
    }
}

fn call_example() {
    //let args = ExampleArgs{};
    let args = HelloRequest {
        name: "Example".into(),
    };

    let reply = call::<_, HelloReply>("Coordinator.Example", args);
    //let reply = match call::<_, HelloReply>("Coordinator.Example", args) {
    //    Ok(res) => res,
    //    Err(err) => eprintln!("call failed")
    //};

    //let res = call("Coordinator.Example", &args, &mut reply);
}

mod rpc;

async fn call<T, R>(rpc_name: &str, args: T) -> Result<R, Box<dyn std::error::Error>> {
    // c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

    let sock_name = rpc::coordinator_socket_name();

    let mut client = GreeterClient::connect(sock_name).await?;

    let request = tonic::Request::new(args);

    Err("1".into())
}
