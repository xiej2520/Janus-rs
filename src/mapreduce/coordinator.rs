//let sock_name = coordinator_socket_name();
//std::fs::remove_file(sock_name);
//
//let listener = UnixListener::bind(sock_name).unwrap();

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::{transport::Server, Request, Response, Status};

use mapreduce::greeter_server::{Greeter, GreeterServer};
use mapreduce::{HelloReply, HelloRequest};

use mapreduce::task_requester_server::{TaskRequester, TaskRequesterServer};
use mapreduce::{TaskFileName, WorkerTaskRequest};

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

pub mod mapreduce {
    tonic::include_proto!("mapreduce"); // The string specified here must match the proto package name
}
#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<HelloReply>, Status> {
        // Return an instance of type HelloReply
        println!("Got a request: {:?}", request);

        let reply = mapreduce::HelloReply {
            message: format!("Hello {}!", request.into_inner().name).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }
}

// Tonic services run on other threads
// Arc<Mutex<_>> for sharing memory between threads
// Channels to pass messages? Need Mutex<Receiver<_>> to borrow mutably

#[derive(Debug)]
pub struct TaskRequesterState {
    tx: Sender<WorkerTaskRequest>,
    rx: Mutex<Receiver<TaskFileName>>,
}

#[tonic::async_trait]
impl TaskRequester for TaskRequesterState {
    async fn task_request(
        &self,
        request: Request<WorkerTaskRequest>,
    ) -> Result<Response<TaskFileName>, Status> {
        match self.tx.send(request.into_inner()).await {
            Ok(_) => (),
            Err(_) => return Err(Status::unknown("Could not communicate with coordinator.")),
        }

        let mut rx = self.rx.lock().await;
        let reply = rx.recv().await.unwrap();

        Ok(Response::new(reply))
    }
}

struct TaskData {
    map_tasks: HashMap<String, Option<Instant>>, // no self-referential struct
    map_done: bool,
    reduce_tasks: HashMap<i32, Instant>,
    reduce_done: bool,
}

struct Coordinator {
    files: Vec<String>,
    n_reduce: i32,
    task_data: Arc<Mutex<TaskData>>,
}

impl Coordinator {
    // n_reduce is number of reduce tasks to use
    fn new(files: Vec<String>, n_reduce: i32) -> Self {
        let task_data = TaskData {
            map_tasks: HashMap::new(),
            map_done: false,
            reduce_tasks: HashMap::new(),
            reduce_done: false,
        };

        Self {
            files,
            n_reduce,
            task_data: Arc::new(Mutex::new(task_data)),
        }
    }

    fn server(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = "[::1]:50051".parse()?;

        let (task_req_tx, mut task_req_rx) = tokio::sync::mpsc::channel(10);
        let (task_res_tx, task_res_rx) = tokio::sync::mpsc::channel(10);

        let task_requester = TaskRequesterState {
            tx: task_req_tx,
            rx: task_res_rx.into(),
        };
        let greeter = MyGreeter::default();

        tokio::spawn(
            Server::builder()
                .add_service(TaskRequesterServer::new(task_requester))
                .add_service(GreeterServer::new(greeter))
                .serve(addr),
        );

        let data_lock = self.task_data.clone();
        tokio::spawn(async move {
            let data_lock = data_lock.clone();
            while let Some(_req) = task_req_rx.recv().await {
                let data = data_lock.lock().await;
                
                let (file_name, task_num, task, n_reduce);
                if !data.map_done {
                    file_name = "rsrc/pg_dorian_gray.txt".into();
                    task_num = -1;
                    task = "map".into();
                    n_reduce = -1;
                }
                else if !data.reduce_done {
                    file_name = "".into();
                    task_num = -1;
                    task = "reduce".into();
                    n_reduce = -1;
                }
                else {
                    file_name = "".into();
                    task_num = -1;
                    task = "".into();
                    n_reduce = -1;
                }
                task_res_tx
                    .send(TaskFileName {
                        file_name,
                        task_num,
                        task,
                        n_reduce,
                    })
                    .await
                    .unwrap();
            }
        });

        Ok(())
    }

    async fn run_mr(self) {
        {
            let mut data = self.task_data.lock().await;
            for file in self.files.iter() {
                data.map_tasks.insert(file.clone(), None);
            }
        }

        while !self.done() {
            let data = self.task_data.lock().await;
            println!(
                "Not done, {} map tasks, {} reduce tasks to be completed.",
                data.map_tasks.len(),
                data.reduce_tasks.len()
            );

            std::thread::sleep(Duration::from_millis(10000));
        }
    }

    fn done(&self) -> bool {
        false
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().len() < 2 {
        eprintln!("Usage: mrcoordinator inputfiles...");
        return Err("1".into());
    }

    let argv: Vec<String> = std::env::args().collect();
    //println!("{:?}", &argv[1..]);

    let mut m = Coordinator::new(argv[1..].iter().map(|s| s.clone()).collect(), 10);

    m.server()?;

    m.run_mr().await;

    Ok(())
    //let addr = "[::1]:50051".parse()?;
    //let greeter = MyGreeter::default();

    //Server::builder()
    //    .add_service(GreeterServer::new(greeter))
    //    .serve(addr)
    //    .await?;

    //Ok(())
}
//grpcurl -plaintext -import-path ./src/mapreduce/proto/ -proto ./src/mapreduce/proto/mr.proto -d '{"name": "Tonic"}' '[::1]:50051' mapreduce.Greeter/SayHello
