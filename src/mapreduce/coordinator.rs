use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::{transport::Server, Request, Response, Status};

use mapreduce::task_requester_server::{TaskRequester, TaskRequesterServer};
use mapreduce::{TaskReply, WorkerTaskRequest};
use mapreduce::{TaskType, WorkerDoneNotif};

use tokio::sync::{Mutex, Notify};

pub mod mapreduce {
    tonic::include_proto!("mapreduce"); // The string specified here must match the proto package name
}

// Channels to pass messages? Need Mutex<Receiver<_>> to borrow mutably

#[derive(Debug)]
pub struct TaskRequesterState {
    task_data: Arc<Mutex<TaskData>>,
}

#[tonic::async_trait]
impl TaskRequester for TaskRequesterState {
    async fn task_request(
        &self,
        _request: Request<WorkerTaskRequest>,
    ) -> Result<Response<TaskReply>, Status> {
        let mut task_data = self.task_data.lock().await;
        //println!(
        //    "Task request received, not done, {} pending and {} active map tasks, {} pending and {} active reduce tasks.",
        //    task_data.pending_map_tasks.len(),
        //    task_data.active_map_tasks.len(),
        //    task_data.pending_reduce_tasks.len(),
        //    task_data.active_reduce_tasks.len(),
        //);

        while !task_data.map_done {
            //dbg!("Map is not done");
            if !task_data.pending_map_tasks.is_empty() {
                //dbg!("Sending map");
                let task_type = TaskType::Map;
                let file_name = task_data.pending_map_tasks.pop().unwrap();
                let task_num = task_data.map_task_counter;
                let n_reduce = task_data.n_reduce;

                task_data.map_task_counter += 1;
                task_data
                    .active_map_tasks
                    .insert(file_name.clone(), Instant::now());

                return Ok(Response::new(TaskReply {
                    file_name,
                    file_nums: vec![],
                    task_num,
                    task_type: task_type.into(),
                    n_reduce,
                }));
            } else {
                //dbg!("Waiting for all map tasks to finish");
                let notify = task_data.notify.clone();
                drop(task_data);
                notify.notified().await;
                //tokio::time::sleep(Duration::from_secs(10)).await;
                task_data = self.task_data.lock().await;
            }
        }

        while !task_data.reduce_done {
            //dbg!("Reduce is not done");
            if !task_data.pending_reduce_tasks.is_empty() {
                //dbg!("Sending reduce");
                let task_type = TaskType::Reduce as i32;
                let task_num = task_data.pending_reduce_tasks.pop().unwrap();
                task_data
                    .active_reduce_tasks
                    .insert(task_num, Instant::now());

                return Ok(Response::new(TaskReply {
                    file_name: "".into(),
                    file_nums: task_data.finished_map_tasks.clone(),
                    task_num,
                    task_type,
                    n_reduce: -1,
                }));
            } else {
                //dbg!("Waiting for all reduce tasks to finish");
                let notify = task_data.notify.clone();
                drop(task_data);
                notify.notified().await;
                //tokio::time::sleep(Duration::from_secs(10)).await;
                task_data = self.task_data.lock().await;
            }
        }

        let task_type = TaskType::Finished as i32;
        Ok(Response::new(TaskReply {
            file_name: "".into(),
            file_nums: vec![],
            task_num: -1,
            task_type,
            n_reduce: -1,
        }))
    }

    async fn task_done(&self, request: Request<WorkerDoneNotif>) -> Result<Response<()>, Status> {
        let request = request.get_ref();

        match TaskType::try_from(request.task_type).unwrap_or(TaskType::Finished) {
            TaskType::Map => {
                let mut task_data = self.task_data.lock().await;
                //eprintln!("Received map done notification {:?}", request);
                if task_data
                    .active_map_tasks
                    .remove(request.file_name.as_str())
                    .is_some()
                {
                    task_data.finished_map_tasks.push(request.task_num);

                    task_data.map_done = task_data.pending_map_tasks.is_empty()
                        && task_data.active_map_tasks.is_empty();

                    if task_data.map_done {
                        //eprintln!("Map tasks done");
                        task_data.notify.notify_waiters();
                        task_data.pending_reduce_tasks = (0..task_data.n_reduce).collect();
                        //eprintln!(
                        //    "Setting pending reduce tasks {:?}",
                        //    task_data.pending_reduce_tasks
                        //);
                    }
                }
                // otherwise assumed to have failed at some point, ignore
            }
            TaskType::Reduce => {
                //eprintln!("Received reduce done notification {:?}", request);
                let mut task_data = self.task_data.lock().await;
                //eprintln!(
                //    "Pending {:?} active {:?}",
                //    task_data.pending_reduce_tasks, task_data.active_reduce_tasks
                //);
                if task_data
                    .active_reduce_tasks
                    .remove(&request.task_num)
                    .is_some()
                {
                    //eprintln!("Removed reduce task {}", &request.task_num);
                    // push to some list tracking it?
                    task_data.reduce_done = task_data.pending_reduce_tasks.is_empty()
                        && task_data.active_reduce_tasks.is_empty();

                    if task_data.reduce_done {
                        //println!("Reduce tasks done");
                        task_data.notify.notify_waiters();
                    }
                    //eprintln!(
                    //    "Pending {:?} active {:?}",
                    //    task_data.pending_reduce_tasks, task_data.active_reduce_tasks
                    //)
                }
            }
            TaskType::Finished => {
                // should not happen
                eprintln!("Unexpected worker done notification\n{:?}", request);
            }
        }

        Ok(Response::new(()))
    }
}

#[derive(Debug)]
struct TaskData {
    n_reduce: i32,
    pending_map_tasks: Vec<String>,
    active_map_tasks: HashMap<String, Instant>,
    map_task_counter: i32,
    finished_map_tasks: Vec<i32>,
    map_done: bool,

    pending_reduce_tasks: Vec<i32>,
    active_reduce_tasks: HashMap<i32, Instant>,
    reduce_done: bool,

    notify: Arc<Notify>,
}

impl TaskData {
    // move active tasks that have taken too long to pending so other workers
    // can pick them up. Must wake any threads sitting on notify separately
    fn reschedule_active_tasks(&mut self, t: Duration) {
        self.active_map_tasks.retain(|file_name, time_started| {
            if time_started.elapsed() > t {
                self.pending_map_tasks.push(file_name.clone());
                false
            } else {
                true
            }
        });
        self.active_reduce_tasks.retain(|&task_id, time_started| {
            if time_started.elapsed() > t {
                self.pending_reduce_tasks.push(task_id);
                false
            } else {
                true
            }
        });
    }
}

struct Coordinator {
    files: Vec<String>,
    task_data: Arc<Mutex<TaskData>>,
    task_notify: Arc<Notify>,
}

impl Coordinator {
    // n_reduce is number of reduce tasks to use
    fn new(files: Vec<String>, n_reduce: i32) -> Self {
        let task_notify = Arc::new(Notify::new());

        let task_data = TaskData {
            n_reduce,
            pending_map_tasks: Vec::new(),
            active_map_tasks: HashMap::new(),
            map_task_counter: 0,
            finished_map_tasks: Vec::new(),
            map_done: false,
            pending_reduce_tasks: Vec::new(),
            active_reduce_tasks: HashMap::new(),
            reduce_done: false,

            notify: task_notify.clone(),
        };

        Self {
            files,
            task_data: Arc::new(Mutex::new(task_data)),
            task_notify,
        }
    }

    fn server(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = "[::1]:50051".parse()?;

        let task_requester = TaskRequesterState {
            task_data: self.task_data.clone(),
        };

        tokio::spawn(
            Server::builder()
                .add_service(TaskRequesterServer::new(task_requester))
                .serve(addr),
        );

        Ok(())
    }

    async fn run_mr(self) {
        {
            let mut data = self.task_data.lock().await;
            data.pending_map_tasks = self.files.clone();
        }

        // wake up every 10 seconds to check if task is done
        while !self.done().await {
            {
                let mut data = self.task_data.lock().await;

                data.reschedule_active_tasks(Duration::from_secs(10));
                // have to wake up to check if pending tasks were moved
                self.task_notify.notify_waiters();

                //println!(
                //    "Not done, {} pending and {} active map tasks, {} pending and {} active reduce tasks.",
                //    data.pending_map_tasks.len(),
                //    data.active_map_tasks.len(),
                //    data.pending_reduce_tasks.len(),
                //    data.active_reduce_tasks.len(),
                //);
                //dbg!(
                //    &data.pending_map_tasks,
                //    &data.active_map_tasks,
                //    &data.pending_reduce_tasks,
                //    &data.active_reduce_tasks
                //);
            }

            std::thread::sleep(Duration::from_millis(10000));
        }
    }

    async fn done(&self) -> bool {
        let task_data = self.task_data.lock().await;
        task_data.map_done && task_data.reduce_done
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().len() < 2 {
        eprintln!("Usage: mrcoordinator inputfiles...");
        return Err("1".into());
    }

    let argv: Vec<String> = std::env::args().collect();

    let mut m = Coordinator::new(argv[1..].to_vec(), 10);

    m.server()?;
    println!("server launched");

    m.run_mr().await;
    println!("run_mr done");

    Ok(())
}

//grpcurl -plaintext -import-path ./src/mapreduce/proto/ -proto ./src/mapreduce/proto/mr.proto -d '{"name": "Tonic"}' '[::1]:50051' mapreduce.Greeter/SayHello
