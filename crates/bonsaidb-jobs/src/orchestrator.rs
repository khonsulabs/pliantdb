use std::collections::{HashMap, VecDeque};

use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    connection::Connection,
    document::CollectionDocument,
    keyvalue::{KeyValue, Timestamp},
    pubsub::PubSub,
    schema::SerializedCollection,
    transmog::Format,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{
    fifo::PriorityFifo,
    job::{Job, Progress, Queueable},
    queue::{self, IdResolver, QueueId, QueueName},
    schema::{self, job::PendingJobs, Queue},
};

#[derive(Clone, Debug)]
pub struct Orchestrator<S = PriorityFifo>
where
    S: Strategy,
{
    sender: flume::Sender<Command<S>>,
}

impl<S> Orchestrator<S>
where
    S: Strategy,
{
    pub fn spawn<Database>(database: Database, strategy: S) -> Self
    where
        Database: Connection + PubSub + KeyValue + 'static,
    {
        let (sender, receiver) = flume::unbounded();
        std::thread::Builder::new()
            .name(String::from("jobs-orchestrator"))
            .spawn(move || ExecutingBackend::run(receiver, database, strategy))
            .unwrap();
        Self { sender }
    }

    pub async fn enqueue<Queue: Into<QueueId> + Send, Payload: Queueable>(
        &self,
        queue: Queue,
        job: &Payload,
    ) -> Result<Job<Payload>, queue::Error> {
        let bytes = Payload::format()
            .serialize(job)
            .map_err(|err| bonsaidb_core::Error::Serialization(err.to_string()))?;
        let (sender, receiver) = oneshot::channel();
        self.sender.send(Command::Enqueue {
            queue: queue.into(),
            payload: Bytes::from(bytes),
            output: sender,
        })?;
        let job = receiver.await??;

        Ok(Job::from(job))
    }

    pub async fn register_worker(&self, config: S::WorkerConfig) -> Result<WorkerId, queue::Error> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(Command::RegisterWorker {
            config,
            output: sender,
        })?;
        receiver.await?
    }

    pub async fn report_progress(
        &self,
        worker: WorkerId,
        progress: Progress,
    ) -> Result<Option<Timestamp>, queue::Error> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(Command::JobProgress {
            worker,
            progress,
            output: sender,
        })?;
        Ok(receiver.await?)
    }

    pub async fn complete_job(&self, worker: WorkerId, result: Bytes) -> Result<(), queue::Error> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(Command::JobComplete {
            worker,
            job_result: result,
            output: sender,
        })?;
        Ok(receiver.await?)
    }
}

enum Command<S: Strategy> {
    Enqueue {
        queue: QueueId,
        payload: Bytes,
        output: oneshot::Sender<Result<CollectionDocument<schema::Job>, queue::Error>>,
    },
    RegisterWorker {
        config: S::WorkerConfig,
        output: oneshot::Sender<Result<WorkerId, queue::Error>>,
    },
    JobProgress {
        worker: WorkerId,
        progress: Progress,
        output: oneshot::Sender<Option<Timestamp>>,
    },
    JobComplete {
        worker: WorkerId,
        job_result: Bytes,
        output: oneshot::Sender<()>,
    },
}

pub struct ExecutingBackend<Database, S>
where
    Database: Connection + PubSub + KeyValue,
    S: Strategy,
{
    receiver: flume::Receiver<Command<S>>,
    backend: Backend<Database>,
    strategy: S,
    workers: HashMap<u64, Worker<S::Worker>>,
    last_worker_id: u64,
}

pub struct Backend<Database>
where
    Database: Connection + PubSub + KeyValue,
{
    database: Database,
    queues_by_name: HashMap<QueueName, CollectionDocument<Queue>>,
    queues: HashMap<u64, VecDeque<CollectionDocument<schema::Job>>>,
}

impl<Database> Backend<Database>
where
    Database: Connection + PubSub + KeyValue,
{
    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn queue(&mut self, queue: u64) -> Option<&mut VecDeque<CollectionDocument<schema::Job>>> {
        self.queues.get_mut(&queue)
    }

    pub fn queue_by_name(
        &mut self,
        queue: &QueueName,
    ) -> Option<&mut VecDeque<CollectionDocument<schema::Job>>> {
        let id = self.queues_by_name.get(queue)?.header.id;
        self.queue(id)
    }
}

impl<Database, S> ExecutingBackend<Database, S>
where
    Database: Connection + PubSub + KeyValue,
    S: Strategy,
{
    fn run(
        receiver: flume::Receiver<Command<S>>,
        database: Database,
        strategy: S,
    ) -> Result<(), queue::Error> {
        let mut queues_by_name = HashMap::new();
        let mut queues = HashMap::new();

        for queue in Queue::all(&database).query()? {
            queues_by_name.insert(queue.contents.name.clone(), queue);
        }

        for (_, job) in database
            .view::<PendingJobs>()
            .query_with_collection_docs()?
            .documents
        {
            let queue = queues
                .entry(job.contents.queue_id)
                .or_insert_with(VecDeque::default);
            queue.push_back(job);
        }

        Self {
            receiver,
            strategy,
            backend: Backend {
                database,
                queues_by_name,
                queues,
            },
            workers: HashMap::new(),
            last_worker_id: 0,
        }
        .orchestrate()
    }

    fn orchestrate(&mut self) -> Result<(), queue::Error> {
        while let Ok(command) = self.receiver.recv() {
            match command {
                Command::Enqueue {
                    queue,
                    payload,
                    output: result,
                } => {
                    drop(result.send(self.enqueue(&queue, payload)));
                }
                Command::RegisterWorker {
                    config,
                    output: result,
                } => {
                    drop(result.send(self.register_worker(config)));
                }
                Command::JobProgress {
                    worker,
                    progress,
                    output: result,
                } => {
                    if let Some(worker) = self.workers.get_mut(&worker.0) {
                        if let Some(job) = &mut worker.current_job {
                            loop {
                                job.contents.progress = progress.clone();
                                match job.update(&self.backend.database) {
                                    Ok(()) => {}
                                    Err(bonsaidb_core::Error::DocumentConflict(..)) => {
                                        if let Some(updated_job) =
                                            schema::Job::get(job.header.id, &self.backend.database)?
                                        {
                                            // Try updating the progress again
                                            *job = updated_job;
                                        } else {
                                            break;
                                        }
                                    }
                                    Err(other) => return Err(queue::Error::from(other)),
                                }
                            }
                            let _ = result.send(job.contents.cancelled_at);
                        }
                    }
                }
                Command::JobComplete {
                    worker,
                    job_result,
                    output,
                } => {
                    if let Some(worker) = self.workers.get_mut(&worker.0) {
                        if let Some(job) = &mut worker.current_job {
                            job.contents.result = Some(job_result.clone());
                            job.contents.returned_at = Some(Timestamp::now());
                            job.update(&self.backend.database)?;

                            self.backend.database.publish_bytes(
                                job_topic(job.header.id).into_bytes(),
                                job_result.0,
                            )?;
                        }
                    }
                    let _ = output.send(());
                }
            }
        }
        Ok(())
    }

    fn enqueue(
        &mut self,
        queue: &QueueId,
        payload: Bytes,
    ) -> Result<CollectionDocument<schema::Job>, queue::Error> {
        let queue_id = self.backend.database.resolve(&queue)?;
        let job = schema::Job {
            queue_id,
            payload,
            enqueued_at: Timestamp::now(),
            progress: Progress::default(),
            result: None,
            returned_at: None,
            cancelled_at: None,
        }
        .push_into(&self.backend.database)?;
        let entries = self
            .backend
            .queues
            .entry(job.contents.queue_id)
            .or_default();
        let insert_at = match entries.binary_search_by(|existing_job| {
            existing_job
                .contents
                .enqueued_at
                .cmp(&job.contents.enqueued_at)
        }) {
            Ok(index) | Err(index) => index,
        };
        entries.insert(insert_at, job.clone());
        Ok(job)
    }

    fn register_worker(&mut self, config: S::WorkerConfig) -> Result<WorkerId, queue::Error> {
        self.last_worker_id += 1;
        self.workers.insert(
            self.last_worker_id,
            Worker {
                current_job: None,
                last_seen: Timestamp::now(),
                strategy_worker: self.strategy.initialize_worker(config, &mut self.backend)?,
            },
        );
        Ok(WorkerId(self.last_worker_id))
    }
}

impl<Database> IdResolver for Backend<Database>
where
    Database: Connection + PubSub + KeyValue,
{
    fn resolve(&self, id: &QueueId) -> Result<u64, queue::Error> {
        match id {
            QueueId::Id(id) => Ok(*id),
            QueueId::Name(name) => self
                .queues_by_name
                .get(name)
                .map(|q| q.header.id)
                .ok_or(queue::Error::NotFound),
        }
    }
}

pub trait Strategy: Sized + Send + Sync + 'static {
    type WorkerConfig: Send + Sync;
    type Worker: Send + Sync;

    fn initialize_worker<Database: Connection + PubSub + KeyValue>(
        &mut self,
        config: Self::WorkerConfig,
        backend: &mut Backend<Database>,
    ) -> Result<Self::Worker, queue::Error>;

    fn dequeue_for_worker<Database: Connection + PubSub + KeyValue>(
        &mut self,
        worker: &Self::Worker,
        backend: &mut Backend<Database>,
    ) -> Result<Option<CollectionDocument<schema::Job>>, queue::Error>;
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct WorkerId(pub(crate) u64);

pub struct Worker<W> {
    current_job: Option<CollectionDocument<schema::Job>>,
    strategy_worker: W,
    last_seen: Timestamp,
}

pub(crate) fn job_topic(id: u64) -> String {
    format!("BONSIADB_JOB_{}_RESULT", id)
}
