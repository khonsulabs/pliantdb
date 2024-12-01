use bonsaidb_core::{
    connection::Connection, document::CollectionDocument, keyvalue::KeyValue, pubsub::PubSub,
};

use crate::{
    orchestrator::{Backend, Strategy},
    queue::{self, QueueId},
    schema,
};

pub struct Config {
    pub tiers: Vec<JobTier>,
}

pub struct JobTier(pub Vec<QueueId>);

pub struct PriorityFifo;

impl Strategy for PriorityFifo {
    type WorkerConfig = Config;
    type Worker = Config;

    fn initialize_worker<Database: Connection + PubSub + KeyValue>(
        &mut self,
        mut config: Self::WorkerConfig,
        backend: &mut Backend<Database>,
    ) -> Result<Self::Worker, queue::Error> {
        for tier in &mut config.tiers {
            for queue in &mut tier.0 {
                queue.resolve(backend.database())?;
            }
        }
        Ok(config)
    }

    fn dequeue_for_worker<Database: Connection + PubSub + KeyValue>(
        &mut self,
        worker: &Self::WorkerConfig,
        backend: &mut Backend<Database>,
    ) -> Result<Option<CollectionDocument<schema::Job>>, queue::Error> {
        for tier in &worker.tiers {
            if let Some((queue_with_oldest_job, _)) = tier
                .0
                .iter()
                .filter_map(|q| {
                    backend
                        .queue(q.as_id().unwrap())
                        .and_then(|jobs| jobs.front().map(|j| (q, j.clone())))
                })
                .max_by(|(_, q1_front), (_, q2_front)| {
                    q1_front
                        .contents
                        .enqueued_at
                        .cmp(&q2_front.contents.enqueued_at)
                })
            {
                return Ok(backend
                    .queue(queue_with_oldest_job.as_id().unwrap())
                    .unwrap()
                    .pop_front());
            }
        }

        Ok(None)
    }
}
