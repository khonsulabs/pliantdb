use std::{fmt::Display, marker::PhantomData, sync::Arc};

use bonsaidb_core::{
    actionable::async_trait,
    arc_bytes::serde::Bytes,
    connection::Connection,
    document::CollectionDocument,
    keyvalue::Timestamp,
    pubsub::{PubSub, Subscriber},
    schema::{Schematic, SerializedCollection},
    transmog::{Format, OwnedDeserializer},
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{
    orchestrator::{job_topic, Orchestrator, WorkerId},
    schema,
};

pub(crate) fn define_collections(schematic: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
    schematic.define_collection::<schema::Job>()
}

pub struct Job<Q: Queueable>(CollectionDocument<schema::Job>, PhantomData<Q>);

impl<Q: Queueable> From<CollectionDocument<schema::Job>> for Job<Q> {
    fn from(doc: CollectionDocument<schema::Job>) -> Self {
        Self(doc, PhantomData)
    }
}

impl<Q: Queueable> Job<Q> {
    pub fn update<Database: Connection>(
        &mut self,
        database: &Database,
    ) -> Result<bool, bonsaidb_core::Error> {
        match schema::Job::get(self.0.header.id, database)? {
            Some(doc) => {
                self.0 = doc;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn wait_for_result<Database: Connection + PubSub>(
        &mut self,
        database: &Database,
    ) -> JobResult<Q> {
        loop {
            let subscriber = database.create_subscriber()?;
            subscriber.subscribe_to(&job_topic(self.0.header.id))?;
            // Check that the job hasn't completed before we could create the subscriber
            self.update(database)?;
            return if let Some(result) = &self.0.contents.result {
                <Q::Format as OwnedDeserializer<JobResult<Q>>>::deserialize_owned(
                    &Q::format(),
                    result,
                )
            } else {
                // Wait for the subscriber to be notified
                match subscriber.receiver().receive() {
                    Ok(message) => {
                        <Q::Format as OwnedDeserializer<JobResult<Q>>>::deserialize_owned(
                            &Q::format(),
                            &message.payload,
                        )
                    }
                    Err(_) => continue,
                }
            }
            .map_err(|err| bonsaidb_core::Error::Serialization(err.to_string()))?;
        }
    }

    #[must_use]
    pub fn progress(&self) -> &Progress {
        &self.0.contents.progress
    }

    #[must_use]
    pub fn enqueued_at(&self) -> Timestamp {
        self.0.contents.enqueued_at
    }

    #[must_use]
    pub fn cancelled_at(&self) -> Option<Timestamp> {
        self.0.contents.cancelled_at
    }
}

#[allow(type_alias_bounds)]
type JobResult<Q: Queueable> = Result<Option<Q::Output>, Q::Error>;

#[async_trait]
pub trait Queueable: Sized + Send + Sync + std::fmt::Debug {
    type Format: bonsaidb_core::transmog::OwnedDeserializer<Self>
        + bonsaidb_core::transmog::OwnedDeserializer<Result<Option<Self::Output>, Self::Error>>;
    type Output: Send + Sync;
    type Error: From<bonsaidb_core::Error> + Send + Sync;

    fn format() -> Self::Format;
}

#[async_trait]
pub trait Executor {
    type Job: Queueable;

    async fn execute(
        &mut self,
        job: Self::Job,
        progress: &mut ProgressReporter,
    ) -> Result<<Self::Job as Queueable>::Output, <Self::Job as Queueable>::Error>;

    async fn execute_with_progress<Database: Connection + PubSub>(
        &mut self,
        worker_id: WorkerId,
        job: &mut CollectionDocument<schema::Job>,
        orchestrator: &Orchestrator,
    ) -> Result<Option<<Self::Job as Queueable>::Output>, <Self::Job as Queueable>::Error> {
        let (mut executor_handle, mut job_handle) = ProgressReporter::new();
        let payload = Self::Job::format()
            .deserialize_owned(&job.contents.payload)
            .unwrap();
        let mut task = self.execute(payload, &mut executor_handle);

        let result = loop {
            tokio::select! {
                output = &mut task => break output.map(Some),
                // TODO have timeout to report to orchestrator with progress
                progress = job_handle.receiver.changed() => {
                    progress.unwrap();
                    // TODO throttle progress changes
                    let progress = job_handle.receiver.borrow_and_update().clone();
                    // TODO properly handle errors. They shouldn't kill the
                    // worker, as the job could complete and communication could
                    // be restored.
                    drop(job_handle.cancel.send(orchestrator.report_progress(worker_id, progress).await.unwrap()));
                }
            }
        };

        let result_bytes = Bytes::from(
            <Self::Job as Queueable>::format()
                .serialize(&result)
                .map_err(|err| bonsaidb_core::Error::Serialization(err.to_string()))?,
        );
        // TODO error handling
        orchestrator
            .complete_job(worker_id, result_bytes)
            .await
            .unwrap();

        result
    }
}

#[derive(Default, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct Progress {
    pub updated_at: Timestamp,
    pub message: Option<Arc<String>>,
    pub step: ProgressStep,
    pub total_steps: u64,
}

#[derive(Default, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct ProgressStep {
    pub name: Option<Arc<String>>,
    pub index: u64,
    pub completion: StepCompletion,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum StepCompletion {
    Indeterminite,
    Percent(u8),
    Count { index: u64, total_steps: u64 },
    Complete,
}

impl Default for StepCompletion {
    fn default() -> Self {
        Self::Indeterminite
    }
}

#[derive(Debug)]
pub struct ProgressReporter {
    current: Progress,
    sender: watch::Sender<Progress>,
    cancel: watch::Receiver<Option<Timestamp>>,
}

struct ProgressReceiver {
    receiver: watch::Receiver<Progress>,
    cancel: watch::Sender<Option<Timestamp>>,
}

impl ProgressReporter {
    fn new() -> (Self, ProgressReceiver) {
        let (sender, receiver) = watch::channel(Progress::default());
        let (cancel_sender, cancel_receiver) = watch::channel(None);
        (
            Self {
                sender,
                cancel: cancel_receiver,
                current: Progress::default(),
            },
            ProgressReceiver {
                receiver,
                cancel: cancel_sender,
            },
        )
    }

    pub fn cancelled_at(&mut self) -> Option<Timestamp> {
        *self.cancel.borrow_and_update()
    }

    pub fn set_message(&mut self, message: impl Display) {
        let message = message.to_string();
        if self.current.message.as_deref() != Some(&message) {
            self.current.message = Some(Arc::new(message));
            self.sender.send(self.current.clone()).unwrap();
        }
    }

    pub fn clear_message(&mut self) {
        if self.current.message.is_some() {
            self.current.message = None;
            self.sender.send(self.current.clone()).unwrap();
        }
    }

    pub fn set_total_steps(&mut self, steps: u64) {
        if self.current.total_steps != steps {
            self.current.total_steps = steps;
            self.sender.send(self.current.clone()).unwrap();
        }
    }

    pub fn set_step(&mut self, step: u64) {
        if self.current.step.index != step {
            self.current.step.index = step;
            self.current.step.name = None;
            self.current.step.completion = StepCompletion::Indeterminite;
            self.sender.send(self.current.clone()).unwrap();
        }
    }

    pub fn set_step_with_name(&mut self, step: u64, name: impl Display) {
        if self.current.step.index != step {
            self.current.step.index = step;
            self.current.step.name = Some(Arc::new(name.to_string()));
            self.current.step.completion = StepCompletion::Indeterminite;
            self.sender.send(self.current.clone()).unwrap();
        }
    }

    pub fn set_step_completion(&mut self, completion: StepCompletion) {
        if self.current.step.completion != completion {
            self.current.step.completion = completion;
            self.sender.send(self.current.clone()).unwrap();
        }
    }

    pub fn set_step_percent_complete(&mut self, percent: f32) {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let percent = StepCompletion::Percent((percent.clamp(0., 1.) * 256.).floor() as u8);
        if self.current.step.completion != percent {
            self.current.step.completion = percent;
            self.sender.send(self.current.clone()).unwrap();
        }
    }

    pub fn set_step_progress(&mut self, index: u64, total_steps: u64) {
        let progress = StepCompletion::Count { index, total_steps };
        if self.current.step.completion != progress {
            self.current.step.completion = progress;
            self.sender.send(self.current.clone()).unwrap();
        }
    }

    pub fn complete_step(&mut self) {
        if self.current.step.completion != StepCompletion::Complete {
            self.current.step.completion = StepCompletion::Complete;
            self.sender.send(self.current.clone()).unwrap();
        }
    }
}
