use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    document::{CollectionDocument, Emit},
    keyvalue::Timestamp,
    schema::{Collection, CollectionViewSchema, View, ViewMapResult},
};
use serde::{Deserialize, Serialize};

use crate::job::Progress;

#[derive(Collection, Clone, Debug, Serialize, Deserialize)]
#[collection(name = "jobs", authority = "bonsaidb", core = bonsaidb_core)]
pub struct Job {
    pub queue_id: u64,
    pub payload: Bytes,
    pub enqueued_at: Timestamp,
    pub progress: Progress,
    pub returned_at: Option<Timestamp>,
    pub cancelled_at: Option<Timestamp>,
    pub result: Option<Bytes>,
}

#[derive(View, Debug, Clone)]
#[view(name = "pending", key = Timestamp, collection = Job, core = bonsaidb_core)]
pub struct PendingJobs;

impl CollectionViewSchema for PendingJobs {
    type View = Self;

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        document.header.emit_key(document.contents.enqueued_at)
    }
}
