use bonsaidb_core::{
    connection::{self, Connection},
    document::{CollectionDocument, Emit},
    schema::{Collection, CollectionViewSchema, View, ViewMapResult},
};
use serde::{Deserialize, Serialize};

use crate::queue::{QueueName, QueueOwner};

#[derive(Collection, Debug, Serialize, Deserialize)]
#[collection(name = "queues", authority = "bonsaidb", core = bonsaidb_core)]
pub struct Queue {
    pub name: QueueName,
}

#[derive(View, Debug, Clone)]
#[view(name = "by-name", collection = Queue, key = String, core = bonsaidb_core)]
pub struct ByOwnerAndName;

impl CollectionViewSchema for ByOwnerAndName {
    type View = Self;

    fn unique(&self) -> bool {
        true
    }

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        document.header.emit_key(document.contents.name.to_string())
    }
}

pub trait ViewExt: Sized {
    fn find_queue(self, owner: &QueueOwner, name: &str) -> Self;
}

impl<'a, Cn> ViewExt for connection::View<'a, Cn, ByOwnerAndName, String>
where
    Cn: Connection,
{
    fn find_queue(self, owner: &QueueOwner, name: &str) -> Self {
        self.with_key(QueueName::format(owner, name))
    }
}
