use std::fmt::Display;

use bonsaidb_core::{
    async_trait::async_trait,
    connection::Connection,
    document::CollectionDocument,
    schema::{Authority, CollectionName, InsertError, SchemaName, Schematic, SerializedCollection},
};
use serde::{Deserialize, Serialize};

use crate::schema::{
    self,
    queue::{ByOwnerAndName, ViewExt},
};

pub(crate) fn define_collections(schematic: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
    schematic.define_collection::<schema::Queue>()
}

pub struct Queue(CollectionDocument<schema::Queue>);

impl Queue {
    pub async fn find<
        Owner: Into<QueueOwner> + Send,
        Name: Into<String> + Send,
        Database: Connection,
    >(
        owner: Owner,
        name: Name,
        database: Database,
    ) -> Result<Option<Self>, bonsaidb_core::Error> {
        let owner = owner.into();
        let name = name.into();
        let existing = database
            .view::<ByOwnerAndName>()
            .find_queue(&owner, &name)
            .query_with_collection_docs()?;
        Ok(existing
            .documents
            .into_iter()
            .next()
            .map(|(_, doc)| Self(doc)))
    }

    pub async fn create<
        Owner: Into<QueueOwner> + Send,
        Name: Into<String> + Send,
        Database: Connection,
    >(
        owner: Owner,
        name: Name,
        database: Database,
    ) -> Result<Self, bonsaidb_core::Error> {
        schema::Queue {
            name: QueueName::new(owner, name),
        }
        .push_into(&database)
        .map(Self)
        .map_err(|err| err.error)
    }

    #[must_use]
    pub const fn id(&self) -> u64 {
        self.0.header.id
    }

    #[must_use]
    pub const fn name(&self) -> &QueueName {
        &self.0.contents.name
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum QueueOwner {
    Collection(CollectionName),
    Authority(Authority),
    Schema(SchemaName),
    Backend,
}

impl Display for QueueOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueOwner::Collection(collection) => write!(f, "collection.{}", collection),
            QueueOwner::Authority(authority) => write!(f, "authority.{}", authority),
            QueueOwner::Schema(schema) => write!(f, "schema.{}", schema),
            QueueOwner::Backend => f.write_str("backend"),
        }
    }
}

impl From<CollectionName> for QueueOwner {
    fn from(name: CollectionName) -> Self {
        Self::Collection(name)
    }
}

impl From<Authority> for QueueOwner {
    fn from(name: Authority) -> Self {
        Self::Authority(name)
    }
}

impl From<SchemaName> for QueueOwner {
    fn from(name: SchemaName) -> Self {
        Self::Schema(name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct QueueName {
    pub owner: QueueOwner,
    pub name: String,
}

impl QueueName {
    pub fn new<Owner: Into<QueueOwner> + Send, Name: Into<String> + Send>(
        owner: Owner,
        name: Name,
    ) -> Self {
        Self {
            owner: owner.into(),
            name: name.into(),
        }
    }

    #[must_use]
    pub fn format(owner: &QueueOwner, name: &str) -> String {
        let mut string = String::new();
        Self::format_into(owner, name, &mut string).unwrap();
        string
    }

    pub fn format_into(
        owner: &QueueOwner,
        name: &str,
        mut writer: impl std::fmt::Write,
    ) -> Result<(), std::fmt::Error> {
        write!(writer, "{}.{}", owner, name)
    }
}

impl Display for QueueName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.owner, self.name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueueId {
    Id(u64),
    Name(QueueName),
}

impl QueueId {
    pub fn resolve<Resolver: IdResolver>(&mut self, resolver: &Resolver) -> Result<u64, Error> {
        let id = resolver.resolve(self)?;
        *self = Self::Id(id);
        Ok(id)
    }

    #[must_use]
    pub fn as_id(&self) -> Option<u64> {
        if let Self::Id(id) = self {
            Some(*id)
        } else {
            None
        }
    }
}

pub trait IdResolver {
    fn resolve(&self, id: &QueueId) -> Result<u64, Error>;
}

#[async_trait]
impl<Database> IdResolver for Database
where
    Database: Connection,
{
    fn resolve(&self, id: &QueueId) -> Result<u64, Error> {
        match id {
            QueueId::Id(id) => Ok(*id),
            QueueId::Name(name) => {
                let existing = self
                    .view::<ByOwnerAndName>()
                    .find_queue(&name.owner, &name.name)
                    .query()?;
                Ok(existing
                    .into_iter()
                    .next()
                    .map(|mapping| mapping.source.id.deserialize())
                    .transpose()?
                    .ok_or(Error::NotFound)?)
            }
        }
    }
}

impl From<u64> for QueueId {
    fn from(id: u64) -> Self {
        Self::Id(id)
    }
}

impl<'a> From<&'a Queue> for QueueId {
    fn from(queue: &'a Queue) -> Self {
        Self::Name(queue.name().clone())
    }
}

impl From<QueueName> for QueueId {
    fn from(name: QueueName) -> Self {
        Self::Name(name)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("queue not found")]
    NotFound,
    #[error("database error: {0}")]
    Database(#[from] bonsaidb_core::Error),
    #[error("internal communication failure")]
    InternalCommunication,
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::InternalCommunication
    }
}

impl<T> From<flume::SendError<T>> for Error {
    fn from(_: flume::SendError<T>) -> Self {
        Self::InternalCommunication
    }
}

impl From<flume::RecvError> for Error {
    fn from(_: flume::RecvError) -> Self {
        Self::InternalCommunication
    }
}

impl<T> From<InsertError<T>> for Error {
    fn from(err: InsertError<T>) -> Self {
        Self::Database(err.error)
    }
}
