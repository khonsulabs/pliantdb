use std::{borrow::Cow, collections::HashSet, hash::Hash, sync::Arc};

use async_trait::async_trait;
use bonsaidb_core::schema::{view, CollectionName, Key, Schema, ViewName};
use bonsaidb_jobs::{Job, Keyed};
use bonsaidb_roots::{tree::KeyEvaluation, StdFile, Tree};

use super::{
    mapper::{Map, Mapper},
    view_document_map_tree_name, view_invalidated_docs_tree_name, view_versions_tree_name, Task,
};
use crate::database::{document_tree_name, Database};

#[derive(Debug)]
pub struct IntegrityScanner<DB> {
    pub database: Database<DB>,
    pub scan: IntegrityScan,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct IntegrityScan {
    pub view_version: u64,
    pub database: Arc<Cow<'static, str>>,
    pub collection: CollectionName,
    pub view_name: ViewName,
}

#[async_trait]
impl<DB> Job for IntegrityScanner<DB>
where
    DB: Schema,
{
    type Output = ();

    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        let documents = self.database.sled().tree(document_tree_name(
            &self.database.data.name,
            &self.scan.collection,
        ))?;

        let view_versions = self.database.sled().tree(view_versions_tree_name(
            &self.database.data.name,
            &self.scan.collection,
        ))?;

        let document_map = self.database.sled().tree(view_document_map_tree_name(
            &self.database.data.name,
            &self.scan.view_name,
        ))?;

        let invalidated_entries = self.database.sled().tree(view_invalidated_docs_tree_name(
            &self.database.data.name,
            &self.scan.view_name,
        ))?;

        let view_name = self.scan.view_name.clone();
        let view_version = self.scan.view_version;
        let roots = self.database.sled().clone();

        let needs_update = tokio::task::spawn_blocking::<_, anyhow::Result<bool>>(move || {
            let document_ids = tree_keys::<u64>(&documents)?;
            let view_is_current_version =
                if let Some(version) = view_versions.get(view_name.to_string().as_bytes())? {
                    if let Ok(version) = u64::from_big_endian_bytes(version.as_slice()) {
                        version == view_version
                    } else {
                        false
                    }
                } else {
                    false
                };

            let missing_entries = if view_is_current_version {
                let stored_document_ids = tree_keys::<u64>(&document_map)?;

                document_ids
                    .difference(&stored_document_ids)
                    .copied()
                    .collect::<HashSet<_>>()
            } else {
                // The view isn't the current version, queue up all documents.
                document_ids
            };

            if !missing_entries.is_empty() {
                // Add all missing entries to the invalidated list. The view
                // mapping job will update them on the next pass.
                let mut transaction =
                    roots.transaction(&[invalidated_entries.name(), view_versions.name()])?;
                let view_versions = transaction.tree(1).unwrap();
                view_versions.set(
                    // TODO This is wasteful
                    view_name.to_string().as_bytes().to_vec(),
                    view_version.as_big_endian_bytes().unwrap().to_vec(),
                )?;
                let invalidated_entries = transaction.tree(0).unwrap();
                for id in &missing_entries {
                    invalidated_entries.set(id.as_big_endian_bytes().unwrap().to_vec(), b"")?;
                }
                transaction.commit()?;

                return Ok(true);
            }

            Ok(false)
        })
        .await??;

        if needs_update {
            let job = self
                .database
                .data
                .storage
                .tasks()
                .jobs
                .lookup_or_enqueue(Mapper {
                    storage: self.database.clone(),
                    map: Map {
                        database: self.database.data.name.clone(),
                        collection: self.scan.collection.clone(),
                        view_name: self.scan.view_name.clone(),
                    },
                })
                .await;
            job.receive().await?.map_err(crate::Error::Other)?;
        }

        self.database
            .data
            .storage
            .tasks()
            .mark_integrity_check_complete(
                self.database.data.name.clone(),
                self.scan.collection.clone(),
                self.scan.view_name.clone(),
            )
            .await;

        Ok(())
    }
}

fn tree_keys<K: Key + Hash + Eq + Clone>(tree: &Tree<StdFile>) -> Result<HashSet<K>, crate::Error> {
    let mut ids = Vec::new();
    tree.scan(
        ..,
        true,
        |key| {
            ids.push(key.clone());
            KeyEvaluation::Skip
        },
        |_, _| Ok(()),
    )?;

    Ok(ids
        .into_iter()
        .map(|key| K::from_big_endian_bytes(&key).map_err(view::Error::KeySerialization))
        .collect::<Result<HashSet<_>, view::Error>>()?)
}

impl<DB> Keyed<Task> for IntegrityScanner<DB>
where
    DB: Schema,
{
    fn key(&self) -> Task {
        Task::IntegrityScan(self.scan.clone())
    }
}

// The reason we use jobs like this is to make sure we can tweak how much is
// happening at any given time.
//
// On the Server level, we'll need to cooperate with all the databases in a
// shared pool of workers. So, we need to come up with a design for the view
// updaters to work within this limitation.
//
// Integrity scan is simple: Have a shared structure on Database that keeps track
// of all integrity scan results. It can check for an existing value and return,
// or make you wait until the job is finished. For views, I suppose the best
// that can be done is a similar approach, but the indexer's output is the last
// transaction id it synced. When a request comes in, a check can be done if
// there are any docs outdated, if so, the client can get the current transaction id
// and ask the ViewScanning service manager to wait until that txid is scanned.
//
// The view can then scan and return the results it finds with confidence it was updated to that time.
// If new requests come in while the current batch is being caught up to,
