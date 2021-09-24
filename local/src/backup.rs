//! Local database tool to save and load databases into plain an easy-to-consume
//! filesystem structure.
//!
//! This tool is provided to ensure you always have a way to get your data
//! backed up in a format that's easily consumable. This tool also provides a
//! safety mechanism allowing a path of migrating across underlying storage
//! layer changes that might have no other migration path.
//!
//! To back up an existing local database:
//!
//! ```sh
//! bonsaidb local-backup <database_path> save
//! ```
//!
//! To restore a backup:
//!
//! ```sh
//! bonsaidb local-backup <database_path> load <backup_location>
//! ```

use std::{
    borrow::Cow,
    convert::TryFrom,
    ffi::OsString,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use bonsaidb_core::{
    document::{Document, Header, Revision},
    schema::{CollectionName, Key},
    transaction::Executed,
};
use flume::Receiver;
use structopt::StructOpt;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::{
    config::Configuration,
    database::{document_tree_name, transaction_tree_name},
    Storage,
};

const TRANSACTIONS_FOLDER_NAME: &str = "_transactions";

/// The command line interface for `bonsaidb local-backup`.
#[derive(StructOpt, Debug)]
pub struct Cli {
    /// The path to the database you wish to operate on.
    pub database_path: PathBuf,

    /// The command to execute on the database.
    #[structopt(subcommand)]
    pub subcommand: Command,
}

/// The command to execute.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Exports all of the data into a straightforward file structure.
    ///
    /// This command will create a single folder within `output_directory` named
    /// `output_name`. Within that folder, one subfolder will be created for
    /// each database [`Database`](crate::Database). Inside of each database
    /// folder will be a folder of any transactions (`_transactions`), as well
    /// as one folder for each collection's [`CollectionName`]. The transactions
    /// folder will have one file for each [`Executed`] transaction. Each
    /// collection folder will contain files named
    /// `<Document.header.id>.<Document.header.revision.id>`, and the files will
    /// contain the raw bytes stored inside of the documents. Assuming you're
    /// using the built in Serialization, the data will be in the
    /// [CBOR](https://cbor.io/) format, otherwise it will be the bytes you
    /// stored within the database.
    ///
    /// This format should make it easy to migrate data as well as back it up
    /// using many traditional methods, and should be considered the official
    /// way to do a full export of a database without using the API.
    Save {
        /// The directory to export the data within. The process will create a
        /// subfolder using `output_name`. If omitted, the export is performed
        /// next to the source database.
        output_directory: Option<PathBuf>,

        /// The name of the folder to export the data to. If not specified, the
        /// ".backup" is appended to the source database's name and used.
        output_name: Option<String>,
    },

    /// Loads all of the data from a previously saved backup. Any documents
    /// with the same IDs will be overwritten by the documents in this backup.
    Load {
        /// The path to the previously saved backup.
        backup: PathBuf,
    },
}

impl Command {
    /// Executes the command.
    pub async fn execute(&self, database_path: PathBuf) -> anyhow::Result<()> {
        match self {
            Self::Save {
                output_directory,
                output_name,
            } => {
                self.save(database_path, output_directory, output_name)
                    .await
            }
            Self::Load { backup } => self.load(&database_path, backup).await,
        }
    }

    async fn save(
        &self,
        database_path: PathBuf,
        output_directory: &Option<PathBuf>,
        output_name: &Option<String>,
    ) -> anyhow::Result<()> {
        if !database_path.exists() {
            anyhow::bail!("database_path does not exist");
        }

        let db = Storage::open_local(&database_path, Configuration::default()).await?;

        let output_directory = if let Some(output_directory) = output_directory {
            output_directory.clone()
        } else {
            database_path.parent().map(ToOwned::to_owned).unwrap()
        };
        let output_name = if let Some(output_name) = output_name.clone() {
            PathBuf::from_str(&output_name)?
        } else {
            let mut name = database_path.file_name().unwrap().to_owned();
            name.push(&OsString::from(".backup"));
            PathBuf::from(name)
        };
        let backup_directory = output_directory.join(output_name);

        // use a channel to split receiving documents to save them and writing
        // to disk. We're using a bounded channel to limit RAM usage, since
        // reading will likely be much faster than writing.
        let (sender, receiver) = flume::bounded(100);
        let document_writer = tokio::spawn(write_documents(receiver, backup_directory));
        tokio::task::spawn_blocking::<_, anyhow::Result<()>>(move || {
            for (database, collection_tree) in
                db.roots().tree_names().into_iter().filter_map(|tree| {
                    // Extract the database_endbase name, but also check that it's a collection

                    if let Some(database_end) = tree.windows(2).position(|t| t.starts_with(b"::")) {
                        let database = String::from_utf8(tree[0..database_end].to_vec()).ok()?;
                        if &tree[database_end..database_end + 14] == b"::collection::" {
                            return Some((database, tree));
                        }
                    }
                    None
                })
            {
                println!(
                    "Exporting {}",
                    String::from_utf8(collection_tree.to_vec()).unwrap()
                );

                let collection_name = String::from_utf8(collection_tree.to_vec())?;
                let collection_name =
                    CollectionName::try_from(collection_name.split("::").last().unwrap())?;

                let database = Arc::new(database);
                let tree = db.roots().tree(&collection_tree);
                for result in tree.iter() {
                    let (_, document) = result?;
                    let document = bincode::deserialize::<Document<'_>>(&document)?;
                    sender.send(BackupEntry::Document {
                        database: database.clone(),
                        collection: collection_name.clone(),
                        document: document.to_owned(),
                    })?;
                }

                if let Ok(tree) = db
                    .roots()
                    .open_tree(transaction_tree_name(&database).as_bytes())
                {
                    for row in tree.iter() {
                        let (_, executed) = row?;
                        let transaction = bincode::deserialize::<Executed<'static>>(&executed)?;
                        sender.send(BackupEntry::Transaction {
                            database: database.clone(),
                            transaction,
                        })?;
                    }
                }
            }

            Ok(())
        })
        .await
        .unwrap()
        .unwrap();

        document_writer.await.unwrap()
    }

    async fn load(&self, database_path: &Path, backup: &Path) -> anyhow::Result<()> {
        let storage = Storage::open_local(database_path, Configuration::default()).await?;
        let (sender, receiver) = flume::bounded(100);

        let document_restorer =
            tokio::task::spawn_blocking(|| restore_documents(receiver, storage));

        let mut databases = tokio::fs::read_dir(&backup).await?;
        while let Some(database_folder) = databases.next_entry().await? {
            let database = match database_folder.file_name().to_str() {
                Some(name) => Arc::new(name.to_owned()),
                None => continue,
            };

            let mut collections = tokio::fs::read_dir(&database_folder.path()).await?;
            while let Some(collection_folder) = collections.next_entry().await? {
                let collection_folder = collection_folder.path();
                let collection = collection_folder
                    .file_name()
                    .unwrap()
                    .to_str()
                    .expect("invalid collection name encountered");
                if collection == TRANSACTIONS_FOLDER_NAME {
                    println!("Restoring executed transactions");

                    let mut entries = tokio::fs::read_dir(&collection_folder).await?;
                    while let Some(entry) = entries.next_entry().await? {
                        let path = entry.path();
                        if path.extension() == Some(&OsString::from("cbor")) {
                            let mut file = File::open(&path).await?;
                            let mut contents = Vec::new();
                            file.read_to_end(&mut contents).await?;

                            let transaction = serde_cbor::from_slice(&contents)?;
                            sender.send(BackupEntry::Transaction {
                                database: database.clone(),
                                transaction,
                            })?;
                        }
                    }
                } else {
                    let collection = CollectionName::try_from(collection)?;
                    println!("Restoring {}", collection);

                    let mut entries = tokio::fs::read_dir(&collection_folder).await?;
                    while let Some(entry) = entries.next_entry().await? {
                        let path = entry.path();
                        if path.extension() == Some(&OsString::from("cbor")) {
                            let file_name = path
                                .file_name()
                                .unwrap()
                                .to_str()
                                .expect("invalid file name encountered");
                            let parts = file_name.split('.').collect::<Vec<_>>();
                            let id = parts[0].parse::<u64>()?;
                            let revision = parts[1].parse::<u32>()?;
                            let mut file = File::open(&path).await?;
                            let mut contents = Vec::new();
                            file.read_to_end(&mut contents).await?;

                            let doc = Document {
                                header: Cow::Owned(Header {
                                    id,
                                    revision: Revision::with_id(revision, &contents),
                                    encryption_key: None, // TODO how to deal with restoring encryption from a backup?
                                }),
                                contents: Cow::Owned(contents),
                            };
                            sender
                                .send_async(BackupEntry::Document {
                                    database: database.clone(),
                                    collection: collection.clone(),
                                    document: doc,
                                })
                                .await?;
                        }
                    }
                }
            }
        }

        drop(sender);

        document_restorer.await?
    }
}

enum BackupEntry {
    Document {
        database: Arc<String>,
        collection: CollectionName,
        document: Document<'static>,
    },
    Transaction {
        database: Arc<String>,
        transaction: Executed<'static>,
    },
}

async fn write_documents(receiver: Receiver<BackupEntry>, backup: PathBuf) -> anyhow::Result<()> {
    if !backup.exists() {
        tokio::fs::create_dir(&backup).await?;
    }

    while let Ok(entry) = receiver.recv_async().await {
        match entry {
            BackupEntry::Document {
                database,
                collection,
                document,
            } => {
                let collection_directory =
                    backup.join(database.as_ref()).join(collection.to_string());
                if !collection_directory.exists() {
                    tokio::fs::create_dir_all(&collection_directory).await?;
                }
                let document_path = collection_directory.join(format!(
                    "{}.{}.cbor",
                    document.header.id, document.header.revision.id
                ));
                let mut file = File::create(&document_path).await?;
                file.write_all(&document.contents).await?;
                file.shutdown().await?;
            }
            BackupEntry::Transaction {
                database,
                transaction,
            } => {
                let transactions_directory = backup.join(database.as_ref()).join("_transactions");
                if !transactions_directory.exists() {
                    tokio::fs::create_dir_all(&transactions_directory).await?;
                }
                let document_path = transactions_directory.join(format!("{}.cbor", transaction.id));
                let mut file = File::create(&document_path).await?;
                file.write_all(&serde_cbor::to_vec(&transaction)?).await?;
                file.shutdown().await?;
            }
        }
    }

    Ok(())
}

#[allow(clippy::needless_pass_by_value)] // it's not needless, it's to avoid a borrow that would need to span a 'static lifetime
fn restore_documents(receiver: Receiver<BackupEntry>, storage: Storage) -> anyhow::Result<()> {
    while let Ok(entry) = receiver.recv() {
        match entry {
            BackupEntry::Document {
                database,
                collection,
                document,
            } => {
                let tree = storage
                    .roots()
                    .tree(document_tree_name(&database, &collection));
                tree.set(
                    document.header.id.as_big_endian_bytes()?.as_ref(),
                    bincode::serialize(&document)?,
                )?;
            }
            BackupEntry::Transaction {
                database,
                transaction,
            } => {
                let tree = storage.roots().tree(transaction_tree_name(&database));
                tree.set(
                    transaction.id.as_big_endian_bytes()?.as_ref(),
                    bincode::serialize(&transaction)?,
                )?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use bonsaidb_core::{
        connection::Connection as _,
        test_util::{Basic, TestDirectory},
    };

    use super::*;
    use crate::Database;

    #[tokio::test]
    async fn backup_restore() -> anyhow::Result<()> {
        let backup_destination = TestDirectory::new("backup-restore.bonsaidb.backup");

        // First, create a database that we'll be restoring. `TestDirectory`
        // will automatically erase the database when it drops out of scope,
        // which is why we're creating a nested scope here.
        let test_doc = {
            let database_directory = TestDirectory::new("backup-restore.bonsaidb");
            let db = Database::<Basic>::open_local(&database_directory, Configuration::default())
                .await?;
            let test_doc = db
                .collection::<Basic>()
                .push(&Basic::new("somevalue"))
                .await?;
            drop(db);

            Command::Save {
                output_directory: None,
                output_name: Some(
                    backup_destination
                        .0
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned(),
                ),
            }
            .execute(database_directory.0.clone())
            .await?;
            test_doc
        };

        // `backup_destination` now contains an export of the database, time to try loading it:
        let database_directory = TestDirectory::new("backup-restore.bonsaidb");
        Command::Load {
            backup: backup_destination.0.clone(),
        }
        .execute(database_directory.0.clone())
        .await?;

        let db =
            Database::<Basic>::open_local(&database_directory, Configuration::default()).await?;
        let doc = db
            .get::<Basic>(test_doc.id)
            .await?
            .expect("Backed up document.not found");
        let contents = doc.contents::<Basic>()?;
        assert_eq!(contents.value, "somevalue");

        Ok(())
    }
}
