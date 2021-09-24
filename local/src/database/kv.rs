use async_trait::async_trait;
use bonsaidb_core::{
    kv::{Command, KeyCheck, KeyOperation, KeyStatus, Kv, Numeric, Output, Timestamp, Value},
    schema::Schema,
};
use bonsaidb_roots::{Buffer, CompareAndSwapError, StdFile, Tree};
use serde::{Deserialize, Serialize};

use crate::{storage::kv::ExpirationUpdate, Database, Error};

#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub value: Value,
    pub expiration: Option<Timestamp>,
}

#[async_trait]
impl<DB> Kv for Database<DB>
where
    DB: Schema,
{
    async fn execute_key_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, bonsaidb_core::Error> {
        let task_self = self.clone();
        tokio::task::spawn_blocking(move || match op.command {
            Command::Set {
                value,
                expiration,
                keep_existing_expiration,
                check,
                return_previous_value,
            } => execute_set_operation(
                &key_tree(&task_self.data.name, op.namespace),
                op.key,
                value,
                expiration,
                keep_existing_expiration,
                check,
                return_previous_value,
                &task_self,
            ),
            Command::Get { delete } => execute_get_operation(
                &key_tree(&task_self.data.name, op.namespace),
                &op.key,
                delete,
                &task_self,
            ),
            Command::Delete => execute_delete_operation(
                &key_tree(&task_self.data.name, op.namespace),
                op.key,
                &task_self,
            ),
            Command::Increment { amount, saturating } => execute_increment_operation(
                &key_tree(&task_self.data.name, op.namespace),
                &op.key,
                &task_self,
                &amount,
                saturating,
            ),
            Command::Decrement { amount, saturating } => execute_decrement_operation(
                &key_tree(&task_self.data.name, op.namespace),
                &op.key,
                &task_self,
                &amount,
                saturating,
            ),
        })
        .await
        .unwrap()
    }
}

fn key_tree(database: &str, namespace: Option<String>) -> String {
    format!("{}.kv.{}", database, namespace.unwrap_or_default())
}

#[allow(clippy::too_many_arguments)]
fn execute_set_operation<DB: Schema>(
    tree_name: &str,
    key: String,
    value: Value,
    expiration: Option<Timestamp>,
    keep_existing_expiration: bool,
    check: Option<KeyCheck>,
    return_previous_value: bool,
    db: &Database<DB>,
) -> Result<Output, bonsaidb_core::Error> {
    let kv_tree = db
        .data
        .storage
        .roots()
        .tree(tree_name.to_string())
        .map_err(Error::from)?;

    let mut entry = Entry { value, expiration };
    let mut inserted = false;
    let mut updated = false;
    let previous_value = fetch_and_update_no_copy(&kv_tree, key.as_bytes(), |existing_value| {
        let should_update = match check {
            Some(KeyCheck::OnlyIfPresent) => existing_value.is_some(),
            Some(KeyCheck::OnlyIfVacant) => existing_value.is_none(),
            None => true,
        };
        if should_update {
            updated = true;
            inserted = existing_value.is_none();
            if keep_existing_expiration && !inserted {
                if let Ok(previous_entry) = bincode::deserialize::<Entry>(&existing_value.unwrap())
                {
                    entry.expiration = previous_entry.expiration;
                }
            }
            let entry_vec = bincode::serialize(&entry).unwrap();
            Some(Buffer::from(entry_vec))
        } else {
            existing_value
        }
    })
    .map_err(Error::from)?;

    if updated {
        db.data.storage.update_key_expiration(ExpirationUpdate {
            tree_key: TreeKey {
                tree: tree_name.to_string(),
                key,
            },
            expiration: entry.expiration,
        });
        if return_previous_value {
            if let Some(Ok(entry)) = previous_value.map(|v| bincode::deserialize::<Entry>(&v)) {
                Ok(Output::Value(Some(entry.value)))
            } else {
                Ok(Output::Value(None))
            }
        } else if inserted {
            Ok(Output::Status(KeyStatus::Inserted))
        } else {
            Ok(Output::Status(KeyStatus::Updated))
        }
    } else {
        Ok(Output::Status(KeyStatus::NotChanged))
    }
}

fn execute_get_operation<DB: Schema>(
    tree_name: &str,
    key: &str,
    delete: bool,
    db: &Database<DB>,
) -> Result<Output, bonsaidb_core::Error> {
    let tree = db
        .data
        .storage
        .roots()
        .tree(tree_name.to_string())
        .map_err(Error::from)?;
    let entry = if delete {
        let entry = tree.remove(key.as_bytes()).map_err(Error::from)?;
        if entry.is_some() {
            db.data.storage.update_key_expiration(ExpirationUpdate {
                tree_key: TreeKey::new(&db.data.name, tree_name, key.to_string()),
                expiration: None,
            });
        }
        entry
    } else {
        tree.get(key.as_bytes()).map_err(Error::from)?
    };

    let entry = entry
        .map(|e| bincode::deserialize::<Entry>(&e))
        .transpose()
        .map_err(Error::from)?
        .map(|e| e.value);
    Ok(Output::Value(entry))
}

fn execute_delete_operation<DB: Schema>(
    tree_name: &str,
    key: String,
    db: &Database<DB>,
) -> Result<Output, bonsaidb_core::Error> {
    let tree = db
        .data
        .storage
        .roots()
        .tree(tree_name.to_string())
        .map_err(Error::from)?;
    let value = tree.remove(key.as_bytes()).map_err(Error::from)?;
    if value.is_some() {
        db.data.storage.update_key_expiration(ExpirationUpdate {
            tree_key: TreeKey::new(&db.data.name, tree_name, key),
            expiration: None,
        });

        Ok(Output::Status(KeyStatus::Deleted))
    } else {
        Ok(Output::Status(KeyStatus::NotChanged))
    }
}

fn execute_increment_operation<DB: Schema>(
    tree_name: &str,
    key: &str,
    db: &Database<DB>,
    amount: &Numeric,
    saturating: bool,
) -> Result<Output, bonsaidb_core::Error> {
    execute_numeric_operation(tree_name, key, db, amount, saturating, increment)
}

fn execute_decrement_operation<DB: Schema>(
    tree_name: &str,
    key: &str,
    db: &Database<DB>,
    amount: &Numeric,
    saturating: bool,
) -> Result<Output, bonsaidb_core::Error> {
    execute_numeric_operation(tree_name, key, db, amount, saturating, decrement)
}

fn execute_numeric_operation<DB: Schema, F: Fn(&Numeric, &Numeric, bool) -> Numeric>(
    tree_name: &str,
    key: &str,
    db: &Database<DB>,
    amount: &Numeric,
    saturating: bool,
    op: F,
) -> Result<Output, bonsaidb_core::Error> {
    let tree = db
        .data
        .storage
        .roots()
        .tree(tree_name.to_string())
        .map_err(Error::from)?;

    let mut current = tree.get(key.as_bytes()).map_err(Error::from)?;
    loop {
        let mut entry = current
            .as_ref()
            .map(|current| bincode::deserialize::<Entry>(current))
            .transpose()
            .map_err(Error::from)?
            .unwrap_or(Entry {
                value: Value::Numeric(Numeric::UnsignedInteger(0)),
                expiration: None,
            });

        match entry.value {
            Value::Numeric(existing) => {
                let value = Value::Numeric(op(&existing, amount, saturating));
                entry.value = value.clone();

                let result_bytes = Buffer::from(bincode::serialize(&entry).unwrap());
                match tree.compare_and_swap(key.as_bytes(), current.as_ref(), Some(result_bytes)) {
                    Ok(_) => return Ok(Output::Value(Some(value))),
                    Err(CompareAndSwapError::Conflict(cur)) => {
                        current = cur;
                    }
                    Err(CompareAndSwapError::Error(other)) => {
                        // TODO should roots errors be able to be put in core?
                        return Err(bonsaidb_core::Error::Database(other.to_string()));
                    }
                }
            }
            Value::Bytes(_) => {
                return Err(bonsaidb_core::Error::Database(String::from(
                    "type of stored `Value` is not `Numeric`",
                )))
            }
        }
    }
}

fn increment(existing: &Numeric, amount: &Numeric, saturating: bool) -> Numeric {
    match amount {
        Numeric::Integer(amount) => {
            let existing_value = existing.as_i64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_add(*amount)
            } else {
                existing_value.wrapping_add(*amount)
            };
            Numeric::Integer(new_value)
        }
        Numeric::UnsignedInteger(amount) => {
            let existing_value = existing.as_u64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_add(*amount)
            } else {
                existing_value.wrapping_add(*amount)
            };
            Numeric::UnsignedInteger(new_value)
        }
        Numeric::Float(amount) => {
            let existing_value = existing.as_f64_lossy();
            let new_value = existing_value + *amount;
            Numeric::Float(new_value)
        }
    }
}

fn decrement(existing: &Numeric, amount: &Numeric, saturating: bool) -> Numeric {
    match amount {
        Numeric::Integer(amount) => {
            let existing_value = existing.as_i64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_sub(*amount)
            } else {
                existing_value.wrapping_sub(*amount)
            };
            Numeric::Integer(new_value)
        }
        Numeric::UnsignedInteger(amount) => {
            let existing_value = existing.as_u64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_sub(*amount)
            } else {
                existing_value.wrapping_sub(*amount)
            };
            Numeric::UnsignedInteger(new_value)
        }
        Numeric::Float(amount) => {
            let existing_value = existing.as_f64_lossy();
            let new_value = existing_value - *amount;
            Numeric::Float(new_value)
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct TreeKey {
    pub tree: String,
    pub key: String,
}

impl TreeKey {
    pub fn new(database: &str, tree: &str, key: String) -> Self {
        Self {
            tree: format!("{}.kv.{}", database, tree),
            key,
        }
    }
}

/// Alternative to `sled::Tree::fetch_and_update` that allows avoiding a data
/// copy when storing the existing value.
/// <https://github.com/spacejam/sled/issues/1353>
fn fetch_and_update_no_copy<K, F>(
    tree: &Tree<StdFile>,
    key: K,
    mut f: F,
) -> Result<Option<Buffer<'static>>, bonsaidb_roots::Error>
where
    K: AsRef<[u8]>,
    F: FnMut(Option<Buffer<'static>>) -> Option<Buffer<'static>>,
{
    let key_ref = key.as_ref();
    let mut current = tree.get(key_ref)?;

    loop {
        let next = f(current.clone());
        match tree.compare_and_swap(key_ref, current.as_ref(), next) {
            Ok(()) => return Ok(current),
            Err(CompareAndSwapError::Conflict(cur)) => {
                current = cur;
            }
            Err(CompareAndSwapError::Error(other)) => return Err(other),
        }
    }
}
