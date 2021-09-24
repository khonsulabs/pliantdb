use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
};

use async_trait::async_trait;
use bonsaidb_core::kv::Timestamp;
use bonsaidb_jobs::Job;
use bonsaidb_roots::{tree::KeyEvaluation, Buffer, StdFile};

use crate::{
    database::kv::{Entry, TreeKey},
    Storage,
};

#[derive(Debug)]
pub struct ExpirationUpdate {
    pub tree_key: TreeKey,
    pub expiration: Option<Timestamp>,
}

#[allow(clippy::needless_pass_by_value)]
pub fn expiration_thread(
    updates: flume::Receiver<ExpirationUpdate>,
    roots: bonsaidb_roots::Roots<StdFile>,
) -> Result<(), bonsaidb_roots::Error> {
    // expiring_keys will be maintained such that the soonest expiration is at the front and furthest in the future is at the back
    let mut tracked_keys = HashMap::<TreeKey, Timestamp>::new();
    let mut expiration_order = VecDeque::<TreeKey>::new();
    loop {
        let update = if expiration_order.is_empty() {
            // No keys are currently tracked for expiration. Block until we receive a key to track.
            match updates.recv() {
                Ok(update) => update,
                Err(_) => break,
            }
        } else {
            // Check to see if we have any remaining time before a key expires
            let timeout = tracked_keys.get(&expiration_order[0]).unwrap();
            let now = Timestamp::now();
            let remaining_time = *timeout - now;
            let received_update = if let Some(remaining_time) = remaining_time {
                // Allow flume to receive updates for the remaining time.
                match updates.recv_timeout(remaining_time) {
                    Ok(update) => Ok(update),
                    Err(flume::RecvTimeoutError::Timeout) => Err(()),
                    Err(flume::RecvTimeoutError::Disconnected) => break,
                }
            } else {
                Err(())
            };

            // If we've received an update, we bubble it up to process
            if let Ok(update) = received_update {
                update
            } else {
                // Reaching this block means that we didn't receive an update to
                // process, and we have at least one key that is ready to be
                // removed.
                while !expiration_order.is_empty()
                    && tracked_keys.get(&expiration_order[0]).unwrap() <= &now
                {
                    let key_to_remove = expiration_order.pop_front().unwrap();
                    tracked_keys.remove(&key_to_remove);
                    let tree = roots.tree(key_to_remove.tree.clone())?;
                    println!("removing {:?}", key_to_remove);
                    tree.remove(key_to_remove.key.as_bytes())?;
                }
                continue;
            }
        };

        if let Some(expiration) = update.expiration {
            let key = if tracked_keys.contains_key(&update.tree_key) {
                // Update the existing entry.
                let existing_entry_index = expiration_order
                    .iter()
                    .enumerate()
                    .find_map(|(index, key)| {
                        if &update.tree_key == key {
                            Some(index)
                        } else {
                            None
                        }
                    })
                    .unwrap();
                expiration_order.remove(existing_entry_index).unwrap()
            } else {
                update.tree_key.clone()
            };

            // Insert the key into the expiration_order queue
            let mut insert_at = None;
            for (index, expiring_key) in expiration_order.iter().enumerate() {
                if tracked_keys.get(expiring_key).unwrap() > &expiration {
                    insert_at = Some(index);
                    break;
                }
            }
            if let Some(insert_at) = insert_at {
                expiration_order.insert(insert_at, key.clone());
            } else {
                expiration_order.push_back(key.clone());
            }
            tracked_keys.insert(key, expiration);
        } else if tracked_keys.remove(&update.tree_key).is_some() {
            let index = expiration_order
                .iter()
                .enumerate()
                .find_map(|(index, key)| {
                    if &update.tree_key == key {
                        Some(index)
                    } else {
                        None
                    }
                })
                .unwrap();
            expiration_order.remove(index);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bonsaidb_core::test_util::{TestDirectory, TimingTest};
    use bonsaidb_roots::StdFile;
    use futures::Future;

    use super::*;

    async fn run_test<
        F: FnOnce(flume::Sender<ExpirationUpdate>, bonsaidb_roots::Roots<StdFile>) -> R + Send,
        R: Future<Output = anyhow::Result<()>> + Send,
    >(
        name: &str,
        test_contents: F,
    ) -> anyhow::Result<()> {
        let dir = TestDirectory::new(name);
        let sled = bonsaidb_roots::Config::new(&dir).open()?;

        let (sender, receiver) = flume::unbounded();
        let task_sled = sled.clone();
        let expiration_task =
            tokio::task::spawn_blocking(move || expiration_thread(receiver, task_sled));
        let checking_task = test_contents(sender, sled);

        // The expiration task is expected to run as long as the test_contents
        // future is running (or if the test drops the sender it can exit
        // early).
        let (r1, r2) = tokio::join!(expiration_task, checking_task);
        r1??;
        r2?;

        Ok(())
    }

    #[tokio::test]
    async fn basic_expiration() -> anyhow::Result<()> {
        run_test("kv-basic-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree("db.kv.atree")?;
                let tree = sled.tree("db.kv.atree")?;
                tree.set(b"akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender.send(ExpirationUpdate {
                    tree_key: TreeKey::new("db", "atree", String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_millis(100)),
                })?;
                if !timing.wait_until(Duration::from_secs(1)).await {
                    println!("basic_expiration restarting due to timing discrepency");
                    continue;
                }
                assert!(tree.get(b"akey")?.is_none());
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn updating_expiration() -> anyhow::Result<()> {
        run_test("kv-updating-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree("db.kv.atree")?;
                let tree = sled.tree("db.kv.atree")?;
                tree.set(b"akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender.send(ExpirationUpdate {
                    tree_key: TreeKey::new("db", "atree", String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_millis(100)),
                })?;
                sender.send(ExpirationUpdate {
                    tree_key: TreeKey::new("db", "atree", String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_secs(1)),
                })?;
                if timing.elapsed() > Duration::from_millis(100)
                    || !timing.wait_until(Duration::from_millis(500)).await
                {
                    continue;
                }
                assert!(tree.get(b"akey")?.is_some());

                timing.wait_until(Duration::from_secs_f32(1.5)).await;
                assert_eq!(tree.get(b"akey")?, None);
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn multiple_keys_expiration() -> anyhow::Result<()> {
        run_test("kv-multiple-keys-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree("db.kv.atree")?;
                let tree = sled.tree("db.kv.atree")?;
                tree.set(b"akey", b"somevalue")?;
                tree.set(b"bkey", b"somevalue")?;

                let timing = TimingTest::new(Duration::from_millis(100));
                sender.send(ExpirationUpdate {
                    tree_key: TreeKey::new("db", "atree", String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_millis(100)),
                })?;
                sender.send(ExpirationUpdate {
                    tree_key: TreeKey::new("db", "atree", String::from("bkey")),
                    expiration: Some(Timestamp::now() + Duration::from_secs(1)),
                })?;

                if !timing.wait_until(Duration::from_millis(200)).await {
                    continue;
                }

                assert!(tree.get(b"akey")?.is_none());
                assert!(tree.get(b"bkey")?.is_some());
                timing.wait_until(Duration::from_millis(1100)).await;
                assert!(tree.get(b"bkey")?.is_none());

                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn clearing_expiration() -> anyhow::Result<()> {
        run_test("kv-clearing-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree("db.kv.atree")?;
                let tree = sled.tree("db.kv.atree")?;
                tree.set(b"akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender.send(ExpirationUpdate {
                    tree_key: TreeKey::new("db", "atree", String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_millis(100)),
                })?;
                sender.send(ExpirationUpdate {
                    tree_key: TreeKey::new("db", "atree", String::from("akey")),
                    expiration: None,
                })?;
                if timing.elapsed() > Duration::from_millis(100) {
                    // Restart, took too long.
                    continue;
                }
                timing.wait_until(Duration::from_millis(150)).await;
                assert!(tree.get(b"akey")?.is_some());
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn out_of_order_expiration() -> anyhow::Result<()> {
        run_test("kv-out-of-order-expiration", |sender, sled| async move {
            let tree = sled.tree("db.kv.atree")?;
            tree.set(b"akey", b"somevalue")?;
            tree.set(b"bkey", b"somevalue")?;
            tree.set(b"ckey", b"somevalue")?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey::new("db", "atree", String::from("akey")),
                expiration: Some(Timestamp::now() + Duration::from_secs(3)),
            })?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey::new("db", "atree", String::from("ckey")),
                expiration: Some(Timestamp::now() + Duration::from_secs(1)),
            })?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey::new("db", "atree", String::from("bkey")),
                expiration: Some(Timestamp::now() + Duration::from_secs(2)),
            })?;
            tokio::time::sleep(Duration::from_millis(1200)).await;
            assert!(tree.get(b"akey")?.is_some());
            assert!(tree.get(b"bkey")?.is_some());
            assert!(tree.get(b"ckey")?.is_none());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"akey")?.is_some());
            assert!(tree.get(b"bkey")?.is_none());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"akey")?.is_none());

            Ok(())
        })
        .await
    }
}

#[derive(Debug)]
pub struct ExpirationLoader {
    pub storage: Storage,
}

#[async_trait]
impl Job for ExpirationLoader {
    type Output = ();

    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        let storage = self.storage.clone();
        let (sender, receiver) = flume::unbounded();

        tokio::task::spawn_blocking(move || {
            // Find all trees that start with <database>.kv.
            for kv_tree in storage
                .data
                .roots
                .tree_names()?
                .into_iter()
                .filter(|t| t.contains(".kv."))
            {
                storage
                    .data
                    .roots
                    .tree(kv_tree.clone())?
                    .scan::<Infallible, _, _, _>(
                        ..,
                        true,
                        |_| KeyEvaluation::ReadData,
                        |key, entry: Buffer<'static>| {
                            if let Ok(entry) = bincode::deserialize::<Entry>(&entry) {
                                if entry.expiration.is_some() {
                                    sender
                                        .send((kv_tree.clone(), key, entry.expiration))
                                        .unwrap();
                                }
                            }

                            Ok(())
                        },
                    )?;
            }

            Result::<(), anyhow::Error>::Ok(())
        });

        while let Ok((tree, key, expiration)) = receiver.recv_async().await {
            self.storage.update_key_expiration(ExpirationUpdate {
                tree_key: TreeKey {
                    tree,
                    key: String::from_utf8(key.to_vec())?,
                },
                expiration,
            });
        }

        Ok(())
    }
}
