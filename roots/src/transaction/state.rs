use std::{
    borrow::Cow,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use lru::LruCache;
use parking_lot::{Mutex, MutexGuard};

use super::{LogEntry, TransactionChanges, TransactionHandle, TreeLock, TreeLocks};

const UNINITIALIZED_ID: u64 = 0;

#[derive(Clone, Debug)]
pub struct State {
    state: Arc<ActiveState>,
}

#[derive(Debug)]
struct ActiveState {
    path: PathBuf,
    current_transaction_id: AtomicU64,
    tree_locks: Mutex<HashMap<Cow<'static, [u8]>, TreeLock>>,
    log_position: Mutex<u64>,
    known_completed_transactions: Mutex<LruCache<u64, bool>>,
}

impl State {
    pub fn from_path(path: impl AsRef<Path>) -> Self {
        Self {
            state: Arc::new(ActiveState {
                path: path.as_ref().to_path_buf(),
                tree_locks: Mutex::default(),
                current_transaction_id: AtomicU64::new(UNINITIALIZED_ID),
                log_position: Mutex::new(0),
                known_completed_transactions: Mutex::new(LruCache::new(1024)),
            }),
        }
    }

    pub fn initialize(&self, current_transaction_id: u64, log_position: u64) {
        let mut state_position = self.state.log_position.lock();
        self.state
            .current_transaction_id
            .compare_exchange(
                UNINITIALIZED_ID,
                current_transaction_id,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .expect("state already initialized");
        *state_position = log_position;
    }

    pub fn current_transaction_id(&self) -> u64 {
        self.state.current_transaction_id.load(Ordering::SeqCst)
    }

    pub fn path(&self) -> &Path {
        &self.state.path
    }

    pub fn len(&self) -> u64 {
        let position = self.state.log_position.lock();
        *position
    }

    fn fetch_tree_locks<'a>(&'a self, trees: &'a [&[u8]], locks: &mut TreeLocks) {
        let mut tree_locks = self.state.tree_locks.lock();
        for tree in trees {
            if let Some(lock) = tree_locks.get(&Cow::Borrowed(*tree)) {
                locks.push(lock.lock());
            } else {
                let lock = TreeLock::new();
                let locked = lock.lock();
                tree_locks.insert(Cow::Owned(tree.to_vec()), lock);
                locks.push(locked);
            }
        }
    }

    pub fn new_transaction(&self, trees: &[&[u8]]) -> TransactionHandle {
        let mut locked_trees = Vec::with_capacity(trees.len());
        self.fetch_tree_locks(trees, &mut locked_trees);

        TransactionHandle {
            locked_trees,
            transaction: LogEntry {
                id: self
                    .state
                    .current_transaction_id
                    .fetch_add(1, Ordering::SeqCst),
                changes: TransactionChanges::default(),
            },
        }
    }

    pub(crate) fn note_transaction_id_status(&self, transaction_id: u64, completed: bool) {
        let mut cache = self.state.known_completed_transactions.lock();
        cache.put(transaction_id, completed);
    }

    pub(crate) fn note_transaction_ids_completed(&self, transaction_ids: &[u64]) {
        let mut cache = self.state.known_completed_transactions.lock();
        for id in transaction_ids {
            cache.put(*id, true);
        }
    }

    pub(crate) fn transaction_id_is_valid(&self, transaction_id: u64) -> Option<bool> {
        let mut cache = self.state.known_completed_transactions.lock();
        cache.get(&transaction_id).copied()
    }
}

impl State {
    pub fn lock_for_write(&self) -> MutexGuard<'_, u64> {
        self.state.log_position.lock()
    }
}
