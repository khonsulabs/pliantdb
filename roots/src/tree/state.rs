use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard};

use super::BTreeRoot;

const UNINITIALIZED_SEQUENCE: u64 = 0;

/// The current state of a tree file. Must be initialized before passing to
/// `TreeFile::new` if the file already exists.
#[derive(Default, Debug, Clone)]
pub struct State<const MAX_ORDER: usize> {
    reader: Arc<RwLock<ActiveState<MAX_ORDER>>>,
    writer: Arc<Mutex<ActiveState<MAX_ORDER>>>,
}

impl<const MAX_ORDER: usize> State<MAX_ORDER> {
    /// Locks the state.
    pub(crate) fn lock(&self) -> MutexGuard<'_, ActiveState<MAX_ORDER>> {
        self.writer.lock()
    }

    /// Locks the state.
    pub(crate) fn read(&self) -> RwLockReadGuard<'_, ActiveState<MAX_ORDER>> {
        self.reader.read()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ActiveState<const MAX_ORDER: usize> {
    pub current_position: u64,
    pub header: BTreeRoot<MAX_ORDER>,
}

impl<const MAX_ORDER: usize> ActiveState<MAX_ORDER> {
    pub const fn initialized(&self) -> bool {
        self.header.sequence != UNINITIALIZED_SEQUENCE
    }

    pub(crate) fn publish(&self, state: &State<MAX_ORDER>) {
        let mut reader = state.reader.write();
        *reader = self.clone();
    }

    pub(crate) fn rollback(&mut self, state: &State<MAX_ORDER>) {
        let reader = state.reader.read();
        self.header = reader.header.clone();
    }
}
