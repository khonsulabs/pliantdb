use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use super::btree_entry::KeyOperation;
use crate::{Buffer, Error};

/// A tree modification.
#[derive(Debug)]
pub struct Modification<'a, T> {
    /// The transaction ID to store with this change.
    pub transaction_id: u64,
    /// The keys to operate upon.
    pub keys: Vec<Buffer<'a>>,
    /// The operation to perform on the keys.
    pub operation: Operation<'a, T>,
}

impl<'a, T> Modification<'a, T> {
    pub(crate) fn reverse(&mut self) -> Result<(), Error> {
        if self.keys.windows(2).all(|w| w[0] < w[1]) {
            self.keys.reverse();
            if let Operation::SetEach(values) = &mut self.operation {
                values.reverse();
            }
            Ok(())
        } else {
            Err(Error::KeysNotOrdered)
        }
    }
}

/// An operation that is performed on a set of keys.
pub enum Operation<'a, T> {
    /// Sets all keys to the value.
    Set(T),
    /// Sets each key to the corresponding entry in this value. The number of
    /// keys must match the number of values.
    SetEach(Vec<T>),
    /// Removes the keys.
    Remove,
    /// Executes the `CompareSwap`. The original value (or `None` if not
    /// present) is the only argument.
    CompareSwap(CompareSwap<'a, T>),
}

impl<'a, T: Debug> Debug for Operation<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Set(arg0) => f.debug_tuple("Set").field(arg0).finish(),
            Self::SetEach(arg0) => f.debug_tuple("SetEach").field(arg0).finish(),
            Self::Remove => write!(f, "Remove"),
            Self::CompareSwap(_) => f.debug_tuple("CompareSwap").finish(),
        }
    }
}

/// A function that is allowed to check the current value of a key and determine
/// how to operate on it. The first parameter is the key, and the second
/// parameter is the current value, if present.
pub type CompareSwapFn<'a, T> = dyn FnMut(&Buffer<'a>, Option<T>) -> KeyOperation<T> + 'a;

/// A wrapper for a [`CompareSwapFn`].
pub struct CompareSwap<'a, T>(&'a mut CompareSwapFn<'a, T>);

impl<'a, T> CompareSwap<'a, T> {
    /// Returns a new wrapped callback.
    pub fn new<F: FnMut(&Buffer<'_>, Option<T>) -> KeyOperation<T> + 'a>(
        callback: &'a mut F,
    ) -> Self {
        Self(callback)
    }
}

impl<'a, T> Debug for CompareSwap<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompareSwap(dyn FnMut)")
    }
}

impl<'a, T> Deref for CompareSwap<'a, T> {
    type Target = CompareSwapFn<'a, T>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T> DerefMut for CompareSwap<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}
