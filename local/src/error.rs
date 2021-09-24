use std::{convert::Infallible, sync::Arc};

use bonsaidb_core::schema::{view, InvalidNameError};
use bonsaidb_roots::AbortError;

use crate::vault;

/// Errors that can occur from interacting with storage.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error occurred interacting with the storage layer, `bonsaidb_roots`.
    #[error("error from storage: {0}")]
    Roots(#[from] bonsaidb_roots::Error),

    /// An error occurred serializing the underlying database structures.
    #[error("error while serializing internal structures: {0}")]
    InternalSerialization(#[from] bincode::Error),

    /// An error occurred serializing the contents of a `Document` or results of a `View`.
    #[error("error while serializing: {0}")]
    Serialization(#[from] serde_cbor::Error),

    /// An internal error occurred while waiting for a message.
    #[error("error while waiting for a message: {0}")]
    InternalCommunication(#[from] flume::RecvError),

    /// An error occurred while executing a view
    #[error("error from view: {0}")]
    View(#[from] view::Error),

    /// An error occurred in the secrets storage layer.
    #[error("a vault error occurred: {0}")]
    Vault(#[from] vault::Error),

    /// An core error occurred.
    #[error("a core error occurred: {0}")]
    Core(#[from] bonsaidb_core::Error),

    /// An unexpected error occurred.
    #[error("an unexpected error occurred: {0}")]
    Other(#[from] Arc<anyhow::Error>),
}

impl From<Error> for bonsaidb_core::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Core(core) => core,
            other => Self::Database(other.to_string()),
        }
    }
}

impl From<InvalidNameError> for Error {
    fn from(err: InvalidNameError) -> Self {
        Self::Core(bonsaidb_core::Error::from(err))
    }
}

impl From<AbortError<Infallible>> for Error {
    fn from(err: AbortError<Infallible>) -> Self {
        match err {
            AbortError::Roots(error) => Self::Roots(error),
            AbortError::Other(_) => unreachable!(),
        }
    }
}

#[test]
fn test_converting_error() {
    use serde::ser::Error as _;
    let err: bonsaidb_core::Error =
        Error::Serialization(serde_cbor::Error::custom("mymessage")).into();
    match err {
        bonsaidb_core::Error::Database(storage_error) => {
            assert!(storage_error.contains("mymessage"));
        }
        _ => unreachable!(),
    }
}
