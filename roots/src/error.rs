use std::fmt::Display;

use thiserror::Error;

/// An error from [`Roots`](crate::Roots).
#[derive(Debug, Error)]
pub enum Error {
    /// An error has occurred. The string contains human-readable error message.
    /// This error is only used in situations where a user is not expected to be
    /// able to recover automatically from the error.
    #[error("{0}")]
    Message(String),
    /// An error occurred while performing IO.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// An unrecoverable data integrity error was encountered.
    #[error("an unrecoverable error with the data on disk has been found: {0}")]
    DataIntegrity(Box<Self>),
    /// A key was too large.
    #[error("key too large")]
    KeyTooLarge,
    /// A multi-key operation did not have its keys ordered.
    #[error("multi-key operation did not have its keys ordered")]
    KeysNotOrdered,
    /// A document ID was too many bytes.
    #[error("document id too large")]
    IdTooLarge,
    /// An internal error occurred. These errors are not intended to be
    /// recoverable and represent some internal error condition.
    #[error("an internal error occurred: {0}")]
    Internal(InternalError),
}

impl Error {
    /// Returns a new [`Error::Message`] instance with the message provided.
    pub(crate) fn message<S: Display>(message: S) -> Self {
        Self::Message(message.to_string())
    }

    pub(crate) fn data_integrity(error: impl Into<Self>) -> Self {
        Self::DataIntegrity(Box::new(error.into()))
    }
}

impl From<&'static str> for Error {
    fn from(message: &'static str) -> Self {
        Self::message(message)
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self::message(message)
    }
}

/// An internal database error.
#[derive(Debug, Error)]
pub enum InternalError {
    #[error("the b-tree header is too large")]
    HeaderTooLarge,
    #[error("the transaction manager has stopped")]
    TransactionManagerStopped,
}
