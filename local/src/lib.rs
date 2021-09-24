//! Local storage backend for `BonsaiDb`.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![cfg_attr(doc, deny(rustdoc::all))]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

/// Configuration options.
pub mod config;
mod database;
mod error;
mod open_trees;
mod storage;
mod tasks;
pub mod vault;
mod views;

#[doc(inline)]
pub use bonsaidb_core as core;

#[cfg(feature = "pubsub")]
pub use self::database::pubsub::Subscriber;
#[cfg(feature = "internal-apis")]
pub use self::storage::OpenDatabase;
pub use self::{database::Database, error::Error, storage::Storage};

// TODO
// #[cfg(feature = "cli")]
// pub mod backup;

#[cfg(test)]
mod tests;
