//! Transactional append-only B-Tree storage for `BonsaiDb`.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::nursery,
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

#[macro_use]
mod managed_file;
mod error;
mod roots;
mod transaction;
pub mod tree;
mod vault;

mod buffer;
mod chunk_cache;
mod context;
#[cfg(test)]
mod test_util;

pub use self::{
    buffer::Buffer,
    chunk_cache::ChunkCache,
    context::Context,
    error::Error,
    managed_file::{fs::StdFile, FileManager, ManagedFile},
    roots::{
        AbortError, CompareAndSwapError, Config, ExecutingTransaction, Roots, TransactionTree, Tree,
    },
    transaction::{TransactionLog, TransactionManager},
    vault::Vault,
};
