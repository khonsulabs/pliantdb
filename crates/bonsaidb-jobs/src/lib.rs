#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    // missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![allow(
    clippy::missing_panics_doc,
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    // clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

use bonsaidb_core::schema::Schematic;

pub fn define_collections(schematic: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
    queue::define_collections(schematic)?;
    job::define_collections(schematic)?;

    Ok(())
}

pub mod fifo;
pub mod job;
pub mod orchestrator;
pub mod queue;
mod schema;
