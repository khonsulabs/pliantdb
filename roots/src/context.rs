use std::sync::Arc;

use crate::{ChunkCache, FileManager, Vault};

/// A shared environment for database operations.
#[derive(Debug, Clone)]
pub struct Context<M: FileManager> {
    /// The file manager for the [`AsyncFile`](crate::AsyncFile) implementor.
    pub file_manager: M,
    /// The optional vault in use.
    pub vault: Option<Arc<dyn Vault>>,
    /// The optional chunk cache to use.
    pub cache: Option<ChunkCache>,
}

impl<M: FileManager> Context<M> {
    /// Returns the vault as a dynamic reference.
    pub fn vault(&self) -> Option<&dyn Vault> {
        self.vault.as_deref()
    }

    /// Returns the context's chunk cache.
    pub fn cache(&self) -> Option<&ChunkCache> {
        self.cache.as_ref()
    }
}
