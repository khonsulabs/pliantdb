use std::{any::Any, path::PathBuf, sync::Arc};

use lru::LruCache;
use parking_lot::Mutex;

use crate::Buffer;

/// A configurable cache that operates at the "chunk" level.
///
/// While writing databases, individual portions of data are often written as a
/// single chunk. These chunks may be stored encrypted on-disk, but the
/// in-memory cache will be after decryption.
///
/// To keep memory usage low, the maximum size for a cached value can be set. It
/// is important that this value be large enough to fit most B-Tree nodes, and
/// that size will depend on how big the tree grows.
#[derive(Clone, Debug)]
#[must_use]
pub struct ChunkCache {
    max_block_length: usize,
    cache: Arc<Mutex<LruCache<ChunkKey, CacheEntry>>>,
}

pub trait AnySendSync: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

impl<T> AnySendSync for T
where
    T: Any + Send + Sync,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct ChunkKey {
    position: u64,
    file_path: Arc<PathBuf>,
}

impl ChunkCache {
    /// Create a new cache with a maximum number of entries (`capacity`) and
    /// `max_chunk_length`. Any chunks longer than `max_chunk_length` will not
    /// be cached. The maximum memory usage of this cache can be calculated as
    /// `capacity * max_chunk_length`, although the actual memory usage will
    /// likely be much smaller as many chunks are small.
    pub fn new(capacity: usize, max_chunk_length: usize) -> Self {
        Self {
            max_block_length: max_chunk_length,
            cache: Arc::new(Mutex::new(LruCache::new(capacity))),
        }
    }

    /// Adds a new cached chunk for `file_path` at `position`.
    pub fn insert(&self, file_path: Arc<PathBuf>, position: u64, buffer: Buffer<'static>) {
        if buffer.len() <= self.max_block_length {
            let mut cache = self.cache.lock();
            cache.put(
                ChunkKey {
                    position,
                    file_path,
                },
                CacheEntry::Buffer(buffer),
            );
        } else {
            println!("Chunk too big to cache");
        }
    }

    /// Adds a new cached chunk for `file_path` at `position`.
    pub fn replace_with_decoded<T: AnySendSync + 'static>(
        &self,
        file_path: Arc<PathBuf>,
        position: u64,
        value: T,
    ) {
        let mut cache = self.cache.lock();
        if let Some(entry) = cache.peek_mut(&ChunkKey {
            position,
            file_path,
        }) {
            *entry = CacheEntry::Decoded(Arc::new(value));
        }
    }

    /// Looks up a previously read chunk for `file_path` at `position`,
    #[must_use]
    pub fn get(&self, file_path: Arc<PathBuf>, position: u64) -> Option<CacheEntry> {
        let mut cache = self.cache.lock();
        cache
            .get(&ChunkKey {
                position,
                file_path,
            })
            .cloned()
    }
}

#[derive(Clone)]
pub enum CacheEntry {
    Buffer(Buffer<'static>),
    Decoded(Arc<dyn AnySendSync>),
}
