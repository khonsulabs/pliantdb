use std::{convert::TryFrom, fmt::Debug};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{
    btree_entry::{BTreeEntry, Reducer},
    read_chunk, BinarySerialization, PagedWriter,
};
use crate::{chunk_cache::CacheEntry, Buffer, ChunkCache, Error, ManagedFile, Vault};

#[derive(Clone, Debug)]
pub struct Interior<I, R> {
    // The key with the highest sort value within.
    pub key: Buffer<'static>,
    /// The location of the node.
    pub position: Pointer<I, R>,
    /// The reduced statistics.
    pub stats: R,
}

impl<I, R> From<BTreeEntry<I, R>> for Interior<I, R>
where
    I: Clone + Debug + BinarySerialization + 'static,
    R: Reducer<I> + Clone + Debug + BinarySerialization + 'static,
{
    fn from(entry: BTreeEntry<I, R>) -> Self {
        let key = entry.max_key().clone();
        let stats = entry.stats();

        Self {
            key,
            stats,
            position: Pointer::Loaded {
                previous_location: None,
                entry: Box::new(entry),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub enum Pointer<I, R> {
    OnDisk(u64),
    Loaded {
        previous_location: Option<u64>,
        entry: Box<BTreeEntry<I, R>>,
    },
}

impl<
        I: BinarySerialization + Debug + Clone + 'static,
        R: Reducer<I> + BinarySerialization + Debug + Clone + 'static,
    > Pointer<I, R>
{
    pub fn load<F: ManagedFile>(
        &mut self,
        writer: &mut PagedWriter<'_, F>,
        current_order: usize,
    ) -> Result<(), Error> {
        match self {
            Pointer::OnDisk(position) => {
                let entry = match writer.read_chunk(*position)? {
                    CacheEntry::Buffer(mut buffer) => {
                        // It's worthless to store this node in the cache
                        // because if we mutate, we'll be rewritten.
                        BTreeEntry::deserialize_from(&mut buffer, current_order)?
                    }
                    CacheEntry::Decoded(node) => node
                        .as_ref()
                        .as_any()
                        .downcast_ref::<BTreeEntry<I, R>>()
                        .unwrap()
                        .clone(),
                };
                *self = Self::Loaded {
                    entry: Box::new(entry),
                    previous_location: Some(*position),
                };
            }
            Pointer::Loaded { .. } => {}
        }
        Ok(())
    }

    pub fn get_mut(&mut self) -> Option<&mut BTreeEntry<I, R>> {
        match self {
            Pointer::OnDisk(_) => None,
            Pointer::Loaded { entry, .. } => Some(entry.as_mut()),
        }
    }

    pub fn map_loaded_entry<
        Output,
        F: ManagedFile,
        Cb: FnOnce(&BTreeEntry<I, R>, &mut F) -> Result<Output, Error>,
    >(
        &self,
        file: &mut F,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
        current_order: usize,
        callback: Cb,
    ) -> Result<Output, Error> {
        match self {
            Pointer::OnDisk(position) => match read_chunk(*position, file, vault, cache)? {
                CacheEntry::Buffer(mut buffer) => {
                    let decoded = BTreeEntry::deserialize_from(&mut buffer, current_order)?;

                    let result = callback(&decoded, file);
                    if let Some(cache) = cache {
                        cache.replace_with_decoded(file.path(), *position, decoded);
                    }
                    result
                }
                CacheEntry::Decoded(value) => {
                    let entry = value
                        .as_ref()
                        .as_any()
                        .downcast_ref::<BTreeEntry<I, R>>()
                        .unwrap();
                    callback(entry, file)
                }
            },
            Pointer::Loaded { entry, .. } => callback(entry, file),
        }
    }
}

impl<
        I: Clone + BinarySerialization + Debug + 'static,
        R: Reducer<I> + Clone + BinarySerialization + Debug + 'static,
    > BinarySerialization for Interior<I, R>
{
    fn serialize_to<W: WriteBytesExt, F: ManagedFile>(
        &mut self,
        writer: &mut W,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        let position = match &mut self.position {
            Pointer::OnDisk(position) => *position,
            Pointer::Loaded {
                entry,
                previous_location,
            } => {
                if entry.dirty || previous_location.is_none() {
                    let bytes = entry.serialize(paged_writer)?;
                    let position = paged_writer.write_chunk(&bytes)?;
                    self.position = Pointer::OnDisk(position);
                    position
                } else {
                    previous_location.unwrap()
                }
            }
        };
        let mut bytes_written = 0;
        // Write the key
        let key_len = u16::try_from(self.key.len()).map_err(|_| Error::KeyTooLarge)?;
        writer.write_u16::<BigEndian>(key_len)?;
        writer.write_all(&self.key)?;
        bytes_written += 2 + key_len as usize;

        writer.write_u64::<BigEndian>(position)?;
        bytes_written += 8;

        bytes_written += self.stats.serialize_to(writer, paged_writer)?;

        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut Buffer<'_>, current_order: usize) -> Result<Self, Error> {
        let key_len = reader.read_u16::<BigEndian>()? as usize;
        if key_len > reader.len() {
            return Err(Error::data_integrity(format!(
                "key length {} found but only {} bytes remaining",
                key_len,
                reader.len()
            )));
        }
        let key = reader.read_bytes(key_len)?.to_owned();

        let position = reader.read_u64::<BigEndian>()?;
        let stats = R::deserialize_from(reader, current_order)?;

        Ok(Self {
            key,
            position: Pointer::OnDisk(position),
            stats,
        })
    }
}
