use std::{
    convert::TryFrom,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::RangeBounds,
};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};

use super::{
    btree_entry::BTreeEntry,
    by_id::{ByIdIndex, ByIdStats},
    by_sequence::{BySequenceIndex, BySequenceStats},
    modify::Modification,
    read_chunk,
    serialization::BinarySerialization,
    KeyEvaluation, KeyRange, PagedWriter, PAGE_SIZE,
};
use crate::{
    chunk_cache::CacheEntry,
    error::InternalError,
    roots::AbortError,
    tree::{
        btree_entry::{KeyOperation, ModificationContext},
        modify::Operation,
    },
    Buffer, ChunkCache, Error, ManagedFile, Vault,
};

#[derive(Clone, Default, Debug)]
pub struct BTreeRoot<const MAX_ORDER: usize> {
    pub transaction_id: u64,
    pub sequence: u64,
    pub by_sequence_root: BTreeEntry<BySequenceIndex, BySequenceStats>,
    pub by_id_root: BTreeEntry<ByIdIndex, ByIdStats>,
}

pub enum ChangeResult<I: BinarySerialization, R: BinarySerialization> {
    Unchanged,
    Changed,
    Split(BTreeEntry<I, R>),
}

impl<const MAX_ORDER: usize> BTreeRoot<MAX_ORDER> {
    pub fn modify<'a, 'w, F: ManagedFile>(
        &'a mut self,
        modification: Modification<'_, Buffer<'static>>,
        writer: &'a mut PagedWriter<'w, F>,
    ) -> Result<(), Error> {
        let transaction_id = modification.transaction_id;

        // Insert into both trees
        let mut changes = EntryChanges {
            current_sequence: self.sequence,
            changes: Vec::with_capacity(modification.keys.len()),
        };
        self.modify_sequence_root(modification, &mut changes, writer)?;

        // Convert the changes into a modification request for the id root.
        let mut values = Vec::with_capacity(changes.changes.len());
        let keys = changes
            .changes
            .into_iter()
            .map(|change| {
                values.push(ByIdIndex {
                    sequence_id: change.sequence,
                    document_size: change.document_size,
                    position: change.document_position,
                });
                change.key
            })
            .collect();
        let id_modifications = Modification {
            transaction_id,
            keys,
            operation: Operation::SetEach(values),
        };
        self.modify_id_root(id_modifications, writer)?;

        if transaction_id != 0 {
            self.transaction_id = transaction_id;
        }

        Ok(())
    }

    fn modify_sequence_root<'a, 'w, F: ManagedFile>(
        &'a mut self,
        mut modification: Modification<'_, Buffer<'static>>,
        changes: &mut EntryChanges,
        writer: &'a mut PagedWriter<'w, F>,
    ) -> Result<(), Error> {
        // Reverse so that pop is efficient.
        modification.reverse()?;

        // Insert into both trees
        let by_sequence_order = dynamic_order::<MAX_ORDER>(
            self.by_sequence_root.stats().number_of_records + modification.keys.len() as u64,
        );
        while !modification.keys.is_empty() {
            match self.by_sequence_root.modify(
                &mut modification,
                &ModificationContext {
                    current_order: by_sequence_order,
                    indexer: |key: &Buffer<'_>,
                              value: Option<&Buffer<'static>>,
                              _existing_index: Option<&BySequenceIndex>,
                              changes: &mut EntryChanges,
                              writer: &mut PagedWriter<'_, F>| {
                        let (document_position, document_size) = if let Some(value) = value {
                            let new_position = writer.write_chunk(value)?;
                            // write_chunk errors if it can't fit within a u32
                            #[allow(clippy::cast_possible_truncation)]
                            let document_size = value.len() as u32;
                            (new_position, document_size)
                        } else {
                            (0, 0)
                        };
                        changes.current_sequence = changes
                            .current_sequence
                            .checked_add(1)
                            .expect("sequence rollover prevented");
                        let key = key.to_owned();
                        changes.changes.push(EntryChange {
                            key: key.clone(),
                            sequence: changes.current_sequence,
                            document_position,
                            document_size,
                        });
                        Ok(KeyOperation::Set(BySequenceIndex {
                            document_id: key,
                            position: document_position,
                            document_size,
                        }))
                    },
                    loader: |index: &BySequenceIndex, writer: &mut PagedWriter<'_, F>| {
                        if index.position > 0 {
                            match writer.read_chunk(index.position) {
                                Ok(CacheEntry::Buffer(buffer)) => Ok(Some(buffer)),
                                Ok(CacheEntry::Decoded(_)) => unreachable!(),
                                Err(err) => Err(err),
                            }
                        } else {
                            Ok(None)
                        }
                    },
                    _phantom: PhantomData,
                },
                None,
                changes,
                writer,
            )? {
                ChangeResult::Unchanged | ChangeResult::Changed => {}
                ChangeResult::Split(upper) => {
                    self.by_sequence_root.split_root(upper);
                }
            }
        }
        self.sequence = changes.current_sequence;
        Ok(())
    }

    fn modify_id_root<'a, 'w, F: ManagedFile>(
        &'a mut self,
        mut modification: Modification<'_, ByIdIndex>,
        writer: &'a mut PagedWriter<'w, F>,
    ) -> Result<(), Error> {
        modification.reverse()?;

        let by_id_order = dynamic_order::<MAX_ORDER>(
            self.by_id_root.stats().total_documents() + modification.keys.len() as u64,
        );

        while !modification.keys.is_empty() {
            match self.by_id_root.modify(
                &mut modification,
                &ModificationContext {
                    current_order: by_id_order,
                    indexer: |_key: &Buffer<'_>,
                              value: Option<&ByIdIndex>,
                              _existing_index,
                              _changes,
                              _writer: &mut PagedWriter<'_, F>| {
                        Ok(value.map_or(KeyOperation::Remove, |value| {
                            KeyOperation::Set(value.clone())
                        }))
                    },
                    loader: |_index, _writer| Ok(None),
                    _phantom: PhantomData,
                },
                None,
                &mut EntryChanges::default(),
                writer,
            )? {
                ChangeResult::Changed | ChangeResult::Unchanged => {}
                ChangeResult::Split(upper) => {
                    self.by_id_root.split_root(upper);
                }
            }
        }

        Ok(())
    }

    pub fn get_multiple<F: ManagedFile, KeyEvaluator, KeyReader>(
        &self,
        keys: &mut KeyRange<'_>,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut F,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), Error>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), Error>,
    {
        let mut positions_to_read = Vec::new();
        self.by_id_root.get(
            keys,
            key_evaluator,
            &mut |key, index| {
                // Deleted keys are stored with a 0 position.
                if index.position > 0 {
                    positions_to_read.push((key, index.position));
                }
                Ok(())
            },
            file,
            vault,
            cache,
        )?;

        // Sort by position on disk
        positions_to_read.sort_by(|a, b| a.1.cmp(&b.1));

        for (key, position) in positions_to_read {
            if position > 0 {
                match read_chunk(position, file, vault, cache)? {
                    CacheEntry::Buffer(contents) => {
                        key_reader(key, contents)?;
                    }
                    CacheEntry::Decoded(_) => unreachable!(),
                };
            }
        }
        Ok(())
    }

    pub fn scan<'k, F: ManagedFile, E, KeyRangeBounds, KeyEvaluator, KeyReader>(
        &self,
        range: &KeyRangeBounds,
        forwards: bool,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut F,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<(), AbortError<E>>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<E>>,
        KeyRangeBounds: RangeBounds<Buffer<'k>> + Debug,
        E: Display + Debug,
    {
        let mut positions_to_read = Vec::new();
        self.by_id_root.scan(
            range,
            forwards,
            key_evaluator,
            &mut |key, index| {
                positions_to_read.push((key, index.position));
                Ok(())
            },
            file,
            vault,
            cache,
        )?;

        // Sort by position on disk
        positions_to_read.sort_by(|a, b| a.1.cmp(&b.1));

        for (key, position) in positions_to_read {
            if position > 0 {
                match read_chunk(position, file, vault, cache)? {
                    CacheEntry::Buffer(contents) => {
                        key_reader(key, contents)?;
                    }
                    CacheEntry::Decoded(_) => unreachable!(),
                };
            }
        }
        Ok(())
    }

    pub fn deserialize(mut bytes: Buffer<'_>) -> Result<Self, Error> {
        let transaction_id = bytes.read_u64::<BigEndian>()?;
        let sequence = bytes.read_u64::<BigEndian>()?;
        let by_sequence_size = bytes.read_u32::<BigEndian>()? as usize;
        let by_id_size = bytes.read_u32::<BigEndian>()? as usize;
        if by_sequence_size + by_id_size != bytes.len() {
            return Err(Error::data_integrity(format!(
                "Header reported index sizes {} and {}, but data has {} remaining",
                by_sequence_size,
                by_id_size,
                bytes.len()
            )));
        };

        let mut by_sequence_bytes = bytes.read_bytes(by_sequence_size)?.to_owned();
        let mut by_id_bytes = bytes.read_bytes(by_id_size)?.to_owned();

        let by_sequence_root = BTreeEntry::deserialize_from(&mut by_sequence_bytes, MAX_ORDER)?;
        let by_id_root = BTreeEntry::deserialize_from(&mut by_id_bytes, MAX_ORDER)?;

        Ok(Self {
            transaction_id,
            sequence,
            by_sequence_root,
            by_id_root,
        })
    }

    pub fn serialize<F: ManagedFile>(
        &mut self,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<Vec<u8>, Error> {
        let mut output = Vec::new();
        output.reserve(PAGE_SIZE);
        output.write_u64::<BigEndian>(self.transaction_id)?;
        output.write_u64::<BigEndian>(self.sequence)?;
        // Reserve space for by_sequence and by_id sizes (2xu16).
        output.write_u64::<BigEndian>(0)?;

        let by_sequence_size = self
            .by_sequence_root
            .serialize_to(&mut output, paged_writer)?;

        let by_id_size = self.by_id_root.serialize_to(&mut output, paged_writer)?;

        let by_sequence_size = u32::try_from(by_sequence_size)
            .ok()
            .ok_or(Error::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[16..20], by_sequence_size);
        let by_id_size = u32::try_from(by_id_size)
            .ok()
            .ok_or(Error::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[20..24], by_id_size);

        Ok(output)
    }
}

/// Returns a value for the "order" (maximum children per node) value for the
/// database. This function is meant to keep the tree shallow while still
/// keeping the nodes smaller along the way. This is an approximation that
/// always returns an order larger than what is needed, but will never return a
/// value larger than `MAX_ORDER`.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn dynamic_order<const MAX_ORDER: usize>(number_of_records: u64) -> usize {
    // Current approximation is the 4th root
    if number_of_records > MAX_ORDER.pow(4) as u64 {
        MAX_ORDER
    } else {
        let estimated_order = 2.max((number_of_records as f64).sqrt().sqrt().ceil() as usize);
        // Add some padding so that we don't have a 100% fill rate.
        let estimated_order = estimated_order + (estimated_order / 3).max(1);
        MAX_ORDER.min(estimated_order)
    }
}

#[test]
fn dynamic_order_tests() {
    assert_eq!(dynamic_order::<10>(0), 3);
    assert_eq!(dynamic_order::<10>(10000), 10);
}

#[derive(Default)]
pub struct EntryChanges {
    pub current_sequence: u64,
    pub changes: Vec<EntryChange>,
}
pub struct EntryChange {
    pub sequence: u64,
    pub key: Buffer<'static>,
    pub document_position: u64,
    pub document_size: u32,
}
