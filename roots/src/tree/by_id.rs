use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::{btree_entry::Reducer, BinarySerialization, PagedWriter};
use crate::{Buffer, Error, ManagedFile};

#[derive(Clone, Debug)]
pub struct ByIdIndex {
    pub sequence_id: u64,
    pub document_size: u32,
    pub position: u64,
}

impl BinarySerialization for ByIdIndex {
    fn serialize_to<W: WriteBytesExt, F: ManagedFile>(
        &mut self,
        writer: &mut W,
        _paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.sequence_id)?;
        writer.write_u32::<BigEndian>(self.document_size)?;
        writer.write_u64::<BigEndian>(self.position)?;
        Ok(20)
    }

    fn deserialize_from(reader: &mut Buffer<'_>, _current_order: usize) -> Result<Self, Error> {
        let sequence_id = reader.read_u64::<BigEndian>()?;
        let document_size = reader.read_u32::<BigEndian>()?;
        let position = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            sequence_id,
            document_size,
            position,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ByIdStats {
    pub alive_documents: u64,
    pub deleted_documents: u64,
    pub total_size: u64,
}

impl ByIdStats {
    pub const fn total_documents(&self) -> u64 {
        self.alive_documents + self.deleted_documents
    }
}

impl BinarySerialization for ByIdStats {
    fn serialize_to<W: WriteBytesExt, F: ManagedFile>(
        &mut self,
        writer: &mut W,
        _paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        writer.write_u64::<BigEndian>(self.alive_documents)?;
        writer.write_u64::<BigEndian>(self.deleted_documents)?;
        writer.write_u64::<BigEndian>(self.total_size)?;
        Ok(24)
    }

    fn deserialize_from(reader: &mut Buffer<'_>, _current_order: usize) -> Result<Self, Error> {
        let alive_documents = reader.read_u64::<BigEndian>()?;
        let deleted_documents = reader.read_u64::<BigEndian>()?;
        let total_size = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            alive_documents,
            deleted_documents,
            total_size,
        })
    }
}

impl Reducer<ByIdIndex> for ByIdStats {
    fn reduce(values: &[&ByIdIndex]) -> Self {
        let (alive_documents, deleted_documents, total_size) = values
            .iter()
            .map(|index| {
                if index.position > 0 {
                    // Alive document
                    (1, 0, u64::from(index.document_size))
                } else {
                    // Deleted
                    (0, 1, 0)
                }
            })
            .reduce(
                |(total_alive, total_deleted, total_size), (alive, deleted, size)| {
                    (
                        total_alive + alive,
                        total_deleted + deleted,
                        total_size + size,
                    )
                },
            )
            .unwrap_or_default();
        Self {
            alive_documents,
            deleted_documents,
            total_size,
        }
    }

    fn rereduce(values: &[&Self]) -> Self {
        Self {
            alive_documents: values.iter().map(|v| v.alive_documents).sum(),
            deleted_documents: values.iter().map(|v| v.deleted_documents).sum(),
            total_size: values.iter().map(|v| v.total_size).sum(),
        }
    }
}
