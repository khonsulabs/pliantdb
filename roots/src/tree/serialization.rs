use byteorder::WriteBytesExt;

use super::PagedWriter;
use crate::{Buffer, Error, ManagedFile};

pub trait BinarySerialization: Send + Sync + Sized {
    fn serialize_to<W: WriteBytesExt, F: ManagedFile>(
        &mut self,
        writer: &mut W,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error>;
    fn serialize<F: ManagedFile>(
        &mut self,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        buffer.reserve(super::PAGE_SIZE);
        self.serialize_to(&mut buffer, paged_writer)?;
        Ok(buffer)
    }
    fn deserialize_from(reader: &mut Buffer<'_>, current_order: usize) -> Result<Self, Error>;
}

impl BinarySerialization for () {
    fn serialize_to<W: WriteBytesExt, F: ManagedFile>(
        &mut self,
        _writer: &mut W,
        _paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        Ok(0)
    }

    fn deserialize_from(_reader: &mut Buffer<'_>, _current_order: usize) -> Result<Self, Error> {
        Ok(())
    }
}
