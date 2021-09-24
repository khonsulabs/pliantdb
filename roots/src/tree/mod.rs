//! Append-only B-Tree implementation
//!
//! The file format is inspired by
//! [Couchstore](https://github.com/couchbase/couchstore). The main difference
//! is that the file header has extra information to allow for cross-tree
//! transactions.
//!
//! ## Numbers and Alignment
//!
//! - All numbers are encoded in big-endian format/network byte order.
//! - All values are tightly packed. There is no padding or alignment that isn't
//!   explicitly included.
//!
//! ## File pages
//!
//! The file is written in pages that are 4,096 bytes long. Each page has single
//! `u8` representing the `PageHeader`. If data needs to span more than one
//! page, every 4,096 byte boundary must contain a `PageHeader::Continuation`.
//!
//! ### File Headers
//!
//! If the header is a `PageHeader::Header`, the contents of the block will be
//! a single chunk that contains a serialized `BTreeRoot`.
//!
//! ## Chunks
//!
//! Each time a document, B-Tree node, or header is written, it is written as a
//! chunk. If a [`Vault`] is in-use, each chunk will be pre-processed by the
//! vault before a `CRC-32-BZIP2` checksum is calculated. A chunk is limited to
//! 4 gigabytes of data (2^32).
//!
//! The chunk is written as:
//!
//! - `u32` - Data length, excluding the header.
//! - `u32` - CRC
//! - `[u8]` - Contents
//!
//! A data block may contain more than one chunk.

use std::{
    convert::{Infallible, TryFrom},
    fmt::{Debug, Display},
    fs::OpenOptions,
    io::SeekFrom,
    marker::PhantomData,
    ops::{Deref, DerefMut, RangeBounds},
    path::Path,
    sync::Arc,
};

use byteorder::{BigEndian, ByteOrder};
use crc::{Crc, CRC_32_BZIP2};

use crate::{
    chunk_cache::CacheEntry,
    managed_file::{FileManager, FileOp, ManagedFile, OpenableFile},
    roots::AbortError,
    tree::btree_root::BTreeRoot,
    Buffer, ChunkCache, Context, Error, TransactionManager, Vault,
};

mod btree_entry;
mod btree_root;
mod by_id;
mod by_sequence;
mod interior;
mod key_entry;
mod modify;
mod serialization;
mod state;

use self::serialization::BinarySerialization;
pub use self::{
    btree_entry::KeyOperation,
    modify::{CompareSwap, CompareSwapFn, Modification, Operation},
    state::State,
};

// The memory used by PagedWriter is PAGE_SIZE * PAGED_WRITER_BATCH_COUNT. E.g,
// 4096 * 4 = 16kb
const PAGE_SIZE: usize = 4096;
const PAGED_WRITER_BATCH_COUNT: usize = 4;

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_BZIP2);

enum PageHeader {
    // Using a strange value so that errors in the page writing/reading
    // algorithm are easier to detect.
    Continuation = 0xF1,
    Header = 1,
    Data = 2,
}

impl TryFrom<u8> for PageHeader {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0xF1 => Ok(Self::Continuation),
            1 => Ok(Self::Header),
            2 => Ok(Self::Data),
            _ => Err(Error::data_integrity(format!(
                "invalid block header: {}",
                value
            ))),
        }
    }
}

/// An append-only tree file.
///
/// ## Generics
/// - `F`: An [`AsyncFile`] implementor.
/// - `MAX_ORDER`: The maximum number of children a node in the tree can
///   contain. This implementation attempts to grow naturally towards this upper
///   limit. Changing this parameter does not automatically rebalance the tree,
///   but over time the tree will be updated.
pub struct TreeFile<F: ManagedFile, const MAX_ORDER: usize> {
    file: <F::Manager as FileManager>::FileHandle,
    /// The state of the file.
    pub state: State<MAX_ORDER>,
    vault: Option<Arc<dyn Vault>>,
    cache: Option<ChunkCache>,
}

impl<F: ManagedFile, const MAX_ORDER: usize> TreeFile<F, MAX_ORDER> {
    /// Returns a tree as contained in `file`.
    ///
    /// `state` should already be initialized using [`Self::initialize_state`] if the file exists.
    pub fn new(
        file: <F::Manager as FileManager>::FileHandle,
        state: State<MAX_ORDER>,
        vault: Option<Arc<dyn Vault>>,
        cache: Option<ChunkCache>,
    ) -> Result<Self, Error> {
        Ok(Self {
            file,
            state,
            vault,
            cache,
        })
    }

    /// Opens a tree file with read-only permissions.
    pub fn read(
        path: impl AsRef<Path>,
        state: State<MAX_ORDER>,
        context: &Context<F::Manager>,
        transactions: Option<&TransactionManager<F::Manager>>,
    ) -> Result<Self, Error> {
        let file = context.file_manager.read(path.as_ref())?;
        Self::initialize_state(&state, path.as_ref(), context, transactions)?;
        Self::new(file, state, context.vault.clone(), context.cache.clone())
    }

    /// Opens a tree file with the ability to read and write.
    pub fn write(
        path: impl AsRef<Path>,
        state: State<MAX_ORDER>,
        context: &Context<F::Manager>,
        transactions: Option<&TransactionManager<F::Manager>>,
    ) -> Result<Self, Error> {
        let file = context.file_manager.append(path.as_ref())?;
        Self::initialize_state(&state, path.as_ref(), context, transactions)?;
        Self::new(file, state, context.vault.clone(), context.cache.clone())
    }

    /// Attempts to load the last saved state of this tree into `state`.
    pub fn initialize_state(
        state: &State<MAX_ORDER>,
        file_path: &Path,
        context: &Context<F::Manager>,
        transaction_manager: Option<&TransactionManager<F::Manager>>,
    ) -> Result<(), Error> {
        {
            let read_state = state.read();
            if read_state.initialized() {
                return Ok(());
            }
        }

        let mut active_state = state.lock();
        if active_state.initialized() {
            return Ok(());
        }

        let mut file_length = context.file_manager.file_length(file_path)?;
        if file_length == 0 {
            active_state.header.sequence = 1;
            return Ok(());
        }

        let excess_length = file_length % PAGE_SIZE as u64;
        if excess_length > 0 {
            // Truncate the file to the proper page size. This should only happen in a recovery situation.
            eprintln!(
                "Tree {:?} has {} extra bytes. Truncating.",
                file_path, excess_length
            );
            let file = OpenOptions::new()
                .append(true)
                .write(true)
                .open(&file_path)?;
            file_length -= excess_length;
            file.set_len(file_length)?;
            file.sync_all()?;
        }

        let mut tree = F::open_for_read(file_path)?;

        // Scan back block by block until we find a header page.
        let mut block_start = file_length - PAGE_SIZE as u64;
        let mut scratch_buffer = vec![0_u8];
        active_state.header = loop {
            // Read the page header
            tree.seek(SeekFrom::Start(block_start))?;
            tree.read_exact(&mut scratch_buffer[0..1])?;

            #[allow(clippy::match_on_vec_items)]
            match PageHeader::try_from(scratch_buffer[0])? {
                PageHeader::Continuation | PageHeader::Data => {
                    if block_start == 0 {
                        return Err(Error::data_integrity(format!(
                            "Tree {:?} contained data, but no valid pages were found",
                            file_path
                        )));
                    }
                    block_start -= PAGE_SIZE as u64;
                    continue;
                }
                PageHeader::Header => {
                    let contents = match read_chunk(
                        block_start + 1,
                        &mut tree,
                        context.vault(),
                        context.cache(),
                    )? {
                        CacheEntry::Buffer(buffer) => buffer,
                        CacheEntry::Decoded(_) => unreachable!(),
                    };
                    let root = BTreeRoot::deserialize(contents)
                        .map_err(|err| Error::DataIntegrity(Box::new(err)))?;
                    if let Some(transaction_manager) = transaction_manager {
                        if !transaction_manager.transaction_was_successful(root.transaction_id)? {
                            // The transaction wasn't written successfully, so
                            // we cannot trust the data present.
                            if block_start == 0 {
                                // No data was ever fully written.
                                break BTreeRoot {
                                    sequence: 1,
                                    ..BTreeRoot::default()
                                };
                            }
                            block_start -= PAGE_SIZE as u64;
                            continue;
                        }
                    }
                    break root;
                }
            }
        };

        active_state.current_position = file_length;
        active_state.publish(state);
        Ok(())
    }

    /// Returns the sequence that wrote this document.
    pub fn push(
        &mut self,
        transaction_id: u64,
        key: Buffer<'static>,
        document: Buffer<'static>,
    ) -> Result<u64, Error> {
        self.file.execute(DocumentWriter {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            modification: Some(Modification {
                transaction_id,
                keys: vec![key],
                operation: Operation::Set(document),
            }),
        })
    }

    /// Returns the sequence that wrote this document.
    pub fn modify(
        &mut self,
        modification: Modification<'_, Buffer<'static>>,
    ) -> Result<u64, Error> {
        self.file.execute(DocumentWriter {
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            modification: Some(modification),
        })
    }

    /// Gets the value stored for `key`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get<'k>(
        &mut self,
        key: &'k [u8],
        in_transaction: bool,
    ) -> Result<Option<Buffer<'static>>, Error> {
        let mut buffer = None;
        self.file.execute(DocumentGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::Single(key),
            key_reader: |_key, value| {
                buffer = Some(value);
                Ok(())
            },
            key_evaluator: |_| KeyEvaluation::ReadData,
        })?;
        Ok(buffer)
    }

    /// Gets the value stored for `key`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_multiple(
        &mut self,
        keys: &[&[u8]],
        in_transaction: bool,
    ) -> Result<Vec<Buffer<'static>>, Error> {
        let mut buffers = Vec::with_capacity(keys.len());
        self.file.execute(DocumentGetter {
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            keys: KeyRange::Multiple(keys),
            key_reader: |_key, value| {
                buffers.push(value);
                Ok(())
            },
            key_evaluator: |_| KeyEvaluation::ReadData,
        })?;
        Ok(buffers)
    }

    /// Gets the value stored for `key`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn get_range<'b, B: RangeBounds<Buffer<'b>> + Debug + 'static>(
        &mut self,
        range: B,
        in_transaction: bool,
    ) -> Result<Vec<(Buffer<'static>, Buffer<'static>)>, Error> {
        let mut results = Vec::new();
        match self.scan::<Infallible, _, _, _>(
            range,
            true,
            in_transaction,
            |_| KeyEvaluation::ReadData,
            |key, value| {
                results.push((key, value));
                Ok(())
            },
        ) {
            Ok(_) => Ok(results),
            Err(AbortError::Other(_)) => unreachable!(),
            Err(AbortError::Roots(error)) => Err(error),
        }
    }

    /// Gets the value stored for `key`.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, callback))
    )]
    pub fn scan<'b, E, B, KeyEvaluator, DataCallback>(
        &mut self,
        range: B,
        forwards: bool,
        in_transaction: bool,
        key_evaluator: KeyEvaluator,
        callback: DataCallback,
    ) -> Result<(), AbortError<E>>
    where
        B: RangeBounds<Buffer<'b>> + Debug + 'static,
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        DataCallback: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<E>>,
        E: Display + Debug,
    {
        self.file.execute(DocumentScanner {
            forwards,
            from_transaction: in_transaction,
            state: &self.state,
            vault: self.vault.as_deref(),
            cache: self.cache.as_ref(),
            range,
            key_reader: callback,
            key_evaluator,
            _phantom: PhantomData,
        })?;
        Ok(())
    }
}

struct DocumentWriter<'a, 'm, const MAX_ORDER: usize> {
    state: &'a State<MAX_ORDER>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    modification: Option<Modification<'m, Buffer<'static>>>,
}

impl<'a, 'm, F: ManagedFile, const MAX_ORDER: usize> FileOp<F>
    for DocumentWriter<'a, 'm, MAX_ORDER>
{
    type Output = Result<u64, Error>;

    #[allow(clippy::shadow_unrelated)] // It is related, but clippy can't tell.
    fn execute(&mut self, file: &mut F) -> Self::Output {
        let mut active_state = self.state.lock();

        let mut data_block = PagedWriter::new(
            PageHeader::Data,
            file,
            self.vault.as_deref(),
            self.cache,
            active_state.current_position,
        );

        // Now that we have the document data's position, we can update the by_sequence and by_id indexes.
        let modification = self.modification.take().unwrap();
        let is_transactional = modification.transaction_id != 0;
        active_state.header.modify(modification, &mut data_block)?;
        let new_header = active_state.header.serialize(&mut data_block)?;
        let (file, after_data) = data_block.finish()?;
        active_state.current_position = after_data;

        // Write a new header.
        let mut header_block = PagedWriter::new(
            PageHeader::Header,
            file,
            self.vault.as_deref(),
            self.cache,
            active_state.current_position,
        );
        header_block.write_chunk(&new_header)?;

        let (file, after_header) = header_block.finish()?;
        active_state.current_position = after_header;

        file.flush()?;

        if !is_transactional {
            active_state.publish(self.state);
        }

        Ok(active_state.header.sequence)
    }
}

/// One or more keys.
#[derive(Debug)]
pub enum KeyRange<'a> {
    /// No keys.
    Empty,
    /// A single key.
    Single(&'a [u8]),
    /// A list of keys.
    Multiple(&'a [&'a [u8]]),
}

impl<'a> KeyRange<'a> {
    fn current_key(&self) -> Option<&'a [u8]> {
        match self {
            KeyRange::Single(key) => Some(*key),
            KeyRange::Multiple(keys) => keys.get(0).copied(),
            KeyRange::Empty => None,
        }
    }
}

impl<'a> Iterator for KeyRange<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        match self {
            KeyRange::Empty => None,
            KeyRange::Single(key) => {
                let key = *key;
                *self = Self::Empty;
                Some(key)
            }
            KeyRange::Multiple(keys) => {
                if keys.is_empty() {
                    None
                } else {
                    let (one_key, remaining) = keys.split_at(1);
                    *keys = remaining;

                    Some(one_key[0])
                }
            }
        }
    }
}

/// The result of evaluating a key that was scanned.
pub enum KeyEvaluation {
    /// Read the data for this key.
    ReadData,
    /// Skip this key.
    Skip,
    /// Stop scanning keys.
    Stop,
}

struct DocumentGetter<
    'a,
    'k,
    E: FnMut(&Buffer<'static>) -> KeyEvaluation,
    R: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), Error>,
    const MAX_ORDER: usize,
> {
    from_transaction: bool,
    state: &'a State<MAX_ORDER>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    keys: KeyRange<'k>,
    key_evaluator: E,
    key_reader: R,
}

impl<'a, 'k, E, R, F: ManagedFile, const MAX_ORDER: usize> FileOp<F>
    for DocumentGetter<'a, 'k, E, R, MAX_ORDER>
where
    E: FnMut(&Buffer<'static>) -> KeyEvaluation,
    R: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), Error>,
{
    type Output = Result<(), Error>;
    fn execute(&mut self, file: &mut F) -> Self::Output {
        if self.from_transaction {
            let state = self.state.lock();
            state.header.get_multiple(
                &mut self.keys,
                &mut self.key_evaluator,
                &mut self.key_reader,
                file,
                self.vault,
                self.cache,
            )
        } else {
            let state = self.state.read();
            state.header.get_multiple(
                &mut self.keys,
                &mut self.key_evaluator,
                &mut self.key_reader,
                file,
                self.vault,
                self.cache,
            )
        }
    }
}

struct DocumentScanner<'a, 'k, E, KeyEvaluator, KeyReader, KeyRangeBounds, const MAX_ORDER: usize>
where
    KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
    KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<E>>,
    KeyRangeBounds: RangeBounds<Buffer<'k>>,
    E: Display + Debug,
{
    forwards: bool,
    from_transaction: bool,
    state: &'a State<MAX_ORDER>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    range: KeyRangeBounds,
    key_evaluator: KeyEvaluator,
    key_reader: KeyReader,
    _phantom: PhantomData<&'k ()>,
}

impl<'a, 'k, E, KeyEvaluator, KeyReader, KeyRangeBounds, F, const MAX_ORDER: usize> FileOp<F>
    for DocumentScanner<'a, 'k, E, KeyEvaluator, KeyReader, KeyRangeBounds, MAX_ORDER>
where
    F: ManagedFile,
    KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
    KeyReader: FnMut(Buffer<'static>, Buffer<'static>) -> Result<(), AbortError<E>>,
    KeyRangeBounds: RangeBounds<Buffer<'k>> + Debug,
    E: Display + Debug,
{
    type Output = Result<(), AbortError<E>>;
    fn execute(&mut self, file: &mut F) -> Self::Output {
        if self.from_transaction {
            let state = self.state.lock();
            state.header.scan(
                &self.range,
                self.forwards,
                &mut self.key_evaluator,
                &mut self.key_reader,
                file,
                self.vault,
                self.cache,
            )
        } else {
            let state = self.state.read();
            state.header.scan(
                &self.range,
                self.forwards,
                &mut self.key_evaluator,
                &mut self.key_reader,
                file,
                self.vault,
                self.cache,
            )
        }
    }
}

/// Writes data in pages, allowing for quick scanning through the file.
pub struct PagedWriter<'a, F: ManagedFile> {
    file: &'a mut F,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    position: u64,
    scratch: Vec<u8>,
    offset: usize,
}

impl<'a, F: ManagedFile> Deref for PagedWriter<'a, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.file
    }
}

impl<'a, F: ManagedFile> DerefMut for PagedWriter<'a, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.file
    }
}

impl<'a, F: ManagedFile> PagedWriter<'a, F> {
    fn new(
        header: PageHeader,
        file: &'a mut F,
        vault: Option<&'a dyn Vault>,
        cache: Option<&'a ChunkCache>,
        position: u64,
    ) -> PagedWriter<'a, F> {
        let mut writer = Self {
            file,
            vault,
            cache,
            position,
            scratch: vec![0; PAGE_SIZE * PAGED_WRITER_BATCH_COUNT],
            offset: 0,
        };
        writer.scratch[0] = header as u8;
        for page_num in 1..PAGED_WRITER_BATCH_COUNT {
            writer.scratch[page_num * PAGE_SIZE] = PageHeader::Continuation as u8;
        }
        writer.offset = 1;
        writer
    }

    fn current_position(&self) -> u64 {
        self.position + self.offset as u64
    }

    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        let bytes_written = data.len();
        self.commit_if_needed(true)?;

        let new_offset = self.offset + data.len();
        let start_page = self.offset / PAGE_SIZE;
        let end_page = (new_offset - 1) / PAGE_SIZE;
        if start_page == end_page && new_offset <= self.scratch.len() {
            // Everything fits within the current page
            self.scratch[self.offset..new_offset].copy_from_slice(data);
            self.offset = new_offset;
        } else {
            // This won't fully fit within the page remainder. First, fill the remainder of the page
            let page_remaining = PAGE_SIZE - self.offset % PAGE_SIZE;
            let (fill_amount, mut remaining) = data.split_at(page_remaining);
            if !fill_amount.is_empty() {
                self.scratch[self.offset..self.offset + fill_amount.len()]
                    .copy_from_slice(fill_amount);
                self.offset += fill_amount.len();
            }

            // If the data is large enough to span multiple pages, continue to do so.
            while remaining.len() >= PAGE_SIZE {
                self.commit_if_needed(true)?;

                let (one_page, after) = remaining.split_at(PAGE_SIZE - 1);
                remaining = after;

                self.scratch[self.offset..self.offset + PAGE_SIZE - 1].copy_from_slice(one_page);
                self.offset += PAGE_SIZE - 1;
            }

            self.commit_if_needed(!remaining.is_empty())?;

            // If there's any data left, add it to the scratch
            if !remaining.is_empty() {
                let final_offset = self.offset + remaining.len();
                self.scratch[self.offset..final_offset].copy_from_slice(remaining);
                self.offset = final_offset;
            }

            self.commit_if_needed(!remaining.is_empty())?;
        }
        Ok(bytes_written)
    }

    /// Writes a chunk of data to the file, after possibly encrypting it.
    /// Returns the position that this chunk can be read from in the file.
    #[allow(clippy::cast_possible_truncation)]
    fn write_chunk(&mut self, contents: &[u8]) -> Result<u64, Error> {
        let possibly_encrypted = Buffer::from(
            self.vault
                .as_ref()
                .map_or_else(|| contents.to_vec(), |vault| vault.encrypt(contents)),
        );
        let length = u32::try_from(possibly_encrypted.len())
            .map_err(|_| Error::data_integrity("chunk too large"))?;
        let crc = CRC32.checksum(&possibly_encrypted);
        let mut position = self.current_position();
        // Ensure that the chunk header can be read contiguously
        let position_relative_to_page = position % PAGE_SIZE as u64;
        if position_relative_to_page + 8 > PAGE_SIZE as u64 {
            // Write the number of zeroes required to pad the current position
            // such that our header's 8 bytes will not be interrupted by a page
            // header.
            let bytes_needed = if position_relative_to_page == 0 {
                1
            } else {
                PAGE_SIZE - position_relative_to_page as usize
            };
            let zeroes = [0; 8];
            self.write(&zeroes[0..bytes_needed])?;
            position = self.current_position();
        }

        if position % PAGE_SIZE as u64 == 0 {
            // A page header will be written before our first byte.
            position += 1;
        }

        self.write_u32::<BigEndian>(length)?;
        self.write_u32::<BigEndian>(crc)?;
        self.write(&possibly_encrypted)?;

        if let Some(cache) = self.cache {
            cache.insert(self.file.path(), position, possibly_encrypted);
        }

        Ok(position)
    }

    fn read_chunk(&mut self, position: u64) -> Result<CacheEntry, Error> {
        read_chunk(position, self.file, self.vault, self.cache)
    }

    fn write_u32<B: ByteOrder>(&mut self, value: u32) -> Result<usize, Error> {
        let mut buffer = [0_u8; 4];
        B::write_u32(&mut buffer, value);
        self.write(&buffer)
    }

    /// Writes the scratch buffer and resets `offset`.
    #[cfg_attr(not(debug_assertions), allow(unused_mut))]
    fn commit(&mut self) -> Result<(), Error> {
        let length = (self.offset + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE;

        // In debug builds, fill the padding with a recognizable number: the
        // answer to the ultimate question of life, the universe and everything.
        // For refence, 42 in hex is 0x2A.
        #[cfg(debug_assertions)]
        if self.offset < length {
            self.scratch[self.offset..length].fill(42);
        }

        self.file.seek(SeekFrom::Start(self.position))?;
        self.file.write_all(&self.scratch[..length])?;
        self.position += length as u64;

        // Set the header to be a continuation block
        self.scratch[0] = PageHeader::Continuation as u8;
        self.offset = 1;
        Ok(())
    }

    fn commit_if_needed(&mut self, about_to_write: bool) -> Result<(), Error> {
        if self.offset == self.scratch.len() {
            self.commit()?;
        } else if about_to_write && self.offset % PAGE_SIZE == 0 {
            self.offset += 1;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(&'a mut F, u64), Error> {
        if self.offset > 0 {
            self.commit()?;
        }
        Ok((self.file, self.position))
    }
}

#[allow(clippy::cast_possible_truncation)]
#[cfg_attr(feature = "tracing", tracing::instrument(skip(file, vault, cache)))]
fn read_chunk<F: ManagedFile>(
    position: u64,
    file: &mut F,
    vault: Option<&dyn Vault>,
    cache: Option<&ChunkCache>,
) -> Result<CacheEntry, Error> {
    if let Some(cache) = cache {
        if let Some(entry) = cache.get(file.path(), position) {
            return Ok(entry);
        }
    }

    // Read the chunk header
    let mut header = [0_u8; 8];
    file.seek(SeekFrom::Start(position))?;
    file.read_exact(&mut header)?;
    let length = BigEndian::read_u32(&header[0..4]) as usize;
    let crc = BigEndian::read_u32(&header[4..8]);

    let mut data_start = position + 8;
    // If the data starts on a page boundary, there will have been a page
    // boundary inserted. Note: We don't read this byte, so technically it's
    // unchecked as part of this process.
    if data_start % PAGE_SIZE as u64 == 0 {
        data_start += 1;
        file.seek(SeekFrom::Current(1))?;
    }

    let data_start_relative_to_page = data_start % PAGE_SIZE as u64;
    let data_end_relative_to_page = data_start_relative_to_page + length as u64;
    // Minus 2 may look like code cruft, but it's due to the correct value being
    // `data_end_relative_to_page as usize - 1`, except that if a write occurs
    // that ends exactly on a page boundary, we omit the boundary.
    let number_of_page_boundaries = (data_end_relative_to_page as usize - 2) / (PAGE_SIZE - 1);

    let data_page_start = data_start - data_start % PAGE_SIZE as u64;
    let total_bytes_to_read = length + number_of_page_boundaries;
    let mut scratch = Vec::new();
    scratch.resize(total_bytes_to_read, 0);
    file.read_exact(&mut scratch)?;

    // We need to remove the `PageHeader::Continuation` bytes before continuing.
    let first_page_relative_end = data_page_start + PAGE_SIZE as u64 - data_start;
    for page in 0..number_of_page_boundaries {
        let write_start = first_page_relative_end as usize + page * (PAGE_SIZE - 1);
        let read_start = write_start + page + 1;
        let length = (length - write_start).min(PAGE_SIZE - 1);
        scratch.copy_within(read_start..read_start + length, write_start);
    }
    scratch.truncate(length);

    // This is an extra sanity check on the above algorithm, but given the
    // thoroughness of the unit test around this functionality, it's only
    // checked in debug mode. The CRC will still fail on bad reads, but noticing
    // the length is incorrect is a sign that the byte removal loop is bad.
    debug_assert_eq!(scratch.len(), length);

    let computed_crc = CRC32.checksum(&scratch);
    if crc != computed_crc {
        return Err(Error::data_integrity(format!(
            "crc32 failure on chunk at position {}",
            position
        )));
    }

    let decrypted = Buffer::from(match vault {
        Some(vault) => vault.decrypt(&scratch),
        None => scratch,
    });

    if let Some(cache) = cache {
        cache.insert(file.path(), position, decrypted.clone());
    }

    Ok(CacheEntry::Buffer(decrypted))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use nanorand::{Pcg64, Rng};

    use super::*;
    use crate::{
        managed_file::{
            fs::StdFileManager,
            memory::{MemoryFile, MemoryFileManager},
        },
        StdFile,
    };

    fn test_paged_write(offset: usize, length: usize) -> Result<(), Error> {
        let mut file = MemoryFile::open_for_append(format!("test-{}-{}", offset, length))?;
        let mut paged_writer = PagedWriter::new(PageHeader::Header, &mut file, None, None, 0);

        let mut scratch = Vec::new();
        scratch.resize(offset.max(length), 0);
        if offset > 0 {
            paged_writer.write(&scratch[..offset])?;
        }
        scratch.fill(1);
        let written_position = paged_writer.write_chunk(&scratch[..length])?;
        drop(paged_writer.finish()?);

        match read_chunk(written_position, &mut file, None, None)? {
            CacheEntry::Buffer(data) => {
                assert_eq!(data.len(), length);
                assert!(data.iter().all(|i| i == &1));
            }
            CacheEntry::Decoded(_) => unreachable!(),
        }

        drop(file);

        Ok(())
    }

    /// Tests the writing of pages at various boundaries. This should cover
    /// every edge case: offset is on page 1/2/3, write stays on first page or
    /// lands on page 1/2/3 end or extends onto page 4.
    #[test]
    fn paged_writer() {
        for offset in 0..=PAGE_SIZE * 2 {
            for length in [
                1,
                PAGE_SIZE / 2,
                PAGE_SIZE,
                PAGE_SIZE * 3 / 2,
                PAGE_SIZE * 2,
            ] {
                if let Err(err) = test_paged_write(offset, length) {
                    unreachable!(
                        "paged writer failure at offset {} length {}: {:?}",
                        offset, length, err
                    );
                }
            }
        }
    }

    fn insert_one_record<F: ManagedFile, const MAX_ORDER: usize>(
        context: &Context<F::Manager>,
        file_path: &Path,
        ids: &mut HashSet<u64>,
        rng: &mut Pcg64,
    ) {
        let id = loop {
            let id = rng.generate::<u64>();
            if ids.insert(id) {
                break id;
            }
        };
        let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
        println!("Inserting: {:?}", id_buffer);
        {
            let state = State::default();
            if ids.len() > 1 {
                TreeFile::<F, MAX_ORDER>::initialize_state(&state, file_path, context, None)
                    .unwrap();
            }
            let file = context.file_manager.append(file_path).unwrap();
            let mut tree = TreeFile::<F, MAX_ORDER>::new(
                file,
                state,
                context.vault.clone(),
                context.cache.clone(),
            )
            .unwrap();
            tree.push(0, id_buffer.clone(), Buffer::from(b"hello world"))
                .unwrap();

            // This shouldn't have to scan the file, as the data fits in memory.
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(&*value.unwrap(), b"hello world");
        }

        // Try loading the file up and retrieving the data.
        {
            let state = State::default();
            TreeFile::<F, MAX_ORDER>::initialize_state(&state, file_path, context, None).unwrap();

            let file = context.file_manager.append(file_path).unwrap();
            let mut tree = TreeFile::<F, MAX_ORDER>::new(
                file,
                state,
                context.vault.clone(),
                context.cache.clone(),
            )
            .unwrap();
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(&*value.unwrap(), b"hello world");
        }
    }

    fn remove_one_record<F: ManagedFile, const MAX_ORDER: usize>(
        context: &Context<F::Manager>,
        file_path: &Path,
        id: u64,
    ) {
        let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
        println!("Removing: {:?}", id_buffer);
        {
            let state = State::default();
            TreeFile::<F, MAX_ORDER>::initialize_state(&state, file_path, context, None).unwrap();
            let file = context.file_manager.append(file_path).unwrap();
            let mut tree = TreeFile::<F, MAX_ORDER>::new(
                file,
                state,
                context.vault.clone(),
                context.cache.clone(),
            )
            .unwrap();
            tree.modify(Modification {
                transaction_id: 0,
                keys: vec![id_buffer.clone()],
                operation: Operation::Remove,
            })
            .unwrap();

            // The row should no longer exist in memory.
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(value, None);
        }

        // Try loading the file up and retrieving the data.
        {
            let state = State::default();
            TreeFile::<F, MAX_ORDER>::initialize_state(&state, file_path, context, None).unwrap();

            let file = context.file_manager.append(file_path).unwrap();
            let mut tree = TreeFile::<F, MAX_ORDER>::new(
                file,
                state,
                context.vault.clone(),
                context.cache.clone(),
            )
            .unwrap();
            let value = tree.get(&id_buffer, false).unwrap();
            assert_eq!(value, None);
        }
    }

    #[test]
    fn simple_inserts() {
        const ORDER: usize = 4;

        let mut rng = Pcg64::new_seed(1);
        let context = Context {
            file_manager: StdFileManager::default(),
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new("btree-tests");
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let mut ids = HashSet::new();
        // Insert up to the limit of a LEAF, which is ORDER - 1.
        for _ in 0..ORDER - 1 {
            insert_one_record::<StdFile, ORDER>(&context, &file_path, &mut ids, &mut rng);
        }
        println!("Successfully inserted up to ORDER - 1 nodes.");

        // The next record will split the node
        insert_one_record::<StdFile, ORDER>(&context, &file_path, &mut ids, &mut rng);
        println!("Successfully introduced one layer of depth.");

        // Insert a lot more.
        for _ in 0..1_000 {
            insert_one_record::<StdFile, ORDER>(&context, &file_path, &mut ids, &mut rng);
        }
    }

    #[test]
    fn remove() {
        const ORDER: usize = 4;

        let mut rng = Pcg64::new_seed(1);
        let context = Context {
            file_manager: StdFileManager::default(),
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new("btree-removals");
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let mut ids = HashSet::new();
        // Insert up to the limit of a LEAF, which is ORDER - 1.
        for _ in 0..100 {
            insert_one_record::<StdFile, ORDER>(&context, &file_path, &mut ids, &mut rng);
        }

        // Remove each of the records
        for id in ids {
            remove_one_record::<StdFile, ORDER>(&context, &file_path, id);
        }
    }

    #[test]
    fn spam_insert_tokio() {
        spam_insert::<StdFile>("tokio");
    }

    #[test]
    #[cfg(feature = "uring")]
    fn spam_insert_uring() {
        spam_insert::<crate::UringFile>("uring");
    }

    fn spam_insert<F: ManagedFile>(name: &str) {
        const ORDER: usize = 100;
        const RECORDS: usize = 1_000;
        let mut rng = Pcg64::new_seed(1);
        let ids = (0..RECORDS).map(|_| rng.generate::<u64>());
        let context = Context {
            file_manager: F::Manager::default(),
            vault: None,
            cache: Some(ChunkCache::new(100, 160_384)),
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("spam-inserts-{}", name));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let state = State::default();
        let file = context.file_manager.append(file_path).unwrap();
        let mut tree =
            TreeFile::<F, ORDER>::new(file, state, context.vault.clone(), context.cache.clone())
                .unwrap();
        for (_index, id) in ids.enumerate() {
            let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
            tree.push(0, id_buffer.clone(), Buffer::from(b"hello world"))
                .unwrap();
        }
    }

    #[test]
    fn bulk_insert_tokio() {
        bulk_insert::<StdFile>("std");
    }

    fn bulk_insert<F: ManagedFile>(name: &str) {
        const ORDER: usize = 10;
        const RECORDS_PER_BATCH: usize = 10;
        const BATCHES: usize = 1000;
        let mut rng = Pcg64::new_seed(1);
        let context = Context {
            file_manager: F::Manager::default(),
            vault: None,
            cache: Some(ChunkCache::new(100, 160_384)),
        };
        let temp_dir = crate::test_util::TestDirectory::new(format!("bulk-inserts-{}", name));
        std::fs::create_dir(&temp_dir).unwrap();
        let file_path = temp_dir.join("tree");
        let state = State::default();
        let file = context.file_manager.append(file_path).unwrap();
        let mut tree =
            TreeFile::<F, ORDER>::new(file, state, context.vault.clone(), context.cache.clone())
                .unwrap();
        for _ in 0..BATCHES {
            let mut ids = (0..RECORDS_PER_BATCH)
                .map(|_| rng.generate::<u64>())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            let modification = Modification {
                transaction_id: 0,
                keys: ids
                    .iter()
                    .map(|id| Buffer::from(id.to_be_bytes().to_vec()))
                    .collect(),
                operation: Operation::Set(Buffer::from(b"hello world")),
            };
            tree.modify(modification).unwrap();

            // Try five random gets
            for _ in 0..5 {
                let index = rng.generate_range(0..ids.len());
                let id = Buffer::from(ids[index].to_be_bytes().to_vec());
                let value = tree.get(&id, false).unwrap();
                assert_eq!(&*value.unwrap(), b"hello world");
            }
        }
    }

    #[test]
    fn batch_get() {
        let context = Context {
            file_manager: MemoryFileManager::default(),
            vault: None,
            cache: None,
        };
        let state = State::default();
        let file = context.file_manager.append("test").unwrap();
        let mut tree = TreeFile::<MemoryFile, 3>::new(
            file,
            state,
            context.vault.clone(),
            context.cache.clone(),
        )
        .unwrap();
        // Create enough records to go 4 levels deep.
        let mut ids = Vec::new();
        for id in 0..3_u32.pow(4) {
            let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
            tree.push(0, id_buffer.clone(), id_buffer.clone()).unwrap();
            ids.push(id_buffer);
        }

        // Get them all
        let mut all_records = tree
            .get_multiple(&ids.iter().map(Buffer::as_slice).collect::<Vec<_>>(), false)
            .unwrap();
        // Order isn't guaranteeed.
        all_records.sort();
        assert_eq!(all_records, ids);

        // Try some ranges
        let mut unbounded_to_five = tree
            .get_range(..ids[5].clone(), false)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        unbounded_to_five.sort();
        assert_eq!(&all_records[..5], &unbounded_to_five);
        let mut one_to_ten_unbounded = tree
            .get_range(ids[1].clone()..ids[10].clone(), false)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        one_to_ten_unbounded.sort();
        assert_eq!(&all_records[1..10], &one_to_ten_unbounded);
        let mut bounded_upper = tree
            .get_range(ids[3].clone()..=ids[50].clone(), false)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        bounded_upper.sort();
        assert_eq!(&all_records[3..=50], &bounded_upper);
        let mut unbounded_upper = tree
            .get_range(ids[60].clone().., false)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        unbounded_upper.sort();
        assert_eq!(&all_records[60..], &unbounded_upper);
        let mut all_through_scan = tree
            .get_range(.., false)
            .unwrap()
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        all_through_scan.sort();
        assert_eq!(&all_records, &all_through_scan);
    }
}
