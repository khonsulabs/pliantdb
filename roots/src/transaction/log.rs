use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::BTreeMap,
    convert::TryFrom,
    fs::OpenOptions,
    io::{SeekFrom, Write},
    marker::PhantomData,
    mem::size_of,
    ops::Deref,
    path::Path,
    sync::Arc,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned, U64};

use super::{State, TransactionHandle};
use crate::{
    managed_file::{FileManager, FileOp, ManagedFile, OpenableFile},
    Context, Error, Vault,
};

const PAGE_SIZE: usize = 1024;

/// A transaction log that records changes for one or more trees.
pub struct TransactionLog<F: ManagedFile> {
    vault: Option<Arc<dyn Vault>>,
    state: State,
    log: <F::Manager as FileManager>::FileHandle,
}

impl<F: ManagedFile> TransactionLog<F> {
    /// Opens a transaction log for writing.
    pub fn open(
        log_path: &Path,
        state: State,
        context: Context<F::Manager>,
    ) -> Result<Self, Error> {
        let log = context.file_manager.append(log_path)?;
        Ok(Self {
            vault: context.vault,
            state,
            log,
        })
    }

    /// Returns the total size of the transaction log file.
    pub fn total_size(&self) -> u64 {
        let state = self.state.lock_for_write();
        *state
    }

    /// Initializes `state` to contain the information about the transaction log
    /// located at `log_path`.
    pub fn initialize_state(state: &State, context: &Context<F::Manager>) -> Result<(), Error> {
        let mut log_length = if context.file_manager.exists(state.path())? {
            context.file_manager.file_length(state.path())?
        } else {
            0
        };
        if log_length == 0 {
            state.initialize(1, 0);
            return Ok(());
        }

        let excess_length = log_length % PAGE_SIZE as u64;
        if excess_length > 0 {
            // Truncate the file to the proper page size. This should only happen in a recovery situation.
            eprintln!(
                "Transaction log has {} extra bytes. Truncating.",
                excess_length
            );
            let file = OpenOptions::new()
                .append(true)
                .write(true)
                .open(state.path())?;
            log_length -= excess_length;
            file.set_len(log_length)?;
            file.sync_all()?;
        }

        let mut file = context.file_manager.read(state.path())?;
        file.execute(StateInitializer {
            state,
            log_length,
            vault: context.vault(),
            _file: PhantomData,
        })
    }

    /// Logs one or more transactions. After this call returns, the transaction
    /// log is guaranteed to be fully written to disk.
    pub fn push(&mut self, handles: Vec<TransactionHandle>) -> Result<(), Error> {
        self.log.execute(LogWriter {
            state: self.state.clone(),
            vault: self.vault.clone(),
            handles,
            _file: PhantomData,
        })
    }

    /// Logs one or more transactions. After this call returns, the transaction
    /// log is guaranteed to be fully written to disk.
    pub fn get(&mut self, id: u64) -> Result<Option<LogEntry<'static>>, Error> {
        self.log.execute(EntryFetcher {
            id,
            state: &self.state,
            vault: self.vault.as_deref(),
        })
    }

    /// Closes the transaction log.
    pub fn close(self) -> Result<(), Error> {
        self.log.close()
    }

    /// Returns the current transaction id.
    pub fn current_transaction_id(&self) -> u64 {
        self.state.current_transaction_id()
    }

    /// Begins a new transaction, exclusively locking `trees`.
    pub fn new_transaction(&self, trees: &[&[u8]]) -> TransactionHandle {
        self.state.new_transaction(trees)
    }

    /// Returns the current state of the log.
    pub fn state(&self) -> State {
        self.state.clone()
    }
}

struct StateInitializer<'a, F> {
    state: &'a State,
    log_length: u64,
    vault: Option<&'a dyn Vault>,
    _file: PhantomData<F>,
}

impl<'a, F: ManagedFile> FileOp<F> for StateInitializer<'a, F> {
    type Output = Result<(), Error>;
    fn execute(&mut self, log: &mut F) -> Result<(), Error> {
        // Scan back block by block until we find a page header with a value of 1.
        let block_start = self.log_length - PAGE_SIZE as u64;
        let mut scratch_buffer = Vec::new();
        scratch_buffer.reserve(PAGE_SIZE);
        scratch_buffer.resize(4, 0);

        let (last_transaction, _) =
            scan_for_transaction(log, &mut scratch_buffer, block_start, false, self.vault)?;

        let last_transaction =
            last_transaction.expect("No entries found in an existing transaction log");

        self.state
            .initialize(last_transaction.id + 1, self.log_length);
        Ok(())
    }
}

fn scan_for_transaction<F: ManagedFile>(
    log: &mut F,
    scratch_buffer: &mut Vec<u8>,
    mut block_start: u64,
    scan_forward: bool,
    vault: Option<&dyn Vault>,
) -> Result<(Option<LogEntry<'static>>, u64), Error> {
    Ok(loop {
        log.seek(SeekFrom::Start(block_start))?;
        // Read the page header
        log.read_exact(&mut scratch_buffer[0..4])?;
        #[allow(clippy::match_on_vec_items)]
        match scratch_buffer[0] {
            0 => {
                if block_start == 0 {
                    break (None, 0);
                }
                if scan_forward {
                    block_start += PAGE_SIZE as u64;
                } else {
                    block_start -= PAGE_SIZE as u64;
                }
                continue;
            }
            1 => {
                // The length is the next 3 bytes.
                let length = (scratch_buffer[1] as usize) << 16
                    | (scratch_buffer[2] as usize) << 8
                    | scratch_buffer[3] as usize;
                if scratch_buffer.len() < length {
                    scratch_buffer.resize(length, 0);
                }
                log.read_exact(scratch_buffer)?;
                let payload = &scratch_buffer[0..length];
                let decrypted = match &vault {
                    Some(vault) => Cow::Owned(vault.decrypt(payload)),
                    None => Cow::Borrowed(payload),
                };
                let transaction = LogEntry::deserialize(&decrypted)
                    .map_err(Error::data_integrity)?
                    .into_owned();
                break (Some(transaction), block_start);
            }
            _ => unreachable!("corrupt transaction log"),
        }
    })
}

pub struct EntryFetcher<'a> {
    pub state: &'a State,
    pub id: u64,
    pub vault: Option<&'a dyn Vault>,
}

impl<'a, F: ManagedFile> FileOp<F> for EntryFetcher<'a> {
    type Output = Result<Option<LogEntry<'static>>, Error>;
    fn execute(&mut self, log: &mut F) -> Result<Option<LogEntry<'static>>, Error> {
        let mut upper_id = self.state.current_transaction_id();
        let mut upper_location = self.state.len();
        let mut lower_id = None;
        let mut lower_location = None;
        let mut scratch_buffer = Vec::new();
        scratch_buffer.reserve(PAGE_SIZE);
        scratch_buffer.resize(4, 0);
        loop {
            let guessed_location =
                guess_page(self.id, lower_location, lower_id, upper_location, upper_id);
            debug_assert_ne!(guessed_location, upper_location);

            // load the transaction at this location
            #[allow(clippy::cast_possible_wrap)]
            let scan_forward = guessed_location >= upper_location;
            let (transaction, location) = scan_for_transaction(
                log,
                &mut scratch_buffer,
                guessed_location,
                scan_forward,
                self.vault,
            )?;
            if let Some(transaction) = transaction {
                self.state.note_transaction_id_status(transaction.id, true);
                match transaction.id.cmp(&self.id) {
                    Ordering::Less => {
                        lower_id = Some(transaction.id);
                        lower_location = Some(location);
                    }
                    Ordering::Equal => {
                        return Ok(Some(transaction));
                    }
                    Ordering::Greater => {
                        upper_id = transaction.id;
                        upper_location = location;
                    }
                }
            } else {
                return Ok(None);
            }
        }
    }
}

struct LogWriter<F> {
    state: State,
    handles: Vec<TransactionHandle>,
    vault: Option<Arc<dyn Vault>>,
    _file: PhantomData<F>,
}

impl<F: ManagedFile> FileOp<F> for LogWriter<F> {
    type Output = Result<(), Error>;
    fn execute(&mut self, log: &mut F) -> Result<(), Error> {
        let mut log_position = self.state.lock_for_write();
        let mut scratch_buffer = Vec::new();
        scratch_buffer.resize(PAGE_SIZE, 0);
        let mut completed_transactions = Vec::new();
        for handle in self.handles.drain(..) {
            completed_transactions.push(handle.transaction.id);
            let mut bytes = handle.transaction.serialize()?;
            if let Some(vault) = &self.vault {
                bytes = vault.encrypt(&bytes);
            }
            // Write out the transaction in pages.
            let total_length = bytes.len() + 3;
            let mut offset = 0;
            while offset < bytes.len() {
                // Write the page header
                let header_len = if offset == 0 {
                    // The first page has the length of the payload as the next 3 bytes.
                    let length = u32::try_from(bytes.len())
                        .map_err(|_| Error::message("transaction too large"))?;
                    if length & 0xFF00_0000 != 0 {
                        return Err(Error::message("transaction too large"));
                    }
                    scratch_buffer[0] = 1;
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        scratch_buffer[1] = (length >> 16) as u8;
                        scratch_buffer[2] = (length >> 8) as u8;
                        scratch_buffer[3] = (length & 0xFF) as u8;
                    }
                    4
                } else {
                    // Set page_header to have a 0 byte for future pages written.
                    scratch_buffer[0] = 0;
                    1
                };

                // Write up to PAGE_SIZE - header_len bytes
                let total_bytes_left = total_length - (offset + 3);
                let bytes_to_write = total_bytes_left.min(PAGE_SIZE - header_len as usize);
                scratch_buffer[header_len..bytes_to_write + header_len]
                    .copy_from_slice(&bytes[offset..offset + bytes_to_write]);
                log.write_all(&scratch_buffer)?;
                offset += bytes_to_write;
                *log_position += PAGE_SIZE as u64;
            }
        }

        drop(log_position);

        log.flush()?;

        self.state
            .note_transaction_ids_completed(&completed_transactions);

        Ok(())
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct LogEntry<'a> {
    pub id: u64,
    pub changes: TransactionChanges<'a>,
}

impl<'a> LogEntry<'a> {
    pub fn into_owned(self) -> LogEntry<'static> {
        LogEntry {
            id: self.id,
            changes: self
                .changes
                .into_iter()
                .map(|(key, value)| (Cow::Owned(key.to_vec()), value.into_owned()))
                .collect(),
        }
    }
}

pub type TransactionChanges<'a> = BTreeMap<Cow<'a, [u8]>, Entries<'a>>;

impl<'a> LogEntry<'a> {
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        // Transaction ID
        buffer.write_u64::<BigEndian>(self.id)?;
        // Number of trees in the transaction
        buffer.write_u16::<BigEndian>(convert_usize(self.changes.len())?)?;
        for (tree, entries) in &self.changes {
            // Length of tree name
            buffer.write_u16::<BigEndian>(convert_usize(tree.len())?)?;
            // The tree name
            buffer.write_all(tree)?;
            // Number of `Entry`s
            buffer.write_u32::<BigEndian>(convert_usize(entries.len())?)?;
            for entry in entries.iter() {
                buffer.write_all(entry.as_bytes())?;
            }
        }
        Ok(buffer)
    }

    pub fn deserialize(mut buffer: &'a [u8]) -> Result<Self, Error> {
        let id = buffer.read_u64::<BigEndian>()?;
        let number_of_trees = buffer.read_u16::<BigEndian>()?;
        let mut changes = BTreeMap::new();
        for _ in 0..number_of_trees {
            let name_len = usize::from(buffer.read_u16::<BigEndian>()?);
            if name_len > buffer.len() {
                return Err(Error::message("invalid tree name length"));
            }
            let name_bytes = &buffer[0..name_len];
            let name = Cow::Borrowed(name_bytes);
            buffer = &buffer[name_len..];

            let entry_count = usize::try_from(buffer.read_u32::<BigEndian>()?).unwrap();
            let byte_len = entry_count * size_of::<Entry>();
            if byte_len > buffer.len() {
                return Err(Error::message("invalid entry length"));
            }
            let entries = LayoutVerified::new_slice(&buffer[..byte_len])
                .ok_or_else(|| Error::message("invalid entry slice"))?;
            buffer = &buffer[byte_len..];

            changes.insert(name, Entries::Borrowed(entries));
        }
        if buffer.is_empty() {
            Ok(Self { id, changes })
        } else {
            Err(Error::message("unexpected trailing bytes"))
        }
    }
}

#[derive(Debug)]
pub enum Entries<'a> {
    Owned(Vec<Entry>),
    Borrowed(LayoutVerified<&'a [u8], [Entry]>),
}

impl<'a> Entries<'a> {
    pub fn into_owned(self) -> Entries<'static> {
        match self {
            Entries::Owned(entries) => Entries::Owned(entries),
            Entries::Borrowed(borrowed) => Entries::Owned(borrowed.to_vec()),
        }
    }
}

impl<'a> Clone for Entries<'a> {
    fn clone(&self) -> Self {
        match self {
            Self::Owned(owned) => Self::Owned(owned.clone()),
            Self::Borrowed(borrowed) => Self::Owned(borrowed.to_vec()),
        }
    }
}

impl<'a> Deref for Entries<'a> {
    type Target = [Entry];

    fn deref(&self) -> &Self::Target {
        match self {
            Entries::Owned(entries) => entries,
            Entries::Borrowed(entries) => entries,
        }
    }
}

impl<'a> Eq for Entries<'a> {}

impl<'a> PartialEq for Entries<'a> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

#[derive(Debug, Clone, Eq, PartialEq, FromBytes, AsBytes, Unaligned)]
#[repr(C)]
pub struct Entry {
    pub document_id: U64<BigEndian>,
    pub sequence_id: U64<BigEndian>,
}

fn convert_usize<T: TryFrom<usize>>(value: usize) -> Result<T, Error> {
    T::try_from(value).map_err(|_| Error::message("too many changed trees in a transaction"))
}

#[test]
fn serialization_tests() {
    let mut transaction = LogEntry {
        id: 1,
        changes: BTreeMap::default(),
    };
    transaction.changes.insert(
        Cow::Borrowed(b"tree1"),
        Entries::Owned(vec![Entry {
            document_id: U64::new(2),
            sequence_id: U64::new(3),
        }]),
    );
    let serialized = transaction.serialize().unwrap();
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);

    // Multiple trees
    let mut transaction = LogEntry {
        id: 1,
        changes: BTreeMap::default(),
    };
    transaction.changes.insert(
        Cow::Borrowed(b"tree1"),
        Entries::Owned(vec![Entry {
            document_id: U64::new(2),
            sequence_id: U64::new(3),
        }]),
    );
    transaction.changes.insert(
        Cow::Borrowed(b"tree2"),
        Entries::Owned(vec![
            Entry {
                document_id: U64::new(4),
                sequence_id: U64::new(5),
            },
            Entry {
                document_id: U64::new(6),
                sequence_id: U64::new(7),
            },
        ]),
    );
    let serialized = transaction.serialize().unwrap();
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn guess_page(
    looking_for: u64,
    lower_location: Option<u64>,
    lower_id: Option<u64>,
    upper_location: u64,
    upper_id: u64,
) -> u64 {
    debug_assert_ne!(looking_for, upper_id);
    let total_pages = upper_location / PAGE_SIZE as u64;

    if let (Some(lower_location), Some(lower_id)) = (lower_location, lower_id) {
        // Estimate inbetween lower and upper
        let current_page = lower_location / PAGE_SIZE as u64;
        let delta_from_current = looking_for - lower_id;
        let local_avg_per_page = (upper_id - lower_id) as f64 / (total_pages - current_page) as f64;
        let delta_estimated_pages = (delta_from_current as f64 * local_avg_per_page).floor() as u64;
        let guess = lower_location + delta_estimated_pages.max(1) * PAGE_SIZE as u64;
        // If our estimate is that the location is beyond or equal to the upper, we'll guess the page before it.
        if guess >= upper_location {
            upper_location - PAGE_SIZE as u64
        } else {
            guess
        }
    } else {
        // Go backwards from upper
        let avg_per_page = upper_id as f64 / total_pages as f64;
        let id_delta = upper_id - looking_for;
        let delta_estimated_pages = (id_delta as f64 * avg_per_page).ceil() as u64;
        let delta_bytes = delta_estimated_pages * PAGE_SIZE as u64;
        upper_location.saturating_sub(delta_bytes)
    }
}

#[cfg(test)]
#[allow(clippy::semicolon_if_nothing_returned, clippy::future_not_send)]
mod tests {

    use nanorand::{Pcg64, Rng};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        managed_file::{
            fs::{StdFile, StdFileManager},
            memory::MemoryFile,
            ManagedFile,
        },
        test_util::RotatorVault,
        transaction::TransactionManager,
        ChunkCache,
    };

    #[test]
    fn file_log_file_tests() {
        log_file_tests::<StdFile>("file_log_file", None, None);
    }

    #[test]
    fn memory_log_file_tests() {
        log_file_tests::<MemoryFile>("memory_log_file", None, None);
    }

    #[test]
    fn file_encrypted_log_manager_tests() {
        log_manager_tests::<StdFile>(
            "encrypted_file_log_manager",
            Some(Arc::new(RotatorVault::new(13))),
            None,
        );
    }

    fn log_file_tests<F: ManagedFile>(
        file_name: &str,
        vault: Option<Arc<dyn Vault>>,
        cache: Option<ChunkCache>,
    ) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        let file_manager = <F::Manager as Default>::default();
        let context = Context {
            file_manager,
            vault,
            cache,
        };
        std::fs::create_dir(&temp_dir).unwrap();
        let log_path = {
            let directory: &Path = &temp_dir;
            directory.join("transactions")
        };

        for id in 1..=1_000 {
            let state = State::from_path(&log_path);
            TransactionLog::<F>::initialize_state(&state, &context).unwrap();
            let mut transactions =
                TransactionLog::<F>::open(&log_path, state, context.clone()).unwrap();
            assert_eq!(transactions.current_transaction_id(), id);
            let mut tx = transactions.new_transaction(&[b"hello"]);

            tx.transaction.changes.insert(
                Cow::Borrowed(b"tree1"),
                Entries::Owned(vec![Entry {
                    document_id: U64::new(2),
                    sequence_id: U64::new(3),
                }]),
            );

            transactions.push(vec![tx]).unwrap();
            transactions.close().unwrap();
        }

        if context.vault.is_some() {
            // Test that we can't open it without encryption
            let state = State::from_path(&temp_dir);
            assert!(TransactionLog::<F>::initialize_state(&state, &context).is_err());
            let mut transactions = TransactionLog::<F>::open(&log_path, state, context).unwrap();

            for id in 0..1_000 {
                let transaction = transactions.get(id).unwrap();
                match transaction {
                    Some(transaction) => assert_eq!(transaction.id, id),
                    None => {
                        unreachable!("failed to fetch transaction {}", id)
                    }
                }
            }
        }
    }

    #[test]
    fn discontiguous_log_file_tests() {
        let temp_dir = tempdir().unwrap();
        let file_manager = StdFileManager::default();
        let context = Context {
            file_manager,
            vault: None,
            cache: None,
        };
        let log_path = temp_dir.path().join("transactions");
        let mut rng = Pcg64::new_seed(1);

        let state = State::from_path(&log_path);
        TransactionLog::<StdFile>::initialize_state(&state, &context).unwrap();
        let mut transactions = TransactionLog::<StdFile>::open(&log_path, state, context).unwrap();

        let mut valid_ids = Vec::new();
        for id in 1..=10_000 {
            assert_eq!(transactions.current_transaction_id(), id);
            let mut tx = transactions.new_transaction(&[b"hello"]);
            if rng.generate::<u8>() < 8 {
                // skip a few ids.
                continue;
            }
            valid_ids.push(tx.id);

            tx.transaction.changes.insert(
                Cow::Borrowed(b"tree1"),
                Entries::Owned(vec![Entry {
                    document_id: U64::new(2),
                    sequence_id: U64::new(3),
                }]),
            );

            transactions.push(vec![tx]).unwrap();
        }

        for id in valid_ids {
            let transaction = transactions.get(id).unwrap();
            match transaction {
                Some(transaction) => assert_eq!(transaction.id, id),
                None => {
                    unreachable!("failed to fetch transaction {}", id)
                }
            }
        }
    }

    #[test]
    fn file_log_manager_tests() {
        log_manager_tests::<StdFile>("file_log_manager", None, None);
    }

    #[test]
    fn memory_log_manager_tests() {
        log_manager_tests::<MemoryFile>("memory_log_manager", None, None);
    }

    fn log_manager_tests<F: ManagedFile>(
        file_name: &str,
        vault: Option<Arc<dyn Vault>>,
        cache: Option<ChunkCache>,
    ) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        std::fs::create_dir(&temp_dir).unwrap();
        let file_manager = <F::Manager as Default>::default();
        let context = Context {
            file_manager,
            vault,
            cache,
        };
        let manager = TransactionManager::spawn(&temp_dir, context).unwrap();
        let mut handles = Vec::new();
        for _ in 0..10 {
            let manager = manager.clone();
            handles.push(std::thread::spawn(move || {
                for id in 0..1_000 {
                    let mut tx = manager.new_transaction(&[id.as_bytes()]);

                    tx.transaction.changes.insert(
                        Cow::Borrowed(b"tree1"),
                        Entries::Owned(vec![Entry {
                            document_id: U64::new(2),
                            sequence_id: U64::new(3),
                        }]),
                    );
                    manager.push(tx).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(manager.current_transaction_id(), 10_001);
    }
}
