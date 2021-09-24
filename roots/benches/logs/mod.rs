use std::{fmt::Display, iter::Take};

use bonsaidb_roots::StdFile;
use criterion::Criterion;
use nanorand::{Pcg64, Rng};
use serde::{Deserialize, Serialize};

use crate::{BenchConfig, SimpleBench};

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: u64,
    pub timestamp: u64,
    pub message: String,
}

const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

impl LogEntry {
    pub fn generate(id: u64, rng: &mut Pcg64) -> LogEntry {
        LogEntry {
            id,
            timestamp: rng.generate(),
            message: (0..rng.generate_range(50..1024))
                .map(|_| {
                    let idx = rng.generate_range(0..ALPHABET.len());
                    &ALPHABET[idx..idx + 1]
                })
                .collect(),
        }
    }

    pub fn generate_batch(
        id_base: u64,
        count: usize,
        sequential: bool,
        rng: &mut Pcg64,
        for_insert: bool,
    ) -> Vec<LogEntry> {
        let mut batch = Vec::new();
        for id in 0..count {
            batch.push(LogEntry {
                id: if sequential {
                    id_base + id as u64
                } else {
                    rng.generate()
                },
                timestamp: rng.generate(),
                message: (0..rng.generate_range(50..1024))
                    .map(|_| {
                        let idx = rng.generate_range(0..ALPHABET.len());
                        &ALPHABET[idx..idx + 1]
                    })
                    .collect(),
            })
        }
        if for_insert {
            // roots requires that keys be sorted on insert.
            batch.sort_by(|a, b| a.id.cmp(&b.id));
        }
        batch
    }
}

mod couchdb;
mod roots;
mod sled;
mod sqlite;

#[derive(Clone)]
pub struct InsertConfig {
    pub sequential_ids: bool,
    pub entries_per_transaction: usize,
    pub transactions: usize,
}

pub struct LogEntryBatchGenerator {
    rng: Pcg64,
    base: u64,
    sequential_ids: bool,
    entries_per_transaction: usize,
}

impl Iterator for LogEntryBatchGenerator {
    type Item = Vec<LogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = LogEntry::generate_batch(
            self.base,
            self.entries_per_transaction,
            self.sequential_ids,
            &mut self.rng,
            true,
        );
        self.base += self.entries_per_transaction as u64;
        Some(batch)
    }
}

pub struct LogEntryGenerator {
    rng: Pcg64,
    base: u64,
    sequential_ids: bool,
}

impl Iterator for LogEntryGenerator {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let id = if self.sequential_ids {
            self.base += 1;
            self.base
        } else {
            self.rng.generate()
        };
        let batch = LogEntry::generate(id, &mut self.rng);
        Some(batch)
    }
}

impl BenchConfig for InsertConfig {
    type GroupState = ();
    type State = LogEntryBatchGenerator;
    type Batch = Vec<LogEntry>;

    fn initialize_group(&self) -> Self::GroupState {}

    fn initialize(&self, _group_state: &Self::GroupState) -> Self::State {
        LogEntryBatchGenerator {
            base: 0,
            rng: Pcg64::new_seed(1),
            sequential_ids: self.sequential_ids,
            entries_per_transaction: self.entries_per_transaction,
        }
    }
}

impl Display for InsertConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}x{}-{}",
            self.transactions,
            self.entries_per_transaction,
            if self.sequential_ids {
                "sequential"
            } else {
                "random"
            }
        )
    }
}

pub struct ReadConfig {
    sequential_ids: bool,
    database_size: usize,
    get_count: usize,
}

impl ReadConfig {
    fn new(sequential_ids: bool, database_size: usize, get_count: usize) -> Self {
        Self {
            sequential_ids,
            database_size,
            get_count,
        }
    }
}

#[derive(Clone)]
pub struct ReadState {
    samples: Vec<LogEntry>,
    offset: usize,
}

impl ReadConfig {
    pub fn database_generator(&self) -> Take<LogEntryGenerator> {
        LogEntryGenerator {
            rng: Pcg64::new_seed(1),
            base: 0,
            sequential_ids: self.sequential_ids,
        }
        .take(self.database_size)
    }

    pub fn for_each_database_chunk<F: FnMut(&[LogEntry])>(
        &self,
        chunk_size: usize,
        mut callback: F,
    ) {
        let mut database_generator = self.database_generator();
        let mut chunk = Vec::new();
        loop {
            chunk.clear();
            while chunk.len() < chunk_size {
                match database_generator.next() {
                    Some(entry) => {
                        chunk.push(entry);
                    }
                    None => break,
                }
            }
            if chunk.is_empty() {
                break;
            }
            chunk.sort_by(|a, b| a.id.cmp(&b.id));
            callback(&chunk);
        }
    }
}

impl BenchConfig for ReadConfig {
    type GroupState = ReadState;
    type State = ReadState;
    type Batch = LogEntry;

    fn initialize_group(&self) -> Self::GroupState {
        // This is wasteful... but it's not being measured by the benchmark.
        let database = self.database_generator();
        let samples_to_collect = self.database_size / 10;
        let skip_between = self.database_size / samples_to_collect;
        let mut samples = Vec::new();
        for (index, entry) in database.enumerate() {
            if index % skip_between == 0 {
                samples.push(entry);
            }
        }
        Pcg64::new_seed(1).shuffle(&mut samples);
        ReadState { samples, offset: 0 }
    }

    fn initialize(&self, group_state: &Self::GroupState) -> Self::State {
        group_state.clone()
    }
}

impl Iterator for ReadState {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.samples.len() {
            self.offset = 0;
        }
        let entry = self.samples[self.offset].clone();
        self.offset += 1;
        Some(entry)
    }
}

impl Display for ReadConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}x{} {} elements",
            self.get_count,
            self.database_size,
            if self.sequential_ids {
                "sequential"
            } else {
                "random"
            }
        )
    }
}

pub fn inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("logs-inserts");
    for (sequential_ids, transactions, entries_per_transaction) in [
        (false, 1_000, 1),
        (true, 1_000, 1),
        (false, 1_000, 100),
        (true, 1_000, 100),
        (false, 100, 1_000),
        (true, 100, 1_000),
        (false, 10, 10_000),
        (true, 10, 10_000),
    ] {
        let config = InsertConfig {
            sequential_ids,
            transactions,
            entries_per_transaction,
        };

        roots::InsertLogs::<StdFile>::run(&mut group, &config);
        sled::InsertLogs::run(&mut group, &config);
        sqlite::InsertLogs::run(&mut group, &config);
        couchdb::InsertLogs::run(&mut group, &config);
    }
}

pub fn gets(c: &mut Criterion) {
    let mut group = c.benchmark_group("logs-gets");
    for (sequential_ids, database_size, get_count) in [
        (false, 1_000, 1),
        (true, 1_000, 1),
        (false, 1_000, 10),
        (true, 1_000, 10),
        (false, 100_000, 1),
        (true, 100_000, 1),
        (false, 100_000, 100),
        (true, 100_000, 100),
        (false, 1_000_000, 1),
        (true, 1_000_000, 1),
        (false, 1_000_000, 1_000),
        (true, 1_000_000, 1_000),
    ] {
        let config = ReadConfig::new(sequential_ids, database_size, get_count);

        roots::ReadLogs::<StdFile>::run(&mut group, &config);
        sled::ReadLogs::run(&mut group, &config);
        sqlite::ReadLogs::run(&mut group, &config);
        couchdb::ReadLogs::run(&mut group, &config);
    }
}

pub fn benches(c: &mut Criterion) {
    inserts(c);
    gets(c);
}
