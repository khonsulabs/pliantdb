use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

use super::{InsertConfig, LogEntry, LogEntryBatchGenerator, ReadConfig, ReadState};
use crate::{BenchConfig, SimpleBench};

pub struct InsertLogs {
    sqlite: Connection,
    _tempfile: NamedTempFile,
    state: LogEntryBatchGenerator,
}

impl SimpleBench for InsertLogs {
    type GroupState = ();
    type Config = InsertConfig;
    const BACKEND: &'static str = "SQLite";

    fn initialize_group(
        _config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
    }

    fn initialize(
        _group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        // For fair testing, this needs to use ACID-compliant settings that a
        // user would use in production. While a WAL might be used in
        // production, it alters more than just insert performance. A more
        // complete benchmark which includes both inserts and queries would be
        // better to compare roots against sqlite's WAL performance.
        let tempfile = NamedTempFile::new()?;
        let sqlite = Connection::open(tempfile.path())?;
        // Sets the journal to what seems to be the most optimal, safe setting
        // for @ecton. See:
        // https://www.sqlite.org/pragma.html#pragma_journal_mode
        sqlite.pragma_update(None, "journal_mode", &"TRUNCATE")?;
        // Sets synchronous to NORMAL, which "should" be safe and provides
        // better performance. See:
        // https://www.sqlite.org/pragma.html#pragma_synchronous
        sqlite.pragma_update(None, "synchronous", &"NORMAL")?;
        sqlite.execute(
            "create table logs (id integer primary key, timestamp integer, message text)",
            [],
        )?;
        Ok(Self {
            sqlite,
            state: config.initialize(config_group_state),
            _tempfile: tempfile,
        })
    }

    fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        self.sqlite.execute("begin transaction;", [])?;

        let mut prepared = self
            .sqlite
            .prepare("insert into logs (id, timestamp, message) values (?, ?, ?)")?;
        for log in self.state.next().unwrap() {
            // sqlite doesn't support u64, so we're going to cast to i64
            prepared.execute(params![log.id as i64, log.timestamp as i64, log.message])?;
        }

        self.sqlite.execute("commit transaction;", [])?;

        Ok(())
    }
}

pub struct ReadLogs {
    sqlite: Connection,
    state: ReadState,
}

impl SimpleBench for ReadLogs {
    type GroupState = NamedTempFile;
    type Config = ReadConfig;
    const BACKEND: &'static str = "SQLite";

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let tempfile = NamedTempFile::new().unwrap();
        let sqlite = Connection::open(tempfile.path()).unwrap();
        sqlite
            .execute(
                "create table logs (id integer primary key, timestamp integer, message text)",
                [],
            )
            .unwrap();

        let mut prepared = sqlite
            .prepare("insert into logs (id, timestamp, message) values (?, ?, ?)")
            .unwrap();
        config.for_each_database_chunk(1_000_000, |chunk| {
            sqlite.execute("begin transaction;", []).unwrap();
            for log in chunk {
                // sqlite doesn't support u64, so we're going to cast to i64
                prepared
                    .execute(params![log.id as i64, log.timestamp as i64, log.message])
                    .unwrap();
            }

            sqlite.execute("commit transaction;", []).unwrap();
        });
        tempfile
    }

    fn initialize(
        group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let sqlite = Connection::open(group_state.path())?;
        Ok(Self {
            sqlite,
            state: config.initialize(config_group_state),
        })
    }

    fn execute_measured(&mut self, config: &Self::Config) -> Result<(), anyhow::Error> {
        if config.get_count == 1 {
            let entry = self.state.next().unwrap();
            let fetched = self.sqlite.query_row(
                "SELECT id, timestamp, message FROM logs WHERE id = ?",
                [entry.id as i64],
                |row| {
                    Ok(LogEntry {
                        id: row.get::<_, i64>(0)? as u64,
                        timestamp: row.get::<_, i64>(1)? as u64,
                        message: row.get(2)?,
                    })
                },
            )?;
            assert_eq!(&fetched, &entry);
        } else {
            let entries = (0..config.get_count)
                .map(|_| (self.state.next().unwrap().id as i64).to_string())
                .collect::<Vec<_>>();

            let mut prepared = self.sqlite.prepare(&format!(
                "SELECT id, timestamp, message FROM logs WHERE id IN ({})",
                entries.join(",")
            ))?;
            let rows = prepared.query([])?.mapped(|row| {
                Ok(LogEntry {
                    id: row.get::<_, i64>(0)? as u64,
                    timestamp: row.get::<_, i64>(1)? as u64,
                    message: row.get(2)?,
                })
            });
            assert_eq!(rows.count(), config.get_count);
        }
        Ok(())
    }
}
