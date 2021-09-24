use http_auth_basic::Credentials;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{InsertConfig, LogEntryBatchGenerator, ReadConfig, ReadState};
use crate::{BenchConfig, SimpleBench};

const USERNAME: &str = "COUCHDB_USER";
const PASSWORD: &str = "COUCHDB_PASSWORD";

pub struct InsertLogs {
    state: LogEntryBatchGenerator,
}

impl SimpleBench for InsertLogs {
    type GroupState = ();
    type Config = InsertConfig;
    const BACKEND: &'static str = "CouchDB";

    fn initialize_group(
        _config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
    }

    fn can_execute() -> bool {
        Self::should_execute()
            && if std::env::var(USERNAME).is_err() {
                println!("Skipping couchdb benchmark. To run, provide environment variables COUCHDB_USER and COUCHDB_PASSWORD.");
                false
            } else {
                true
            }
    }

    fn initialize(
        _group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let username = std::env::var(USERNAME).expect("missing username");
        let password = std::env::var(PASSWORD)
            .map_err(|_| anyhow::anyhow!("missing {} environment variable", PASSWORD))?;
        let authorization_header = Credentials::new(&username, &password).as_http_header();

        // Delete the database
        ureq::delete("http://localhost:5984/roots-log-benchmark")
            .set("Authorization", &authorization_header)
            .call()?;

        // Create the database
        ureq::put("http://localhost:5984/roots-log-benchmark")
            .set("Authorization", &authorization_header)
            .call()?;

        // Set the security model to none, allowing the benchmark to execute without security.
        ureq::put("http://localhost:5984/roots-log-benchmark/_security")
            .set("Authorization", &authorization_header)
            .send_json(Value::Object(Map::default()))?;

        Ok(Self {
            state: config.initialize(config_group_state),
        })
    }

    fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        ureq::post("http://localhost:5984/roots-log-benchmark/_bulk_docs").send_json(
            serde_json::to_value(&Documents {
                docs: self.state.next().unwrap(),
            })?,
        )?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Documents<T> {
    docs: Vec<T>,
}

#[derive(Serialize, Deserialize)]
struct LogEntryDoc {
    #[serde(rename = "_id")]
    id: String,
    timestamp: u64,
    message: String,
}

pub struct ReadLogs {
    state: ReadState,
}

impl SimpleBench for ReadLogs {
    type GroupState = ();
    type Config = ReadConfig;
    const BACKEND: &'static str = "CouchDB";

    fn can_execute() -> bool {
        Self::should_execute()
            && if std::env::var(USERNAME).is_err() {
                println!("Skipping couchdb benchmark. To run, provide environment variables COUCHDB_USER and COUCHDB_PASSWORD.");
                false
            } else {
                true
            }
    }

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let username = std::env::var(USERNAME).expect("missing username");
        let password = std::env::var(PASSWORD)
            .map_err(|_| anyhow::anyhow!("missing {} environment variable", PASSWORD))
            .unwrap();
        let authorization_header = Credentials::new(&username, &password).as_http_header();

        // Delete the database
        ureq::delete("http://localhost:5984/roots-log-benchmark")
            .set("Authorization", &authorization_header)
            .call()
            .unwrap();

        // Create the database
        ureq::put("http://localhost:5984/roots-log-benchmark")
            .set("Authorization", &authorization_header)
            .call()
            .unwrap();

        // Set the security model to none, allowing the benchmark to execute without security.
        ureq::put("http://localhost:5984/roots-log-benchmark/_security")
            .set("Authorization", &authorization_header)
            .send_json(Value::Object(Map::default()))
            .unwrap();

        config.for_each_database_chunk(10_000, |chunk| {
            ureq::post("http://localhost:5984/roots-log-benchmark/_bulk_docs")
                .send_json(
                    serde_json::to_value(&Documents {
                        docs: chunk
                            .iter()
                            .map(|entry| LogEntryDoc {
                                id: entry.id.to_string(),
                                timestamp: entry.timestamp,
                                message: entry.message.clone(),
                            })
                            .collect(),
                    })
                    .unwrap(),
                )
                .unwrap();
        });
    }

    fn initialize(
        _group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            state: config.initialize(config_group_state),
        })
    }

    fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        let entry = self.state.next().unwrap();
        let result = ureq::get(&format!(
            "http://localhost:5984/roots-log-benchmark/{}",
            entry.id
        ))
        .call()?
        .into_json::<LogEntryDoc>()?;
        assert_eq!(&result.timestamp, &entry.timestamp);
        assert_eq!(&result.message, &entry.message);

        Ok(())
    }
}
