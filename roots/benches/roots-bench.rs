//! This benchmark suite aims at providing a fair set of benchmarks to both help
//! understand the performance characteristics of this crate, but also to look
//! for potential areas of improvement. No benchmark suite is without its flaws,
//! and no benchmark suite can accurately predict performance of a specific
//! application.
//!
//! If performance is critical for your application, you should create
//! benchmarks that simulate your application's needs rather than trusting
//! arbitrary benchmarks.
//!
//! To use, invoke through cargo bench:
//!
//! ```sh
//! cargo bench --bench roots-bench
//!
//! The environment variable `DBS` can be used to limit benchmarks to a specific database. E.g., `DBS=roots,sqlite cargo bench --bench roots-bench`

use std::fmt::Display;

mod logs;

pub trait SimpleBench: Sized {
    type Config: BenchConfig;
    type GroupState;
    const BACKEND: &'static str;

    fn should_execute() -> bool {
        std::env::var("DBS").map_or(true, |var| {
            var.to_ascii_lowercase()
                .contains(&Self::BACKEND.to_ascii_lowercase())
        })
    }

    fn can_execute() -> bool {
        Self::should_execute()
    }

    fn run(group: &mut criterion::BenchmarkGroup<WallTime>, config: &Self::Config) {
        if !Self::can_execute() {
            return;
        }

        // When tracing is enabled, we output flamegraphs of the benchmarks.
        #[cfg(feature = "tracing")]
        {
            use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
            let fmt_layer = tracing_subscriber::fmt::Layer::default();

            let (flame_layer, _guard) = tracing_flame::FlameLayer::with_file(format!(
                "{}-{}.folded",
                Self::BACKEND,
                config
            ))
            .unwrap();
            let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
                .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))
                .unwrap();

            let subscriber = tracing_subscriber::Registry::default()
                .with(flame_layer)
                .with(filter_layer)
                .with(fmt_layer);

            tracing::subscriber::with_default(subscriber, || bench.execute_iterations(config))
        }

        #[cfg(not(feature = "tracing"))]
        {
            Self::execute_iterations(group, config)
        }
    }

    fn initialize_group(
        config: &Self::Config,
        group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState;

    fn initialize(
        group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error>;

    fn execute_measured(&mut self, config: &Self::Config) -> Result<(), anyhow::Error>;

    fn execute_iterations(group: &mut BenchmarkGroup<WallTime>, config: &Self::Config) {
        let config_group_state = config.initialize_group();
        let group_state = Self::initialize_group(config, &config_group_state);
        group.bench_with_input(
            BenchmarkId::new(Self::BACKEND, config),
            config,
            |b, config| {
                let mut bench =
                    Self::initialize(&group_state, config, &config_group_state).unwrap();
                b.iter(|| bench.execute_measured(config))
            },
        );
    }
}

pub trait BenchConfig: Display {
    type GroupState;
    type State: Iterator<Item = Self::Batch>;
    type Batch;

    fn initialize_group(&self) -> Self::GroupState;

    fn initialize(&self, group_state: &Self::GroupState) -> Self::State;
}

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId,
};

criterion_group!(benches, logs::benches);
criterion_main!(benches);
