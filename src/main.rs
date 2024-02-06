use clap::Parser;
use tracing_core::{Level, LevelFilter};
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt,
    util::SubscriberInitExt,
    Layer,
};

pub mod coordinator_filters;
pub mod coordinator_service;
pub mod executor_server;
pub mod extractor;
pub mod package;
pub mod server;
pub mod server_config;
pub mod state;
pub mod task_allocator;

mod api;
mod blob_storage;
mod caching;
mod cmd;
mod coordinator;
mod coordinator_client;
mod data_repository_manager;
mod executor;
mod extractor_router;
mod grpc_helper;
mod metadata_index;
mod task_store;
mod test_util;
mod tls;
mod tonic_streamer;
mod utils;
mod vector_index;
mod vectordbs;

struct OtelGuard;

impl OtelGuard {
    fn new() -> Self {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(LevelFilter::from_level(Level::INFO)),
            )
            .init();

        OtelGuard
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

/// The version of the crate that is being built. This is set by the build
/// script.
pub const VERSION: &str = concat!(
    "git branch: ",
    env!("VERGEN_GIT_BRANCH"),
    " - sha:",
    env!("VERGEN_GIT_SHA")
);

/// The prelude module contains all the commonly used types and traits that are
/// used across the crate. This is mostly used to avoid having to import a lot
/// of things from different modules.
pub mod prelude {
    pub use anyhow::{anyhow, Context};
    pub use tracing::{debug, error, info, instrument, trace, warn};
}

#[tokio::main]
async fn main() {
    // When this guard is dropped (at the end of this function, by default), the
    // opentelemetry tracer is automatically shut down.
    let _otel_guard = OtelGuard::new();

    cmd::Cli::parse().run().await;
}
