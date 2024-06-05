use anyhow::{anyhow, Result};
use clap::Parser;
use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    runtime,
    runtime::Tokio,
    trace::{BatchConfig, RandomIdGenerator, Sampler},
    Resource,
};
use rustls::crypto::CryptoProvider;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, Layer};

pub mod coordinator_filters;
pub mod coordinator_service;
pub mod metrics;
pub mod server;
pub mod server_config;
pub mod state;
pub mod task_allocator;

mod api;
mod api_utils;
mod blob_storage;
mod caching;
mod cmd;
mod coordinator;
mod coordinator_client;
mod data_manager;
mod extractor_router;
mod forwardable_coordinator;
mod garbage_collector;
mod grpc_helper;
mod ingest_extracted_content;
mod metadata_storage;
mod scheduler;
mod test_util;
mod tls;
mod tonic_streamer;
mod utils;
mod vector_index;
mod vectordbs;

//  test modules
#[cfg(test)]
mod test_utils;

fn setup_stdout_tracing() -> Result<()> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(
            opentelemetry_stdout::SpanExporterBuilder::default()
                .with_encoder(|writer, data| {
                    serde_json::to_writer_pretty(writer, &data).unwrap();
                    Ok(())
                })
                .build(),
            Tokio,
        )
        .build();
    let tracer = provider.tracer("indexify");
    global::set_tracer_provider(provider);
    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| anyhow!("failed to set global default subscriber: {}", e))
}

const DEFAULT_PERCENT_TRACED: f64 = 0.01;

fn get_percent_traced() -> f64 {
    match std::env::var("INDEXIFY_TRACE_PERCENT") {
        Ok(s) => match s.parse::<f64>() {
            Ok(f) => f,
            Err(_) => {
                eprintln!(
                    "failed to parse INDEXIFY_TRACE_PERCENT, using default {}",
                    DEFAULT_PERCENT_TRACED
                );
                DEFAULT_PERCENT_TRACED
            }
        },
        Err(_) => {
            eprintln!(
                "INDEXIFY_TRACE_PERCENT not set, using default {}",
                DEFAULT_PERCENT_TRACED
            );
            DEFAULT_PERCENT_TRACED
        }
    }
}

fn setup_otlp_tracing() -> Result<()> {
    let endpoint = match std::env::var("INDEXIFY_TRACE_ENDPOINT") {
        Ok(s) => s,
        Err(_) => return Err(anyhow!("trace endpoint not configured")),
    };
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                .with_id_generator(RandomIdGenerator::default())
                .with_sampler(Sampler::TraceIdRatioBased(get_percent_traced()))
                .with_resource(Resource::new(vec![KeyValue::new(
                    "service.name",
                    "indexify",
                )])),
        )
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .http()
                .with_timeout(std::time::Duration::from_secs(10))
                .with_endpoint(endpoint),
        )
        .with_batch_config(BatchConfig::default())
        .install_batch(runtime::Tokio)?;
    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| anyhow!("failed to set global default subscriber: {}", e))
}

const DATADOG_DEFAULT_ENDPOINT: &str = "http://localhost:8126";

fn setup_datadog_tracing() -> Result<()> {
    let endpoint =
        std::env::var("INDEXIFY_TRACE_ENDPOINT").unwrap_or(DATADOG_DEFAULT_ENDPOINT.to_string());
    let tracer = opentelemetry_datadog::new_pipeline()
        .with_service_name("indexify")
        .with_api_version(opentelemetry_datadog::ApiVersion::Version05)
        .with_agent_endpoint(endpoint)
        .with_trace_config(
            opentelemetry_sdk::trace::config()
                .with_sampler(Sampler::TraceIdRatioBased(get_percent_traced()))
                .with_id_generator(RandomIdGenerator::default()),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;
    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| anyhow!("failed to set global default subscriber: {}", e))
}

fn setup_tracing(trace_type: &str) -> Result<()> {
    match trace_type {
        "stdout" => setup_stdout_tracing(),
        "datadog" => setup_datadog_tracing(),
        "otlp" => setup_otlp_tracing(),
        _ => Err(anyhow!("invalid trace type")),
    }
}

pub(crate) fn setup_fmt_tracing() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    println!("Running with tracing filter {}", env_filter);
    let subscriber = tracing_subscriber::Registry::default().with(
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_filter(env_filter),
    );
    if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("failed to set global default subscriber: {}", e);
    }
}

struct OtelGuard;

impl OtelGuard {
    fn new() -> Self {
        if let Ok(trace_type) = std::env::var("INDEXIFY_TRACE") {
            if let Err(e) = setup_tracing(&trace_type) {
                eprintln!("failed to setup tracing with type {}: {}", trace_type, e);
                setup_fmt_tracing();
            }
        } else {
            setup_fmt_tracing();
        }

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

    CryptoProvider::install_default(rustls::crypto::ring::default_provider()).unwrap();

    cmd::Cli::parse().run().await;
}
