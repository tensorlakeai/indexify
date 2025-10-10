use std::path::PathBuf;

use anyhow::{Ok, Result};
use clap::Parser;
use config::ServerConfig;
use opentelemetry::global;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{SdkTracerProvider, TracerProviderBuilder};
use service::Service;
use tracing::{error, info_span};
use tracing_subscriber::{
    Layer,
    fmt::{
        self,
        format::{Format, JsonFields},
    },
    layer::SubscriberExt,
};

mod blob_store;
mod config;
mod data_model;
mod executor_api;
mod executors;
mod gc_test;
mod http_objects;
mod http_objects_v1;
mod indexify_ui;
mod integration_test;
mod integration_test_executor_catalog;
mod metrics;
mod processor;
mod reconciliation_test;
mod routes;
mod routes_internal;
mod routes_v1;
mod service;
mod state_store;
mod utils;

#[cfg(test)]
mod testing;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "config file", help = "Path to config file")]
    config: Option<PathBuf>,
}

fn get_env_filter() -> tracing_subscriber::EnvFilter {
    // RUST_LOG used to control logging level.
    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::default()
            .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
    })
}

fn get_log_layer<S>(structured_logging: bool) -> Box<dyn Layer<S> + Send + Sync>
where
    S: for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    S: tracing::Subscriber,
{
    // Create an OTLP pipeline exporter for a `trace_demo` service.
    if structured_logging {
        return Box::new(
            fmt::layer()
                .event_format(
                    Format::default()
                        .json()
                        .with_span_list(false)
                        .flatten_event(true),
                )
                .fmt_fields(JsonFields::default()),
        );
    }

    Box::new(tracing_subscriber::fmt::layer().compact())
}

fn setup_tracing(config: &ServerConfig) -> Result<Option<SdkTracerProvider>> {
    let env_filter_layer = get_env_filter();
    let log_layer = get_log_layer(config.structured_logging());
    let subscriber =
        tracing_subscriber::Registry::default().with(log_layer.with_filter(env_filter_layer));

    if !config.telemetry.enable_tracing {
        if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
            error!("logger was already initiated, continuing: {:?}", e);
        }
        return Ok(None);
    }

    let mut span_exporter = SpanExporter::builder().with_tonic();
    if let Some(endpoint) = &config.telemetry.endpoint {
        span_exporter = span_exporter.with_endpoint(endpoint.clone());
    }
    let span_exporter = span_exporter.build()?;

    let tracer_provider = TracerProviderBuilder::default()
        .with_simple_exporter(span_exporter)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    Ok(Some(tracer_provider))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = match cli.config {
        Some(path) => config::ServerConfig::from_path(path.to_str().unwrap()).unwrap(),
        None => config::ServerConfig::default(),
    };

    let tracing_provider = setup_tracing(&config)
        .inspect_err(|e| {
            error!("Error setting up tracing: {:?}", e);
        })
        .unwrap();

    let root_span = info_span!(
        "indexify",
        env = config.env,
        "indexify-instance" = config.instance_id()
    );
    let _guard = root_span.enter();

    let service = Service::new(config).await;
    if let Err(err) = service {
        error!("Error creating service: {:?}", err);
        return;
    }
    if let Err(err) = service.unwrap().start().await {
        error!("Error starting service: {:?}", err);
    }

    // export traces before shutdown
    if let Some(tracer_provider) = tracing_provider {
        if let Err(err) = tracer_provider.force_flush() {
            error!("Error flushing traces: {:?}", err);
        }
        if let Err(err) = tracer_provider.shutdown() {
            error!("Error shutting down tracer provider: {:?}", err);
        }
    }
}
