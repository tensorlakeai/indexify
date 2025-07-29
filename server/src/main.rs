use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Ok, Result};
use clap::Parser;
use config::ServerConfig;
use opentelemetry::global;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{SdkTracerProvider, TracerProviderBuilder};
use service::Service;
use tracing::error;
use tracing_appender;
use tracing_subscriber::{
    fmt::{
        self,
        format::{Format, JsonFields},
    },
    layer::SubscriberExt,
    Layer,
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

/// Builds up a configuration for the local log layer.
///
/// The idea here is that we want to be able to use a separate local log file
/// with detailed event information (more detailed than we want to send to our
/// log collector). So we allow the user to configure a target->level map for
/// local logs; to keep these from growing too crazily large, we make sure to
/// rotate them on a daily basis.
fn get_local_log_layer<S>(config: &ServerConfig) -> Box<dyn Layer<S> + Send + Sync>
where
    S: for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    S: tracing::Subscriber,
{
    let Some(log_file_path) = &config.telemetry.local_log_file else {
        // Return Identity layer if no local log file is configured
        return Box::new(tracing_subscriber::layer::Identity::new());
    };

    // Build Targets filter from config
    let mut targets = tracing_subscriber::filter::Targets::new();
    for (target, level_str) in &config.telemetry.local_log_targets {
        let level = tracing::Level::from_str(level_str).unwrap_or_else(|_| {
            error!(
                "Invalid log level '{}' for target '{}', defaulting to DEBUG",
                level_str, target
            );
            tracing::Level::DEBUG
        });
        targets = targets.with_target(target, level);
    }

    let file_appender = tracing_appender::rolling::daily(
        Path::new(log_file_path).parent().unwrap_or(Path::new(".")),
        Path::new(log_file_path)
            .file_name()
            .unwrap_or(OsStr::new("local.log")),
    );
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false) // No ANSI colors for file output
        .with_filter(targets);

    // Store the guard to prevent it from being dropped
    std::mem::forget(_guard);

    Box::new(file_layer)
}

fn setup_tracing(config: ServerConfig) -> Result<Option<SdkTracerProvider>> {
    let structured_logging = !config.dev;
    let env_filter_layer = get_env_filter();
    let log_layer = get_log_layer(structured_logging);
    let local_log_layer = get_local_log_layer(&config);
    let subscriber = tracing_subscriber::Registry::default()
        .with(log_layer.with_filter(env_filter_layer))
        .with(local_log_layer);

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

    let tracing_provider = setup_tracing(config.clone())
        .inspect_err(|e| {
            error!("Error setting up tracing: {:?}", e);
        })
        .unwrap();

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
