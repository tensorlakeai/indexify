use std::{env, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use config::ServerConfig;
use opentelemetry::{global, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::Sampler};
use service::Service;
use tracing::error;
use tracing_subscriber::{
    fmt::{
        self,
        format::{Format, JsonFields},
    },
    layer::SubscriberExt,
    Layer,
};

mod config;
mod executors;
mod gc;
mod http_objects;
mod routes;
mod scheduler;
mod service;
mod system_tasks;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, help = "Development mode")]
    dev: bool,
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

fn get_log_layer<S>(structured_logging: bool) -> Box<dyn Layer<S> + Send + Sync + 'static>
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

fn setup_tracing(config: ServerConfig) -> Result<()> {
    let structured_logging = !config.dev;
    let env_filter_layer = get_env_filter();
    let log_layer = get_log_layer(structured_logging);
    let subscriber = tracing_subscriber::Registry::default()
        .with(env_filter_layer)
        .with(log_layer);

    if !config.tracing.enabled {
        if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
            error!("logger was already initiated, continuing: {:?}", e);
        }
        return Ok(());
    }

    let mut span_exporter: Option<opentelemetry_otlp::TonicExporterBuilder> = None;
    // If endpoint is configured use it, otherwise use the otlp defaults.
    if let Some(endpoint) = config.tracing.endpoint.clone() {
        span_exporter.replace(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        );
    }
    let span_exporter = span_exporter.unwrap_or(opentelemetry_otlp::new_exporter().tonic());

    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                .with_sampler(Sampler::AlwaysOn)
                .with_resource(opentelemetry_sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new("service.name", "indexify-server"),
                    opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                ])),
        )
        .with_exporter(span_exporter)
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer("tracing-otel-subscriber");

    // Create a layer with the configured tracer
    let otel_layer = tracing_opentelemetry::layer()
        .with_error_records_to_exceptions(true)
        .with_tracer(tracer);
    global::set_tracer_provider(tracer_provider.clone());

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    tracing::subscriber::set_global_default(subscriber.with(otel_layer))?;

    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let mut config = match cli.config {
        Some(path) => config::ServerConfig::from_path(path.to_str().unwrap()).unwrap(),
        None => config::ServerConfig::default(),
    };

    // Override config with cli arguments.
    if cli.dev {
        config.dev = true;
    }

    if let Err(err) = setup_tracing(config.clone()) {
        error!("Error setting up tracing: {:?}", err);
        return;
    }

    let service = Service::new(config).await;
    if let Err(err) = service {
        error!("Error creating service: {:?}", err);
        return;
    }
    if let Err(err) = service.unwrap().start().await {
        error!("Error starting service: {:?}", err);
    }

    // export traces before shutdown
    opentelemetry::global::shutdown_tracer_provider();
}
