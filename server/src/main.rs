use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use config::ServerConfig;
use opentelemetry::global;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::TracerProvider;
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
mod gc_test;
mod http_objects;
mod integration_test;
mod routes;
mod service;
#[cfg(test)]
mod testing;

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

    let mut span_exporter = SpanExporter::builder().with_tonic();
    if let Some(endpoint) = &config.tracing.endpoint {
        span_exporter = span_exporter.with_endpoint(endpoint.clone());
    }
    let span_exporter = span_exporter.build()?;

    let tracer_provider = TracerProvider::builder()
        .with_simple_exporter(span_exporter)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

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
