use std::path::PathBuf;

use clap::Parser;
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use service::Service;
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

mod config;
mod executors;
mod gc;
mod http_objects;
mod routes;
mod scheduler;
mod server;
mod service;
mod system_tasks;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "config file")]
    config: Option<PathBuf>,
}

#[derive(Clone)]
struct MetricsData {
    counter: Counter<u64>,
}

impl Default for MetricsData {
    fn default() -> Self { Self::new() }
}

impl MetricsData {
    pub fn new() -> MetricsData {
        let counter = meter("").u64_counter("counter1")
            .with_description("Simple counter for testin")
            .init();

        MetricsData {
            counter
        }
    }
}

fn init_provider() -> prometheus::Registry {
    let registry = prometheus::Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build();

    let mut provider = SdkMeterProvider::builder();

    if let Ok(exporter) = exporter {
        provider = provider.with_reader(exporter);
    };

    opentelemetry::global::set_meter_provider(provider.build());

    registry
}

#[tokio::main]
async fn main() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(env_filter))
        .init();

    let cli = Cli::parse();
    let config = match cli.config {
        Some(path) => config::ServerConfig::from_path(path.to_str().unwrap()).unwrap(),
        None => config::ServerConfig::default(),
    };
    let service = Service::new(config);
    if let Err(err) = service.start().await {
        error!("Error starting service: {}", err);
    }
}
