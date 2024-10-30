use std::{path::PathBuf, sync::Arc};
use std::sync::Mutex;
use anyhow::anyhow;
use clap::Parser;
use opentelemetry::{global::meter, metrics::ObservableCounter};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use service::Service;
use state_store::IndexifyState;
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

use crate::http_objects::IndexifyAPIError;

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
    unallocated_tasks: ObservableCounter<u64>,
}

impl MetricsData {
    pub fn new(indexify_state: Arc<IndexifyState>) -> MetricsData {
        let unallocated_tasks = meter("unallocated_tasks_counter")
            .u64_observable_counter("unallocated_tasks")
            .with_description("Counter that displays unallocated tasks in the server")
            .with_callback({
                let indexify_state_lock = Mutex::new(indexify_state.clone());

                move |observer| {
                    let indexify_state = indexify_state_lock.lock().unwrap();
                    let unallocated_tasks = indexify_state.reader().unallocated_tasks();

                    match unallocated_tasks {
                        Ok(tasks) => {
                            for task in tasks {
                                let compute_graph = indexify_state
                                    .reader()
                                    .get_compute_graph(&task.namespace, &task.compute_graph_name)
                                    .map_err(|_| {
                                        IndexifyAPIError::internal_error(anyhow!(
                                            "Unable to read metrics"
                                        ))
                                    })
                                    .unwrap();

                                let compute_graph = match compute_graph {
                                    None => {
                                        continue;
                                    }
                                    Some(x) => x,
                                };

                                let node = compute_graph.nodes.get(&task.compute_fn_name).unwrap();

                                let image_version = node.image_version();
                                let image_name = node.image_name().to_string();

                                let version_kv = opentelemetry::KeyValue::new(
                                    "image_version",
                                    image_version.to_string(),
                                );
                                let name_kv =
                                    opentelemetry::KeyValue::new("image_name", image_name);
                                observer.observe(1, &[version_kv, name_kv])
                            }
                        }
                        Err(_) => {}
                    }
                }
            })
            .init();

        MetricsData { unallocated_tasks }
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
