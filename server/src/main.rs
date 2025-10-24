use std::path::PathBuf;

use ::tracing::{error, info_span};
use clap::Parser;
use service::Service;

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
mod middleware;
mod processor;
mod reconciliation_test;
mod routes;
mod routes_internal;
mod routes_v1;
mod service;
mod state_store;
mod tracing;
use tracing::setup_tracing;
mod utils;

mod queue;
#[cfg(test)]
mod testing;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "config file", help = "Path to config file")]
    config: Option<PathBuf>,
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
