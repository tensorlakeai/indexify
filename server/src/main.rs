use std::path::PathBuf;

use ::tracing::info_span;
use anyhow::Context;
use clap::Parser;
use service::Service;

mod blob_store;
mod cloud_events;
mod config;
mod data_model;
mod executor_api;
mod executors;
mod gc_test;
mod http_objects;
mod http_objects_v1;
mod indexify_ui;
mod metrics;
mod middleware;
mod processor;
mod routes;
mod routes_internal;
mod routes_v1;
mod service;
mod state_store;
mod tracing;
use tracing::setup_tracing;
mod pb_helpers;
mod utils;

#[cfg(test)]
mod integration_test;
#[cfg(test)]
mod integration_test_blocking_calls;
#[cfg(test)]
mod integration_test_executor_catalog;
#[cfg(test)]
mod integration_test_scanner;
mod queue;
#[cfg(test)]
mod reconciliation_test;
#[cfg(test)]
mod testing;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "config file", help = "Path to config file")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = match cli.config {
        Some(path) => config::ServerConfig::from_path(path.to_str().unwrap()).unwrap(),
        None => config::ServerConfig::default(),
    };

    setup_tracing(&config)?;

    let root_span = info_span!(
        "indexify",
        env = config.env,
        "indexify-instance" = config.instance_id()
    );
    let _guard = root_span.enter();

    let mut service = Service::new(config)
        .await
        .context("Failed to create service")?;
    service.start().await.context("Failed to start service")?;

    Ok(())
}
