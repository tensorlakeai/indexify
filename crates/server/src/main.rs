use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use service::Service;

mod blob_store;
mod cloud_events;
mod config;
mod data_model;
mod executor_api;
mod executors;
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
mod integration_test_buffer;
#[cfg(test)]
mod integration_test_executor_catalog;
#[cfg(test)]
mod integration_test_http_routes;
#[cfg(test)]
mod integration_test_idle_reclamation;
#[cfg(test)]
mod integration_test_sandboxes;
#[cfg(test)]
mod integration_test_scanner;
#[cfg(test)]
mod integration_test_vacuum;
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

    /// Generate Cloud OpenAPI spec and exit
    #[arg(long)]
    gen_cloud_openapi: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.gen_cloud_openapi {
        use routes_v1::ApiDoc;
        use utoipa::OpenApi;

        let spec = ApiDoc::openapi()
            .to_pretty_json()
            .expect("Failed to generate OpenAPI JSON");
        std::fs::write("openapi.json", spec).expect("Failed to write openapi.json");
        println!("Generated openapi.json");
        return Ok(());
    }

    let config = match cli.config {
        Some(path) => config::ServerConfig::from_path(path.to_str().unwrap()).unwrap(),
        None => config::ServerConfig::default(),
    };

    setup_tracing(&config)?;

    start_indexify(config).await
}

#[::tracing::instrument(skip(config), fields(env = config.env, instance_id = config.instance_id()))]
async fn start_indexify(config: config::ServerConfig) -> anyhow::Result<()> {
    let mut service = Service::new(config)
        .await
        .context("Failed to create service")?;
    service.start().await.context("Failed to start service")
}
