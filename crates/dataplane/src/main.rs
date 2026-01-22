use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;

mod config;
mod daemon_binary;
mod daemon_client;
mod driver;
mod function_container_manager;
mod metrics;
mod network_rules;
mod otel_tracing;
mod resources;
mod service;
mod state_file;

use config::DataplaneConfig;
use otel_tracing::setup_tracing;
use service::Service;
use tracing::{info, instrument};

#[derive(Parser)]
#[command(name = "indexify-dataplane")]
#[command(version, about = "Indexify Dataplane Service", long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = match cli.config {
        Some(path) => DataplaneConfig::from_path(path.to_str().unwrap())?,
        None => DataplaneConfig::default(),
    };

    setup_tracing(&config)?;

    metrics::init_provider(
        &config.telemetry,
        &config.instance_id(),
        &config.executor_id,
    )?;

    start_dataplane(config).await
}

#[instrument(skip(config), fields(env = config.env, instance_id = config.instance_id()))]
async fn start_dataplane(config: DataplaneConfig) -> anyhow::Result<()> {
    info!(
        server_addr = %config.server_addr,
        tls_enabled = config.tls.enabled,
        "Starting Indexify Dataplane"
    );

    let daemon_path =
        daemon_binary::extract_daemon_binary().context("Failed to extract daemon binary")?;
    info!(daemon_path = %daemon_path.display(), "Daemon binary ready");

    let service = Service::new(config)
        .await
        .context("Failed to create service")?;

    service.run().await
}
