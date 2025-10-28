use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::Parser;
use dataplane_core::{DataplaneService, config::Config};
use tracing::info;
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(name = "indexify-executor")]
#[command(about = "Indexify Dataplane Executor", long_about = None)]
struct Args {
    /// Path to the configuration file
    #[arg(short = 'c', long = "config", value_name = "FILE")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("starting indexify dataplane");
    let config = match &args.config {
        Some(config_path) => {
            info!("loading config from: {:?}", config_path);
            Config::from_path(config_path.to_str().unwrap())?
        }
        None => {
            info!("no config file provided, using default configuration");
            Config::default()
        }
    };

    info!("Executor ID: {}", config.executor_id);

    let dataplane = Arc::new(DataplaneService::new(config));

    info!("Dataplane service started. Press Ctrl-C to stop.");

    // Start the service in a separate task
    let service_handle = tokio::spawn({
        let dataplane_clone = dataplane.clone();
        async move {
            if let Err(e) = dataplane_clone.start().await {
                tracing::error!("Dataplane service error: {}", e);
            }
        }
    });

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;

    info!("Ctrl-C received, shutting down gracefully...");

    // Shutdown the service
    dataplane.shutdown().await?;

    // Wait for the service to complete
    service_handle.await?;

    info!("Dataplane service stopped successfully");

    Ok(())
}
