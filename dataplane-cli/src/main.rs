use anyhow::Result;
use clap::Parser;
use dataplane_core::{config::Config, DataplaneService};
use std::path::PathBuf;
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

    let _dataplane = DataplaneService::new(config);

    info!("Dataplane service started. Press Ctrl-C to stop.");

    tokio::signal::ctrl_c().await?;

    info!("Shutting down gracefully...");

    Ok(())
}
