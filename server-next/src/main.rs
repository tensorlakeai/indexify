use std::path::PathBuf;

use clap::Parser;
use service::Service;
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod config;
mod executors;
mod http_objects;
mod routes;
mod scheduler;
mod server;
mod service;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "config file")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
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
