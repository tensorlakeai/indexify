use std::{path::PathBuf, process};

use clap::Parser;
use service::Service;
use tracing::error;

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
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
        println!("Error setting up tracing: {}", err);
        process::exit(1);
    }

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
