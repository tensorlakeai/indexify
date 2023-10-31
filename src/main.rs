use anyhow::{Error, Result};

use clap::{Parser, Subcommand};
use indexify::{
    coordinator_service::CoordinatorServer, executor_server::ExecutorServer, server,
    server_config::ExecutorConfig, server_config::ServerConfig,
};
use std::sync::Arc;
use tracing::{debug, info};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

use opentelemetry::global;

use tracing_core::Level;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Debug, Parser)]
#[command(name = "indexify")]
#[command(about = "CLI for the Indexify Server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Start the server")]
    Server {
        #[arg(long)]
        config_path: String,

        #[arg(short, long)]
        dev_mode: bool,
    },
    Coordinator {
        #[arg(short, long)]
        config_path: String,
    },
    Executor {
        #[arg(short, long)]
        config_path: String,

        #[arg(long)]
        advertise_ip: Option<String>,

        #[arg(long)]
        advertise_port: Option<u64>,

        #[arg(long)]
        coordinator_addr: Option<String>,
    },
    InitConfig {
        config_path: String,
    },
    Package {
        #[arg(short, long)]
        config_path: String,

        #[arg(short, long)]
        dev: bool,
    },
}

struct OtelGuard {}

impl OtelGuard {
    fn new() -> Self {
        tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::from_level(
                Level::INFO,
            ))
            .with(tracing_subscriber::fmt::layer())
            .init();

        OtelGuard {}
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let _otel_guard = OtelGuard::new();
    // Parse CLI and any env variables
    let args: Cli = Cli::parse();
    let version = format!(
        "git branch: {} - sha:{}",
        env!("VERGEN_GIT_BRANCH"),
        env!("VERGEN_GIT_SHA")
    );
    info!("indexify version: {}", version);

    match args.command {
        Commands::Server {
            config_path,
            dev_mode,
        } => {
            info!("starting indexify server....");
            let config = ServerConfig::from_path(&config_path)?;
            debug!("Server config is: {:?}", config);
            let server = server::Server::new(Arc::new(config.clone()))?;
            let server_handle = tokio::spawn(async move {
                server.run().await.unwrap();
            });
            if dev_mode {
                let coordinator = CoordinatorServer::new(Arc::new(config.clone())).await?;
                let coordinator_handle = tokio::spawn(async move {
                    coordinator.run().await.unwrap();
                });
                tokio::try_join!(server_handle, coordinator_handle)?;
                return Ok(());
            }
            tokio::try_join!(server_handle)?;
        }
        Commands::InitConfig { config_path } => {
            println!("Initializing config file at: {}", &config_path);
            ServerConfig::generate(config_path).unwrap();
        }
        Commands::Coordinator { config_path } => {
            info!("starting indexify coordinator....");

            let config = ServerConfig::from_path(&config_path)?;
            let coordinator = CoordinatorServer::new(Arc::new(config)).await?;
            coordinator.run().await?
        }
        Commands::Executor {
            config_path,
            advertise_ip,
            advertise_port,
            coordinator_addr,
        } => {
            info!("starting indexify executor....");

            let config = ExecutorConfig::from_path(&config_path)?
                .with_advertise_ip(advertise_ip)
                .with_advertise_port(advertise_port)
                .with_coordinator_addr(coordinator_addr);
            let executor_server = ExecutorServer::new(Arc::new(config)).await?;
            executor_server.run().await?
        }
        Commands::Package { config_path, dev } => {
            info!("starting indexify packager....");

            let packager = indexify::package::Packager::new(config_path, dev)?;
            packager.package().await?;
        }
    }
    global::shutdown_tracer_provider();
    Ok(())
}
