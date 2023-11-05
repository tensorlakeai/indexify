use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use indexify::{
    coordinator::CoordinatorServer,
    executor::ExecutorServer,
    server,
    server_config::ExecutorConfig,
    server_config::{CoordinatorConfig, ServerConfig}, raft::coordinator_node::RaftCoordinatorNode,
};
use std::sync::Arc;
use tracing::{debug, info};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::{
    resource::{DEPLOYMENT_ENVIRONMENT, SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use tracing_core::Level;
use tracing_subscriber::util::SubscriberInitExt;

// Raft
use axum::{Extension, Router};
use indexify::raft::{raft_api, management, memstore::{MemNodeId, MemStore}, network::IndexifyRaftNetwork, coordinator_config::CoordinatorRaftApp};
use openraft::{Raft, Config};

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
    StartServer {
        #[arg(long)]
        config_path: String,

        #[arg(long)]
        coordinator_config_path: Option<String>,

        #[arg(short, long)]
        dev_mode: bool,
    },
    Coordinator {
        #[arg(short, long)]
        config_path: String,
    },
    CoordinatorRaft {
        #[arg(short, long)]
        node_id: u64,

        #[arg(short, long)]
        config_path: String,
    },
    Executor {
        #[arg(short, long)]
        config_path: String,
    },
    InitConfig {
        config_path: String,
    },
}

// Create a Resource that captures information about the entity for which telemetry is recorded.
fn resource() -> Resource {
    Resource::from_schema_url(
        [
            KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
            KeyValue::new(DEPLOYMENT_ENVIRONMENT, "develop"),
        ],
        SCHEMA_URL,
    )
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

    match args.command {
        Commands::StartServer {
            config_path,
            coordinator_config_path,
            dev_mode,
        } => {
            info!("starting indexify server....");
            info!("version: {}", version);
            let config = ServerConfig::from_path(&config_path)?;
            debug!("Server config is: {:?}", config);
            let server = server::Server::new(Arc::new(config.clone()))?;
            let server_handle = tokio::spawn(async move {
                server.run().await.unwrap();
            });
            if dev_mode {
                let coordinator_config =
                    CoordinatorConfig::from_path(&coordinator_config_path.unwrap().clone())?;
                let coordinator = CoordinatorServer::new(Arc::new(coordinator_config)).await?;
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
            info!("version: {}", version);

            let config = CoordinatorConfig::from_path(&config_path)?;
            let coordinator = CoordinatorServer::new(Arc::new(config)).await?;
            coordinator.run().await?
        }
        Commands::CoordinatorRaft { node_id, config_path } => {
            info!("starting indexify coordinator node using Raft protocol....");
            info!("version: {}", version);

            // load the config
            let coordinator_config = CoordinatorConfig::from_path(&config_path)?;

            // port is coordinator_config "listen_port" plus node_id
            let socket_addr = format!(
                "{}:{}",
                coordinator_config.listen_if,
                coordinator_config.listen_port + node_id
            );

            println!("socket_addr: {}", socket_addr);
            
            let config = Config {
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            };

            let coordinator_node = RaftCoordinatorNode::new(node_id, socket_addr.clone(), config, coordinator_config).await?;
            coordinator_node.run().await?;
        }
        Commands::Executor { config_path } => {
            info!("starting indexify executor....");
            info!("version: {}", version);

            let config = ExecutorConfig::from_path(&config_path)?;
            let executor_server = ExecutorServer::new(Arc::new(config)).await?;
            executor_server.run().await?
        }
    }
    global::shutdown_tracer_provider();
    Ok(())
}
