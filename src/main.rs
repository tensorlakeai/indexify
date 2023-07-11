use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use indexify::{CoordinatorWorker, ServerConfig};
use std::sync::Arc;
use tracing::info;

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
        #[arg(short, long)]
        config_path: String,

        #[arg(short, long)]
        dev_mode: bool,
    },
    Coordinator {
        #[arg(short, long)]
        config_path: String,
    },
    InitConfig {
        config_path: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;
    //tracing_subscriber::fmt()
    //.with_max_level(tracing::Level::DEBUG)
    //.with_test_writer()
    //.init();

    let args = Cli::parse();
    match args.command {
        Commands::StartServer {
            config_path,
            dev_mode,
        } => {
            info!("starting indexify server....");

            let config = indexify::ServerConfig::from_path(config_path)?;
            let server = indexify::Server::new(Arc::new(config.clone()))?;
            let server_handle = tokio::spawn(async move {
                server.run().await.unwrap();
            });
            if dev_mode {
                let coordinator = CoordinatorWorker::new(Arc::new(config.clone())).await?;
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
            indexify::ServerConfig::generate(config_path).unwrap();
        }
        Commands::Coordinator { config_path } => {
            info!("starting indexify coordinator....");

            let config = ServerConfig::from_path(config_path)?;
            let coordinator = CoordinatorWorker::new(Arc::new(config)).await?;
            coordinator.run().await?
        }
    }
    Ok(())
}
