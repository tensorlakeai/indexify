use anyhow::{Error, Result};

use clap::{Args, Parser, Subcommand};
use indexify::{
    coordinator_service::CoordinatorServer, executor_server::ExecutorServer, extractor, server,
    server_config::ExecutorConfig, server_config::ExtractorConfig, server_config::ServerConfig,
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
enum ExtractorCmd {
    #[command(about = "join the extractor to indexify")]
    Start {
        #[arg(
            long,
            help = "address of the extractor to advertise to indexify control plane"
        )]
        advertise_addr: Option<String>,

        #[arg(long, help = "address of the indexify server")]
        coordinator_addr: String,
    },
    Package {
        #[arg(short, long)]
        dev: bool,

        #[arg(short, long)]
        verbose: bool,
    },
    Extract {
        #[arg(short = 'e', long)]
        extractor_path: Option<String>,

        #[arg(short, long)]
        text: Option<String>,

        #[arg(short, long)]
        file: Option<String>,
    },
}

#[derive(Debug, Args)]
struct ExtractorCmdArgs {
    #[arg(
        global = true,
        short = 'c',
        long,
        help = "path to the extractor config file",
        default_value = "indexify.yaml"
    )]
    config_path: Option<String>,

    #[command(subcommand)]
    commands: ExtractorCmd,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Start the server")]
    Server {
        #[arg(long, short = 'c', help = "path to the server config file")]
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
    Extractor(ExtractorCmdArgs),
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
    let args: Cli = Cli::parse();
    let version = format!(
        "git branch: {} - sha:{}",
        env!("VERGEN_GIT_BRANCH"),
        env!("VERGEN_GIT_SHA")
    );
    match args.command {
        Commands::Server {
            config_path,
            dev_mode,
        } => {
            info!("starting indexify server, version: {}", version);
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
            info!("starting indexify coordinator, version: {}", version);

            let config = ServerConfig::from_path(&config_path)?;
            let coordinator = CoordinatorServer::new(Arc::new(config)).await?;
            coordinator.run().await?
        }
        Commands::Extractor(args) => {
            let config_path = args
                .config_path
                .unwrap_or_else(|| "indexify.yaml".to_string());
            info!("using config file: {}", &config_path);
            match args.commands {
                ExtractorCmd::Extract {
                    extractor_path,
                    text,
                    file,
                } => {
                    let extracted_content = extractor::run_extractor(extractor_path, text, file)?;
                    println!("{}", serde_json::to_string_pretty(&extracted_content)?);
                }
                ExtractorCmd::Package { dev, verbose } => {
                    info!("starting indexify packager, version: {}", version);
                    let packager = indexify::package::Packager::new(config_path, dev)?;
                    packager.package(verbose).await?;
                }
                ExtractorCmd::Start {
                    advertise_addr,
                    coordinator_addr,
                } => {
                    info!("starting indexify executor, version: {}", version);
                    let extractor_config =
                        Arc::new(ExtractorConfig::from_path(config_path.clone())?);
                    let executor_config = ExecutorConfig::default()
                        .with_advertise_addr(advertise_addr)?
                        .with_coordinator_addr(coordinator_addr);
                    let executor_server =
                        ExecutorServer::new(Arc::new(executor_config), extractor_config).await?;
                    executor_server.run().await?
                }
            }
        }
    }
    global::shutdown_tracer_provider();
    Ok(())
}
