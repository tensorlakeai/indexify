use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use indexify::{CoordinatorServer, ExecutorServer, ServerConfig};
// use opentelemetry::sdk::export::trace::SpanExporter;
use opentelemetry::{
    global,
    // sdk::trace::TracerProvider,
    // TracerProvider as _
    trace::{Tracer},
};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

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
    Executor {
        #[arg(short, long)]
        config_path: String,
    },
    InitConfig {
        config_path: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Setup Tracing and OpenTelemetry

    // Implement OpenTelemetry Tracer
    let otlp_exporter = opentelemetry_otlp::new_exporter().tonic();
    let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(otlp_exporter)
            .install_simple()?;
    let otlp_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    // Hook it up to tracing
    let subscriber = tracing_subscriber::Registry::default().with(otlp_layer);
    tracing::subscriber::set_global_default(subscriber)?;

    // Parse CLI and start application
    let args = Cli::parse();
    let version = format!(
        "git branch: {} - sha:{}",
        env!("VERGEN_GIT_BRANCH"),
        env!("VERGEN_GIT_SHA")
    );
    match args.command {
        Commands::StartServer {
            config_path,
            dev_mode,
        } => {
            info!("starting indexify server....");
            info!("version: {}", version);

            let config = indexify::ServerConfig::from_path(&config_path)?;
            let server = indexify::Server::new(Arc::new(config.clone()))?;
            let server_handle = tokio::spawn(async move {
                server.run().await.unwrap();
            });
            if dev_mode {
                let coordinator = CoordinatorServer::new(Arc::new(config.clone())).await?;
                let coordinator_handle = tokio::spawn(async move {
                    coordinator.run().await.unwrap();
                });

                let executor_server = ExecutorServer::new(Arc::new(config.clone())).await?;
                let executor_handle = tokio::spawn(async move {
                    executor_server.run().await.unwrap();
                });
                tokio::try_join!(server_handle, coordinator_handle, executor_handle)?;
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
            info!("version: {}", version);

            let config = ServerConfig::from_path(&config_path)?;
            let coordinator = CoordinatorServer::new(Arc::new(config)).await?;
            coordinator.run().await?
        }
        Commands::Executor { config_path } => {
            info!("starting indexify executor....");
            info!("version: {}", version);

            let config = ServerConfig::from_path(&config_path)?;
            let executor_server = ExecutorServer::new(Arc::new(config)).await?;
            executor_server.run().await?
        }
    }

    // Is shutdown here? Should probably also be configured on drop?
    global::shutdown_tracer_provider();
    Ok(())
}
