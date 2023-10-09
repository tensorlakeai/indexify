use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use indexify::{CoordinatorServer, ExecutorServer, ServerConfig};
use opentelemetry::global;
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use std::sync::Arc;
use tracing::{debug, info};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::EnvFilter;

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

fn initialize_otlp_tracer(service_name: String, trace_id_ratio: f64) -> Result<(), Error> {
    let filter = EnvFilter::from_default_env();

    // TODO: Traces should also be piped to stdout, not only tonic
    // Implement OpenTelemetry Tracer
    let otlp_exporter = opentelemetry_otlp::new_exporter().http();
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(
            opentelemetry::sdk::trace::config()
                .with_resource(Resource::new(vec![KeyValue::new(
                    "service.name",
                    service_name,
                )]))
                // TODO: In production, we can change this config
                .with_sampler(opentelemetry::sdk::trace::Sampler::TraceIdRatioBased(
                    trace_id_ratio,
                )),
        )
        .with_batch_config(opentelemetry::sdk::trace::BatchConfig::default())
        .install_batch(opentelemetry::sdk::runtime::Tokio)?;
    let otlp_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Hook it up to tracing
    let subscriber = tracing_subscriber::registry().with(filter).with(otlp_layer);
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
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
            dev_mode,
        } => {
            info!("starting indexify server....");
            info!("version: {}", version);
            let config = indexify::ServerConfig::from_path(&config_path)?;
            let service_name = if dev_mode {
                "indexify.api_server_dev".to_string()
            } else {
                "indexify.api_server".to_string()
            };
            initialize_otlp_tracer(service_name, config.trace_id_ratio)?;
            debug!("Server config is: {:?}", config);
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
            initialize_otlp_tracer("indexify.coordinator".to_string(), config.trace_id_ratio)?;
            let coordinator = CoordinatorServer::new(Arc::new(config)).await?;
            coordinator.run().await?
        }
        Commands::Executor { config_path } => {
            info!("starting indexify executor....");
            info!("version: {}", version);

            let config = ServerConfig::from_path(&config_path)?;
            initialize_otlp_tracer("indexify.executor".to_string(), config.trace_id_ratio)?;
            let executor_server = ExecutorServer::new(Arc::new(config)).await?;
            executor_server.run().await?
        }
    }
    global::shutdown_tracer_provider();
    Ok(())
}
