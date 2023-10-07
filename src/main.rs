use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use indexify::{CoordinatorServer, ExecutorServer, ServerConfig};
use opentelemetry::metrics::MeterProvider;
// use opentelemetry::sdk::export::trace::SpanExporter;
use opentelemetry::{
    global,
    // sdk::trace::TracerProvider,
    // TracerProvider as _
    trace::Tracer,
};
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::runtime;
use std::sync::Arc;
use tracing::{debug, info};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
// use opentelemetry::metrics::MeterProvider;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Spawn the tracer
    // let subscriber = tracing_subscriber::FmtSubscriber::new();
    // tracing::subscriber::set_global_default(subscriber)?;
    //tracing_subscriber::fmt()
    //.with_max_level(tracing::Level::DEBUG)
    //.with_test_writer()
    //.init();

    // // Spawn telemetry for metrics in prometheus
    // let registry = prometheus::Registry::new();
    // // configure OpenTelemetry to use this registry
    // let exporter = opentelemetry_prometheus::exporter()
    //     .with_registry(registry.clone())
    //     .build()?;

    // let provider = MeterProvider::builder().with_reader(exporter).build();
    // let meter = provider.meter("my-app");
    let filter = EnvFilter::from_default_env();

    // Implement OpenTelemetry Tracer
    let otlp_exporter = opentelemetry_otlp::new_exporter().tonic();
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(
            opentelemetry::sdk::trace::config().with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                "indexify-main-service",
            )])),
        )
        .with_batch_config(opentelemetry::sdk::trace::BatchConfig::default())
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;
    let otlp_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    // Hook it up to tracing
    let subscriber = tracing_subscriber::registry().with(filter).with(otlp_layer);
    tracing::subscriber::set_global_default(subscriber)?;

    // // Implement a meter provider
    // let metric_exporter = opentelemetry_stdout::MetricsExporterBuilder::default()
    //     // uncomment the below lines to pretty print output.
    //     //  .with_encoder(|writer, data|
    //     //    Ok(serde_json::to_writer_pretty(writer, &data).unwrap()))
    //     .build();
    // let reader = PeriodicReader::builder(metric_exporter, runtime::Tokio).build();
    // MeterProvider::builder()
    //     .with_reader(reader)
    //     .with_resource(Resource::new(vec![KeyValue::new(
    //         "service.name",
    //         "indexify-main-metrics",
    //     )]))
    //     .build();

    info!("Spinning up");

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
            debug!("Server config is: {:?}", config);
            let server = indexify::Server::new(Arc::new(config.clone()))?;
            debug!("Server struct is: {:?}", config);
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
