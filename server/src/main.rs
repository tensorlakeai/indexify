use std::path::PathBuf;

use clap::Parser;
use service::Service;
use tracing::error;
use tracing_subscriber::{
    fmt::{
        self,
        format::{Format, JsonFields},
    },
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

mod config;
mod executors;
mod gc;
mod http_objects;
mod routes;
mod scheduler;
mod service;
mod system_tasks;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, help = "Development mode")]
    dev: bool,
    #[arg(short, long, value_name = "config file", help = "Path to config file")]
    config: Option<PathBuf>,
}

fn setup_tracing(structured_logging: bool) {
    // RUST_LOG used to control logging level.
    let env_filter_layer =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            tracing_subscriber::EnvFilter::default()
                .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        });

    if structured_logging {
        let log_layer = fmt::layer()
            .event_format(
                Format::default()
                    .json()
                    .with_span_list(false)
                    .flatten_event(true),
            )
            .fmt_fields(JsonFields::default());
        tracing_subscriber::registry()
            .with(env_filter_layer)
            .with(log_layer)
            .init();
        return;
    }

    tracing_subscriber::registry()
        .with(env_filter_layer)
        .with(tracing_subscriber::fmt::layer().compact())
        .init();
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = match cli.config {
        Some(path) => config::ServerConfig::from_path(path.to_str().unwrap()).unwrap(),
        None => config::ServerConfig::default(),
    };
    setup_tracing(!cli.dev);

    let service = Service::new(config).await;
    if let Err(err) = service {
        error!("Error creating service: {:?}", err);
        return;
    }
    if let Err(err) = service.unwrap().start().await {
        error!("Error starting service: {:?}", err);
    }
}
