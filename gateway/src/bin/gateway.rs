use clap::Parser;
use gateway::{config::Config, create_service};
use std::path::PathBuf;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "gateway")]
#[command(about = "A GRPC gateway server")]
struct Args {
    #[arg(short, long, help = "Configuration file path")]
    config: Option<PathBuf>,

    #[arg(long, help = "Unix domain socket path")]
    socket_path: Option<String>,

    #[arg(long, help = "Log level (trace, debug, info, warn, error)")]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut config = if let Some(config_path) = args.config {
        match Config::load_from_file(&config_path) {
            Ok(config) => config,
            Err(e) => {
                eprintln!("Failed to load config from {config_path:?}: {e}");
                eprintln!("Using default configuration");
                Config::default()
            }
        }
    } else {
        Config::default()
    };

    config.merge_with_args(args.socket_path, args.log_level);

    init_tracing(&config.server.log_level)?;

    info!("Starting gateway server");
    info!("Socket path: {}", config.server.socket_path);
    info!("Log level: {}", config.server.log_level);

    if std::path::Path::new(&config.server.socket_path).exists() {
        warn!("Socket file already exists, removing: {}", config.server.socket_path);
        std::fs::remove_file(&config.server.socket_path)?;
    }

    let uds = tokio::net::UnixListener::bind(&config.server.socket_path)?;
    let uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

    let service = create_service();

    info!("Gateway server listening on unix socket: {}", config.server.socket_path);

    Server::builder()
        .add_service(service)
        .serve_with_incoming(uds_stream)
        .await?;

    Ok(())
}

fn init_tracing(level: &str) -> anyhow::Result<()> {
    let level = level.parse::<tracing::Level>()
        .map_err(|_| anyhow::anyhow!("Invalid log level: {}", level))?;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    tracing_subscriber::EnvFilter::new(format!("gateway={level}"))
                })
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    Ok(())
}