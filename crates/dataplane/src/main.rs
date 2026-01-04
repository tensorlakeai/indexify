use std::path::PathBuf;

use clap::Parser;

mod agent;
mod config;
mod container_driver;
mod docker;
mod fork_exec;
mod function_executors;
mod objects;
mod tracing;

use tracing::setup_tracing;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "config file", help = "Path to config file")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = if let Some(path) = cli.config {
        config::Config::from_path(path.to_str().unwrap())?
    } else {
        config::Config::default()
    };

    setup_tracing(&config)?;

    let agent = agent::Agent::new(config);
    agent.start().await
}
