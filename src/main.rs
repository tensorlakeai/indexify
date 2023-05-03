use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use std::sync::Arc;
use tracing::log::info;

/// The command-line interface (CLI) for the Indexify Server.
/// The CLI provides commands for starting the server and initializing the configuration file.
#[derive(Debug, Parser)]
#[command(name = "indexify")]
#[command(about = "CLI for the Indexify Server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// An enumeration of available subcommands for the Indexify Server CLI.
#[derive(Debug, Subcommand)]
enum Commands {
    /// The `start` subcommand, which starts the Indexify Server.
    /// The server reads its configuration from the specified configuration file.
    #[command(about = "Start the server")]
    Start {
        /// The path to the configuration file that the server should use.
        #[arg(short, long)]
        config_path: String,
    },
    /// The `init-config` subcommand, which initializes a new configuration file.
    /// The generated configuration file can be customized and used to start the server.
    InitConfig {
        /// The path where the new configuration file should be created.
        config_path: String,
    },
}

/// The entry point of the Indexify Server CLI.
/// This function parses the command-line arguments, executes the specified subcommand,
/// and handles any errors that may occur.
#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize the tracing subscriber for logging.
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    // Parse the command-line arguments.
    let args = Cli::parse();
    // Execute the specified subcommand.
    match args.command {
        Commands::Start { config_path } => {
            info!("starting indexify server....");

            // Load the server configuration from the specified file.
            let config = indexify::ServerConfig::from_path(config_path)?;
            // Create a new server instance with the loaded configuration.
            let server = indexify::Server::new(Arc::new(config))?;
            // Start the server and wait for it to complete.
            server.run().await?
        }
        Commands::InitConfig { config_path } => {
            // Print a message indicating the location of the new configuration file.
            println!("Initializing config file at: {}", &config_path);
            // Generate a new configuration file at the specified path.
            indexify::ServerConfig::generate(config_path).unwrap();
        }
    }
    // Return success.
    Ok(())
}
