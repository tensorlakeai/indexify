use clap::{Args, Parser, Subcommand};

mod coordinator;
mod extractor;
mod init_config;
mod server;

/// Global arguments for the CLI. These are arguments that are shared across all
/// subcommands.
#[derive(Debug, Args)]
pub struct GlobalArgs {
    /// how verbose the logging should be
    #[arg(
        global = true,
        short = 'v',
        long = None,
        default_value = "1",
        action = clap::ArgAction::Count,
    )]
    pub verbosity: u8,
}

/// The list of commands that can be run on indexify.
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Start the server
    Server(server::Args),
    Coordinator(coordinator::Args),
    InitConfig(init_config::Args),
    Extractor(extractor::Args),
}

/// The main CLI struct. This is the root of the CLI tree.
#[derive(Debug, Parser)]
#[command(name = "indexify")]
#[command(about = "CLI for the Indexify Server", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    #[command(flatten)]
    pub global_args: GlobalArgs,
}

impl Cli {
    /// Run the CLI
    pub async fn run(self) {
        match self.command {
            Commands::Server(args) => args.run(self.global_args).await,
            Commands::Coordinator(args) => args.run(self.global_args).await,
            Commands::InitConfig(args) => args.run(self.global_args).await,
            Commands::Extractor(args) => args.run(self.global_args).await,
        }
    }
}
