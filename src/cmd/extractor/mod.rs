use clap::{Args as ClapArgs, Subcommand};

use super::GlobalArgs;
use crate::prelude::*;

mod extract;
mod info;
mod new;
mod package;
mod start;

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

impl Args {
    pub async fn run(self, global_args: GlobalArgs) {
        let Self {
            command,
        } = self;

        match command {
            Command::Extract(args) => args.run(global_args).await,
            Command::New(args) => args.run(global_args).await,
            Command::Package(args) => args.run(global_args).await,
            Command::Start(args) => args.run(global_args).await,
            Command::Info(args) => args.run(global_args).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Extract(extract::Args),
    New(new::Args),
    Package(package::Args),
    Info(info::Args),
    /// join the extractor to indexify
    Start(start::Args),
}
