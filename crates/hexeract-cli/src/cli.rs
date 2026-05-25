use clap::Parser;
use clap::Subcommand;

use crate::commands;

/// Hexeract command-line interface.
#[derive(Parser, Debug)]
#[command(name = "hexeract")]
#[command(version, about, long_about = None)]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Operate on the outbox storage.
    Outbox {
        #[command(subcommand)]
        action: commands::outbox::OutboxAction,
    },
}

impl Cli {
    pub(crate) async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        match self.command {
            Commands::Outbox { action } => action.run().await,
        }
    }
}
