//! Hexeract command-line interface (`hexeract`).
//!
//! The binary exposes operations on the framework's runtime building
//! blocks. Each feature ships its own command namespace; `outbox` is
//! shipped in v0.1.0 with the `patch`, `apply` and `check` actions.
//!
//! Run `hexeract --help` for the full command tree.

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod cli;
mod commands;

use cli::Cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("warn,hexeract_cli=info")),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    Cli::parse().run().await
}
