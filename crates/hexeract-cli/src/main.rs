//! Hexeract command-line interface (`hexeract`).
//!
//! The binary exposes operations on the framework's runtime building
//! blocks. Each feature ships its own command namespace:
//!
//! - `outbox` (shipped in v0.1.0) with `patch`, `apply` and `check` actions.
//! - `bus` (shipped in v0.2.0) with `declare`, `peek` and `purge` actions.
//!
//! Run `hexeract --help` for the full command tree.

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod cli;
mod commands;
mod error;

use cli::Cli;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("warn,hexeract_cli=info")),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime must build")
        .block_on(Cli::parse().run());

    match result {
        Ok(()) => {}
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(err.exit_code());
        }
    }
}
