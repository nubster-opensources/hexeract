pub(crate) mod schema;

use clap::Subcommand;

use crate::error::CliError;

/// Actions targeting the scheduler storage.
#[derive(Subcommand, Debug)]
pub(crate) enum SchedulerAction {
    /// Print the scheduler schema DDL for the selected dialect.
    Schema(schema::SchemaArgs),
}

impl SchedulerAction {
    // `Schema` needs no database and has no `.await`, but upcoming actions
    // (list, inspect, replay) do; keep the signature `async` so `Cli::run`
    // dispatches uniformly across every namespace.
    #[allow(clippy::unused_async)]
    pub(crate) async fn run(self) -> Result<(), CliError> {
        match self {
            Self::Schema(args) => args.run(),
        }
    }
}
