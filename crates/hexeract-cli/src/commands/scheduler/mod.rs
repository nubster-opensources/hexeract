pub(crate) mod list;
pub(crate) mod open;
pub(crate) mod schema;
pub(crate) mod view;

use clap::Subcommand;

use crate::error::CliError;

/// Actions targeting the scheduler storage.
#[derive(Subcommand, Debug)]
pub(crate) enum SchedulerAction {
    /// Print the scheduler schema DDL for the selected dialect.
    Schema(schema::SchemaArgs),
    /// List non-terminal (pending and paused) schedules.
    List(list::ListArgs),
}

impl SchedulerAction {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        match self {
            Self::Schema(args) => args.run(),
            Self::List(args) => args.run().await,
        }
    }
}
