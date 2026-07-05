pub(crate) mod dead_letter;
pub(crate) mod inspect;
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
    /// Show the full state of one schedule by id.
    Inspect(inspect::InspectArgs),
    /// Dead-letter queue operations.
    DeadLetter {
        #[command(subcommand)]
        action: dead_letter::DeadLetterAction,
    },
}

impl SchedulerAction {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        match self {
            Self::Schema(args) => args.run(),
            Self::List(args) => args.run().await,
            Self::Inspect(args) => args.run().await,
            Self::DeadLetter { action } => action.run().await,
        }
    }
}
