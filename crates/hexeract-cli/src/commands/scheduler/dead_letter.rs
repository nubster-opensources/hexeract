//! `scheduler dead-letter list` and `scheduler dead-letter replay` subcommands.

use clap::{Args, Subcommand};
use uuid::Uuid;

use super::open::{AnyScheduleAdmin, DatabaseArgs};
use super::view::{OutputFormat, ScheduleView, render};
use crate::error::CliError;

/// Dead-letter queue operations.
#[derive(Subcommand, Debug)]
pub(crate) enum DeadLetterAction {
    /// List dead-lettered schedules.
    List(DeadLetterListArgs),
    /// Replay a dead-lettered schedule: reset attempts and reschedule now.
    Replay(DeadLetterReplayArgs),
}

impl DeadLetterAction {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        match self {
            Self::List(args) => args.run().await,
            Self::Replay(args) => args.run().await,
        }
    }
}

/// List dead-lettered schedules.
#[derive(Args, Debug)]
pub(crate) struct DeadLetterListArgs {
    #[command(flatten)]
    db: DatabaseArgs,
    #[arg(long, value_enum, default_value_t = OutputFormat::default())]
    format: OutputFormat,
    #[arg(long, default_value_t = 50)]
    limit: usize,
}

impl DeadLetterListArgs {
    async fn run(self) -> Result<(), CliError> {
        let admin = AnyScheduleAdmin::open(&self.db.conn, &self.db.table).await?;
        let snapshots = admin
            .list_dead_letter(self.limit)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        let views: Vec<ScheduleView> = snapshots.iter().map(ScheduleView::from).collect();
        print!("{}", render(&views, self.format)?);
        Ok(())
    }
}

/// Replay a dead-lettered schedule: reset attempts and reschedule now.
#[derive(Args, Debug)]
pub(crate) struct DeadLetterReplayArgs {
    schedule_id: Uuid,
    #[command(flatten)]
    db: DatabaseArgs,
}

impl DeadLetterReplayArgs {
    async fn run(self) -> Result<(), CliError> {
        let admin = AnyScheduleAdmin::open(&self.db.conn, &self.db.table).await?;
        admin
            .replay(self.schedule_id)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        println!("Replayed schedule {}.", self.schedule_id);
        Ok(())
    }
}
