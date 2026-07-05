use clap::Args;

use super::open::{AnyScheduleAdmin, DatabaseArgs};
use super::view::{OutputFormat, ScheduleView, render};
use crate::error::CliError;

/// List non-terminal (pending and paused) schedules.
#[derive(Args, Debug)]
pub(crate) struct ListArgs {
    #[command(flatten)]
    db: DatabaseArgs,
    #[arg(long, value_enum, default_value_t = OutputFormat::default())]
    format: OutputFormat,
    #[arg(long, default_value_t = 50)]
    limit: usize,
}

impl ListArgs {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        let admin = AnyScheduleAdmin::open(&self.db.conn, &self.db.table).await?;
        let snapshots = admin
            .list_pending(self.limit)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        let views: Vec<ScheduleView> = snapshots.iter().map(ScheduleView::from).collect();
        print!("{}", render(&views, self.format)?);
        Ok(())
    }
}
