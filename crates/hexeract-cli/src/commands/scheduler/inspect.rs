use clap::Args;
use uuid::Uuid;

use super::open::{AnyScheduleAdmin, DatabaseArgs};
use super::view::{OutputFormat, ScheduleView, render};
use crate::error::CliError;

/// Show the full state of one schedule by id.
#[derive(Args, Debug)]
pub(crate) struct InspectArgs {
    schedule_id: Uuid,
    #[command(flatten)]
    db: DatabaseArgs,
    #[arg(long, value_enum, default_value_t = OutputFormat::default())]
    format: OutputFormat,
}

impl InspectArgs {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        let admin = AnyScheduleAdmin::open(&self.db.conn, &self.db.table).await?;
        let snapshot = admin
            .inspect(self.schedule_id)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?
            .ok_or_else(|| CliError::Fatal(format!("no schedule {}", self.schedule_id).into()))?;
        let views = [ScheduleView::from(&snapshot)];
        print!("{}", render(&views, self.format)?);
        Ok(())
    }
}
