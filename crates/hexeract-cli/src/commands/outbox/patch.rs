use clap::Args;
use hexeract_outbox_sql::Dialect;

use crate::error::CliError;

/// Print the canonical outbox schema SQL templated with the given table name.
#[derive(Args, Debug)]
pub(crate) struct PatchArgs {
    /// Outbox table name. Must match `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    #[arg(long, default_value = "audit_outbox", env = "HEXERACT_OUTBOX_TABLE")]
    table: String,
}

impl PatchArgs {
    pub(crate) fn run(self) -> Result<(), CliError> {
        let sql = Dialect::Postgres
            .schema_ddl(&self.table)
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        println!("{sql}");
        Ok(())
    }
}
