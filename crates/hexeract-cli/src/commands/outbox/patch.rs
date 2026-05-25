use clap::Args;
use hexeract_outbox_postgres::render_schema;

/// Print the canonical outbox schema SQL templated with the given table name.
#[derive(Args, Debug)]
pub(crate) struct PatchArgs {
    /// Outbox table name. Must match `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    #[arg(long, default_value = "audit_outbox", env = "HEXERACT_OUTBOX_TABLE")]
    table: String,
}

impl PatchArgs {
    pub(crate) fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let sql = render_schema(&self.table)?;
        println!("{sql}");
        Ok(())
    }
}
