use clap::Args;
use hexeract_outbox_postgres::render_schema;
use tokio_postgres::NoTls;

/// Apply the canonical outbox schema to a target PostgreSQL database.
///
/// Intended for POCs and development. Production deployments should run
/// their own migration tooling (sqlx-cli, refinery, dbmate, Flyway, ...)
/// using the SQL exposed by `hexeract outbox patch`.
#[derive(Args, Debug)]
pub(crate) struct ApplyArgs {
    /// PostgreSQL connection URL (e.g. `postgres://user:pass@host:5432/db`).
    #[arg(long, env = "DATABASE_URL")]
    conn: String,
    /// Outbox table name. Must match `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    #[arg(long, default_value = "audit_outbox", env = "HEXERACT_OUTBOX_TABLE")]
    table: String,
    /// Skip the production safety prompt. Required to actually run the DDL.
    #[arg(long = "yes-i-know")]
    yes_i_know: bool,
}

impl ApplyArgs {
    pub(crate) async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.yes_i_know {
            eprintln!("Refusing to apply DDL without --yes-i-know.");
            eprintln!();
            eprintln!("Applying DDL from a running application is a POC and development");
            eprintln!("convenience. Production deployments should use a versioned migration");
            eprintln!(
                "tool fed by `hexeract outbox patch --table {}`.",
                self.table
            );
            eprintln!();
            eprintln!("If you really mean to run the DDL now, re-run with --yes-i-know.");
            std::process::exit(2);
        }

        let sql = render_schema(&self.table)?;

        tracing::info!(table = %self.table, "connecting to PostgreSQL");
        let (client, connection) = tokio_postgres::connect(&self.conn, NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "PostgreSQL connection task error");
            }
        });

        tracing::info!(table = %self.table, "applying canonical outbox schema");
        client.batch_execute(&sql).await?;

        println!("Schema applied to table `{}`.", self.table);
        Ok(())
    }
}
