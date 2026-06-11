use clap::Args;
use hexeract_outbox_sql::Dialect;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::NoTls;

use super::check::is_ssl_disabled;
use crate::error::CliError;

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
    /// Required to apply DDL; without it, the command refuses and prints guidance.
    #[arg(long = "yes-i-know")]
    yes_i_know: bool,
}

impl ApplyArgs {
    pub(crate) async fn run(self) -> Result<(), CliError> {
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
            return Err(CliError::SafetyFlagMissing(
                "--yes-i-know is required to apply DDL".to_owned(),
            ));
        }

        let sql = Dialect::Postgres
            .schema_ddl(&self.table)
            .map_err(|e| CliError::Fatal(Box::new(e)))?;

        tracing::info!(table = %self.table, "connecting to PostgreSQL");
        let client = connect(&self.conn).await?;

        tracing::info!(table = %self.table, "applying canonical outbox schema");
        client
            .batch_execute(&sql)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;

        println!("Schema applied to table `{}`.", self.table);
        Ok(())
    }
}

/// Connect to PostgreSQL, using TLS unless `sslmode=disable` is present in the URL.
async fn connect(url: &str) -> Result<tokio_postgres::Client, CliError> {
    if is_ssl_disabled(url) {
        tracing::warn!("TLS disabled via sslmode=disable; credentials will be sent in cleartext");
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "PostgreSQL connection task error");
            }
        });
        Ok(client)
    } else {
        let builder = native_tls::TlsConnector::builder()
            .build()
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        let connector = MakeTlsConnector::new(builder);
        let (client, connection) = tokio_postgres::connect(url, connector)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "PostgreSQL connection task error");
            }
        });
        Ok(client)
    }
}
