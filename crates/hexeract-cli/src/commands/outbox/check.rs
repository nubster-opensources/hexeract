use clap::Args;
use tokio_postgres::NoTls;

/// Validate that the target outbox table exists with the expected columns.
///
/// Returns exit code 0 on success, 1 when the table is missing or
/// incomplete (with a remediation message printed to stderr).
#[derive(Args, Debug)]
pub struct CheckArgs {
    /// PostgreSQL connection URL.
    #[arg(long, env = "DATABASE_URL")]
    conn: String,
    /// Outbox table name to validate.
    #[arg(long, default_value = "audit_outbox", env = "HEXERACT_OUTBOX_TABLE")]
    table: String,
}

const REQUIRED_COLUMNS: &[&str] = &[
    "id",
    "event_id",
    "event_type",
    "payload",
    "subject_id",
    "created_at",
    "attempts",
    "last_error",
    "next_retry_at",
    "delivered_at",
];

impl CheckArgs {
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let (client, connection) = tokio_postgres::connect(&self.conn, NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "PostgreSQL connection task error");
            }
        });

        let rows = client
            .query(
                "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
                &[&self.table],
            )
            .await?;

        if rows.is_empty() {
            eprintln!("Table `{}` does not exist.", self.table);
            eprintln!(
                "Run `hexeract outbox patch --table {}` to get the canonical SQL.",
                self.table
            );
            std::process::exit(1);
        }

        let actual: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        let missing: Vec<&&str> = REQUIRED_COLUMNS
            .iter()
            .filter(|expected| !actual.iter().any(|a| a == **expected))
            .collect();

        if missing.is_empty() {
            println!(
                "Table `{}` is valid ({} required columns present).",
                self.table,
                REQUIRED_COLUMNS.len()
            );
            Ok(())
        } else {
            eprintln!("Table `{}` is missing columns: {missing:?}", self.table);
            eprintln!(
                "Run `hexeract outbox patch --table {}` to compare against the canonical schema.",
                self.table
            );
            std::process::exit(1);
        }
    }
}
