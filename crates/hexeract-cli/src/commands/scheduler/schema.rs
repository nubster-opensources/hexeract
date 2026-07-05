use clap::Args;
use clap::ValueEnum;
use hexeract_scheduler_sql::Dialect;
use hexeract_scheduler_sql::schema::schema_ddl;

use crate::error::CliError;

/// SQL dialect selection for the scheduler schema DDL.
#[derive(ValueEnum, Clone, Copy, Debug, Default)]
pub(crate) enum DialectArg {
    #[default]
    Postgres,
    MySql,
    Sqlite,
}

impl DialectArg {
    pub(crate) fn to_dialect(self) -> Dialect {
        match self {
            Self::Postgres => Dialect::Postgres,
            Self::MySql => Dialect::MySql,
            Self::Sqlite => Dialect::Sqlite,
        }
    }
}

/// Print the scheduler schema DDL for the selected dialect.
#[derive(Args, Debug)]
pub(crate) struct SchemaArgs {
    /// SQL dialect to render the DDL for.
    #[arg(long, value_enum, default_value_t = DialectArg::Postgres)]
    dialect: DialectArg,
    /// Scheduler table name. Must match `^[a-zA-Z_][a-zA-Z0-9_]*$`.
    #[arg(
        long,
        default_value = "scheduled_messages",
        env = "HEXERACT_SCHEDULER_TABLE"
    )]
    table: String,
}

impl SchemaArgs {
    pub(crate) fn run(self) -> Result<(), CliError> {
        let sql = schema_ddl(self.dialect.to_dialect(), &self.table)
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        println!("{sql}");
        Ok(())
    }
}
