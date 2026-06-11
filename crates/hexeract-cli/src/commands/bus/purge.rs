use clap::Args;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use lapin::options::QueuePurgeOptions;
use lapin::types::ShortString;

use crate::error::CliError;

/// CLI arguments for `hexeract bus purge`.
///
/// The purge is only carried out when the operator opts in explicitly via
/// `--yes-i-know`, matching the safety-flag contract of `outbox apply`.
#[derive(Args, Debug)]
pub(crate) struct PurgeArgs {
    /// AMQP connection string.
    #[arg(long, env = "HEXERACT_BUS_URL")]
    conn: String,
    /// Queue name to purge.
    #[arg(long)]
    queue: String,
    /// Required to purge; without it, the command refuses and prints guidance.
    #[arg(long = "yes-i-know")]
    yes_i_know: bool,
}

impl PurgeArgs {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        if !self.yes_i_know {
            eprintln!("Refusing to purge without --yes-i-know.");
            eprintln!();
            eprintln!("Purging a queue is irreversible: all messages are dropped permanently.");
            eprintln!(
                "If you really mean to purge `{}` now, re-run with --yes-i-know.",
                self.queue
            );
            return Err(CliError::SafetyFlagMissing(
                "--yes-i-know is required to purge a queue".to_owned(),
            ));
        }

        let connection = RabbitMqConnection::connect(&self.conn)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        let purged = channel
            .queue_purge(
                ShortString::from(self.queue.as_str()),
                QueuePurgeOptions::default(),
            )
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        println!("purged {purged} message(s) from `{}`", self.queue);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::commands::bus::BusAction;

    #[derive(Parser)]
    #[command(name = "hexeract")]
    struct TestCli {
        #[command(subcommand)]
        action: BusAction,
    }

    #[tokio::test]
    async fn purge_without_safety_flag_returns_safety_error_without_connecting() {
        let cli = TestCli::try_parse_from([
            "hexeract",
            "purge",
            "--conn",
            "amqp://127.0.0.1:1",
            "--queue",
            "orders.received",
        ])
        .expect("must parse");
        let BusAction::Purge(args) = cli.action else {
            panic!("expected purge subcommand");
        };
        let result = args.run().await;
        let err = result.expect_err("must refuse without --yes-i-know");
        assert!(err.to_string().contains("yes-i-know"));
    }

    #[tokio::test]
    async fn purge_without_safety_flag_produces_exit_code_2() {
        let cli = TestCli::try_parse_from([
            "hexeract",
            "purge",
            "--conn",
            "amqp://127.0.0.1:1",
            "--queue",
            "orders.received",
        ])
        .expect("must parse");
        let BusAction::Purge(args) = cli.action else {
            panic!("expected purge subcommand");
        };
        let err = args
            .run()
            .await
            .expect_err("must refuse without --yes-i-know");
        assert_eq!(
            err.exit_code(),
            2,
            "missing safety flag must produce exit code 2, not 1"
        );
    }

    #[test]
    fn purge_parses_with_safety_flag() {
        let cli = TestCli::try_parse_from([
            "hexeract",
            "purge",
            "--conn",
            "amqp://localhost:5672",
            "--queue",
            "orders.received",
            "--yes-i-know",
        ])
        .expect("must parse");
        let BusAction::Purge(args) = cli.action else {
            panic!("expected purge subcommand");
        };
        assert!(args.yes_i_know);
    }
}
