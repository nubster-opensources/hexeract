use clap::Args;
use hexeract_bus::Queue;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use lapin::options::QueuePurgeOptions;
use lapin::types::ShortString;

use crate::conn_string::ConnString;
use crate::error::CliError;

/// CLI arguments for `hexeract bus purge`.
///
/// The purge is only carried out when the operator opts in explicitly via
/// `--yes-i-know`, matching the safety-flag contract of `outbox apply`.
#[derive(Args, Debug)]
pub(crate) struct PurgeArgs {
    /// AMQP connection string.
    ///
    /// Carries broker credentials in its userinfo component. Prefer
    /// setting `HEXERACT_BUS_URL` in the environment over passing this on
    /// the command line: argv is readable by every local user via
    /// `/proc/<pid>/cmdline` or `ps aux`, and shells persist it in history.
    #[arg(long, env = "HEXERACT_BUS_URL", hide_env_values = true)]
    conn: ConnString,
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

        // Validate the queue name before touching the network: it is
        // cheap, local input validation, and it must reject an oversize
        // or control-character-bearing name with an ordinary error
        // rather than a panic (#366).
        let queue = Queue::new(self.queue.as_str()).map_err(|e| CliError::Fatal(Box::new(e)))?;

        let connection = RabbitMqConnection::connect(self.conn.as_str())
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        let purged = channel
            .queue_purge(
                ShortString::from(queue.name.as_str()),
                QueuePurgeOptions::default(),
            )
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        println!("purged {purged} message(s) from `{}`", queue.name);
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

    #[tokio::test]
    async fn purge_with_overlong_queue_name_fails_instead_of_panicking() {
        // Regression for #366: `ShortString::from` panics past 255 bytes,
        // but `Queue::new` (127-byte limit) must reject this first with a
        // normal error, never a panic.
        let cli = TestCli::try_parse_from([
            "hexeract",
            "purge",
            "--conn",
            "amqp://127.0.0.1:1",
            "--queue",
            &"q".repeat(300),
            "--yes-i-know",
        ])
        .expect("must parse");
        let BusAction::Purge(args) = cli.action else {
            panic!("expected purge subcommand");
        };
        let result = args.run().await;
        assert!(result.is_err(), "an oversize queue name must be rejected");
    }
}
