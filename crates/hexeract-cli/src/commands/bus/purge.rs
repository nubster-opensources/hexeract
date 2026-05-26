use clap::Args;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use lapin::options::QueuePurgeOptions;
use lapin::types::ShortString;

/// CLI arguments for `hexeract bus purge`.
///
/// Mirrors the `outbox apply --yes-i-know` safety flag: the purge is
/// only carried out when the operator opts in explicitly.
#[derive(Args, Debug)]
pub(crate) struct PurgeArgs {
    /// AMQP connection string.
    #[arg(long, env = "HEXERACT_BUS_URL")]
    conn: String,
    /// Queue name to purge.
    #[arg(long)]
    queue: String,
    /// Explicit safety confirmation. Without it, the command exits
    /// without touching the broker.
    #[arg(long = "yes-i-know")]
    yes_i_know: bool,
}

impl PurgeArgs {
    pub(crate) async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.yes_i_know {
            return Err("refusing to purge without the explicit `--yes-i-know` safety flag".into());
        }

        let connection = RabbitMqConnection::connect(&self.conn).await?;
        let channel = connection.create_channel().await?;
        let purged = channel
            .queue_purge(
                ShortString::from(self.queue.as_str()),
                QueuePurgeOptions::default(),
            )
            .await?;
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
    async fn purge_without_safety_flag_returns_error_without_connecting() {
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
