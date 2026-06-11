use clap::Args;
use clap::builder::RangedU64ValueParser;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use lapin::options::BasicGetOptions;
use lapin::options::BasicNackOptions;
use lapin::types::ShortString;

use crate::error::CliError;

const DEFAULT_PEEK_COUNT: u32 = 1;

/// CLI arguments for `hexeract bus peek`.
#[derive(Args, Debug)]
pub(crate) struct PeekArgs {
    /// AMQP connection string.
    #[arg(long, env = "HEXERACT_BUS_URL")]
    conn: String,
    /// Queue name to peek into.
    #[arg(long)]
    queue: String,
    /// Maximum number of messages to dump. Must be at least 1.
    #[arg(
        long,
        default_value_t = DEFAULT_PEEK_COUNT,
        value_parser = RangedU64ValueParser::<u32>::new().range(1..)
    )]
    count: u32,
}

impl PeekArgs {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        let connection = RabbitMqConnection::connect(&self.conn)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        let queue = ShortString::from(self.queue.as_str());

        // Accumulate all delivery tags first, printing each message as we go.
        // We nack them all at the end with `multiple: true` so that none are
        // returned to the queue mid-loop; this ensures successive `basic_get`
        // calls each see a different message rather than the same head message.
        let mut last_delivery_tag: Option<u64> = None;
        let mut dumped = 0u32;

        for _ in 0..self.count {
            let candidate = channel
                .basic_get(queue.clone(), BasicGetOptions { no_ack: false })
                .await
                .map_err(|e| CliError::Fatal(Box::new(e)))?;
            let Some(message) = candidate else {
                break;
            };
            dumped += 1;
            last_delivery_tag = Some(message.delivery_tag);
            print_delivery(dumped, &message);
        }

        // Requeue everything we fetched in one atomic operation.
        // Note: setting the `redelivered` flag is unavoidable with `basic_nack`;
        // consumers using that flag for poison detection should be aware.
        if let Some(tag) = last_delivery_tag {
            channel
                .basic_nack(
                    tag,
                    BasicNackOptions {
                        multiple: true,
                        requeue: true,
                    },
                )
                .await
                .map_err(|e| CliError::Fatal(Box::new(e)))?;
        }

        if dumped == 0 {
            println!("(queue `{}` is empty)", self.queue);
        }
        Ok(())
    }
}

fn print_delivery(index: u32, message: &lapin::message::BasicGetMessage) {
    let props = &message.delivery.properties;
    let message_type = props
        .kind()
        .as_ref()
        .map_or("<unknown>", lapin::types::ShortString::as_str);
    let message_id = props
        .message_id()
        .as_ref()
        .map_or("<unknown>", lapin::types::ShortString::as_str);
    let correlation_id = props
        .correlation_id()
        .as_ref()
        .map_or("<unknown>", lapin::types::ShortString::as_str);
    let payload = std::str::from_utf8(&message.delivery.data).unwrap_or("<non-utf8 payload>");
    println!(
        "#{index} type={message_type} message_id={message_id} correlation_id={correlation_id}"
    );
    println!("    payload: {payload}");
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::commands::bus::BusAction;

    #[derive(Debug, Parser)]
    #[command(name = "hexeract")]
    struct TestCli {
        #[command(subcommand)]
        action: BusAction,
    }

    #[test]
    fn peek_parses_with_defaults() {
        let cli = TestCli::try_parse_from([
            "hexeract",
            "peek",
            "--conn",
            "amqp://localhost:5672",
            "--queue",
            "orders.received",
        ])
        .expect("must parse");
        let BusAction::Peek(args) = cli.action else {
            panic!("expected peek subcommand");
        };
        assert_eq!(args.count, DEFAULT_PEEK_COUNT);
        assert_eq!(args.queue, "orders.received");
    }

    #[test]
    fn peek_accepts_explicit_count() {
        let cli = TestCli::try_parse_from([
            "hexeract",
            "peek",
            "--conn",
            "amqp://localhost:5672",
            "--queue",
            "orders.received",
            "--count",
            "10",
        ])
        .expect("must parse");
        let BusAction::Peek(args) = cli.action else {
            panic!("expected peek subcommand");
        };
        assert_eq!(args.count, 10);
    }

    #[test]
    fn peek_rejects_count_zero() {
        let result = TestCli::try_parse_from([
            "hexeract",
            "peek",
            "--conn",
            "amqp://localhost:5672",
            "--queue",
            "orders.received",
            "--count",
            "0",
        ]);
        assert!(
            result.is_err(),
            "--count 0 must be rejected before connecting to the broker"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains('0') || err.contains("range"),
            "error message should reference the invalid value or valid range: {err}"
        );
    }
}
