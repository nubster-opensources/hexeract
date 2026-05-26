use clap::Args;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use lapin::options::BasicGetOptions;
use lapin::options::BasicNackOptions;
use lapin::types::ShortString;

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
    /// Maximum number of messages to dump.
    #[arg(long, default_value_t = DEFAULT_PEEK_COUNT)]
    count: u32,
}

impl PeekArgs {
    pub(crate) async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let connection = RabbitMqConnection::connect(&self.conn).await?;
        let channel = connection.create_channel().await?;
        let queue = ShortString::from(self.queue.as_str());

        let mut dumped = 0;
        for _ in 0..self.count {
            let candidate = channel
                .basic_get(queue.clone(), BasicGetOptions { no_ack: false })
                .await?;
            let Some(message) = candidate else {
                break;
            };
            dumped += 1;
            print_delivery(dumped, &message);
            // Re-queue so peek is non-destructive.
            channel
                .basic_nack(
                    message.delivery_tag,
                    BasicNackOptions {
                        multiple: false,
                        requeue: true,
                    },
                )
                .await?;
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

    #[derive(Parser)]
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
}
