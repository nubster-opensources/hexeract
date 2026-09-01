use clap::Args;
use clap::builder::RangedU64ValueParser;
use hexeract_bus::Queue;
use lapin::Channel;
use lapin::message::BasicGetMessage;
use lapin::options::BasicGetOptions;
use lapin::options::BasicNackOptions;
use lapin::types::ShortString;

use crate::commands::bus::transport_security::TransportSecurityArgs;
use crate::conn_string::ConnString;
use crate::error::CliError;

const DEFAULT_PEEK_COUNT: u32 = 1;

/// Number of payload bytes printed by default before truncating.
///
/// Payloads routinely carry personal data or secrets, and a producer bug
/// can put a multi-megabyte body on a queue; dumping either whole into a
/// terminal, CI log or pipe by default is unsafe. 1 KiB is enough to
/// recognize a message's shape without doing either.
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024;

/// Marker appended after a truncated payload preview.
const TRUNCATION_MARKER: &str = " ...<truncated, use --raw or --max-bytes to see more>";

/// CLI arguments for `hexeract bus peek`.
#[derive(Args, Debug)]
pub(crate) struct PeekArgs {
    /// AMQP connection string.
    ///
    /// A broker outside loopback requires `amqps://`; plain `amqp://` is
    /// refused unless `--insecure-plaintext` is passed.
    ///
    /// Carries broker credentials in its userinfo component. Prefer
    /// setting `HEXERACT_BUS_URL` in the environment over passing this on
    /// the command line: argv is readable by every local user via
    /// `/proc/<pid>/cmdline` or `ps aux`, and shells persist it in history.
    #[arg(long, env = "HEXERACT_BUS_URL", hide_env_values = true)]
    conn: ConnString,
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
    /// Print the full, unredacted payload instead of a capped preview.
    ///
    /// Ignores `--max-bytes`. Payload bytes routinely contain personal
    /// data or secrets; only pass this when every reader of this
    /// terminal, log or pipe is trusted with the full message body.
    #[arg(long)]
    raw: bool,
    /// Maximum number of payload bytes to print before truncating.
    ///
    /// Ignored when `--raw` is set.
    #[arg(long, default_value_t = DEFAULT_MAX_PAYLOAD_BYTES)]
    max_bytes: usize,
    #[command(flatten)]
    transport: TransportSecurityArgs,
}

impl PeekArgs {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        // Validate the queue name before touching the network: it is
        // cheap, local input validation, and it must reject an oversize
        // or control-character-bearing name with an ordinary error
        // rather than a panic (#366).
        let queue = Queue::new(self.queue.as_str()).map_err(|e| CliError::Fatal(Box::new(e)))?;
        let queue_short = ShortString::from(queue.name.as_str());

        let connection = self.transport.connect(self.conn.as_str()).await?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;

        // Accumulate all delivery tags first, printing each message as we go.
        // We nack them all at the end with `multiple: true` so that none are
        // returned to the queue mid-loop; this ensures successive `basic_get`
        // calls each see a different message rather than the same head message.
        let mut last_delivery_tag: Option<u64> = None;
        let mut dumped = 0u32;
        let fetch_result = self
            .fetch_messages(&channel, queue_short, &mut last_delivery_tag, &mut dumped)
            .await;

        // Release whatever was fetched regardless of whether fetching
        // succeeded: a network error partway through the loop must not
        // leave already-fetched messages un-acked until the process
        // exits (see #368). `release_fetched` runs before the fetch
        // error (if any) is propagated.
        let release_result = release_fetched(&channel, last_delivery_tag).await;

        fetch_result?;
        release_result?;

        if dumped == 0 {
            println!("(queue `{}` is empty)", queue.name);
        }
        Ok(())
    }

    /// Fetch up to `self.count` messages, printing each as it arrives.
    ///
    /// Writes the delivery tag of the last fetched message and the
    /// running count into `last_delivery_tag`/`dumped` even when it
    /// returns early on error, so the caller can still release whatever
    /// was fetched before the failure.
    async fn fetch_messages(
        &self,
        channel: &Channel,
        queue: ShortString,
        last_delivery_tag: &mut Option<u64>,
        dumped: &mut u32,
    ) -> Result<(), CliError> {
        for _ in 0..self.count {
            let candidate = channel
                .basic_get(queue.clone(), BasicGetOptions { no_ack: false })
                .await
                .map_err(|e| CliError::Fatal(Box::new(e)))?;
            let Some(message) = candidate else {
                break;
            };
            *dumped += 1;
            *last_delivery_tag = Some(message.delivery_tag);
            self.print_delivery(*dumped, &message);
        }
        Ok(())
    }

    fn print_delivery(&self, index: u32, message: &BasicGetMessage) {
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
        println!("    payload: {}", self.render_payload(payload));
    }

    /// Render `payload` for display, truncating it unless `--raw` was passed.
    fn render_payload<'a>(&self, payload: &'a str) -> std::borrow::Cow<'a, str> {
        if self.raw {
            return std::borrow::Cow::Borrowed(payload);
        }
        let (shown, truncated) = truncate_payload(payload, self.max_bytes);
        if truncated {
            std::borrow::Cow::Owned(format!("{shown}{TRUNCATION_MARKER}"))
        } else {
            std::borrow::Cow::Borrowed(shown)
        }
    }
}

/// Requeue every message fetched so far in one atomic `basic_nack`.
///
/// Setting the AMQP `redelivered` flag is unavoidable with `basic_nack`;
/// consumers using that flag for poison detection should be aware that
/// `bus peek` inflates it for every message it looks at (see the
/// `Peek` subcommand's `--help` text).
async fn release_fetched(
    channel: &Channel,
    last_delivery_tag: Option<u64>,
) -> Result<(), CliError> {
    let Some(tag) = last_delivery_tag else {
        return Ok(());
    };
    channel
        .basic_nack(
            tag,
            BasicNackOptions {
                multiple: true,
                requeue: true,
            },
        )
        .await
        .map_err(|e| CliError::Fatal(Box::new(e)))
}

/// Truncate `payload` to at most `max_bytes` bytes, respecting UTF-8
/// character boundaries so the preview never ends mid-codepoint.
///
/// Returns the (possibly shortened) slice and whether it was truncated.
fn truncate_payload(payload: &str, max_bytes: usize) -> (&str, bool) {
    if payload.len() <= max_bytes {
        return (payload, false);
    }
    let mut end = max_bytes;
    while end > 0 && !payload.is_char_boundary(end) {
        end -= 1;
    }
    (&payload[..end], true)
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
        assert!(!args.raw);
        assert_eq!(args.max_bytes, DEFAULT_MAX_PAYLOAD_BYTES);
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

    #[test]
    fn peek_accepts_raw_and_max_bytes_flags() {
        let cli = TestCli::try_parse_from([
            "hexeract",
            "peek",
            "--conn",
            "amqp://localhost:5672",
            "--queue",
            "orders.received",
            "--raw",
            "--max-bytes",
            "64",
        ])
        .expect("must parse");
        let BusAction::Peek(args) = cli.action else {
            panic!("expected peek subcommand");
        };
        assert!(args.raw);
        assert_eq!(args.max_bytes, 64);
    }

    #[tokio::test]
    async fn peek_with_overlong_queue_name_fails_to_run_instead_of_panicking() {
        // Regression for #366: `ShortString::from` panics past 255 bytes,
        // but `Queue::new` (127-byte limit) must reject this first with a
        // normal error, never a panic. The connection target is
        // unreachable on purpose: the queue-name check must happen before
        // any network attempt.
        let cli = TestCli::try_parse_from([
            "hexeract",
            "peek",
            "--conn",
            "amqp://127.0.0.1:1",
            "--queue",
            &"q".repeat(300),
        ])
        .expect("must parse");
        let BusAction::Peek(args) = cli.action else {
            panic!("expected peek subcommand");
        };
        let result = args.run().await;
        assert!(result.is_err(), "an oversize queue name must be rejected");
    }

    #[test]
    fn truncate_payload_keeps_short_payload_intact() {
        let (shown, truncated) = truncate_payload("short", 1024);
        assert_eq!(shown, "short");
        assert!(!truncated);
    }

    #[test]
    fn truncate_payload_cuts_at_the_byte_cap() {
        let payload = "a".repeat(2048);
        let (shown, truncated) = truncate_payload(&payload, 1024);
        assert_eq!(shown.len(), 1024);
        assert!(truncated);
    }

    #[test]
    fn truncate_payload_never_splits_a_utf8_character() {
        // Each "é" is 2 bytes; capping at an odd byte count must back off
        // to the previous character boundary rather than slicing mid-char.
        let payload = "é".repeat(10);
        let (shown, truncated) = truncate_payload(&payload, 5);
        assert!(truncated);
        assert!(std::str::from_utf8(shown.as_bytes()).is_ok());
        assert!(shown.len() <= 5);
    }
}
