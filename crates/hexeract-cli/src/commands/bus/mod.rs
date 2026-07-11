use clap::Subcommand;

pub(crate) mod declare;
pub(crate) mod peek;
pub(crate) mod purge;

use crate::error::CliError;

/// Actions targeting the bus broker (RabbitMQ).
#[derive(Subcommand, Debug)]
pub(crate) enum BusAction {
    /// Apply a topology declared in a TOML file to a RabbitMQ broker.
    Declare(declare::DeclareArgs),
    /// Dump the first messages from a queue without acking them.
    ///
    /// Every peeked message is released back to the queue via a
    /// `basic_nack` with `requeue: true`; RabbitMQ has no "read without
    /// side effects" primitive. This flips each message's AMQP
    /// `redelivered` flag to `true`, so consumers that use that flag for
    /// poison-message detection should account for peeks inflating it.
    /// Payload previews are capped at `--max-bytes` (1 KiB by default);
    /// pass `--raw` to print the full payload.
    Peek(peek::PeekArgs),
    /// Drop every message from a queue.
    Purge(purge::PurgeArgs),
}

impl BusAction {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        match self {
            Self::Declare(args) => args.run().await,
            Self::Peek(args) => args.run().await,
            Self::Purge(args) => args.run().await,
        }
    }
}
