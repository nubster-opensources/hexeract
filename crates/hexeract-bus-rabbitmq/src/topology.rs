//! Topology declaration helpers for RabbitMQ.
//!
//! These are dev-convenience helpers (proof of concept). Each function
//! opens a short-lived `lapin::Channel` on the supplied connection,
//! issues the matching AMQP command and lets the channel close on
//! drop. A long-running service should declare its topology once at
//! startup rather than calling these helpers on the hot path.

use hexeract_bus::Binding;
use hexeract_bus::BusError;
use hexeract_bus::Exchange;
use hexeract_bus::Queue;
use lapin::Channel;
use lapin::options::ExchangeDeclareOptions;
use lapin::options::QueueBindOptions;
use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;
use lapin::types::ShortString;

use crate::connection::RabbitMqConnection;
use crate::transport::exchange_kind_to_lapin;

/// Declare `exchange` on the broker.
///
/// Opens a fresh channel, issues `exchange.declare` with the
/// `durable` / `auto_delete` flags carried by [`Exchange`] and closes
/// the channel on drop.
///
/// # Errors
///
/// Returns [`BusError::Connection`] if the channel cannot be opened,
/// or [`BusError::Transport`] if the broker rejects the declaration
/// (typically a mismatch with a pre-existing exchange).
pub async fn declare_exchange(
    connection: &RabbitMqConnection,
    exchange: &Exchange,
) -> Result<(), BusError> {
    connection
        .with_channel(|channel| async move { declare_exchange_on(&channel, exchange).await })
        .await
}

/// Declare `queue` on the broker.
///
/// Opens a fresh channel, issues `queue.declare` with the `durable`
/// / `exclusive` / `auto_delete` flags carried by [`Queue`] and
/// closes the channel on drop.
///
/// # Errors
///
/// Returns [`BusError::Connection`] if the channel cannot be opened,
/// or [`BusError::Transport`] if the broker rejects the declaration.
pub async fn declare_queue(connection: &RabbitMqConnection, queue: &Queue) -> Result<(), BusError> {
    connection
        .with_channel(|channel| async move { declare_queue_on(&channel, queue).await })
        .await
}

/// Bind a queue to an exchange under a routing key.
///
/// Opens a fresh channel, issues `queue.bind` and closes the channel
/// on drop.
///
/// # Errors
///
/// Returns [`BusError::Connection`] if the channel cannot be opened,
/// or [`BusError::Transport`] if the broker rejects the binding
/// (typically when the queue or exchange is missing).
pub async fn bind_queue(
    connection: &RabbitMqConnection,
    binding: &Binding,
) -> Result<(), BusError> {
    connection
        .with_channel(|channel| async move { bind_queue_on(&channel, binding).await })
        .await
}

/// Apply a complete topology in a single channel.
///
/// Declares every exchange in `exchanges`, then every queue in
/// `queues`, then every binding in `bindings`. The order matters:
/// bindings reference the exchange and the queue they connect, so
/// both must be declared first.
///
/// # Errors
///
/// Returns [`BusError::Connection`] if the channel cannot be opened,
/// or [`BusError::Transport`] on the first declaration the broker
/// rejects.
pub async fn ensure_topology(
    connection: &RabbitMqConnection,
    exchanges: &[Exchange],
    queues: &[Queue],
    bindings: &[Binding],
) -> Result<(), BusError> {
    connection
        .with_channel(|channel| async move {
            for exchange in exchanges {
                declare_exchange_on(&channel, exchange).await?;
            }
            for queue in queues {
                declare_queue_on(&channel, queue).await?;
            }
            for binding in bindings {
                bind_queue_on(&channel, binding).await?;
            }
            Ok(())
        })
        .await
}

async fn declare_exchange_on(channel: &Channel, exchange: &Exchange) -> Result<(), BusError> {
    let options = ExchangeDeclareOptions {
        durable: exchange.durable,
        auto_delete: exchange.auto_delete,
        ..ExchangeDeclareOptions::default()
    };
    channel
        .exchange_declare(
            ShortString::from(exchange.name.as_str()),
            exchange_kind_to_lapin(exchange.kind),
            options,
            FieldTable::default(),
        )
        .await
        .map_err(|err| BusError::Transport(Box::new(err)))?;
    Ok(())
}

async fn declare_queue_on(channel: &Channel, queue: &Queue) -> Result<(), BusError> {
    let options = QueueDeclareOptions {
        durable: queue.durable,
        exclusive: queue.exclusive,
        auto_delete: queue.auto_delete,
        ..QueueDeclareOptions::default()
    };
    channel
        .queue_declare(
            ShortString::from(queue.name.as_str()),
            options,
            FieldTable::default(),
        )
        .await
        .map_err(|err| BusError::Transport(Box::new(err)))?;
    Ok(())
}

async fn bind_queue_on(channel: &Channel, binding: &Binding) -> Result<(), BusError> {
    channel
        .queue_bind(
            ShortString::from(binding.queue.as_str()),
            ShortString::from(binding.exchange.as_str()),
            ShortString::from(binding.routing_key.as_str()),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|err| BusError::Transport(Box::new(err)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hexeract_bus::ExchangeKind;
    use hexeract_bus::RoutingKey;

    use super::*;
    use crate::connection::RabbitMqConnection;

    /// All helpers fail with `BusError::Connection` when the broker
    /// is unreachable because they need a channel to function. End-
    /// to-end coverage of the success paths lives in the integration
    /// test under `tests/integration.rs`.
    #[tokio::test]
    async fn ensure_topology_returns_connection_error_on_unreachable_broker() {
        let connection_result = RabbitMqConnection::connect("amqp://127.0.0.1:1").await;
        // The connect itself already fails — no usable connection to
        // call ensure_topology on.
        let err = connection_result.expect_err("must fail to connect");
        assert!(matches!(err, BusError::Connection(_)));
    }

    #[test]
    fn helpers_compile_for_basic_topology() {
        // Pure compile-time check that the helper signatures accept
        // the canonical bus-side topology values.
        let _exchange = Exchange::new("orders", ExchangeKind::Topic).unwrap();
        let _queue = Queue::new("orders.received").unwrap();
        let _key = RoutingKey::new("orders.*").unwrap();
    }
}
