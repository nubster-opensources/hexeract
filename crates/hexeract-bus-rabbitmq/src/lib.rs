//! RabbitMQ transport backend for the Hexeract messaging framework.
//!
//! This crate plugs the `hexeract-bus` [`hexeract_bus::Transport`]
//! contract on top of [`lapin`], the de-facto async AMQP 0.9.1 client
//! for Rust. It ships three building blocks:
//!
//! - [`RabbitMqConnection`]: a thin wrapper over [`lapin::Connection`]
//!   that exposes a bounded reconnect loop.
//! - [`ChannelPool`]: a small per-publisher pool of [`lapin::Channel`]
//!   handles, reusing channels across publishes instead of opening one
//!   per call.
//! - [`RabbitMqTransport`]: the actual [`hexeract_bus::Transport`]
//!   implementation. Constructed from an AMQP URI and either the
//!   default exchange or a typed [`hexeract_bus::Exchange`].
//!
//! Integration testing relies on `testcontainers` against a real
//! RabbitMQ container; those tests are tagged `#[ignore]` and run via
//! `cargo test -p hexeract-bus-rabbitmq -- --ignored`.

/// Bounded reconnect loop around a `lapin` connection.
pub mod connection;
/// Per-publisher pool of `lapin` channels.
pub mod pool;
/// [`hexeract_bus::Transport`] implementation backed by RabbitMQ.
pub mod transport;

pub use connection::RabbitMqConnection;
pub use pool::ChannelPool;
pub use pool::PooledChannel;
pub use transport::RabbitMqTransport;
