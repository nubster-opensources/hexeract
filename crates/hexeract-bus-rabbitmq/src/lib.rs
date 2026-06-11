//! RabbitMQ transport backend for the Hexeract messaging framework.
//!
//! This crate plugs the `hexeract-bus` [`hexeract_bus::Transport`]
//! contract on top of [`lapin`], the de-facto async AMQP 0.9.1 client
//! for Rust. It ships four building blocks:
//!
//! - [`RabbitMqConnection`]: a thin wrapper over [`lapin::Connection`]
//!   that exposes a bounded retry loop for the **initial** connection.
//!   Once connected, there is no automatic reconnect: if the broker
//!   drops the connection the worker surfaces
//!   [`hexeract_bus::BusError::Connection`] and the caller rebuilds the
//!   worker (see [`worker::RabbitMqWorker::run`] for the recommended
//!   supervisor loop).
//! - [`ChannelPool`]: a small per-publisher pool of [`lapin::Channel`]
//!   handles, reusing channels across publishes instead of opening one
//!   per call.
//! - [`RabbitMqTransport`]: the actual [`hexeract_bus::Transport`]
//!   implementation. Constructed from an AMQP URI and either the
//!   default exchange or a typed [`hexeract_bus::Exchange`].
//! - The [`topology`] module: dev-convenience helpers that apply
//!   `hexeract-bus` topology declarations on a running broker.
//!
//! Integration testing relies on `testcontainers` against a real
//! RabbitMQ container; those tests are tagged `#[ignore]` and run via
//! `cargo test -p hexeract-bus-rabbitmq -- --ignored`.

/// Publisher confirm mapping shared by the transport and the worker.
pub(crate) mod confirm;
/// Bounded reconnect loop around a `lapin` connection.
pub mod connection;
/// Per-publisher pool of `lapin` channels.
pub mod pool;
/// Topology declaration helpers backed by lapin.
pub mod topology;
/// [`hexeract_bus::Transport`] implementation backed by RabbitMQ.
pub mod transport;
/// Consumer worker dispatching to typed handlers.
pub mod worker;

pub use connection::RabbitMqConnection;
pub use pool::ChannelPool;
pub use pool::PooledChannel;
pub use topology::bind_queue;
pub use topology::declare_exchange;
pub use topology::declare_queue;
pub use topology::ensure_topology;
pub use transport::RabbitMqTransport;
pub use worker::AckMode;
pub use worker::RabbitMqWorker;
pub use worker::RabbitMqWorkerBuilder;
pub use worker::RabbitMqWorkerConfig;
