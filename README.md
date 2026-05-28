# Hexeract

> The 6-dimension Rust messaging framework: Mediator, Bus, Outbox, Sagas, Scheduler, Request/Reply.

[![crates.io](https://img.shields.io/crates/v/hexeract-outbox.svg?label=crates.io)](https://crates.io/crates/hexeract-outbox)
[![docs.rs](https://img.shields.io/docsrs/hexeract-outbox?label=docs.rs)](https://docs.rs/hexeract-outbox)
[![CI](https://github.com/nubster-opensources/hexeract/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nubster-opensources/hexeract/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](./docs/MSRV_POLICY.md)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Status](https://img.shields.io/badge/status-pre--alpha-orange)](#status)
[![Made with Rust](https://img.shields.io/badge/made%20with-Rust-orange?logo=rust)](https://www.rust-lang.org/)

Hexeract is a server-side messaging framework written in Rust. It unifies in-process mediator handlers, external message bus transports, transactional outbox and inbox, long-running sagas, scheduled messages and request/reply RPC in a single coherent SDK. The framework relies on Rust's type system and procedural macros to provide compile-time guarantees in place of runtime reflection.

Hexeract is sponsored by [Nubster](https://nubster.com).

## Status

🚀 **v0.2.0: Bus RabbitMQ shipped.** Outbox MVP remains stable from v0.1.0, and the bus brings a unified `Transport` trait with a first RabbitMQ backend powered by `lapin`, a consumer worker with ack modes and retry policy, topology helpers, an end-to-end pub/sub example and a `hexeract bus` CLI namespace.

| Feature | v0.1.0 | v0.2.0 |
| --- | --- | --- |
| Transactional outbox (PostgreSQL) | ✅ | ✅ |
| Worker poll loop with `SKIP LOCKED` | ✅ | ✅ |
| Fluent builder API | ✅ | ✅ |
| `hexeract outbox` CLI | ✅ | ✅ |
| Bus core (`Message`, `BusEnvelope`, `Transport`, `Handler<M>`) | ⏳ | ✅ |
| RabbitMQ backend (`lapin` connection pool, publish, consume, retry) | ⏳ | ✅ |
| Topology types (`Exchange`, `Queue`, `Binding`, `RoutingKey`) | ⏳ | ✅ |
| `hexeract bus declare / peek / purge` CLI | ⏳ | ✅ |
| Mediator | ⏳ v0.3.0 | ⏳ v0.3.0 |
| Polyglot bus (NATS, Kafka, SQS) | ⏳ v0.9.0 | ⏳ v0.9.0 |
| Sagas, Scheduler, Request and Reply | ⏳ later | ⏳ later |

See the [CHANGELOG](./CHANGELOG.md) for the detailed history.

## Quick start

### Outbox (PostgreSQL)

Add the umbrella crate with the `outbox-postgres` feature to your `Cargo.toml`:

```toml
[dependencies]
hexeract = { version = "0.2", features = ["outbox-postgres"] }
```

> Power users who prefer a strict SemVer per crate can keep depending on `hexeract-outbox`, `hexeract-outbox-postgres`, `hexeract-bus`, `hexeract-bus-rabbitmq` etc. directly.

Declare a domain event, a handler and wire a worker:

```rust
use std::time::Duration;
use hexeract::core::HandlerContext;
use hexeract::outbox::{Event, Handler, OutboxError, OutboxPublisher};
use hexeract::outbox_postgres::{PgOutboxPublisher, PgOutboxWorkerBuilder};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct UserRegistered { user_id: Uuid }

impl Event for UserRegistered {
    const EVENT_TYPE: &'static str = "users.registered";
}

struct AuditWriter;
impl Handler<UserRegistered> for AuditWriter {
    type Error = OutboxError;
    async fn handle(&self, event: UserRegistered, _ctx: &HandlerContext) -> Result<(), Self::Error> {
        // ... write to audit storage ...
        Ok(())
    }
}

# async fn run(pool: deadpool_postgres::Pool) -> Result<(), Box<dyn std::error::Error>> {
let publisher = PgOutboxPublisher::new(pool.clone(), "audit_outbox")?;

let worker = PgOutboxWorkerBuilder::new(pool.clone())
    .table_name("audit_outbox")
    .register_handler::<UserRegistered, _>(AuditWriter)
    .poll_interval(Duration::from_millis(50))
    .build()?;

let cancel = CancellationToken::new();
let join = tokio::spawn(worker.run(cancel.clone()));

// inside a business use case:
let mut client = pool.get().await?;
let mut tx = client.transaction().await?;
let event_id = publisher.publish_in_tx(&mut tx, &UserRegistered { user_id: Uuid::new_v4() }).await?;
tx.commit().await?;
println!("published event {event_id}");

cancel.cancel();
join.await??;
# Ok(()) }
```

See [`docs/tutorial/getting-started.md`](./docs/tutorial/getting-started.md) and the runnable [`examples/`](./crates/hexeract-outbox-postgres/examples/) for the full integration walkthrough.

### Bus (RabbitMQ)

Add the umbrella crate with the `bus-rabbitmq` feature to your `Cargo.toml`:

```toml
[dependencies]
hexeract = { version = "0.2", features = ["bus-rabbitmq"] }
```

Declare a domain message, a handler and wire a publisher plus a worker:

```rust
use hexeract::bus::{Handler, Message, Transport};
use hexeract::bus_rabbitmq::{RabbitMqConnection, RabbitMqTransport, RabbitMqWorkerBuilder};
use hexeract::core::HandlerContext;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct OrderPlaced { order_id: Uuid }

impl Message for OrderPlaced {
    const MESSAGE_TYPE: &'static str = "orders.placed";
}

struct Projector;
impl Handler<OrderPlaced> for Projector {
    type Error = hexeract::bus::BusError;
    async fn handle(&self, msg: OrderPlaced, _ctx: &HandlerContext) -> Result<(), Self::Error> {
        // ... project to read model, forward to downstream system, ...
        let _ = msg.order_id;
        Ok(())
    }
}

# async fn run(uri: &str) -> Result<(), Box<dyn std::error::Error>> {
let transport = RabbitMqTransport::new(uri).await?;
let consumer_conn = RabbitMqConnection::connect(uri).await?;

let worker = RabbitMqWorkerBuilder::new(consumer_conn)
    .queue("orders.received")
    .register_handler::<OrderPlaced, _>(Projector)
    .build()?;

let cancel = CancellationToken::new();
let join = tokio::spawn(worker.run(cancel.clone()));

let message_id = transport
    .publish("orders.received", &OrderPlaced { order_id: Uuid::new_v4() })
    .await?;
println!("published message {message_id}");

cancel.cancel();
join.await??;
# Ok(()) }
```

> **In production**, declare your topology once at service startup (or out of band through the CLI). The `topology::ensure_topology` helper and the CLI live for dev convenience; do not call them on the hot path.

The `hexeract bus` CLI provisions and inspects a broker without writing ad-hoc `lapin` scripts:

```bash
export HEXERACT_BUS_URL=amqp://guest:guest@localhost:5672

# 1. Apply a typed topology described in TOML.
hexeract bus declare --topology crates/hexeract-cli/examples/topology.toml

# 2. Peek the first messages of a queue (non-destructive, requeues each delivery).
hexeract bus peek --queue orders.received --count 5

# 3. Drop every message in a queue (gated by an explicit safety flag).
hexeract bus purge --queue orders.received --yes-i-know
```

See the runnable [`crates/hexeract-bus-rabbitmq/examples/03_bus_pubsub.rs`](./crates/hexeract-bus-rabbitmq/examples/03_bus_pubsub.rs) for an end-to-end pub/sub against a real RabbitMQ container, and [`crates/hexeract-cli/examples/topology.toml`](./crates/hexeract-cli/examples/topology.toml) for the topology file format consumed by `hexeract bus declare`.

### Mediator (in-process)

Add the umbrella crate with the `mediator` feature to your `Cargo.toml`:

```toml
[dependencies]
hexeract = { version = "0.3", features = ["mediator"] }
```

Register a command handler and dispatch through the mediator:

```rust
use hexeract::core::{Command, CommandHandler, HandlerContext, HexeractError};
use hexeract::mediator::MediatorBuilder;

struct Greet { name: String }

impl Command for Greet {
    type Output = String;
}

struct GreetHandler;

impl CommandHandler<Greet> for GreetHandler {
    type Error = HexeractError;
    async fn handle(&self, cmd: Greet, _ctx: &HandlerContext) -> Result<String, Self::Error> {
        Ok(format!("hello {}", cmd.name))
    }
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mediator = MediatorBuilder::new()
    .register_command_handler::<Greet, _>(GreetHandler)
    .build()?;

let greeting = mediator.send(Greet { name: "world".into() }).await?;
assert_eq!(greeting, "hello world");
# Ok(()) }
```

Queries (`Mediator::query`) and notifications (`Mediator::publish`) follow the same pattern. Notifications fan out to every handler registered for the type in registration order; failures are aggregated so siblings keep running. Wire your own [`Middleware`] implementations through `MediatorBuilder::with_middleware` to add tracing, timeouts or any cross-cutting behavior around every dispatch.

## Why Hexeract

Building event-driven services in Rust today means manually wiring a broker client, an outbox table, a job queue, a workflow library and a saga state machine together. Hexeract closes that gap with a single SDK that covers the full surface area while keeping each feature independently usable:

- **Mediator**, dispatch commands to handlers in-process, type-safe and reflection-free.
- **Bus**, send messages to RabbitMQ, NATS, Kafka or AWS SQS through a unified transport abstraction.
- **Outbox**, save business state and outgoing messages atomically in a single database transaction.
- **Sagas**, orchestrate long-running workflows with persisted state, retries and compensations.
- **Scheduler**, schedule messages for later delivery, with cron, delays, exponential backoff retries and dead-letter handling.
- **Request/Reply**, perform RPC-style synchronous calls on top of an asynchronous bus via correlation identifiers.

The bet behind Hexeract is that Rust's compile-time guarantees turn the outbox pattern from a vigilance discipline into something the type system enforces.

## What Hexeract is **not**

To stay focused, the following are explicitly out of scope:

- **Not a service mesh.** No automatic mTLS or network policies between services. Use Linkerd or Istio.
- **Not a broker.** Hexeract is a client; you keep your existing RabbitMQ, NATS or Kafka.
- **Not a standalone workflow engine.** Sagas live inside your services, not in a dedicated cluster. Use Temporal or Airflow when you need that shape.
- **Not an event streaming engine.** No real-time stream processing. Use Kafka Streams or Apache Flink.

## Audience

- **Rust backend teams** building microservices who want a cohesive messaging toolkit instead of stacking incompatible crates.
- **Developers migrating to Rust** looking for a cohesive messaging SDK.
- **Polyglot teams** with part of their stack moving to Rust and the need to stay interoperable on a shared bus alongside their Node, Python or Go services.

## Contributing

Contributions are welcome. Please read [`CONTRIBUTING.md`](./CONTRIBUTING.md) first for the workflow and conventions, and [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md) for the community guidelines. For vulnerability reports, see [`SECURITY.md`](./SECURITY.md). For open-ended questions and design conversations, use [GitHub Discussions](https://github.com/nubster-opensources/hexeract/discussions).

Stability and versioning are documented in [`docs/SEMVER_POLICY.md`](./docs/SEMVER_POLICY.md) and [`docs/MSRV_POLICY.md`](./docs/MSRV_POLICY.md).

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual-licensed as above, without any additional terms or conditions.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details, including the Contributor License Agreement (CLA).

Copyright © Nubster.
