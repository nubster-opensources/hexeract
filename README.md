# Hexeract

> Rust messaging framework, Wolverine-style: Mediator, Bus, Outbox, Sagas, Scheduler, Request/Reply.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Status](https://img.shields.io/badge/status-pre--alpha-orange)](#status)
[![Made with Rust](https://img.shields.io/badge/made%20with-Rust-orange?logo=rust)](https://www.rust-lang.org/)

Hexeract is a server-side messaging framework written in Rust. It unifies in-process mediator handlers, external message bus transports, transactional outbox and inbox, long-running sagas, scheduled messages and request/reply RPC in a single coherent SDK. The framework relies on Rust's type system and procedural macros to provide compile-time guarantees in place of the runtime reflection used by .NET frameworks such as Wolverine.

Hexeract is sponsored by [Encelade Technologies](https://encelade.tech).

## Status

🚧 **Pre-alpha, no usable release yet.**

| Phase | State |
| --- | --- |
| 1. Product vision and scope | ✅ Closed (2026-05-21) |
| 2. Technical architecture | ⏳ In progress |
| 3. Open source strategy | ⏳ Pending |
| 4. Proof of concept (1 to 2 weeks spike) | ⏳ Pending |
| 5. v0.1.0 public release | ⏳ Target Q4 2026 |

The repository is intentionally private during vision and architecture phases. It will be opened to the public once Phase 3 publishes the technical design. **Do not depend on it yet**, anything can change until v0.1.0.

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
- **Ex-.NET migrants** who valued Wolverine and look for its Rust counterpart.
- **Polyglot teams** with part of their stack moving to Rust and the need to stay interoperable on a shared bus alongside their Node, Python or Go services.

## Contributing

Hexeract is in pre-alpha and its public API is unstable. The repository is currently private to keep the design conversation focused. Once Phase 2 finalises the architecture, the repo will be opened and a `CONTRIBUTING.md` will document the contribution model.

## License

Hexeract is distributed under the terms of the [MIT license](./LICENSE).

Copyright © Encelade Technologies.
