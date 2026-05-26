# Hexeract Documentation

Hexeract is a six-dimension Rust messaging framework: **Mediator**, **Bus**, **Outbox**, **Sagas**, **Scheduler**, **Request/Reply**. This index points at the documentation that is shipped today (v0.2.0).

## Start here

| If you want to | Read |
| --- | --- |
| Persist outgoing events transactionally with PostgreSQL | [Outbox quick start](getting-started/outbox-quick-start.md) |
| Publish and consume messages on RabbitMQ | [Bus quick start](getting-started/bus-quick-start.md) |
| Migrate an existing project from v0.1.0 to v0.2.0 | [Migration v0.1 to v0.2](operations/migration-v0.1-v0.2.md) |
| Operate Hexeract services in production | [Production checklist](operations/production-checklist.md) |

## Architecture

Visual overview of the building blocks and the flows they implement.

- [Workspace overview](architecture/overview.md) (crate dependency graph and roles)
- [Outbox flow](architecture/outbox-flow.md) (business transaction → envelope → worker → handler)
- [Bus flow](architecture/bus-flow.md) (publish → AMQP → consume → ack)

## Concepts

One file per cross-cutting concept the API exposes.

- [Message and envelope](concepts/message-envelope.md)
- [Outbox pattern](concepts/outbox-pattern.md)
- [Topology types](concepts/topology.md) (`Exchange`, `Queue`, `Binding`, `RoutingKey`)
- [Worker lifecycle](concepts/worker.md)
- [Ack modes](concepts/ack-modes.md) (`Auto`, `Manual`)
- [Retry policy and dead-letter routing](concepts/retry-policy.md)
- [Correlation ID propagation](concepts/correlation-id.md)

## Reference

Stable artefacts that operators reach for.

- [Outbox PostgreSQL schema](reference/outbox-postgres-schema.md)
- [`hexeract-bus` API](reference/hexeract-bus.md)
- [`hexeract-bus-rabbitmq` API](reference/hexeract-bus-rabbitmq.md)
- [`hexeract-outbox` API](reference/hexeract-outbox.md)
- [`hexeract-outbox-postgres` API](reference/hexeract-outbox-postgres.md)
- [`hexeract` CLI](reference/cli.md)

## Operations

How to run Hexeract services after the SDK leaves your hands.

- [Production checklist](operations/production-checklist.md)
- [Observability](operations/observability.md)
- [Troubleshooting](operations/troubleshooting.md)
- [Migration v0.1 to v0.2](operations/migration-v0.1-v0.2.md)

## Design notes

Records of the structuring decisions taken along the way.

- [Outbox MVP requirements (v0.1.0)](design/outbox-mvp-requirements.md)

## Project policies

Stable contracts that contributors and operators rely on.

- [Governance](GOVERNANCE.md)
- [Release process](RELEASE_PROCESS.md)
- [SemVer policy](SEMVER_POLICY.md)
- [MSRV policy](MSRV_POLICY.md)
