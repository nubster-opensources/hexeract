# Hexeract Documentation

Hexeract is a six-dimension Rust messaging framework: **Mediator**, **Bus**, **Outbox**, **Sagas**, **Scheduler**, **Request/Reply**. This index points at the documentation that is shipped today (v0.4.0).

## Start here

| If you want to | Read |
| --- | --- |
| Dispatch commands, queries and notifications in process | [Mediator quick start](getting-started/mediator-quick-start.md) |
| Persist outgoing events transactionally with PostgreSQL, MySQL or SQLite | [Outbox quick start](getting-started/outbox-quick-start.md) |
| Publish and consume messages on RabbitMQ | [Bus quick start](getting-started/bus-quick-start.md) |
| Migrate an existing project from v0.3.0 to v0.4.0 | [Migration v0.3 to v0.4](operations/migration-v0.3-v0.4.md) |
| Migrate an existing project from v0.1.0 to v0.2.0 | [Migration v0.1 to v0.2](operations/migration-v0.1-v0.2.md) |
| Operate Hexeract services in production | [Production checklist](operations/production-checklist.md) |

## Migrating from another framework

If you come from the .NET ecosystem, these guides map your existing concepts onto Hexeract:

- [From MediatR (.NET)](migration/from-mediatr.md)
- [From Wolverine (.NET)](migration/from-wolverine.md)

## Architecture

Visual overview of the building blocks and the flows they implement.

- [Workspace overview](architecture/overview.md) (crate dependency graph and roles)
- [Mediator flow](architecture/mediator-flow.md) (registry, dispatch sequence, fan-out fail-safe)
- [Outbox flow](architecture/outbox-flow.md) (business transaction → envelope → worker → handler)
- [Bus flow](architecture/bus-flow.md) (publish → AMQP → consume → ack)

## Concepts

One file per cross-cutting concept the API exposes.

- [Message and envelope](concepts/message-envelope.md)
- [Mediator CQRS semantics](concepts/mediator-cqrs.md) (`Command`, `Query`, `Notification` contracts)
- [Middleware pipeline](concepts/middleware-pipeline.md) (onion order, `Next`, `Terminal`, built-ins)
- [The `#[handler]` macro](concepts/handler-macro.md) (auto-discovery via `inventory`)
- [Outbox pattern](concepts/outbox-pattern.md)
- [Topology types](concepts/topology.md) (`Exchange`, `Queue`, `Binding`, `RoutingKey`)
- [Worker lifecycle](concepts/worker.md)
- [Ack modes](concepts/ack-modes.md) (`Manual`, `AckOnReceive`, `Unacknowledged`)
- [SQLite outbox concurrency](concepts/sqlite-outbox-concurrency.md) (single-writer model, backend choice)
- [Retry policy and dead-letter routing](concepts/retry-policy.md)
- [Correlation ID propagation](concepts/correlation-id.md)

## Cookbook

Recipes for the most common wirings.

- [Wire tracing and timeout around every dispatch](cookbook/wire-tracing-and-timeout.md)
- [Telescope outbox inside a mediator command handler](cookbook/outbox-plus-mediator.md)
- [A handler that holds state (DB pool, configuration)](cookbook/handler-with-state.md)
- [Fan out a domain event to multiple subscribers](cookbook/notification-fan-out.md)
- [Catch missing handler wirings in CI](cookbook/sanity-check-handlers.md)

## Reference

Stable artefacts that operators reach for.

- [`hexeract-mediator` API](reference/hexeract-mediator.md)
- [`hexeract-middleware` API](reference/hexeract-middleware.md)
- [`hexeract-macros` API](reference/hexeract-macros.md)
- [`hexeract-outbox` API](reference/hexeract-outbox.md)
- [`hexeract-outbox-sql` API](reference/hexeract-outbox-sql.md) (PostgreSQL, MySQL, SQLite on `sqlx`)
- [`hexeract-outbox-postgres` migration](reference/hexeract-outbox-postgres.md) (removed in 0.5.0, redirect to `hexeract-outbox-sql`)
- [`hexeract-bus` API](reference/hexeract-bus.md)
- [`hexeract-bus-rabbitmq` API](reference/hexeract-bus-rabbitmq.md)
- [Outbox PostgreSQL schema](reference/outbox-postgres-schema.md)
- [`hexeract` CLI](reference/cli.md)

## Operations

How to run Hexeract services after the SDK leaves your hands.

- [Production checklist](operations/production-checklist.md)
- [Observability](operations/observability.md)
- [Troubleshooting](operations/troubleshooting.md)
- [Migration v0.3 to v0.4](operations/migration-v0.3-v0.4.md)
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
