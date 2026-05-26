# Hexeract Outbox PostgreSQL examples

Runnable examples for the `hexeract-outbox-postgres` crate. Each file is a
self-contained Rust binary executed via Cargo's `--example` flag.

| # | Example | Demonstrates |
|---|---|---|
| 02 | [`02_outbox_two_databases.rs`](02_outbox_two_databases.rs) | End-to-end Outbox flow with two isolated PostgreSQL databases (operational + audit). Showcases `publish_in_tx`, the `PgOutboxWorkerBuilder` fluent API, `SELECT ... FOR UPDATE SKIP LOCKED` polling, and a handler that owns its own connection pool. |

Run any example from the repository root:

```sh
cargo run --example 02_outbox_two_databases -p hexeract-outbox-postgres
```

## Prerequisites

- A running Docker daemon. The examples use
  [`testcontainers`](https://crates.io/crates/testcontainers) to spin up
  ephemeral PostgreSQL instances, so no manual database setup is required.
- Rust toolchain matching the workspace MSRV (see
  [`docs/MSRV_POLICY.md`](../../../docs/MSRV_POLICY.md)).

## What next

For a guided walkthrough that wires the Outbox into an existing service, see
[`docs/tutorial/getting-started.md`](../../../docs/tutorial/getting-started.md).
