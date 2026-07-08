# Scheduler quick start

This guide walks you through scheduling a one-shot and a recurring message with the Hexeract scheduler, backed by PostgreSQL. By the end you will have a `ScheduleStore` seeded with two schedules, a worker polling and dispatching due occurrences onto the message bus, and a `SchedulerControl` handle inspecting and cancelling a live schedule.

Estimated time: **10 minutes** (assuming you already have a PostgreSQL instance and a RabbitMQ broker reachable).

## 1. Add the dependencies

The scheduler is backend-agnostic core plus a `sqlx`-backed SQL store, one Cargo feature per engine. This guide uses `scheduler-sql-postgres`; `scheduler-sql-mysql` and `scheduler-sql-sqlite` expose the same store shape (`MySqlScheduleStore` / `SqliteScheduleStore`). Dispatch goes out through `BusSink`, gated behind `scheduler-bus`. Scheduled events implement the `Event` trait from `hexeract-outbox`, a mandatory dependency of `hexeract-scheduler`, so the umbrella `outbox` feature is required too.

```toml
[dependencies]
hexeract = { version = "0.5", features = [
  "scheduler",
  "scheduler-bus",
  "scheduler-sql-postgres",
  "outbox",
  "bus-rabbitmq",
] }

# Already in most async Rust services:
sqlx = { version = "0.8", features = ["runtime-tokio", "tls-rustls-ring", "postgres", "uuid"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tokio-util = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v7"] }
```

- `scheduler` : `ScheduledMessage`, `Trigger`, `Target`, `SchedulerBuilder`, `SchedulerWorker`, `SchedulerControl`.
- `scheduler-bus` : `BusSink`, dispatching due occurrences onto the message bus.
- `scheduler-sql-postgres` : `PgScheduleStore`.
- `outbox` : the `Event` trait that scheduled events implement.
- `bus-rabbitmq` : the RabbitMQ transport `BusSink` dispatches through.

## 2. Apply the schema

The scheduler needs a single table. Generate the canonical DDL with the `hexeract` CLI and apply it through your own migration tooling; the CLI output is the single source of truth for the table shape (see [`hexeract-scheduler-sql` reference](../reference/hexeract-scheduler-sql.md) and the [CLI reference](../reference/cli.md)):

```sh
cargo install hexeract-cli
hexeract scheduler schema --dialect postgres --table scheduled_messages > migrations/0001_scheduled_messages.sql
```

`--dialect` accepts `postgres` (default), `my-sql` or `sqlite`; the command is offline DDL generation, it opens no connection.

Once the migration has run, construct the store from a pool:

```rust
use hexeract::scheduler_sql::PgScheduleStore;

let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
let store = PgScheduleStore::new(pool, "scheduled_messages")?;
```

`"scheduled_messages"` matches `DEFAULT_TABLE_NAME`; pass your own table name if you generated the schema under a different one. `PgScheduleStore` is `Clone`: the pool and the cached SQL strings are reference-counted, so cloning it into the worker and the control handle below is cheap.

## 3. Declare the event

```rust
use hexeract::outbox::Event;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ReminderDue {
    pub reminder_id: Uuid,
    pub note: String,
}

impl Event for ReminderDue {
    const EVENT_TYPE: &'static str = "reminders.due";
}
```

`EVENT_TYPE` becomes `ScheduledMessage::event_type` on every occurrence built from this event. Pick a stable identifier; the convention is `"<bounded-context>.<verb>"`.

## 4. Schedule a one-shot reminder

```rust
use hexeract::scheduler::{ScheduledMessage, ScheduleStore, Target};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

let reminder = ReminderDue {
    reminder_id: Uuid::new_v4(),
    note: "renew your subscription".to_owned(),
};
let message = ScheduledMessage::delay(
    Target::bus("reminders.due"),
    SystemTime::now() + Duration::from_secs(60),
    &reminder,
)?;
store.insert(&message, 5).await?;
```

`delay` mints the schedule identifier as a `UUIDv7`, encodes `reminder` as JSON and sets the trigger to fire once, 60 seconds from now. `insert` persists it with a maximum of 5 delivery attempts before dead-lettering. `message.occurrence_id()` (derived from `schedule_id` and `scheduled_for`) is the stable deduplication key a consumer dedupes on under the at-least-once delivery contract; see [Scheduler delivery](../concepts/scheduler-delivery.md).

## 5. Schedule a recurring reminder

```rust
use hexeract::scheduler::CronExpression;

let expression = "0 0 9 * * *";
let first_occurrence = CronExpression::parse(expression)?
    .next_occurrence(SystemTime::now())?
    .expect("a daily cron expression always has a next occurrence");

let daily_digest = ReminderDue {
    reminder_id: Uuid::new_v4(),
    note: "daily digest".to_owned(),
};
let message = ScheduledMessage::cron(
    Target::bus("reminders.due"),
    expression,
    first_occurrence,
    &daily_digest,
)?;
store.insert(&message, 5).await?;
```

`"0 0 9 * * *"` is a six-field expression with a leading seconds field: `seconds minute hour day-of-month month day-of-week`, so it fires at 09:00:00 UTC every day. `CronExpression::parse` also accepts a plain five-field expression (no seconds) or a macro such as `@daily`; every occurrence is evaluated in UTC. See [Scheduler triggers](../concepts/scheduler-triggers.md) for the full trigger and misfire model.

## 6. Spawn the worker

```rust
use hexeract::bus_rabbitmq::RabbitMqTransport;
use hexeract::scheduler::{BusSink, SchedulerBuilder};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

async fn run_scheduler(
    store: PgScheduleStore,
    rabbitmq_uri: &str,
    exchange: hexeract::bus::Exchange,
) -> Result<(), Box<dyn std::error::Error>> {
    let transport = RabbitMqTransport::with_exchange(rabbitmq_uri, exchange).await?;
    let worker = SchedulerBuilder::new(store, BusSink::new(transport))
        .poll_interval(Duration::from_millis(500))
        .build()?;

    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let handle = tokio::spawn(async move { worker.run(worker_cancel).await });

    // ... serve requests ...

    cancel.cancel();
    handle.await??;
    Ok(())
}
```

`SchedulerBuilder::new` starts from production-ready defaults; `poll_interval` and the other seven setters (`batch_size`, `lease`, `retry_base_delay`, `retry_max_delay`, `jitter`, `min_cycle_delay`, `dispatch_timeout`) override individual tuning fields, and `build()` rejects an incoherent configuration with `SchedulerError::InvalidConfiguration`. `run` drives the polling loop until `cancel` is triggered: each cycle claims due occurrences under a lease, dispatches through the sink under `dispatch_timeout`, then retries with bounded exponential backoff or dead-letters once the attempt budget is exhausted.

## 7. Operate a live schedule

```rust
use hexeract::scheduler::SchedulerControl;
use std::sync::Arc;

let control = SchedulerControl::new(Arc::new(store.clone()));

let snapshot = control
    .inspect(message.schedule_id)
    .await?
    .ok_or("schedule not found")?;
tracing::info!(status = ?snapshot.status, scheduled_for = ?snapshot.scheduled_for, "live schedule state");

control.cancel(message.schedule_id).await?;
```

`SchedulerControl` is the lifecycle facade application code drives a single schedule through: `inspect` reads a snapshot, `pause` / `resume` toggle claim eligibility, `cancel` excludes the schedule from future claims (a no-op if it is already terminal). The same operations are available from the command line without writing Rust: `hexeract scheduler list`, `hexeract scheduler inspect <schedule-id>` and `hexeract scheduler dead-letter list|replay` against `--conn "$DATABASE_URL"`, documented in the [CLI reference](../reference/cli.md).

## What next

- [Scheduler triggers](../concepts/scheduler-triggers.md): delay versus cron, cron field layout, UTC evaluation and misfire policy.
- [Scheduler delivery](../concepts/scheduler-delivery.md): at-least-once delivery, leases, idempotence and dead-lettering.
- [`hexeract-scheduler` API reference](../reference/hexeract-scheduler.md): the full public surface.
- [`hexeract-scheduler-sql` API reference](../reference/hexeract-scheduler-sql.md): the store constructors and schema DDL.
- The runnable [`examples/06_scheduled_reminder.rs`](../../crates/hexeract-examples/examples/06_scheduled_reminder.rs) demonstrates the full one-shot and recurring flow against real PostgreSQL and RabbitMQ containers, including inspect and cancel: `cargo run --example 06_scheduled_reminder -p hexeract-examples` (Docker required).
