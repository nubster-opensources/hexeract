# Scheduler delivery, leases and dead-letter

The scheduler dispatches a due occurrence to its sink at least once, never exactly once. This page explains the at-least-once contract, how a soft lease keeps a crashed worker from losing an occurrence, why `OccurrenceId` is the deduplication key downstream consumers rely on, how a failed dispatch is retried with backoff, and what happens once a schedule exhausts its attempt budget.

## At-least-once delivery

`ScheduleStore::claim_due` selects due, unleased, eligible occurrences and, in one atomic step, advances the attempt counter and stamps a fresh lease before returning them to the worker. The worker then dispatches outside any transaction. If the process crashes between the claim and the acknowledgement, nothing rolls the claim back: the lease simply expires and another worker reclaims the occurrence. That is what makes delivery at-least-once rather than exactly-once: a sink can observe the same occurrence more than once, and the scheduler makes no attempt to hide that from the consumer. Downstream code is expected to tolerate or dedupe a repeat delivery rather than assume single delivery.

## Leases and crash recovery

`LeasedOccurrence` is the record returned by `claim_due`: the `ScheduledMessage` to dispatch, the number of `attempts` consumed so far (including the current one), the `max_attempts` budget, and `leased_until`, the instant until which this claim holds the occurrence. While the lease is held, a competing worker skips the occurrence entirely; `claim_due` only returns occurrences that are not yet due, not still leased, and not paused, cancelled or terminal.

The lease duration is a `SchedulerWorkerConfig` setting (`lease`, 300 seconds by default, sized to cover `batch_size` times `dispatch_timeout`) passed into `claim_due` on every poll cycle. It exists specifically for the crash case: a worker that dies mid-dispatch never releases its claim explicitly, so the store instead waits out `leased_until` and then lets the occurrence be claimed again. Because the attempt counter is advanced at claim time and not only on failure, a crash-and-reclaim cycle still counts against the attempt budget, so a poison occurrence eventually reaches `max_attempts` instead of being retried forever.

## Idempotence with OccurrenceId

`OccurrenceId::derive(schedule_id, scheduled_for)` computes a stable identifier for one firing of a schedule: a `UUIDv5` built from the schedule's `Uuid` and the signed offset of `scheduled_for` from the Unix epoch. The same schedule firing at the same instant always derives the same `OccurrenceId`, across processes and across a redelivery caused by a lease expiry. `LeasedOccurrence::occurrence_id()` exposes it for a claimed occurrence.

This is the dedup key consumers are expected to use: since the scheduler guarantees at-least-once and not exactly-once, a sink or handler that must not process the same firing twice should record the `OccurrenceId` it has already handled and discard a repeat. The runnable example `crates/hexeract-examples/examples/06_scheduled_reminder.rs` wires a one-shot and a recurring reminder through `BusSink`, and its handler deduplicates on the occurrence id that `BusSink` stamps as the bus message id, demonstrating the full idempotence pattern end to end.

## Retry and backoff

When a dispatch fails and the occurrence still has attempt budget left, `SchedulerWorker::on_failure` computes the next retry delay from three `SchedulerWorkerConfig` fields: `retry_base_delay` (1 second by default), `retry_max_delay` (300 seconds by default) and `jitter` (`true` by default). The delay grows exponentially with the attempt count, `retry_base_delay` doubled once per attempt, capped at `retry_max_delay`. When `jitter` is enabled the worker draws a uniformly random duration between zero and that capped value (full jitter) instead of using it directly, which spreads out retries from many occurrences that failed around the same time. The occurrence is not reclaimed until this retry deadline passes, and its attempt counter, already advanced at claim time, is left untouched: the failure only pushes the lease out and records the error.

## Dead-letter

`LeasedOccurrence::is_exhausted()` is true once `attempts >= max_attempts`. When a dispatch fails and the occurrence is already exhausted, the worker calls `ScheduleStore::mark_dead_lettered` instead of scheduling a retry: the schedule moves to the `DeadLettered` status with the last error recorded, and it is permanently excluded from future claims until an operator acts on it.

Dead-lettered schedules are inspected and replayed through the CLI, never by hand-editing the table. `hexeract scheduler dead-letter list` lists them, most recently dead-lettered first, and accepts `--conn`, `--table`, `--format text|json` and `--limit` (default 50). `hexeract scheduler dead-letter replay <SCHEDULE_ID>` resets the attempt counter to zero, clears the last error and reschedules the occurrence for now; it accepts `--conn` and `--table` but not `--format`, since replay produces no listing to render. See the [CLI reference](../reference/cli.md) for the full command surface.

## Where to read next

- [Scheduler triggers](scheduler-triggers.md)
- [`hexeract-scheduler` API reference](../reference/hexeract-scheduler.md)
