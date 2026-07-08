# Scheduler triggers, cron expressions and misfire policy

A `ScheduledMessage` fires under a `Trigger`: either once, at a fixed instant, or repeatedly, on a cron schedule. This page explains the two trigger kinds, the cron expression forms `CronExpression::parse` accepts, why every occurrence is computed in UTC, and how the scheduler behaves when a worker is late (misfire) versus when a schedule is deliberately paused.

## Triggers

`Trigger` is a non-exhaustive enum with two variants, built through two constructors:

- `Trigger::delay(at: SystemTime) -> Self` fires exactly once, at the given instant. It cannot fail: any `SystemTime` is a valid delay target.
- `Trigger::cron(expression: &str) -> Result<Self, SchedulerError>` fires repeatedly, on the recurrence described by `expression`. It parses and validates the expression up front, so a `Trigger::Cron` value always wraps an expression that is known to re-parse when occurrences are computed later.

`Trigger::is_recurring()` distinguishes the two (`true` only for `Cron`), and `Trigger::kind()` returns a stable lowercase tag, `"delay"` or `"cron"`, for logging and snapshots. Because `Trigger` is `#[non_exhaustive]`, application code builds instances through these two constructors and matches on the enum with a wildcard arm.

## Cron expressions

A cron trigger wraps a `CronExpression`, which `CronExpression::parse` validates through the `isochron` cron engine before it is accepted. Three forms are recognized:

1. **Five fields**: `minute hour day-of-month month day-of-week`.
2. **Six fields**: the same five fields with a leading seconds field, `second minute hour day-of-month month day-of-week`.
3. **A supported macro**, such as `@daily`.

Validation checks field count, ranges, steps, lists, and named months and days, all up front at construction. An expression that fails any of these checks is rejected with `SchedulerError::InvalidTrigger` and never becomes a `Trigger`.

```rust
// Five fields: minute hour day-of-month month day-of-week.
// Fires once a day at 00:00 UTC.
Trigger::cron("0 0 * * *")?;

// Six fields: second minute hour day-of-month month day-of-week.
// Fires once a day at 09:00:00 UTC.
Trigger::cron("0 0 9 * * *")?;

// Six fields, every 2 seconds.
Trigger::cron("*/2 * * * * *")?;

// Macro form.
Trigger::cron("@daily")?;
```

An expression that re-parses successfully is guaranteed to keep re-parsing later: `CronExpression::next_occurrence` re-runs `isochron::CronSchedule::parse` on the stored text on every call, so a `SchedulerError::Internal` there would indicate an engine inconsistency, not a bad input.

## UTC evaluation

Every occurrence is computed in UTC. `CronExpression::next_occurrence` converts the `SystemTime` anchor to a UTC `OffsetDateTime`, asks `isochron` for the next match strictly after that anchor, and converts the result back to a `SystemTime`. There is no per-schedule time zone offset anywhere in this path: the field values in a cron expression (hour, minute, and so on) are always UTC field values.

This is an explicit non-goal of the crate, not an oversight: `hexeract-scheduler` does not track or apply time zones, daylight-saving transitions, or locale calendars. A schedule meant to fire at "09:00 local time" has to be expressed in UTC by the caller, and re-expressed if the intended local offset changes (for example across a daylight-saving boundary).

## Misfire policy

The scheduler follows fire-once semantics: when a worker was unable to poll for a while (down, backed up, or simply slower than the schedule's period) and multiple occurrences of a recurring trigger have come due in the meantime, the next poll produces exactly one catch-up occurrence, never a burst of one per missed tick.

This is implemented by `CronExpression::next_due(now, previous_due)`, which anchors the search on `max(now, previous_due)` rather than walking forward occurrence by occurrence from `previous_due`. The worker calls it after a successful delivery, with the current time and the occurrence that was just delivered, and reschedules to the single result. A schedule that was due for ten missed minutely ticks is not queued ten times: the next `next_due` computation collapses those ten misses into one due instant, and the schedule realigns onto the future from there.

## Pause versus misfire

A misfire and a pause both mean a schedule's occurrence was not delivered on time, but the scheduler treats them differently:

- **Misfire** (the worker fell behind a running schedule): the next delivery collapses all missed ticks into a single catch-up occurrence, as described above. The schedule keeps running afterward.
- **Pause** (`SchedulerControl::pause`, an explicit operator or application action): the schedule is excluded from claims entirely. No catch-up is scheduled while paused, and none is produced on `resume`. `SchedulerControl::resume` unpauses a one-shot delay, or a cron schedule whose stored occurrence is still in the future, with that occurrence intact. For a cron schedule whose stored occurrence fell in the past while paused, `resume` computes the next strictly future occurrence and realigns to it instead of firing once per missed tick; if the expression has no future occurrence left, the schedule stays paused.

In short: a misfire always catches up once; a pause never catches up, on either the ticks it skipped or the moment it resumes.

## Where to read next

- [Scheduler delivery](scheduler-delivery.md)
- [`hexeract-scheduler` API reference](../reference/hexeract-scheduler.md)
