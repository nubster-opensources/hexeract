//! Mapping between the scheduler domain types and the scalar column values
//! every SQL backend reads and writes.
//!
//! The conversions live here, away from the dialect modules, so the row
//! decoding and the bind ordering share one source of truth: the discriminant
//! strings, the trigger and target reconstruction, and the lifecycle status
//! derived from the timestamp columns. The dialect modules only deal with
//! their driver's row and value types.

use std::time::SystemTime;

use uuid::Uuid;

use hexeract_scheduler::ScheduleStatus;
use hexeract_scheduler::ScheduledMessage;
use hexeract_scheduler::SchedulerError;
use hexeract_scheduler::Target;
use hexeract_scheduler::Trigger;

/// Discriminant stored in `trigger_kind` for a one-shot delay trigger.
pub(crate) const TRIGGER_DELAY: &str = "delay";
/// Discriminant stored in `trigger_kind` for a recurring cron trigger.
pub(crate) const TRIGGER_CRON: &str = "cron";

/// Discriminant stored in `target_kind` for the in-process mediator target.
pub(crate) const TARGET_MEDIATOR: &str = "mediator";
/// Discriminant stored in `target_kind` for the transactional outbox target.
pub(crate) const TARGET_OUTBOX: &str = "outbox";
/// Discriminant stored in `target_kind` for the message-bus target.
pub(crate) const TARGET_BUS: &str = "bus";

/// The `(trigger_kind, cron_expr)` column pair for a trigger.
///
/// A delay carries no cron expression; a cron trigger carries its validated
/// expression text. The borrow is tied to `trigger`.
///
/// # Errors
///
/// Returns [`SchedulerError::Internal`] for a trigger kind this backend does
/// not know how to persist.
pub(crate) fn trigger_columns(
    trigger: &Trigger,
) -> Result<(&'static str, Option<&str>), SchedulerError> {
    match trigger {
        Trigger::Delay(_) => Ok((TRIGGER_DELAY, None)),
        Trigger::Cron(expression) => Ok((TRIGGER_CRON, Some(expression.as_str()))),
        other => Err(SchedulerError::internal(format!(
            "unsupported trigger kind for SQL persistence: {other:?}"
        ))),
    }
}

/// The `(target_kind, target_routing_key)` column pair for a dispatch target.
///
/// Only the bus target carries a routing key. The borrow is tied to `target`.
///
/// # Errors
///
/// Returns [`SchedulerError::Internal`] for a target kind this backend does
/// not know how to persist.
pub(crate) fn target_columns(
    target: &Target,
) -> Result<(&'static str, Option<&str>), SchedulerError> {
    match target {
        Target::Mediator => Ok((TARGET_MEDIATOR, None)),
        Target::Outbox => Ok((TARGET_OUTBOX, None)),
        Target::Bus { routing_key } => Ok((TARGET_BUS, Some(routing_key.as_str()))),
        other => Err(SchedulerError::internal(format!(
            "unsupported target kind for SQL persistence: {other:?}"
        ))),
    }
}

/// Rebuild a [`Trigger`] from its stored columns.
///
/// A delay trigger fires at `scheduled_for`; a cron trigger is rebuilt from
/// its stored expression, which is revalidated by the constructor.
///
/// # Errors
///
/// Returns [`SchedulerError::Internal`] if the kind is unknown or a cron row
/// is missing its expression, or [`SchedulerError::InvalidTrigger`] if a
/// stored cron expression no longer parses.
pub(crate) fn build_trigger(
    kind: &str,
    cron_expr: Option<String>,
    scheduled_for: SystemTime,
) -> Result<Trigger, SchedulerError> {
    match kind {
        TRIGGER_DELAY => Ok(Trigger::delay(scheduled_for)),
        TRIGGER_CRON => {
            let expression = cron_expr.ok_or_else(|| {
                SchedulerError::internal("cron schedule row is missing its cron expression")
            })?;
            Trigger::cron(&expression)
        }
        other => Err(SchedulerError::internal(format!(
            "unknown trigger kind in storage: {other:?}"
        ))),
    }
}

/// Rebuild a [`Target`] from its stored columns.
///
/// # Errors
///
/// Returns [`SchedulerError::Internal`] if the kind is unknown or a bus row
/// is missing its routing key.
pub(crate) fn build_target(
    kind: &str,
    routing_key: Option<String>,
) -> Result<Target, SchedulerError> {
    match kind {
        TARGET_MEDIATOR => Ok(Target::mediator()),
        TARGET_OUTBOX => Ok(Target::outbox()),
        TARGET_BUS => {
            let routing_key = routing_key.ok_or_else(|| {
                SchedulerError::internal("bus target row is missing its routing key")
            })?;
            Ok(Target::bus(routing_key))
        }
        other => Err(SchedulerError::internal(format!(
            "unknown target kind in storage: {other:?}"
        ))),
    }
}

/// Reassemble a [`ScheduledMessage`] from the scalar columns a backend decoded
/// from a row.
///
/// # Errors
///
/// Returns the error of [`build_trigger`] or [`build_target`] if a
/// discriminant column holds a value this backend cannot map.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_message(
    schedule_id: Uuid,
    event_type: String,
    payload: Vec<u8>,
    trigger_kind: &str,
    cron_expr: Option<String>,
    scheduled_for: SystemTime,
    target_kind: &str,
    routing_key: Option<String>,
) -> Result<ScheduledMessage, SchedulerError> {
    let trigger = build_trigger(trigger_kind, cron_expr, scheduled_for)?;
    let target = build_target(target_kind, routing_key)?;
    Ok(ScheduledMessage::restore(
        schedule_id,
        event_type,
        payload,
        target,
        trigger,
        scheduled_for,
    ))
}

/// Map the three terminal-timestamp presence flags to a terminal status.
///
/// The terminal states are mutually exclusive in storage (the acknowledgement
/// statements never revive a terminal schedule), but a defensive priority is
/// applied anyway: dead-lettered, then cancelled, then delivered. Returns
/// `None` when no terminal timestamp is set.
pub(crate) fn terminal_status(
    delivered: bool,
    cancelled: bool,
    dead_lettered: bool,
) -> Option<ScheduleStatus> {
    if dead_lettered {
        Some(ScheduleStatus::DeadLettered)
    } else if cancelled {
        Some(ScheduleStatus::Cancelled)
    } else if delivered {
        Some(ScheduleStatus::Delivered)
    } else {
        None
    }
}

/// Combine a terminal status with the paused flag into the lifecycle
/// [`ScheduleStatus`].
///
/// A terminal status wins; otherwise the schedule is paused or pending. The
/// paused flag only applies to an otherwise pending schedule.
pub(crate) fn status_with_paused(terminal: Option<ScheduleStatus>, paused: bool) -> ScheduleStatus {
    terminal.unwrap_or(if paused {
        ScheduleStatus::Paused
    } else {
        ScheduleStatus::Pending
    })
}

/// Clamp a stored attempt counter into the domain's `u32`, treating a negative
/// value (which the schema never writes) as zero.
pub(crate) fn attempts_from_i64(value: i64) -> u32 {
    u32::try_from(value.max(0)).unwrap_or(u32::MAX)
}

/// Convert an attempt budget into the signed integer the schema stores,
/// saturating at [`i32::MAX`].
pub(crate) fn max_attempts_to_i32(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn instant() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_000)
    }

    #[test]
    fn trigger_columns_maps_delay_without_a_cron_expression() {
        let trigger = Trigger::delay(instant());
        let (kind, cron) = trigger_columns(&trigger).unwrap();
        assert_eq!(kind, TRIGGER_DELAY);
        assert!(cron.is_none());
    }

    #[test]
    fn trigger_columns_maps_cron_with_its_expression() {
        let trigger = Trigger::cron("0 0 * * *").unwrap();
        let (kind, cron) = trigger_columns(&trigger).unwrap();
        assert_eq!(kind, TRIGGER_CRON);
        assert_eq!(cron, Some("0 0 * * *"));
    }

    #[test]
    fn target_columns_maps_each_variant() {
        assert_eq!(
            target_columns(&Target::mediator()).unwrap(),
            (TARGET_MEDIATOR, None)
        );
        assert_eq!(
            target_columns(&Target::outbox()).unwrap(),
            (TARGET_OUTBOX, None)
        );
        let bus = Target::bus("orders.placed");
        let (kind, key) = target_columns(&bus).unwrap();
        assert_eq!(kind, TARGET_BUS);
        assert_eq!(key, Some("orders.placed"));
    }

    #[test]
    fn build_trigger_round_trips_a_delay() {
        let trigger = build_trigger(TRIGGER_DELAY, None, instant()).unwrap();
        assert_eq!(trigger, Trigger::delay(instant()));
    }

    #[test]
    fn build_trigger_round_trips_a_cron() {
        let trigger = build_trigger(TRIGGER_CRON, Some("0 0 * * *".to_owned()), instant()).unwrap();
        assert!(trigger.is_recurring());
    }

    #[test]
    fn build_trigger_rejects_a_cron_without_an_expression() {
        let error = build_trigger(TRIGGER_CRON, None, instant()).unwrap_err();
        assert!(matches!(error, SchedulerError::Internal(_)));
    }

    #[test]
    fn build_trigger_rejects_an_unknown_kind() {
        let error = build_trigger("weekly", None, instant()).unwrap_err();
        assert!(matches!(error, SchedulerError::Internal(_)));
    }

    #[test]
    fn build_target_round_trips_each_variant() {
        assert_eq!(
            build_target(TARGET_MEDIATOR, None).unwrap(),
            Target::Mediator
        );
        assert_eq!(build_target(TARGET_OUTBOX, None).unwrap(), Target::Outbox);
        assert_eq!(
            build_target(TARGET_BUS, Some("orders.placed".to_owned())).unwrap(),
            Target::bus("orders.placed")
        );
    }

    #[test]
    fn build_target_rejects_a_bus_without_a_routing_key() {
        let error = build_target(TARGET_BUS, None).unwrap_err();
        assert!(matches!(error, SchedulerError::Internal(_)));
    }

    #[test]
    fn build_target_rejects_an_unknown_kind() {
        let error = build_target("webhook", None).unwrap_err();
        assert!(matches!(error, SchedulerError::Internal(_)));
    }

    #[test]
    fn terminal_status_prioritises_dead_letter_then_cancel_then_deliver() {
        assert_eq!(terminal_status(false, false, false), None);
        assert_eq!(
            terminal_status(true, false, false),
            Some(ScheduleStatus::Delivered)
        );
        assert_eq!(
            terminal_status(false, true, false),
            Some(ScheduleStatus::Cancelled)
        );
        assert_eq!(
            terminal_status(true, true, true),
            Some(ScheduleStatus::DeadLettered)
        );
    }

    #[test]
    fn status_with_paused_orders_terminal_states_before_paused() {
        assert_eq!(status_with_paused(None, false), ScheduleStatus::Pending);
        assert_eq!(status_with_paused(None, true), ScheduleStatus::Paused);
        assert_eq!(
            status_with_paused(Some(ScheduleStatus::Delivered), true),
            ScheduleStatus::Delivered
        );
        assert_eq!(
            status_with_paused(Some(ScheduleStatus::DeadLettered), false),
            ScheduleStatus::DeadLettered
        );
    }

    #[test]
    fn attempts_conversions_clamp_out_of_range_values() {
        assert_eq!(attempts_from_i64(-1), 0);
        assert_eq!(attempts_from_i64(3), 3);
        assert_eq!(max_attempts_to_i32(5), 5);
        assert_eq!(max_attempts_to_i32(u32::MAX), i32::MAX);
    }

    #[test]
    fn build_message_reassembles_every_field() {
        let message = build_message(
            Uuid::from_u128(7),
            "reminders.due".to_owned(),
            b"{}".to_vec(),
            TRIGGER_DELAY,
            None,
            instant(),
            TARGET_OUTBOX,
            None,
        )
        .unwrap();
        assert_eq!(message.schedule_id, Uuid::from_u128(7));
        assert_eq!(message.event_type, "reminders.due");
        assert_eq!(message.target, Target::Outbox);
        assert_eq!(message.trigger, Trigger::delay(instant()));
        assert_eq!(message.scheduled_for, instant());
    }
}
