//! CLI-facing view of a scheduled message.
//!
//! [`hexeract_scheduler::ScheduleSnapshot`] is a domain type; it never leaks
//! past this module. Commands convert it into a [`ScheduleView`] and render
//! that instead, so the CLI's output format (text columns or JSON) can evolve
//! independently of the scheduler crate's internals.

use clap::ValueEnum;
use hexeract_scheduler::{ScheduleSnapshot, ScheduleStatus, Trigger};
use serde::Serialize;

use crate::error::CliError;

/// Output format selected on the command line.
#[derive(ValueEnum, Clone, Copy, Debug, Default)]
pub(crate) enum OutputFormat {
    /// Human-readable, column-aligned table (the default).
    #[default]
    Text,
    /// Machine-readable JSON array.
    Json,
}

/// A schedule as presented by the CLI. The domain snapshot never leaks.
#[derive(Serialize, Debug)]
pub(crate) struct ScheduleView {
    pub(crate) schedule_id: String,
    pub(crate) status: String,
    pub(crate) scheduled_for: String,
    pub(crate) attempts: u32,
    pub(crate) max_attempts: u32,
    pub(crate) trigger: String,
    pub(crate) last_error: Option<String>,
}

impl From<&ScheduleSnapshot> for ScheduleView {
    fn from(snapshot: &ScheduleSnapshot) -> Self {
        Self {
            schedule_id: snapshot.schedule_id.to_string(),
            status: status_label(snapshot.status).to_owned(),
            scheduled_for: format_time(snapshot.scheduled_for),
            attempts: snapshot.attempts,
            max_attempts: snapshot.max_attempts,
            trigger: trigger_label(&snapshot.trigger),
            last_error: snapshot.last_error.clone(),
        }
    }
}

/// Map a status to its stable, lowercase CLI label.
///
/// `ScheduleStatus` is `#[non_exhaustive]`, so a catch-all arm is required
/// even though every current variant is handled explicitly.
fn status_label(status: ScheduleStatus) -> &'static str {
    match status {
        ScheduleStatus::Pending => "pending",
        ScheduleStatus::Paused => "paused",
        ScheduleStatus::Delivered => "delivered",
        ScheduleStatus::Cancelled => "cancelled",
        ScheduleStatus::DeadLettered => "dead-lettered",
        _ => "unknown",
    }
}

/// Map a trigger to its CLI label (`"delay"` or `"cron:<expression>"`).
///
/// `Trigger` is `#[non_exhaustive]`, so a catch-all arm is required even
/// though every current variant is handled explicitly.
fn trigger_label(trigger: &Trigger) -> String {
    match trigger {
        Trigger::Delay(_) => "delay".to_owned(),
        Trigger::Cron(expression) => format!("cron:{}", expression.as_str()),
        _ => "unknown".to_owned(),
    }
}

/// Format an instant as RFC 3339, falling back to a fixed marker on failure
/// (out-of-range instants) rather than panicking.
fn format_time(time: std::time::SystemTime) -> String {
    let offset = time::OffsetDateTime::from(time);
    offset
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "invalid".to_owned())
}

/// Render a slice of views in the requested format.
///
/// # Errors
///
/// Returns [`CliError::Fatal`] if JSON serialization fails.
pub(crate) fn render(views: &[ScheduleView], format: OutputFormat) -> Result<String, CliError> {
    match format {
        OutputFormat::Json => {
            serde_json::to_string_pretty(views).map_err(|e| CliError::Fatal(Box::new(e)))
        }
        OutputFormat::Text => Ok(render_text(views)),
    }
}

/// Render views as a hand-aligned, human-readable table.
fn render_text(views: &[ScheduleView]) -> String {
    use std::fmt::Write as _;

    let mut out = String::from(
        "SCHEDULE ID                            STATUS         SCHEDULED FOR              ATTEMPTS  TRIGGER\n",
    );
    for view in views {
        let _ = writeln!(
            out,
            "{:<38} {:<14} {:<26} {:>4}/{:<4} {}",
            view.schedule_id,
            view.status,
            view.scheduled_for,
            view.attempts,
            view.max_attempts,
            view.trigger,
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use hexeract_scheduler::{ScheduleSnapshot, ScheduleStatus, Trigger};
    use std::time::UNIX_EPOCH;

    fn snapshot() -> ScheduleSnapshot {
        ScheduleSnapshot::new(
            uuid::Uuid::nil(),
            ScheduleStatus::Pending,
            UNIX_EPOCH,
            1,
            5,
            Trigger::delay(UNIX_EPOCH),
            None,
        )
    }

    #[test]
    fn view_maps_snapshot_fields() {
        let view = ScheduleView::from(&snapshot());
        assert_eq!(view.status, "pending");
        assert_eq!(view.trigger, "delay");
        assert_eq!(view.attempts, 1);
    }

    #[test]
    fn json_render_is_valid_array() {
        let views = [ScheduleView::from(&snapshot())];
        let out = render(&views, OutputFormat::Json).expect("json");
        assert!(out.trim_start().starts_with('['));
        assert!(out.contains("\"status\": \"pending\""));
    }

    #[test]
    fn text_render_contains_header_and_id() {
        let views = [ScheduleView::from(&snapshot())];
        let out = render(&views, OutputFormat::Text).expect("text");
        assert!(out.contains("SCHEDULE ID"));
        assert!(out.contains(&uuid::Uuid::nil().to_string()));
    }
}
