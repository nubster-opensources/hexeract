//! Test-only capture of every span and event emitted through `tracing`
//! during a test's scope.
//!
//! Support for tests, not production code: unlike every other module this
//! brief adds, this one is implemented for real, not inertly. A test that
//! asserts on a span's fields or an event's level needs a subscriber that
//! actually records them; there is no neutral value that would let such an
//! assertion mean anything.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// One span captured for test assertions: its static name and every field
/// recorded on it (at creation and through any later `span.record`), keyed
/// by field name and rendered as text.
#[derive(Debug, Clone)]
pub(crate) struct CapturedSpan {
    pub(crate) name: &'static str,
    pub(crate) fields: BTreeMap<&'static str, String>,
}

/// One event captured for test assertions: its level, the name of the span
/// it was recorded inside (if any, through `.instrument`), and every field
/// it carries.
#[derive(Debug, Clone)]
pub(crate) struct CapturedEvent {
    pub(crate) level: Level,
    pub(crate) span_name: Option<&'static str>,
    pub(crate) fields: BTreeMap<&'static str, String>,
}

/// A `tracing::field::Visit` that renders every field it visits as text and
/// collects it by name.
#[derive(Debug, Default)]
struct FieldsVisitor(BTreeMap<&'static str, String>);

impl Visit for FieldsVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }
}

/// Index of a captured span's position in [`Recorded::spans`], stashed in
/// that span's own extensions so a later `on_record` can find it by [`Id`]
/// rather than by name, which several spans of the same call may share.
struct SpanIndex(usize);

#[derive(Debug, Default)]
struct Recorded {
    spans: Vec<CapturedSpan>,
    events: Vec<CapturedEvent>,
}

/// The [`Layer`] doing the actual recording. Kept private: a test only ever
/// touches it through [`SpanCapture`].
struct CaptureLayer {
    recorded: Arc<Mutex<Recorded>>,
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut visitor = FieldsVisitor::default();
        attrs.record(&mut visitor);
        let name = attrs.metadata().name();

        let index = {
            let mut recorded = self.recorded.lock().unwrap_or_else(PoisonError::into_inner);
            let index = recorded.spans.len();
            recorded.spans.push(CapturedSpan {
                name,
                fields: visitor.0,
            });
            index
        };

        if let Some(span_ref) = ctx.span(id) {
            span_ref.extensions_mut().insert(SpanIndex(index));
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let mut visitor = FieldsVisitor::default();
        values.record(&mut visitor);

        let Some(span_ref) = ctx.span(id) else {
            return;
        };
        let Some(&SpanIndex(index)) = span_ref.extensions().get::<SpanIndex>() else {
            return;
        };
        let mut recorded = self.recorded.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(span) = recorded.spans.get_mut(index) {
            span.fields.extend(visitor.0);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut visitor = FieldsVisitor::default();
        event.record(&mut visitor);
        let span_name = ctx
            .event_span(event)
            .map(|span_ref| span_ref.metadata().name());

        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .push(CapturedEvent {
                level: *event.metadata().level(),
                span_name,
                fields: visitor.0,
            });
    }
}

/// Handle onto every span and event captured while its [`tracing::subscriber::DefaultGuard`]
/// stays alive.
///
/// Built by [`Self::install`], which returns both this handle and the
/// guard: dropping the guard restores whatever subscriber was previously
/// installed for the current thread. `#[tokio::test]` is single-threaded by
/// default, so the guard covers everything a test spawns on that runtime. A
/// `multi_thread` test would not be captured faithfully: the guard is
/// thread-local, so anything emitted from another worker thread escapes it.
pub(crate) struct SpanCapture {
    recorded: Arc<Mutex<Recorded>>,
}

impl SpanCapture {
    /// Install a fresh capturing subscriber as the default for the current
    /// thread, and return a handle onto it alongside the guard that keeps
    /// it installed.
    pub(crate) fn install() -> (Self, tracing::subscriber::DefaultGuard) {
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let layer = CaptureLayer {
            recorded: Arc::clone(&recorded),
        };
        let subscriber = Registry::default().with(layer);
        let guard = tracing::subscriber::set_default(subscriber);
        (Self { recorded }, guard)
    }

    /// Every span captured so far whose static name is exactly `name`, in
    /// the order it was opened.
    pub(crate) fn spans_named(&self, name: &str) -> Vec<CapturedSpan> {
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .spans
            .iter()
            .filter(|span| span.name == name)
            .cloned()
            .collect()
    }

    /// Every event captured so far, in the order it was emitted.
    pub(crate) fn events(&self) -> Vec<CapturedEvent> {
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .clone()
    }

    /// Every field value captured so far, across every span and every
    /// event, in no particular order.
    ///
    /// Built for one purpose: proving a canary planted in a payload or an
    /// application header never reached an observability field.
    pub(crate) fn every_field_value(&self) -> Vec<String> {
        let recorded = self.recorded.lock().unwrap_or_else(PoisonError::into_inner);
        recorded
            .spans
            .iter()
            .flat_map(|span| span.fields.values().cloned())
            .chain(
                recorded
                    .events
                    .iter()
                    .flat_map(|event| event.fields.values().cloned()),
            )
            .collect()
    }
}
