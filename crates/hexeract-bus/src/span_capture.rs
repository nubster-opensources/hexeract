//! Test-only capture of every span and event emitted through `tracing`
//! during a test's scope.
//!
//! Support for tests, not production code: unlike every other module this
//! brief adds, this one is implemented for real, not inertly. A test that
//! asserts on a span's fields or an event's level needs a subscriber that
//! actually records them; there is no neutral value that would let such an
//! assertion mean anything.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

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

thread_local! {
    /// Where this thread's capture, if any, is currently recording.
    ///
    /// The layer below is installed once, globally, and writes here. A
    /// thread with no active capture is one where this is `None`, and the
    /// layer then drops what it sees.
    static ACTIVE_CAPTURE: RefCell<Option<Arc<Mutex<Recorded>>>> = const { RefCell::new(None) };
}

/// Installed exactly once for the whole test binary.
static GLOBAL_SUBSCRIBER: OnceLock<()> = OnceLock::new();

/// The [`Layer`] doing the actual recording. Kept private: a test only ever
/// touches it through [`SpanCapture`].
///
/// Stateless, and installed as the process-wide subscriber rather than per
/// thread. That distinction is load-bearing. `tracing` caches each
/// callsite's `Interest` globally, so a thread-local subscriber lets a
/// thread that has none decide a callsite is never of interest, after which
/// a concurrently running test captures nothing at all and fails claiming
/// the span was never opened. One always-listening subscriber makes that
/// verdict stable; which thread is recording is then decided here, where it
/// costs nothing.
struct CaptureLayer;

impl CaptureLayer {
    /// Run `record` against this thread's active capture, if it has one.
    fn with_active<R>(record: impl FnOnce(&mut Recorded) -> R) -> Option<R> {
        ACTIVE_CAPTURE.with(|active| {
            let active = active.borrow();
            let recorded = active.as_ref()?;
            let mut recorded = recorded.lock().unwrap_or_else(PoisonError::into_inner);
            Some(record(&mut recorded))
        })
    }
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut visitor = FieldsVisitor::default();
        attrs.record(&mut visitor);
        let name = attrs.metadata().name();

        let Some(index) = Self::with_active(|recorded| {
            let index = recorded.spans.len();
            recorded.spans.push(CapturedSpan {
                name,
                fields: visitor.0,
            });
            index
        }) else {
            return;
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
        Self::with_active(|recorded| {
            if let Some(span) = recorded.spans.get_mut(index) {
                span.fields.extend(visitor.0);
            }
        });
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut visitor = FieldsVisitor::default();
        event.record(&mut visitor);
        let span_name = ctx
            .event_span(event)
            .map(|span_ref| span_ref.metadata().name());

        Self::with_active(|recorded| {
            recorded.events.push(CapturedEvent {
                level: *event.metadata().level(),
                span_name,
                fields: visitor.0,
            });
        });
    }
}

/// Stops this thread's capture when dropped, so one test never records what
/// the next one emits on the same thread.
pub(crate) struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        ACTIVE_CAPTURE.with(|active| *active.borrow_mut() = None);
    }
}

/// Handle onto every span and event this thread captured while its
/// [`CaptureGuard`] was alive.
///
/// `#[tokio::test]` is single-threaded by default, so a capture covers
/// everything its test drives on that runtime. A `multi_thread` test would
/// not be captured faithfully: recording is per thread, so anything emitted
/// from another worker thread escapes it.
pub(crate) struct SpanCapture {
    recorded: Arc<Mutex<Recorded>>,
}

impl SpanCapture {
    /// Start capturing on the current thread, and return a handle onto what
    /// it records alongside the guard that stops it.
    ///
    /// The subscriber itself is process-wide and installed at most once, on
    /// the first call: see [`CaptureLayer`] for why anything thread-local
    /// there would make tests lose spans under a parallel runner.
    pub(crate) fn install() -> (Self, CaptureGuard) {
        GLOBAL_SUBSCRIBER.get_or_init(|| {
            let subscriber = Registry::default().with(CaptureLayer);
            // Only ever fails if something else claimed the global
            // subscriber first, in which case this binary's captures would
            // stay empty and say so through failing assertions.
            let _ = tracing::subscriber::set_global_default(subscriber);
            tracing::callsite::rebuild_interest_cache();
        });

        let recorded = Arc::new(Mutex::new(Recorded::default()));
        ACTIVE_CAPTURE.with(|active| *active.borrow_mut() = Some(Arc::clone(&recorded)));
        (Self { recorded }, CaptureGuard)
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
