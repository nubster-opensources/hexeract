//! Integration tests for the in-process mediator.
//!
//! These tests cover the acceptance criteria that span multiple modules,
//! in particular middleware composition (AC8), cross-task sharing through
//! `Mediator::clone` (AC9) and end-to-end dispatch from middleware to
//! handler and back (AC10).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use hexeract_core::{
    BoxOutput, Command, CommandHandler, CorrelationId, HandlerContext, HexeractError,
    MessageEnvelope, Middleware, Next, Notification, NotificationHandler, Query, QueryHandler,
};
use hexeract_mediator::{Mediator, MediatorBuilder};

struct GreetUser {
    name: String,
}

impl Command for GreetUser {
    type Output = String;
}

struct GreetHandler;

impl CommandHandler<GreetUser> for GreetHandler {
    type Error = HexeractError;

    async fn handle(&self, cmd: GreetUser, _ctx: &HandlerContext) -> Result<String, Self::Error> {
        Ok(format!("hello {}", cmd.name))
    }
}

struct GetAnswer;

impl Query for GetAnswer {
    type Output = u32;
}

struct AnswerHandler;

impl QueryHandler<GetAnswer> for AnswerHandler {
    type Error = HexeractError;

    async fn handle(&self, _q: GetAnswer, _ctx: &HandlerContext) -> Result<u32, Self::Error> {
        Ok(42)
    }
}

#[derive(Clone)]
struct UserCreated {
    id: u32,
}

impl Notification for UserCreated {}

struct RecordingNotifHandler {
    seen: Arc<Mutex<Vec<u32>>>,
}

impl NotificationHandler<UserCreated> for RecordingNotifHandler {
    type Error = HexeractError;

    async fn handle(
        &self,
        notif: Arc<UserCreated>,
        _ctx: &HandlerContext,
    ) -> Result<(), Self::Error> {
        self.seen
            .lock()
            .expect("recorder mutex poisoned")
            .push(notif.id);
        Ok(())
    }
}

#[derive(Clone)]
struct CountingMiddleware {
    before: Arc<AtomicUsize>,
    after: Arc<AtomicUsize>,
}

impl CountingMiddleware {
    fn new() -> Self {
        Self {
            before: Arc::new(AtomicUsize::new(0)),
            after: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn before(&self) -> usize {
        self.before.load(Ordering::SeqCst)
    }

    fn after(&self) -> usize {
        self.after.load(Ordering::SeqCst)
    }
}

impl Middleware for CountingMiddleware {
    async fn execute(
        &self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
        next: Next,
    ) -> Result<BoxOutput, HexeractError> {
        self.before.fetch_add(1, Ordering::SeqCst);
        let result = next.run(envelope, ctx).await;
        self.after.fetch_add(1, Ordering::SeqCst);
        result
    }
}

fn build_mediator_with_middleware(mw: CountingMiddleware) -> (Mediator, Arc<Mutex<Vec<u32>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mediator = MediatorBuilder::new()
        .with_middleware(mw)
        .register_command_handler::<GreetUser, _>(GreetHandler)
        .register_query_handler::<GetAnswer, _>(AnswerHandler)
        .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
            seen: Arc::clone(&seen),
        })
        .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
            seen: Arc::clone(&seen),
        })
        .build()
        .expect("build must succeed");
    (mediator, seen)
}

// AC8: middlewares wrap every dispatch channel (send, query, publish).
#[tokio::test]
async fn middleware_wraps_send_and_query_and_publish() {
    let mw = CountingMiddleware::new();
    let (mediator, _seen) = build_mediator_with_middleware(mw.clone());

    let greeting = mediator
        .send(GreetUser {
            name: "world".into(),
        })
        .await
        .expect("send must succeed");
    assert_eq!(greeting, "hello world");

    let answer = mediator.query(GetAnswer).await.expect("query must succeed");
    assert_eq!(answer, 42);

    mediator
        .publish(UserCreated { id: 1 })
        .await
        .expect("publish must succeed");

    // 1 send + 1 query + 2 notification handlers = 4 dispatches through
    // the middleware chain, observed both on entry and on exit.
    assert_eq!(mw.before(), 4, "middleware must run before each dispatch");
    assert_eq!(mw.after(), 4, "middleware must resume after each dispatch");
}

// AC9: Mediator::clone is cheap and shares the registry across tasks.
#[tokio::test]
async fn mediator_is_clone_and_shareable_across_tasks() {
    let mw = CountingMiddleware::new();
    let (mediator, _seen) = build_mediator_with_middleware(mw.clone());

    let mut handles = Vec::new();
    for i in 0..8_u32 {
        let mediator = mediator.clone();
        handles.push(tokio::spawn(async move {
            let answer = mediator.query(GetAnswer).await.expect("query must succeed");
            let greeting = mediator
                .send(GreetUser {
                    name: format!("task-{i}"),
                })
                .await
                .expect("send must succeed");
            mediator
                .publish(UserCreated { id: i })
                .await
                .expect("publish must succeed");
            (answer, greeting)
        }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let (answer, greeting) = handle.await.expect("task must not panic");
        assert_eq!(answer, 42);
        assert_eq!(greeting, format!("hello task-{i}"));
    }

    // 8 tasks each ran 1 send + 1 query + 2 notification handlers.
    assert_eq!(mw.before(), 8 * 4);
    assert_eq!(mw.after(), 8 * 4);
}

// AC10 end-to-end: middleware observes the envelope, handler returns a
// typed output, the value flows back through the chain unchanged.
#[tokio::test]
async fn middleware_handler_pipeline_end_to_end() {
    struct EnvelopeInspector {
        observed: Arc<Mutex<Vec<String>>>,
    }

    impl Middleware for EnvelopeInspector {
        async fn execute(
            &self,
            envelope: &MessageEnvelope,
            ctx: &HandlerContext,
            next: Next,
        ) -> Result<BoxOutput, HexeractError> {
            self.observed
                .lock()
                .expect("inspector mutex poisoned")
                .push(envelope.type_name().to_string());
            next.run(envelope, ctx).await
        }
    }

    let observed = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mediator = MediatorBuilder::new()
        .with_middleware(EnvelopeInspector {
            observed: Arc::clone(&observed),
        })
        .register_command_handler::<GreetUser, _>(GreetHandler)
        .register_query_handler::<GetAnswer, _>(AnswerHandler)
        .register_notification_handler::<UserCreated, _>(RecordingNotifHandler {
            seen: Arc::clone(&seen),
        })
        .build()
        .expect("build must succeed");

    let greeting = mediator
        .send(GreetUser {
            name: "pierrick".into(),
        })
        .await
        .expect("send must succeed");
    let answer = mediator.query(GetAnswer).await.expect("query must succeed");
    mediator
        .publish(UserCreated { id: 7 })
        .await
        .expect("publish must succeed");

    assert_eq!(greeting, "hello pierrick");
    assert_eq!(answer, 42);
    assert_eq!(*seen.lock().unwrap(), vec![7]);

    let observed = observed.lock().unwrap().clone();
    assert_eq!(observed.len(), 3);
    assert!(observed[0].ends_with("::GreetUser"));
    assert!(observed[1].ends_with("::GetAnswer"));
    assert!(observed[2].ends_with("::UserCreated"));
}

// ── Correlation-id propagation (issue #227) ──────────────────────────────────

struct CapturingCommandHandler {
    captured: Arc<Mutex<Option<CorrelationId>>>,
}

impl CommandHandler<GreetUser> for CapturingCommandHandler {
    type Error = HexeractError;

    async fn handle(&self, cmd: GreetUser, ctx: &HandlerContext) -> Result<String, Self::Error> {
        *self.captured.lock().expect("mutex poisoned") = Some(ctx.correlation_id);
        Ok(format!("hello {}", cmd.name))
    }
}

struct CapturingQueryHandler {
    captured: Arc<Mutex<Option<CorrelationId>>>,
}

impl QueryHandler<GetAnswer> for CapturingQueryHandler {
    type Error = HexeractError;

    async fn handle(&self, _q: GetAnswer, ctx: &HandlerContext) -> Result<u32, Self::Error> {
        *self.captured.lock().expect("mutex poisoned") = Some(ctx.correlation_id);
        Ok(42)
    }
}

struct CapturingNotifHandler {
    captured: Arc<Mutex<Vec<CorrelationId>>>,
}

impl NotificationHandler<UserCreated> for CapturingNotifHandler {
    type Error = HexeractError;

    async fn handle(&self, _n: Arc<UserCreated>, ctx: &HandlerContext) -> Result<(), Self::Error> {
        self.captured
            .lock()
            .expect("mutex poisoned")
            .push(ctx.correlation_id);
        Ok(())
    }
}

// AC-corr-1: send_with_correlation_id forwards the supplied id to the handler.
#[tokio::test]
async fn send_with_correlation_id_propagates_to_handler() {
    let captured = Arc::new(Mutex::new(None));
    let mediator = MediatorBuilder::new()
        .register_command_handler::<GreetUser, _>(CapturingCommandHandler {
            captured: Arc::clone(&captured),
        })
        .build()
        .expect("build must succeed");

    let known_id = CorrelationId::new();
    mediator
        .send_with_correlation_id(
            GreetUser {
                name: "world".into(),
            },
            known_id,
        )
        .await
        .expect("send must succeed");

    let observed = captured.lock().unwrap().expect("handler must have run");
    assert_eq!(
        observed, known_id,
        "handler must receive the caller-supplied CorrelationId"
    );
}

// AC-corr-2: send without a supplied id still generates a fresh one (regression).
#[tokio::test]
async fn send_without_correlation_id_generates_a_fresh_one() {
    let captured = Arc::new(Mutex::new(None));
    let mediator = MediatorBuilder::new()
        .register_command_handler::<GreetUser, _>(CapturingCommandHandler {
            captured: Arc::clone(&captured),
        })
        .build()
        .expect("build must succeed");

    mediator
        .send(GreetUser {
            name: "world".into(),
        })
        .await
        .expect("send must succeed");

    assert!(
        captured.lock().unwrap().is_some(),
        "handler must still receive a correlation id"
    );
}

// AC-corr-3: query_with_correlation_id forwards the supplied id to the handler.
#[tokio::test]
async fn query_with_correlation_id_propagates_to_handler() {
    let captured = Arc::new(Mutex::new(None));
    let mediator = MediatorBuilder::new()
        .register_query_handler::<GetAnswer, _>(CapturingQueryHandler {
            captured: Arc::clone(&captured),
        })
        .build()
        .expect("build must succeed");

    let known_id = CorrelationId::new();
    mediator
        .query_with_correlation_id(GetAnswer, known_id)
        .await
        .expect("query must succeed");

    let observed = captured.lock().unwrap().expect("handler must have run");
    assert_eq!(
        observed, known_id,
        "handler must receive the caller-supplied CorrelationId"
    );
}

// AC-corr-4: publish_with_correlation_id fans out the same id to all handlers.
#[tokio::test]
async fn publish_with_correlation_id_propagates_same_id_to_all_handlers() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mediator = MediatorBuilder::new()
        .register_notification_handler::<UserCreated, _>(CapturingNotifHandler {
            captured: Arc::clone(&captured),
        })
        .register_notification_handler::<UserCreated, _>(CapturingNotifHandler {
            captured: Arc::clone(&captured),
        })
        .build()
        .expect("build must succeed");

    let known_id = CorrelationId::new();
    mediator
        .publish_with_correlation_id(UserCreated { id: 1 }, known_id)
        .await
        .expect("publish must succeed");

    let observed = captured.lock().unwrap().clone();
    assert_eq!(observed.len(), 2, "both handlers must have run");
    assert!(
        observed.iter().all(|id| *id == known_id),
        "all handlers must receive the same caller-supplied CorrelationId"
    );
}

// Helper for AC-corr-5: a command handler that re-dispatches a notification
// through the same mediator, forwarding its inbound correlation id.
struct ChainedCommandHandler {
    mediator: Arc<std::sync::OnceLock<Mediator>>,
}

impl CommandHandler<GreetUser> for ChainedCommandHandler {
    type Error = HexeractError;

    async fn handle(&self, cmd: GreetUser, ctx: &HandlerContext) -> Result<String, Self::Error> {
        let mediator = self
            .mediator
            .get()
            .expect("mediator must be set before dispatch");
        // Forward the inbound correlation id to the follow-up notification.
        mediator
            .publish_with_correlation_id(UserCreated { id: 42 }, ctx.correlation_id)
            .await
            .expect("nested publish must succeed");
        Ok(format!("hello {}", cmd.name))
    }
}

// AC-corr-5: the causal chain can be threaded end-to-end through send -> publish.
#[tokio::test]
async fn causal_chain_can_be_threaded_from_command_handler_to_notification() {
    // This simulates the documented production-checklist pattern: a command
    // handler receives `ctx.correlation_id` and forwards it when publishing a
    // follow-up notification, keeping the causal chain intact.
    let notif_ids = Arc::new(Mutex::new(Vec::new()));
    let notif_ids_for_mediator = Arc::clone(&notif_ids);

    let mediator_cell: Arc<std::sync::OnceLock<Mediator>> = Arc::new(std::sync::OnceLock::new());
    let mediator_cell_for_handler = Arc::clone(&mediator_cell);

    let mediator = MediatorBuilder::new()
        .register_command_handler::<GreetUser, _>(ChainedCommandHandler {
            mediator: mediator_cell_for_handler,
        })
        .register_notification_handler::<UserCreated, _>(CapturingNotifHandler {
            captured: notif_ids_for_mediator,
        })
        .build()
        .expect("build must succeed");

    mediator_cell.set(mediator.clone()).ok();

    let chain_root = CorrelationId::new();
    mediator
        .send_with_correlation_id(
            GreetUser {
                name: "chain".into(),
            },
            chain_root,
        )
        .await
        .expect("chained send must succeed");

    let observed = notif_ids.lock().unwrap().clone();
    assert_eq!(observed.len(), 1, "notification handler must have run");
    assert_eq!(
        observed[0], chain_root,
        "notification must carry the same CorrelationId as the originating command"
    );
}
