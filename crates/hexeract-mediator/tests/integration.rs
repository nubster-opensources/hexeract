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
    BoxOutput, Command, CommandHandler, HandlerContext, HexeractError, MessageEnvelope, Middleware,
    Next, Notification, NotificationHandler, Query, QueryHandler,
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
