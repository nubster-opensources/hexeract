//! End-to-end integration tests using the real `#[handler]` macro
//! against a real `Mediator` and `verify_handlers()`.

#![allow(
    clippy::unused_async,
    reason = "test handlers stay `async` to match the trait the macro expands to"
)]

use hexeract_core::{Command, HandlerContext, HexeractError, Notification, Query};
use hexeract_macros::handler;
use hexeract_mediator::{HandlersVerificationError, MediatorBuilder};

struct CreateUser {
    name: String,
}

impl Command for CreateUser {
    type Output = u64;
}

struct UserService;

#[handler(command)]
impl UserService {
    async fn handle(&self, cmd: CreateUser, _ctx: &HandlerContext) -> Result<u64, HexeractError> {
        Ok(u64::try_from(cmd.name.len()).unwrap_or(u64::MAX))
    }
}

struct GetUserCount;

impl Query for GetUserCount {
    type Output = u32;
}

#[handler(query)]
async fn get_user_count(_q: GetUserCount, _ctx: &HandlerContext) -> Result<u32, HexeractError> {
    Ok(7)
}

#[derive(Clone)]
struct UserCreated {
    #[allow(dead_code, reason = "carried for shape parity with real notifications")]
    id: u64,
}

impl Notification for UserCreated {}

#[handler(notification)]
async fn audit_user_created(_n: UserCreated, _ctx: &HandlerContext) -> Result<(), HexeractError> {
    Ok(())
}

#[tokio::test]
async fn handler_macro_generates_command_trait_impl_dispatched_through_mediator() {
    let mediator = MediatorBuilder::new()
        .register_command_handler::<CreateUser, _>(UserService)
        .register_query_handler::<GetUserCount, _>(GetUserCountHandler)
        .register_notification_handler::<UserCreated, _>(AuditUserCreatedHandler)
        .build()
        .expect("build must succeed");

    let count = mediator
        .send(CreateUser {
            name: "pierrick".into(),
        })
        .await
        .expect("dispatch must succeed");
    assert_eq!(count, 8);

    let n = mediator
        .query(GetUserCount)
        .await
        .expect("query must succeed");
    assert_eq!(n, 7);

    mediator
        .publish(UserCreated { id: 1 })
        .await
        .expect("publish must succeed");
}

#[test]
fn verify_handlers_passes_when_all_handler_macros_have_matching_registrations() {
    MediatorBuilder::new()
        .register_command_handler::<CreateUser, _>(UserService)
        .register_query_handler::<GetUserCount, _>(GetUserCountHandler)
        .register_notification_handler::<UserCreated, _>(AuditUserCreatedHandler)
        .verify_handlers()
        .expect("every declared handler is registered");
}

#[test]
fn verify_handlers_reports_missing_when_a_handler_macro_is_unregistered() {
    let result = MediatorBuilder::new()
        .register_command_handler::<CreateUser, _>(UserService)
        // GetUserCountHandler and AuditUserCreatedHandler intentionally
        // left unregistered to trigger a verification failure.
        .verify_handlers();
    let Err(HandlersVerificationError::Missing { missing }) = result else {
        panic!("expected Missing variant");
    };
    assert!(
        missing
            .iter()
            .any(|m| m.handler_type_name.contains("GetUserCount"))
    );
    assert!(
        missing
            .iter()
            .any(|m| m.handler_type_name.contains("AuditUserCreated"))
    );
}
