//! End-to-end example of a stateful [`CommandHandler`].
//!
//! Run with `cargo run --example 01_command_handler -p hexeract-core`.

use hexeract_core::{
    Command, CommandHandler, CorrelationId, HandlerContext, HexeractError, MessageId,
};
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Debug)]
struct CreateUser {
    email: String,
}

impl Command for CreateUser {
    type Output = Uuid;
}

#[derive(Debug, thiserror::Error)]
enum UserServiceError {
    #[error("email cannot be empty")]
    EmptyEmail,
}

impl From<UserServiceError> for HexeractError {
    fn from(value: UserServiceError) -> Self {
        Self::handler_failed(value)
    }
}

struct InMemoryUserRepo {
    created: Mutex<Vec<(Uuid, String)>>,
}

impl InMemoryUserRepo {
    fn new() -> Self {
        Self {
            created: Mutex::new(Vec::new()),
        }
    }

    fn count(&self) -> usize {
        self.created.lock().expect("poisoned").len()
    }
}

impl CommandHandler<CreateUser> for InMemoryUserRepo {
    type Error = UserServiceError;

    async fn handle(&self, cmd: CreateUser, ctx: &HandlerContext) -> Result<Uuid, Self::Error> {
        if cmd.email.is_empty() {
            return Err(UserServiceError::EmptyEmail);
        }
        let id = Uuid::new_v4();
        tracing::info!(
            message_id = %ctx.message_id,
            correlation_id = %ctx.correlation_id,
            user_id = %id,
            email = %cmd.email,
            "user created"
        );
        self.created.lock().expect("poisoned").push((id, cmd.email));
        Ok(id)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let repo = InMemoryUserRepo::new();

    let ctx = HandlerContext::new(MessageId::new(), CorrelationId::new());
    let id = repo
        .handle(
            CreateUser {
                email: "alice@example.com".into(),
            },
            &ctx,
        )
        .await?;
    println!("created user with id {id}");

    let ctx2 = HandlerContext::new(MessageId::new(), CorrelationId::new());
    let err = repo
        .handle(
            CreateUser {
                email: String::new(),
            },
            &ctx2,
        )
        .await
        .expect_err("empty email should be rejected");
    println!("expected failure: {err}");

    println!("total users created: {}", repo.count());
    Ok(())
}
