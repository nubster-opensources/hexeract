//! End-to-end example of a stateful [`CommandHandler`].
//!
//! Run with `cargo run --example 01_command_handler -p hexeract-examples`.

use hexeract::core::{
    Command, CommandHandler, CorrelationId, HandlerContext, HexeractError, MessageId,
};
use std::sync::Mutex;
use std::sync::PoisonError;
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
        self.created
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
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
        self.created
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, cmd.email));
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
    tracing::info!(%id, "created user");

    let ctx2 = HandlerContext::new(MessageId::new(), CorrelationId::new());
    let result = repo
        .handle(
            CreateUser {
                email: String::new(),
            },
            &ctx2,
        )
        .await;
    let Err(err) = result else {
        return Err("empty email should have been rejected".into());
    };
    tracing::info!(%err, "expected failure");

    tracing::info!(total = repo.count(), "total users created");
    Ok(())
}
