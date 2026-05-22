/// A message expressing the intent to mutate state.
///
/// Each command has exactly one [`CommandHandler`](crate::handler::CommandHandler)
/// registered with the [`Mediator`](https://docs.rs/hexeract). The handler
/// returns the associated [`Command::Output`] type.
///
/// Implementors should be cheap to clone or move, since the framework owns
/// the command after dispatch.
///
/// # Example
///
/// ```
/// use hexeract_core::Command;
/// use uuid::Uuid;
///
/// struct RegisterUser {
///     pub email: String,
/// }
///
/// impl Command for RegisterUser {
///     type Output = Uuid;
/// }
/// ```
pub trait Command: Send + Sync + 'static {
    /// The result type returned by the handler upon successful execution.
    type Output: Send + Sync + 'static;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyCommand;
    impl Command for DummyCommand {
        type Output = u64;
    }

    fn assert_command<C: Command>() {}

    #[test]
    fn dummy_command_implements_trait() {
        assert_command::<DummyCommand>();
    }
}
