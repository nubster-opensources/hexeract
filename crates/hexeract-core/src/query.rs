/// A read-only message asking for information.
///
/// Each query has exactly one [`QueryHandler`](crate::handler::QueryHandler).
/// Unlike a [`Command`](crate::command::Command), a query must not mutate
/// observable state. The framework treats both with the same machinery, but
/// the distinction is encouraged for clarity.
///
/// # Example
///
/// ```
/// use hexeract_core::Query;
/// use uuid::Uuid;
///
/// struct FindUserById {
///     pub id: Uuid,
/// }
///
/// struct User {
///     pub id: Uuid,
///     pub email: String,
/// }
///
/// impl Query for FindUserById {
///     type Output = Option<User>;
/// }
/// ```
pub trait Query: Send + Sync + 'static {
    /// The result type returned by the handler upon successful execution.
    type Output: Send + Sync + 'static;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyQuery;
    impl Query for DummyQuery {
        type Output = String;
    }

    fn assert_query<Q: Query>() {}

    #[test]
    fn dummy_query_implements_trait() {
        assert_query::<DummyQuery>();
    }
}
