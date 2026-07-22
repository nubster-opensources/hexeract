use crate::Message;

/// A message that expects a single typed reply.
///
/// `Request` layers the request-reply pattern on top of [`Message`]: the
/// associated [`Request::Reply`] type names the message the responder
/// sends back, correlated by identifier.
pub trait Request: Message {
    /// The reply message produced by the responder for this request.
    type Reply: Message;

    /// Routing key this request is published to.
    ///
    /// Defaults to [`Message::MESSAGE_TYPE`], which is the right answer when
    /// exactly one responder owns the type. Override it when the request must
    /// reach a dedicated queue rather than the type's usual destination.
    const DESTINATION: &'static str = Self::MESSAGE_TYPE;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct GetBalance {
        account_id: uuid::Uuid,
    }
    impl Message for GetBalance {
        const MESSAGE_TYPE: &'static str = "accounts.get_balance";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Balance {
        cents: u64,
    }
    impl Message for Balance {
        const MESSAGE_TYPE: &'static str = "accounts.balance";
    }

    impl Request for GetBalance {
        type Reply = Balance;
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct ReindexShard {
        shard: u16,
    }
    impl Message for ReindexShard {
        const MESSAGE_TYPE: &'static str = "search.reindex_shard";
    }
    impl Request for ReindexShard {
        type Reply = Balance;
        const DESTINATION: &'static str = "search.commands";
    }

    #[test]
    fn request_names_its_reply_type() {
        assert_eq!(
            <GetBalance as Request>::Reply::MESSAGE_TYPE,
            "accounts.balance"
        );
        assert_eq!(GetBalance::MESSAGE_TYPE, "accounts.get_balance");
    }

    #[test]
    fn destination_defaults_to_the_message_type() {
        assert_eq!(
            <GetBalance as Request>::DESTINATION,
            GetBalance::MESSAGE_TYPE
        );
    }

    #[test]
    fn destination_can_be_overridden_by_the_contract() {
        assert_eq!(<ReindexShard as Request>::DESTINATION, "search.commands");
    }
}
