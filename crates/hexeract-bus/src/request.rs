use crate::Message;

/// A message that expects a single typed reply.
///
/// `Request` layers the request-reply pattern on top of [`Message`]: the
/// associated [`Request::Reply`] type names the message the responder
/// sends back, correlated by identifier.
pub trait Request: Message {
    /// The reply message produced by the responder for this request.
    type Reply: Message;
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

    #[test]
    fn request_names_its_reply_type() {
        assert_eq!(
            <GetBalance as Request>::Reply::MESSAGE_TYPE,
            "accounts.balance"
        );
        assert_eq!(GetBalance::MESSAGE_TYPE, "accounts.get_balance");
    }
}
