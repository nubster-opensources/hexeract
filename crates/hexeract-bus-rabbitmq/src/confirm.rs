//! Publisher confirm mapping shared by the transport and the worker.

use hexeract_bus::BusError;
use lapin::Confirmation;

/// Map a broker confirmation to a publish outcome.
///
/// Only an ack without a returned message proves the message is safely
/// queued. An ack carrying a returned message means the mandatory
/// publish was unroutable, a nack means the broker could not store the
/// message, and `NotRequested` means publisher confirms were never
/// enabled on the channel: all three map to an error.
///
/// `context` names the publish being confirmed (for example
/// `"publish"` or `"dead-letter publish"`) and prefixes the error
/// messages; `routing_key` identifies the destination in
/// [`BusError::Unroutable`].
pub(crate) fn confirmation_to_result(
    confirmation: Confirmation,
    context: &'static str,
    routing_key: &str,
) -> Result<(), BusError> {
    match confirmation {
        Confirmation::Ack(None) => Ok(()),
        Confirmation::Ack(Some(returned)) | Confirmation::Nack(Some(returned)) => {
            Err(BusError::Unroutable {
                routing_key: routing_key.to_owned(),
                reply_text: returned.reply_text.to_string(),
                reply_code: returned.reply_code,
            })
        }
        Confirmation::Nack(None) => Err(BusError::Transport(
            format!("{context} nacked by the broker").into(),
        )),
        Confirmation::NotRequested => Err(BusError::Internal(format!(
            "{context} completed without publisher confirms enabled"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmation_to_result_accepts_unreturned_ack() {
        let result = confirmation_to_result(Confirmation::Ack(None), "publish", "orders.placed");
        assert!(result.is_ok());
    }

    #[test]
    fn confirmation_to_result_rejects_nack_with_context() {
        let err = confirmation_to_result(Confirmation::Nack(None), "dead-letter publish", "dlq")
            .unwrap_err();
        assert!(matches!(err, BusError::Transport(_)));
        let source = std::error::Error::source(&err).expect("source must be set");
        assert!(source.to_string().contains("dead-letter publish"));
    }

    #[test]
    fn confirmation_to_result_rejects_unconfirmed_channel() {
        let err = confirmation_to_result(Confirmation::NotRequested, "publish", "orders.placed")
            .unwrap_err();
        assert!(matches!(err, BusError::Internal(_)));
    }
}
