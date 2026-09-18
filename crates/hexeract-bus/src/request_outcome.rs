//! Closed-set categorization of a finished request-reply call, and of the
//! transport-side cause behind a [`RequestOutcome::TransportFailed`] call.
//!
//! Grouping [`crate::RequestError`] into one of these variants is what a
//! metric label needs: a bounded, stable vocabulary rather than the open set
//! of error variants and their sources. [`RequestOutcome::of`] documents
//! which error groups into which outcome, and why.

use crate::RequestError;

/// Closed-set outcome of one request-reply call, as observed by the caller.
///
/// Built from a call's `Result<T, RequestError>` by [`RequestOutcome::of`],
/// and rendered for a metric label or a span field by
/// [`RequestOutcome::as_str`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestOutcome {
    /// The call returned a typed reply.
    Succeeded,
    /// The call did not complete, on either side of publication, within its
    /// one end-to-end local deadline.
    TimedOut,
    /// The responder reported a failure.
    RemoteFailed,
    /// The request could not be published, or the reply channel was lost.
    /// See [`TransportCause`] for which of the three sites.
    TransportFailed,
    /// The client refused the call before anything was published:
    /// [`RequestError::AtCapacity`], [`RequestError::Closed`], or a
    /// [`crate::ProtocolViolation::IdentityCollision`].
    Refused,
    /// Shutdown began after the request was admitted for publication, so
    /// whether the responder acted on it is unknown.
    PublicationUnknown,
    /// A reply arrived but violated the protocol, or could not be decoded
    /// into the expected reply type.
    InvalidReply,
    /// The call's future was dropped before [`crate::request_client`]
    /// established an outcome.
    Cancelled,
}

impl RequestOutcome {
    /// Categorize a finished call's result into its closed-set outcome.
    ///
    /// Inert in this revision: always reports [`Self::Succeeded`],
    /// regardless of `result`. The real mapping lands with the
    /// implementation, once the tests exercising each of its branches are
    /// in place and red.
    pub(crate) fn of<T>(result: &Result<T, RequestError>) -> Self {
        let _ = result;
        Self::Succeeded
    }

    /// Render this outcome as its stable metric-label spelling.
    ///
    /// Inert in this revision: always renders the empty string, regardless
    /// of `self`. The real, frozen spellings land with the implementation.
    pub(crate) fn as_str(self) -> &'static str {
        ""
    }
}

/// Transport-side cause behind a [`RequestOutcome::TransportFailed`] call.
///
/// Distinguishes the three sites [`crate::request_client`] observes a
/// transport failure at. None carries the underlying [`crate::BusError`] or
/// its `Display`: a broker's own words may name a host, a queue or a
/// credential, so they never reach a span or an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportCause {
    /// Publishing the request itself failed.
    PublicationFailed,
    /// The reply channel was lost while the call was waiting.
    ReplyChannelLost,
    /// The reply inbox was reconnecting when the call was made.
    ReplyInboxReconnecting,
}

impl TransportCause {
    /// Render this cause as its stable metric-label spelling.
    ///
    /// Inert in this revision: always renders the empty string, regardless
    /// of `self`.
    pub(crate) fn as_str(self) -> &'static str {
        ""
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::remote_error::RemoteErrorType;
    use crate::request_error::ProtocolViolation;
    use hexeract_core::RequestId;

    /// A `T` fixture that no test here needs to inspect: only the `Result`
    /// shape (`Ok`/`Err`) and the `RequestError` variant it carries matter to
    /// [`RequestOutcome::of`].
    #[derive(Debug)]
    struct Reply;

    /// Always `Ok` by construction: the point of this fixture is to give
    /// `of` a successful result to classify, so the wrap clippy flags here
    /// is exactly what the test needs.
    #[allow(clippy::unnecessary_wraps)]
    fn ok() -> Result<Reply, RequestError> {
        Ok(Reply)
    }

    fn err(error: RequestError) -> Result<Reply, RequestError> {
        Err(error)
    }

    /// `RequestOutcome::of` must cover every variant of
    /// [`RequestError`], with `Protocol(IdentityCollision)` routed to `Refused` and
    /// every other `Protocol` variant, alongside `Decode`, routed to
    /// `InvalidReply`.
    #[test]
    fn of_maps_every_request_error_variant_to_its_documented_outcome() {
        assert_eq!(RequestOutcome::of(&ok()), RequestOutcome::Succeeded);

        assert_eq!(
            RequestOutcome::of(&err(RequestError::Timeout {
                elapsed: Duration::from_millis(1),
                last_rejection: None,
                rejected_deliveries: 0,
            })),
            RequestOutcome::TimedOut
        );

        assert_eq!(
            RequestOutcome::of(&err(RequestError::Remote {
                error_type: RemoteErrorType::Internal,
                request_id: RequestId::new(),
            })),
            RequestOutcome::RemoteFailed
        );

        assert_eq!(
            RequestOutcome::of(&err(RequestError::Transport(crate::BusError::connection(
                "lost", true,
            )))),
            RequestOutcome::TransportFailed
        );

        assert_eq!(
            RequestOutcome::of(&err(RequestError::AtCapacity)),
            RequestOutcome::Refused
        );
        assert_eq!(
            RequestOutcome::of(&err(RequestError::Closed)),
            RequestOutcome::Refused
        );
        assert_eq!(
            RequestOutcome::of(&err(RequestError::Protocol(
                ProtocolViolation::IdentityCollision
            ))),
            RequestOutcome::Refused,
            "an identity collision at registration is a pre-publication refusal, not a reply that failed to validate"
        );
        assert_eq!(
            RequestOutcome::of(&err(RequestError::Encode(crate::BusError::Internal(
                "bad payload".to_owned()
            )))),
            RequestOutcome::Refused
        );

        assert_eq!(
            RequestOutcome::of(&err(RequestError::PublicationUnknown)),
            RequestOutcome::PublicationUnknown
        );

        assert_eq!(
            RequestOutcome::of(&err(RequestError::Protocol(
                ProtocolViolation::UnexpectedReplyType {
                    expected: "a",
                    actual: "b".to_owned(),
                }
            ))),
            RequestOutcome::InvalidReply,
            "a protocol violation other than an identity collision arrives after publication, so it groups with a reply that failed to decode"
        );
        assert_eq!(
            RequestOutcome::of(&err(RequestError::Decode(crate::BusError::Internal(
                "bad json".to_owned()
            )))),
            RequestOutcome::InvalidReply
        );
    }
}
