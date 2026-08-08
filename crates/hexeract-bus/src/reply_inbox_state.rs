//! Shared state holding the name of the reply inbox a
//! [`crate::RequestClient`] stamps as the return address of every
//! request.

/// Whether the client currently has a usable reply inbox.
///
/// Read by [`crate::RequestClient`] on every call it registers, right
/// after registering with the [`crate::RequestRegistry`], never before:
/// see the [`crate::RequestClient::new`] docs for why that order is what
/// makes this state's guarantee hold. A call observing
/// [`Self::Reconnecting`] fails fast with [`crate::RequestError::Transport`]
/// instead of publishing toward an inbox that no longer exists.
///
/// This does not guarantee that no request is ever published toward a
/// dead inbox: a call that read [`Self::Ready`] and already published
/// just before the connection dropped still loses that message, which is
/// inherent to any drop, not a defect this state closes. What it
/// guarantees, provided the transport supervisor that owns this state
/// marks it [`Self::Reconnecting`] before it drains the
/// [`crate::RequestRegistry`], is narrower and exact: from the moment the
/// supervisor observes the drop, no caller waits out its full timeout
/// because of a dead inbox.
///
/// That qualifier is part of the guarantee, not a caveat on it. This
/// state says nothing about how long observing the drop takes, and the
/// supervisor observes it only when its consumer stream ends. A broker
/// that closes the socket, or a peer that sends an RST, ends that stream
/// at once. A network partition that silently swallows packets does not:
/// the drop surfaces only when the AMQP heartbeat expires. A caller whose
/// timeout is shorter than that interval still waits it out in full,
/// exactly as it would have without this state.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyInboxState {
    /// A declared inbox is serving replies.
    Ready(String),
    /// The connection dropped; no new call may be published until a
    /// fresh inbox exists.
    Reconnecting,
}
