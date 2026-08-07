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
/// [`crate::RequestRegistry`], is narrower and exact: no caller waits out
/// its full timeout because of a dead inbox.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyInboxState {
    /// A declared inbox is serving replies.
    Ready(String),
    /// The connection dropped; no new call may be published until a
    /// fresh inbox exists.
    Reconnecting,
}
