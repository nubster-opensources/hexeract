//! Message bus primitives for the Hexeract messaging framework.
//!
//! This crate ships the backend-agnostic primitives the rest of the bus
//! ecosystem composes on top of: the [`Message`] marker trait, the
//! in-flight [`BusEnvelope`] carried across the wire, the unified
//! [`BusError`] type, the strongly-typed topology declarations
//! ([`Exchange`], [`Queue`], [`Binding`], [`RoutingKey`]), the
//! backend-agnostic [`Transport`] publish contract and the
//! consumer-side dispatch primitives ([`Handler`], [`ErasedHandler`],
//! [`TypedHandler`]).
//!
//! It also ships the backend-agnostic request-reply primitives: the
//! [`Request`] trait naming a typed reply, [`RequestClient`] and
//! [`RequestError`] on the caller side, [`RequestHandler`] and
//! [`RepliedHandler`] on the responder side, and the [`RequestRegistry`]
//! (with its RAII [`PendingReply`] guard) routing a reply back to its
//! in-flight request by request identity. The wire contract between the two sides
//! ([`REPLY_STATUS_HEADER`], [`REPLY_STATUS_OK`], [`REPLY_STATUS_ERROR`],
//! [`REPLY_ERROR_MESSAGE_TYPE`], [`RemoteErrorPayload`]) is documented in
//! full in `docs/concepts/request-reply.md` in the workspace.
//!
//! Backend implementations live in companion crates such as
//! `hexeract-bus-rabbitmq`.

/// In-flight representation of a message crossing the bus.
pub mod envelope;
/// Errors raised by the bus primitives, transports and workers.
pub mod error;
/// Consumer-side dispatch primitives invoked by the bus worker.
pub mod handler;
/// Marker trait for domain messages that flow through the bus.
pub mod message;
/// Contract for publishing a raw message with a caller-supplied id.
pub mod raw_publish;
/// Sanitized failure payload published on the request-reply error channel.
pub mod remote_error;
/// Adapter that erases a [`RequestHandler`] into an [`ErasedHandler`].
pub mod replied_handler;
/// Wire contract for the request-reply error path.
pub mod reply_status;
/// Trait for messages that expect a single typed reply.
pub mod request;
/// Generic request-reply client built on top of a [`Transport`].
pub mod request_client;
/// Errors observed by the caller of a request-reply round trip.
pub mod request_error;
/// Responder-side handler that produces a typed reply for a [`Request`].
pub mod request_handler;
/// Rendezvous point between request callers and reply deliveries, keyed by
/// request identity.
pub mod request_registry;
/// Wire constants of the request-reply protocol.
pub mod rpc_protocol;
/// Strongly-typed topology declarations shared by transports.
pub mod topology;
/// Backend-agnostic publish contract implemented by bus backends.
pub mod transport;

pub use envelope::BusEnvelope;
pub use error::BusError;
pub use handler::BoxFuture;
pub use handler::ErasedHandler;
pub use handler::Handler;
pub use handler::TypedHandler;
pub use message::Message;
pub use raw_publish::RawBusPublish;
pub use remote_error::RemoteErrorPayload;
pub use remote_error::RemoteErrorType;
pub use replied_handler::RepliedHandler;
pub use reply_status::REPLY_ERROR_MESSAGE_TYPE;
pub use reply_status::REPLY_STATUS_ERROR;
pub use reply_status::REPLY_STATUS_HEADER;
pub use reply_status::REPLY_STATUS_OK;
pub use request::Request;
pub use request_client::RequestClient;
pub use request_error::RequestError;
pub use request_handler::RequestHandler;
pub use request_registry::PendingReply;
pub use request_registry::RequestRegistry;
pub use rpc_protocol::PROTOCOL_VERSION;
pub use rpc_protocol::PROTOCOL_VERSION_HEADER;
pub use rpc_protocol::REQUEST_ID_HEADER;
pub use topology::Binding;
pub use topology::Exchange;
pub use topology::ExchangeKind;
pub use topology::Queue;
pub use topology::RoutingKey;
pub use transport::Transport;
