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
//! Backend implementations live in companion crates such as
//! `hexeract-bus-rabbitmq`.

/// Rendezvous point correlating replies with their in-flight request.
pub mod correlation;
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
/// Wire contract for the request-reply error path.
pub mod reply_status;
/// Trait for messages that expect a single typed reply.
pub mod request;
/// Generic request-reply client built on top of a [`Transport`].
pub mod request_client;
/// Errors observed by the caller of a request-reply round trip.
pub mod request_error;
/// Strongly-typed topology declarations shared by transports.
pub mod topology;
/// Backend-agnostic publish contract implemented by bus backends.
pub mod transport;

pub use correlation::CorrelationRegistry;
pub use correlation::PendingReply;
pub use envelope::BusEnvelope;
pub use error::BusError;
pub use handler::BoxFuture;
pub use handler::ErasedHandler;
pub use handler::Handler;
pub use handler::TypedHandler;
pub use message::Message;
pub use raw_publish::RawBusPublish;
pub use reply_status::REPLY_ERROR_MESSAGE_TYPE;
pub use reply_status::REPLY_STATUS_ERROR;
pub use reply_status::REPLY_STATUS_HEADER;
pub use reply_status::REPLY_STATUS_OK;
pub use reply_status::RemoteErrorPayload;
pub use request::Request;
pub use request_client::RequestClient;
pub use request_error::RequestError;
pub use topology::Binding;
pub use topology::Exchange;
pub use topology::ExchangeKind;
pub use topology::Queue;
pub use topology::RoutingKey;
pub use transport::Transport;
