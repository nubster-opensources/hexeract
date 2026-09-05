//! End-to-end authenticity and integrity for envelopes crossing the bus.
//!
//! TLS protects one connection hop. It does not authenticate an envelope
//! across relays, and it does not protect against a legitimate broker
//! principal turned hostile. This module signs a deterministic canonical
//! representation of an envelope with an Ed25519 key, so a consumer can
//! establish who published a message and that nothing in it changed on the
//! way, before any typed decoding happens.
//!
//! Verification is required by default. An application that deliberately
//! runs without it opts out explicitly, the way plaintext transport is opted
//! into.

/// Errors raised while signing or verifying an envelope.
pub mod error;
/// Wire constants of the envelope security protocol.
pub mod protocol;
