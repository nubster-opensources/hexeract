//! The `outbox` facade feature must re-export the symbols named below through
//! the `hexeract` umbrella crate. A compilation is the whole check: removing a
//! re-export, or the feature that gates it, breaks this file.

#![cfg(feature = "outbox")]

use hexeract_umbrella_tests::resolve_types;

#[test]
fn outbox_feature_reexports_its_symbols() {
    resolve_types!(
        hexeract::outbox::OutboxEnvelope,
        hexeract::outbox::OutboxError,
        hexeract::outbox::OutboxWorkerConfig,
    );
}
