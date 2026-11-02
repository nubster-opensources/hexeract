//! The `core` facade feature must re-export the symbols named below through
//! the `hexeract` umbrella crate. A compilation is the whole check: removing a
//! re-export, or the feature that gates it, breaks this file.

#![cfg(feature = "core")]

use hexeract_umbrella_tests::resolve_types;

#[test]
fn core_feature_reexports_its_symbols() {
    resolve_types!(
        hexeract::core::HexeractError,
        hexeract::core::HandlerContext,
        hexeract::core::CorrelationId,
        hexeract::core::MessageId,
        hexeract::core::RequestId,
        hexeract::core::MessageEnvelope,
    );
}

struct Probe;

impl hexeract::core::Command for Probe {
    type Output = ();
}

fn requires_command<C: hexeract::core::Command>() {}

#[test]
fn core_feature_reexports_the_command_trait() {
    requires_command::<Probe>();
}
