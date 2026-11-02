//! The `bus` facade feature must re-export the symbols named below through
//! the `hexeract` umbrella crate. A compilation is the whole check: removing a
//! re-export, or the feature that gates it, breaks this file.

#![cfg(feature = "bus")]

use hexeract_umbrella_tests::resolve_types;

#[test]
fn bus_feature_reexports_its_symbols() {
    resolve_types!(
        hexeract::bus::BusError,
        hexeract::bus::BusEnvelope,
        hexeract::bus::Exchange,
        hexeract::bus::ExchangeKind,
        hexeract::bus::Queue,
        hexeract::bus::Deadline,
    );
}
