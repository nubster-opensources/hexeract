//! The `bus-rabbitmq` facade feature must re-export the symbols named below through
//! the `hexeract` umbrella crate. A compilation is the whole check: removing a
//! re-export, or the feature that gates it, breaks this file.

use hexeract_umbrella_tests::resolve_types;

#[test]
fn bus_rabbitmq_feature_reexports_its_symbols() {
    resolve_types!(
        hexeract::bus_rabbitmq::RabbitMqConnection,
        hexeract::bus_rabbitmq::RabbitMqConnectionConfig,
        hexeract::bus_rabbitmq::RabbitMqTransport,
        hexeract::bus_rabbitmq::RabbitMqTransportConfig,
        hexeract::bus_rabbitmq::RabbitMqWorkerBuilder,
        hexeract::bus_rabbitmq::RabbitMqRequestClientConfig,
        hexeract::bus_rabbitmq::AmqpMetadataLimits,
        hexeract::bus_rabbitmq::ChannelPool,
    );
}
