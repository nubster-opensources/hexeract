//! Docker-backed proof that RabbitMQ clients can use a private CA and mTLS.

mod harness;

use std::time::Duration;

use hexeract_bus_rabbitmq::{
    RabbitMqConnectionConfig, RabbitMqRequestClientConfigBuilder, RabbitMqTransport,
    connect_request_client_with_config,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[ignore = "requires Docker"]
async fn transport_and_request_client_connect_with_private_ca_and_mtls() {
    let broker = harness::start_tls_rabbitmq().await;
    let connection_config =
        RabbitMqConnectionConfig::default().with_tls_config(harness::client_tls_config());

    let transport = RabbitMqTransport::new_with_config(broker.uri(), &connection_config)
        .await
        .expect("transport must trust the private CA and present a client certificate");
    drop(transport);

    let cancel = CancellationToken::new();
    let client = connect_request_client_with_config(
        broker.uri(),
        Duration::from_secs(5),
        cancel,
        RabbitMqRequestClientConfigBuilder::new()
            .connection_config(connection_config)
            .build(),
    )
    .await
    .expect("request client must configure TLS on both owned connections");
    client.close().await;
}
