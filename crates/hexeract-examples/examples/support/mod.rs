//! Broker bootstrap shared by the bus examples. Kept out of each example so the
//! code a reader studies is the bus, not the container plumbing.

use std::error::Error;

use testcontainers::ContainerAsync;
use testcontainers::GenericImage;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;

/// The RabbitMQ image the bus examples start.
///
/// An exact tag, never a floating one: an example must not change behaviour
/// without a commit.
pub(crate) const BROKER_IMAGE_TAG: &str = "4.4.0";

/// Start a throwaway RabbitMQ broker and return it with its AMQP URI.
///
/// The container stops when the returned handle is dropped. Readiness is the
/// broker's own startup line, matched as a prefix so the wait does not depend
/// on how many plugins the image enables.
pub(crate) async fn start_rabbitmq_broker()
-> Result<(ContainerAsync<GenericImage>, String), Box<dyn Error>> {
    let container = GenericImage::new("rabbitmq", BROKER_IMAGE_TAG)
        .with_exposed_port(5672.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5672).await?;
    let uri = format!("amqp://{host}:{port}");
    Ok((container, uri))
}
