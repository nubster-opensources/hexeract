//! Synchronous-over-async RPC against a real RabbitMQ broker.
//!
//! One [`RequestClient`] plays out three acts against two responder
//! workers on the same broker:
//!
//! - Act 1: a nominal round trip. The `Echo` responder answers every
//!   [`Ping`] with a [`Pong`] carrying the same sequence number.
//! - Act 2: a timeout. [`Silence`] requests are published to a queue
//!   that exists (so the publish is routable) but that no worker ever
//!   consumes from: a mute responder. The client's `request_with_timeout`
//!   gives up well before its deadline would starve the example.
//! - Act 3: a remote error. The `Failing` responder always rejects
//!   [`Divide`] requests, and the rejection reaches the caller as
//!   [`RequestError::Remote`] rather than a timeout.
//!
//! Run with (requires a running Docker daemon):
//!
//! ```bash
//! cargo run --example 07_request_reply -p hexeract-examples
//! ```

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use hexeract::bus::BusError;
use hexeract::bus::Message;
use hexeract::bus::Queue;
use hexeract::bus::Request;
use hexeract::bus::RequestError;
use hexeract::bus::RequestHandler;
use hexeract::bus_rabbitmq::RabbitMqConnection;
use hexeract::bus_rabbitmq::RabbitMqTransport;
use hexeract::bus_rabbitmq::RabbitMqWorkerBuilder;
use hexeract::bus_rabbitmq::connect_request_client;
use hexeract::bus_rabbitmq::declare_queue;
use hexeract::core::HandlerContext;
use serde::Deserialize;
use serde::Serialize;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

// `RabbitMqTransport` publishes on the default exchange, which only ever
// routes a message to the queue whose name equals the routing key. The
// request client uses `Request::MESSAGE_TYPE` as that routing key (see
// `RequestClient::request`), so each queue below must be named exactly
// after the matching request's `MESSAGE_TYPE`, not after the queue's role.
const PING_QUEUE: &str = "examples.ping";
const DIVIDE_QUEUE: &str = "examples.divide";
const SILENCE_QUEUE: &str = "examples.silence";
const RETRY_ATTEMPTS: u32 = 5;
const RETRY_BASE_DELAY: Duration = Duration::from_millis(200);

/// Request answered by the `Echo` responder with the same `seq`.
#[derive(Debug, Serialize, Deserialize)]
struct Ping {
    seq: u64,
}
impl Message for Ping {
    const MESSAGE_TYPE: &'static str = "examples.ping";
}
impl Request for Ping {
    type Reply = Pong;
}

/// Reply carrying back the `seq` the caller sent in its [`Ping`].
#[derive(Debug, Serialize, Deserialize)]
struct Pong {
    seq: u64,
}
impl Message for Pong {
    const MESSAGE_TYPE: &'static str = "examples.pong";
}

/// Responder that echoes the request's `seq` back in the reply.
struct Echo;
impl RequestHandler<Ping> for Echo {
    type Error = BusError;

    async fn handle(&self, request: Ping, _ctx: &HandlerContext) -> Result<Pong, BusError> {
        Ok(Pong { seq: request.seq })
    }
}

/// Request published to a queue nobody consumes from, to observe a
/// request-reply timeout rather than a nominal reply or a remote error.
#[derive(Debug, Serialize, Deserialize)]
struct Silence {
    note: String,
}
impl Message for Silence {
    const MESSAGE_TYPE: &'static str = "examples.silence";
}
impl Request for Silence {
    type Reply = NeverReplied;
}

/// Reply type for [`Silence`]. Never actually produced: no responder
/// consumes the silence queue, so this type only needs to exist to
/// satisfy [`Request::Reply`].
#[derive(Debug, Serialize, Deserialize)]
struct NeverReplied;
impl Message for NeverReplied {
    const MESSAGE_TYPE: &'static str = "examples.never_replied";
}

/// Request always rejected by the `Failing` responder, to prove a
/// remote error reaches the caller as [`RequestError::Remote`].
#[derive(Debug, Serialize, Deserialize)]
struct Divide {
    numerator: i64,
    denominator: i64,
}
impl Message for Divide {
    const MESSAGE_TYPE: &'static str = "examples.divide";
}
impl Request for Divide {
    type Reply = Quotient;
}

/// Reply carrying the result of a [`Divide`] request. Never actually
/// produced by the `Failing` responder in this example.
#[derive(Debug, Serialize, Deserialize)]
struct Quotient {
    value: i64,
}
impl Message for Quotient {
    const MESSAGE_TYPE: &'static str = "examples.quotient";
}

/// Responder that always fails, standing in for a downstream that
/// refuses to serve a request (here: division is not offered).
struct Failing;
impl RequestHandler<Divide> for Failing {
    type Error = BusError;

    async fn handle(&self, _request: Divide, _ctx: &HandlerContext) -> Result<Quotient, BusError> {
        Err(BusError::Internal(
            "division is not available in this example".to_owned(),
        ))
    }
}

/// Start a RabbitMQ container and return its AMQP connection uri.
async fn setup_rabbit() -> Result<(ContainerAsync<RabbitMq>, String), Box<dyn Error>> {
    let container = RabbitMq::default().start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5672).await?;
    let uri = format!("amqp://{host}:{port}");
    Ok((container, uri))
}

/// Declare a durable, non-exclusive queue so a request routed to it is
/// accepted by the broker even before any worker consumes from it.
/// RabbitMQ 4 rejects transient non-exclusive queues, and neither the
/// request client nor `RabbitMqWorker::run` declare their own consume
/// queue, so it must exist upfront.
async fn declare_request_queue(uri: &str, name: &str) -> Result<(), Box<dyn Error>> {
    let connection = RabbitMqConnection::connect(uri).await?;
    let queue = Queue::new(name)?;
    declare_queue(&connection, &queue).await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let (_rabbitmq, uri) = setup_rabbit().await?;

    // The silence queue is declared but never consumed: a routable
    // publish target with a mute responder behind it.
    declare_request_queue(&uri, PING_QUEUE).await?;
    declare_request_queue(&uri, DIVIDE_QUEUE).await?;
    declare_request_queue(&uri, SILENCE_QUEUE).await?;

    let cancel = CancellationToken::new();

    let echo_transport = Arc::new(RabbitMqTransport::new(&uri).await?);
    let echo_worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(&uri, RETRY_ATTEMPTS, RETRY_BASE_DELAY).await?,
    )
    .queue(PING_QUEUE)
    .register_request_handler::<Ping, _>(Echo, Arc::clone(&echo_transport))
    .build()?;
    let echo_cancel = cancel.clone();
    let echo_handle = tokio::spawn(async move { echo_worker.run(echo_cancel).await });

    let failing_transport = Arc::new(RabbitMqTransport::new(&uri).await?);
    let failing_worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(&uri, RETRY_ATTEMPTS, RETRY_BASE_DELAY).await?,
    )
    .queue(DIVIDE_QUEUE)
    .register_request_handler::<Divide, _>(Failing, Arc::clone(&failing_transport))
    .build()?;
    let failing_cancel = cancel.clone();
    let failing_handle = tokio::spawn(async move { failing_worker.run(failing_cancel).await });

    let client = connect_request_client(&uri, Duration::from_secs(10), cancel.clone()).await?;
    tracing::info!("request client and both responders ready");

    tracing::info!("act 1: nominal round trip");
    let pong = client.request(&Ping { seq: 7 }).await?;
    if pong.seq != 7 {
        return Err(format!("expected echo of 7, got {}", pong.seq).into());
    }
    tracing::info!(seq = pong.seq, "echo replied");

    tracing::info!("act 2: mute responder times out");
    let outcome = client
        .request_with_timeout(
            &Silence {
                note: "is anybody listening".to_owned(),
            },
            Duration::from_secs(2),
        )
        .await;
    match outcome {
        Err(RequestError::Timeout(after)) => {
            tracing::info!(?after, "mute responder timed out as expected");
        }
        Ok(_) => return Err("expected a timeout, got a reply from a mute responder".into()),
        Err(other) => return Err(format!("expected RequestError::Timeout, got {other:?}").into()),
    }

    tracing::info!("act 3: remote error reaches the caller");
    let outcome = client
        .request(&Divide {
            numerator: 10,
            denominator: 0,
        })
        .await;
    match outcome {
        Err(RequestError::Remote {
            error_type,
            request_id,
        }) => {
            tracing::info!(?error_type, %request_id, "remote handler error received");
        }
        Ok(_) => return Err("expected a remote error, got a reply".into()),
        Err(other) => return Err(format!("expected RequestError::Remote, got {other:?}").into()),
    }

    cancel.cancel();
    echo_handle.await??;
    failing_handle.await??;
    tracing::info!("request-reply example completed");
    Ok(())
}
