//! Synchronous-over-async RPC against a real RabbitMQ broker.
//!
//! One [`RequestClient`] plays out five acts against four responder
//! workers on the same broker:
//!
//! - Act 1: a nominal round trip. The `Echo` responder answers every
//!   [`Ping`] with a [`Pong`] carrying the same sequence number.
//! - Act 2: a timeout. [`Silence`] requests are published to a queue
//!   that exists (so the publish is routable) but that no worker ever
//!   consumes from: a mute responder. A per-call [`RequestOptions`]
//!   timeout gives up well before its deadline would starve the example.
//! - Act 3: a genuine remote fault. The `Faulty` responder always fails
//!   [`Explode`] with an internal error, and the failure reaches the
//!   caller as [`RequestError::Remote`]: the protocol's error channel is
//!   reserved for exactly this, a fault the caller cannot resolve.
//! - Act 4: a business rejection. The `Divider` responder never fails a
//!   [`Divide`] request: a zero denominator is an expected, named
//!   outcome carried by the [`DivisionOutcome`] reply, not a protocol
//!   failure.
//! - Act 5: causal propagation through `RequestOptions::with_correlation_id`.
//!   The `Relay` responder receives a [`RelayedPing`] and forwards it to
//!   `Echo` as a second request-reply hop, joining the same causal chain it
//!   was called on: the two calls share their `correlation_id` and get
//!   distinct, unrelated `request_id`s.
//!
//! Run with (requires a running Docker daemon):
//!
//! ```bash
//! cargo run --example 07_request_reply -p hexeract-examples
//! ```

use std::error::Error;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::Duration;

use hexeract::bus::BusError;
use hexeract::bus::Message;
use hexeract::bus::Queue;
use hexeract::bus::Request;
use hexeract::bus::RequestClient;
use hexeract::bus::RequestContext;
use hexeract::bus::RequestError;
use hexeract::bus::RequestHandler;
use hexeract::bus::RequestOptions;
use hexeract::bus_rabbitmq::RabbitMqConnection;
use hexeract::bus_rabbitmq::RabbitMqTransport;
use hexeract::bus_rabbitmq::RabbitMqWorkerBuilder;
use hexeract::bus_rabbitmq::connect_request_client;
use hexeract::bus_rabbitmq::declare_queue;
use serde::Deserialize;
use serde::Serialize;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

// `RabbitMqTransport` publishes on the default exchange, which only ever
// routes a message to the queue whose name equals the routing key. The
// request client uses `Request::DESTINATION` as that routing key (see
// `RequestClient::request`), which defaults to `MESSAGE_TYPE` and none of
// the requests below override it, so each queue must be named exactly
// after the matching request's `MESSAGE_TYPE`, not after the queue's role.
const PING_QUEUE: &str = "examples.ping";
const DIVIDE_QUEUE: &str = "examples.divide";
const SILENCE_QUEUE: &str = "examples.silence";
const EXPLODE_QUEUE: &str = "examples.explode";
const RELAY_QUEUE: &str = "examples.relayed_ping";
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
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Pong {
    seq: u64,
}
impl Message for Pong {
    const MESSAGE_TYPE: &'static str = "examples.pong";
}

/// Responder that echoes the request's `seq` back in the reply, and
/// records both the `correlation_id` and the `request_id` of the last
/// request it handled, so the relay act below can prove the causal chain
/// survived the extra hop while the per-call request identity did not.
struct Echo {
    last_correlation: Arc<StdMutex<Option<Uuid>>>,
    last_request_id: Arc<StdMutex<Option<Uuid>>>,
}
impl RequestHandler<Ping> for Echo {
    type Error = BusError;

    async fn handle(&self, request: Ping, ctx: &RequestContext<'_>) -> Result<Pong, BusError> {
        *self
            .last_correlation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(*ctx.handler.correlation_id.as_uuid());
        *self
            .last_request_id
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(*ctx.request_id.as_uuid());
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

/// Request whose responder always fails with a genuine fault, to prove
/// such a failure reaches the caller as [`RequestError::Remote`] rather
/// than a timeout or a decoded business outcome.
#[derive(Debug, Serialize, Deserialize)]
struct Explode {
    reason: String,
}
impl Message for Explode {
    const MESSAGE_TYPE: &'static str = "examples.explode";
}
impl Request for Explode {
    type Reply = Ack;
}

/// Reply type for [`Explode`]. Never actually produced: the `Faulty`
/// responder always fails, so this type only needs to exist to satisfy
/// [`Request::Reply`].
#[derive(Debug, Serialize, Deserialize)]
struct Ack;
impl Message for Ack {
    const MESSAGE_TYPE: &'static str = "examples.ack";
}

/// Responder that always fails with an internal error, standing in for a
/// downstream dependency the responder cannot reach. This is a fault,
/// not a business outcome, so it belongs on the protocol's error
/// channel rather than in a `Reply` value.
struct Faulty;
impl RequestHandler<Explode> for Faulty {
    type Error = BusError;

    async fn handle(&self, _request: Explode, _ctx: &RequestContext<'_>) -> Result<Ack, BusError> {
        Err(BusError::Internal(
            "downstream dependency unreachable".to_owned(),
        ))
    }
}

/// Request whose reply models a business rejection as a value.
#[derive(Debug, Serialize, Deserialize)]
struct Divide {
    numerator: i64,
    denominator: i64,
}
impl Message for Divide {
    const MESSAGE_TYPE: &'static str = "examples.divide";
}
impl Request for Divide {
    type Reply = DivisionOutcome;
}

/// Reply carrying either the quotient or the business rejection of a
/// zero denominator. A zero denominator is an expected, named outcome
/// the caller pattern-matches on, never a protocol failure: the error
/// channel of the protocol is reserved for faults.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum DivisionOutcome {
    Quotient(i64),
    DivisionByZero,
}
impl Message for DivisionOutcome {
    const MESSAGE_TYPE: &'static str = "examples.division_outcome";
}

/// Responder that never fails on a zero denominator: it replies with
/// [`DivisionOutcome::DivisionByZero`] instead.
struct Divider;
impl RequestHandler<Divide> for Divider {
    type Error = BusError;

    async fn handle(
        &self,
        request: Divide,
        _ctx: &RequestContext<'_>,
    ) -> Result<DivisionOutcome, BusError> {
        if request.denominator == 0 {
            return Ok(DivisionOutcome::DivisionByZero);
        }
        Ok(DivisionOutcome::Quotient(
            request.numerator / request.denominator,
        ))
    }
}

/// Request whose handler forwards to `Echo` via `RequestOptions`'s
/// `correlation_id` override, to demonstrate a handler continuing the
/// caller's causal chain into a second request-reply hop rather than
/// starting a fresh one.
#[derive(Debug, Serialize, Deserialize)]
struct RelayedPing {
    seq: u64,
}
impl Message for RelayedPing {
    const MESSAGE_TYPE: &'static str = "examples.relayed_ping";
}
impl Request for RelayedPing {
    type Reply = Pong;
}

/// Responder whose handler issues its own request on the caller's
/// `correlation_id`, via
/// `RequestOptions::new().with_correlation_id(ctx.handler.correlation_id)`,
/// and records that `correlation_id` plus its own `request_id`, so the
/// example can prove `Echo` observed the same correlation but a distinct
/// request identity on the forwarded call.
struct Relay {
    client: Arc<RequestClient<RabbitMqTransport>>,
    last_correlation: Arc<StdMutex<Option<Uuid>>>,
    last_request_id: Arc<StdMutex<Option<Uuid>>>,
}
impl RequestHandler<RelayedPing> for Relay {
    type Error = BusError;

    async fn handle(
        &self,
        request: RelayedPing,
        ctx: &RequestContext<'_>,
    ) -> Result<Pong, BusError> {
        *self
            .last_correlation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(*ctx.handler.correlation_id.as_uuid());
        *self
            .last_request_id
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(*ctx.request_id.as_uuid());
        let options = RequestOptions::new().with_correlation_id(ctx.handler.correlation_id);
        self.client
            .request_with(Ping { seq: request.seq }, options)
            .await
            .map_err(|error| BusError::Internal(error.to_string()))
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

/// Declare every queue a responder below consumes from, or that a
/// request in this example routes to.
async fn declare_all_request_queues(uri: &str) -> Result<(), Box<dyn Error>> {
    // The silence queue is declared but never consumed: a routable
    // publish target with a mute responder behind it.
    declare_request_queue(uri, PING_QUEUE).await?;
    declare_request_queue(uri, DIVIDE_QUEUE).await?;
    declare_request_queue(uri, SILENCE_QUEUE).await?;
    declare_request_queue(uri, EXPLODE_QUEUE).await?;
    declare_request_queue(uri, RELAY_QUEUE).await?;
    Ok(())
}

/// Join handles of every responder worker spawned for this example.
struct ResponderHandles {
    echo: tokio::task::JoinHandle<Result<(), BusError>>,
    divide: tokio::task::JoinHandle<Result<(), BusError>>,
    explode: tokio::task::JoinHandle<Result<(), BusError>>,
    relay: tokio::task::JoinHandle<Result<(), BusError>>,
}

impl ResponderHandles {
    /// Await every responder task after the cancellation token has been
    /// fired, surfacing the first failure encountered.
    async fn join_all(self) -> Result<(), Box<dyn Error>> {
        self.echo.await??;
        self.divide.await??;
        self.explode.await??;
        self.relay.await??;
        Ok(())
    }
}

/// Correlation and request ids captured mid-flight by `Echo` and `Relay`,
/// so act 5 can prove the causal chain survived the relay hop while the
/// per-call request identity did not.
struct HandlerCaptures {
    echo_correlation: Arc<StdMutex<Option<Uuid>>>,
    echo_request_id: Arc<StdMutex<Option<Uuid>>>,
    relay_correlation: Arc<StdMutex<Option<Uuid>>>,
    relay_request_id: Arc<StdMutex<Option<Uuid>>>,
}

/// Spawn every responder worker this example needs: `Echo`, `Divider`,
/// `Faulty` and `Relay`.
async fn spawn_responders(
    uri: &str,
    cancel: &CancellationToken,
) -> Result<(ResponderHandles, HandlerCaptures), Box<dyn Error>> {
    let echo_correlation = Arc::new(StdMutex::new(None));
    let echo_request_id = Arc::new(StdMutex::new(None));
    let echo_transport = Arc::new(RabbitMqTransport::new(uri).await?);
    let echo_worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(uri, RETRY_ATTEMPTS, RETRY_BASE_DELAY).await?,
    )
    .queue(PING_QUEUE)
    .register_request_handler::<Ping, _>(
        Echo {
            last_correlation: Arc::clone(&echo_correlation),
            last_request_id: Arc::clone(&echo_request_id),
        },
        Arc::clone(&echo_transport),
    )
    .build()?;
    let echo_cancel = cancel.clone();
    let echo = tokio::spawn(async move { echo_worker.run(echo_cancel).await });

    let divide_transport = Arc::new(RabbitMqTransport::new(uri).await?);
    let divide_worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(uri, RETRY_ATTEMPTS, RETRY_BASE_DELAY).await?,
    )
    .queue(DIVIDE_QUEUE)
    .register_request_handler::<Divide, _>(Divider, Arc::clone(&divide_transport))
    .build()?;
    let divide_cancel = cancel.clone();
    let divide = tokio::spawn(async move { divide_worker.run(divide_cancel).await });

    let explode_transport = Arc::new(RabbitMqTransport::new(uri).await?);
    let explode_worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(uri, RETRY_ATTEMPTS, RETRY_BASE_DELAY).await?,
    )
    .queue(EXPLODE_QUEUE)
    .register_request_handler::<Explode, _>(Faulty, Arc::clone(&explode_transport))
    .build()?;
    let explode_cancel = cancel.clone();
    let explode = tokio::spawn(async move { explode_worker.run(explode_cancel).await });

    // The relay responder needs its own request client (a second,
    // independent connection pair) to issue the forwarded `Ping` from
    // inside its handler, distinct from the transport it uses to publish
    // its own reply to `RelayedPing`.
    let relay_correlation = Arc::new(StdMutex::new(None));
    let relay_request_id = Arc::new(StdMutex::new(None));
    // Shorter than the outer client's timeout (below), so on a slow broker
    // the inner hop times out first: a relay timeout is then unambiguously
    // attributable to the forwarded call, never to the outer one.
    let relay_client =
        Arc::new(connect_request_client(uri, Duration::from_secs(5), cancel.clone()).await?);
    let relay_reply_transport = Arc::new(RabbitMqTransport::new(uri).await?);
    let relay_worker = RabbitMqWorkerBuilder::new(
        RabbitMqConnection::connect_with_retry(uri, RETRY_ATTEMPTS, RETRY_BASE_DELAY).await?,
    )
    .queue(RELAY_QUEUE)
    .register_request_handler::<RelayedPing, _>(
        Relay {
            client: relay_client,
            last_correlation: Arc::clone(&relay_correlation),
            last_request_id: Arc::clone(&relay_request_id),
        },
        Arc::clone(&relay_reply_transport),
    )
    .build()?;
    let relay_cancel = cancel.clone();
    let relay = tokio::spawn(async move { relay_worker.run(relay_cancel).await });

    Ok((
        ResponderHandles {
            echo,
            divide,
            explode,
            relay,
        },
        HandlerCaptures {
            echo_correlation,
            echo_request_id,
            relay_correlation,
            relay_request_id,
        },
    ))
}

/// Act 1: a nominal round trip. `Echo` answers a [`Ping`] with a
/// [`Pong`] carrying the same sequence number.
async fn run_nominal_round_trip(
    client: &RequestClient<RabbitMqTransport>,
) -> Result<(), Box<dyn Error>> {
    tracing::info!("act 1: nominal round trip");
    let pong = client.request(Ping { seq: 7 }).await?;
    if pong.seq != 7 {
        return Err(format!("expected echo of 7, got {}", pong.seq).into());
    }
    tracing::info!(seq = pong.seq, "echo replied");
    Ok(())
}

/// Act 2: a mute responder. No worker consumes the silence queue, so
/// the call times out rather than hangs forever.
async fn run_timeout(client: &RequestClient<RabbitMqTransport>) -> Result<(), Box<dyn Error>> {
    tracing::info!("act 2: mute responder times out");
    let options = RequestOptions::new().with_timeout(Duration::from_secs(2));
    let outcome = client
        .request_with(
            Silence {
                note: "is anybody listening".to_owned(),
            },
            options,
        )
        .await;
    match outcome {
        Err(RequestError::Timeout(after)) => {
            tracing::info!(?after, "mute responder timed out as expected");
            Ok(())
        }
        Ok(_) => Err("expected a timeout, got a reply from a mute responder".into()),
        Err(other) => Err(format!("expected RequestError::Timeout, got {other:?}").into()),
    }
}

/// Act 3: a genuine remote fault. `Faulty` always fails, and the
/// failure reaches the caller as [`RequestError::Remote`]: the
/// protocol's error channel is reserved for exactly this, a fault the
/// caller cannot resolve by matching on a value.
async fn run_remote_fault(client: &RequestClient<RabbitMqTransport>) -> Result<(), Box<dyn Error>> {
    tracing::info!("act 3: a genuine remote fault reaches the caller");
    let outcome = client
        .request(Explode {
            reason: "simulated dependency outage".to_owned(),
        })
        .await;
    match outcome {
        Err(RequestError::Remote {
            error_type,
            request_id,
        }) => {
            tracing::info!(?error_type, %request_id, "remote fault received");
            Ok(())
        }
        Ok(_) => Err("expected a remote error, got a reply".into()),
        Err(other) => Err(format!("expected RequestError::Remote, got {other:?}").into()),
    }
}

/// Act 4: a business rejection travels as a `Reply` value, never as a
/// protocol failure. `Divider` never fails on a zero denominator: it
/// replies with the named outcome instead.
async fn run_business_rejection(
    client: &RequestClient<RabbitMqTransport>,
) -> Result<(), Box<dyn Error>> {
    tracing::info!("act 4: a business rejection travels as a Reply value");
    let outcome = client
        .request(Divide {
            numerator: 10,
            denominator: 2,
        })
        .await?;
    if outcome != DivisionOutcome::Quotient(5) {
        return Err(format!("expected Quotient(5), got {outcome:?}").into());
    }
    tracing::info!("division succeeded");

    let outcome = client
        .request(Divide {
            numerator: 10,
            denominator: 0,
        })
        .await?;
    if outcome != DivisionOutcome::DivisionByZero {
        return Err(format!("expected DivisionByZero, got {outcome:?}").into());
    }
    tracing::info!("division by zero rejected as a business outcome, not a protocol error");
    Ok(())
}

/// Act 5: `RequestOptions::with_correlation_id` propagates the causal
/// chain through a relay hop. `Relay` forwards the inbound
/// [`RelayedPing`] to `Echo` on the same `correlation_id` it received,
/// minting its own, unrelated `request_id` for that second call: the two
/// handlers must observe the same `correlation_id` and two distinct
/// `request_id`s.
async fn run_causal_propagation(
    client: &RequestClient<RabbitMqTransport>,
    captures: &HandlerCaptures,
) -> Result<(), Box<dyn Error>> {
    tracing::info!(
        "act 5: RequestOptions::with_correlation_id propagates the causal chain through a relay hop"
    );
    let relayed = client.request(RelayedPing { seq: 21 }).await?;
    if relayed.seq != 21 {
        return Err(format!("expected echo of 21, got {}", relayed.seq).into());
    }

    let relay_correlation = *captures
        .relay_correlation
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let echo_correlation = *captures
        .echo_correlation
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if relay_correlation.is_none() || relay_correlation != echo_correlation {
        return Err(format!(
            "expected the relayed call and the forwarded ping to share their correlation_id, \
             got relay={relay_correlation:?} echo={echo_correlation:?}"
        )
        .into());
    }
    tracing::info!(
        correlation_id = ?relay_correlation,
        "the relay hop kept the caller's causal chain"
    );

    let relay_request_id = *captures
        .relay_request_id
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let echo_request_id = *captures
        .echo_request_id
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if relay_request_id.is_none()
        || echo_request_id.is_none()
        || relay_request_id == echo_request_id
    {
        return Err(format!(
            "expected the relayed call and the forwarded ping to carry distinct request_ids, \
             got relay={relay_request_id:?} echo={echo_request_id:?}"
        )
        .into());
    }
    tracing::info!(
        relay_request_id = ?relay_request_id,
        echo_request_id = ?echo_request_id,
        "the relay hop, unlike the correlation_id, did not carry its own request_id forward"
    );
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
    declare_all_request_queues(&uri).await?;

    let cancel = CancellationToken::new();
    let (responders, captures) = spawn_responders(&uri, &cancel).await?;

    let client = connect_request_client(&uri, Duration::from_secs(10), cancel.clone()).await?;
    tracing::info!("request client and all responders ready");

    run_nominal_round_trip(&client).await?;
    run_timeout(&client).await?;
    run_remote_fault(&client).await?;
    run_business_rejection(&client).await?;
    run_causal_propagation(&client, &captures).await?;

    // Closing the client rejects calls that have not reached publication with
    // `RequestError::Closed`. A request already accepted by the transport
    // reports `RequestError::PublicationUnknown`, because retrying it could
    // duplicate side effects. The shared token is cancelled after admitted
    // publications drain, which then winds down every responder worker above.
    // Unlike simply dropping the client, `close` also waits for its own reply
    // consumer task to have actually stopped before returning.
    client.close().await;
    responders.join_all().await?;
    tracing::info!("request-reply example completed");
    Ok(())
}
