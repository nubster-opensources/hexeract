//! Shared testcontainers helper for the crate's Docker-backed integration
//! suite.
//!
//! Spins up a fresh RabbitMQ container per test and exposes its resolved
//! AMQP URI, plus a raw publish helper that hand-crafts the AMQP
//! properties a [`BusEnvelope`] restores from, so a test can act as a
//! foreign producer without going through [`hexeract_bus_rabbitmq::RabbitMqTransport`].

#![cfg(test)]
#![allow(
    dead_code,
    reason = "not every test binary in this crate uses every helper"
)]

use std::time::Duration;

use hexeract_bus::BusEnvelope;
use hexeract_bus_rabbitmq::{OwnedIdentity, OwnedTLSConfig};
use lapin::BasicProperties;
use lapin::Channel;
use lapin::options::BasicPublishOptions;
use lapin::types::AMQPValue;
use lapin::types::FieldTable;
use lapin::types::ShortString;
use testcontainers::ContainerAsync;
use testcontainers::CopyTargetOptions;
use testcontainers::GenericImage;
use testcontainers::Image;
use testcontainers::ImageExt;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::rabbitmq::RabbitMq;

/// A running RabbitMQ test container, kept alive for the container's
/// AMQP URI to stay reachable.
pub(crate) struct RunningBroker {
    container: ContainerAsync<RabbitMq>,
    host: String,
    port: u16,
    uri: String,
}

/// A RabbitMQ broker configured for TLS only, with a private CA and mandatory
/// client certificate authentication.
pub(crate) struct RunningTlsBroker {
    _container: ContainerAsync<GenericImage>,
    uri: String,
}

impl RunningTlsBroker {
    /// The AMQPS URI of the running broker.
    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }
}

impl RunningBroker {
    /// The AMQP URI of the running broker.
    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    /// The host the broker's AMQP port is published on.
    ///
    /// Paired with [`Self::port`] for the one case [`Self::uri`] cannot serve:
    /// addressing this same broker under different credentials. Prefer
    /// [`Self::uri`] everywhere else, so a test never hard-codes the shape of
    /// an AMQP URI.
    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    /// The published port the broker's AMQP listener is reachable on.
    ///
    /// See [`Self::host`] for when to reach for this rather than
    /// [`Self::uri`].
    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    /// Stop the underlying container immediately, simulating a broker
    /// crash so a test can assert on how a client reacts to a dropped
    /// connection.
    ///
    /// The process is killed with no grace period, so open sockets are left
    /// hanging exactly as they would be by a broker that died. Use
    /// [`Self::stop_gracefully`] for an orderly shutdown, where the broker
    /// gets to close its connections first: the two look nothing alike to a
    /// client, and a test that picks the wrong one proves something other
    /// than what it claims.
    pub(crate) async fn stop(&self) {
        self.container
            .stop_with_timeout(Some(0))
            .await
            .expect("rabbitmq container must stop");
    }

    /// Stop the underlying container the way an operator would, letting the
    /// broker close its connections before the process goes away.
    ///
    /// Simulates an administrative shutdown. See [`Self::stop`] for the
    /// abrupt counterpart, and for why the difference matters.
    pub(crate) async fn stop_gracefully(&self) {
        self.container
            .stop()
            .await
            .expect("rabbitmq container must stop");
    }

    /// Freeze the underlying container's process, simulating a network
    /// blip that a client's auto-recovering connection must survive rather
    /// than fail forever. Pair with [`Self::unpause`].
    pub(crate) async fn pause(&self) {
        self.container
            .pause()
            .await
            .expect("rabbitmq container must pause");
    }

    /// Resume a container previously frozen by [`Self::pause`].
    pub(crate) async fn unpause(&self) {
        self.container
            .unpause()
            .await
            .expect("rabbitmq container must resume");
    }
}

/// How many subjects the start policy builds before it gives up.
///
/// A broker that fails to boot is overwhelmingly a one-off: the same commit
/// re-run on the same runner comes up. Three attempts turn that one-off into
/// a non-event, and stop well short of hiding a broker that is genuinely
/// unable to start behind a quarter of an hour of retrying.
const BROKER_START_ATTEMPTS: u32 = 3;

/// How long the start policy waits between two readiness probes.
///
/// Short enough that a broker which comes up quickly is not held back by the
/// polling grain, long enough that a budget of two minutes costs a few hundred
/// probes rather than a busy loop.
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Hand back the first subject that reports itself ready, rebuilding it from
/// scratch on every attempt that overruns `budget`.
///
/// The distinction this function exists for is between recreating and waiting
/// longer. A RabbitMQ container whose Erlang distribution dies at startup is
/// reported as started by Docker, so the call that creates it returns
/// successfully and the broker inside is already dead. No amount of further
/// waiting revives it, and no retry driven by an error can fire, because
/// there was no error. Only building a new subject recovers.
///
/// Knows nothing of Docker or RabbitMQ: `make` builds a subject, `is_ready`
/// is polled until it says yes or the budget runs out, and `describe` is read
/// once, from the last subject, to explain a failure that repeats. Splitting
/// readiness from description keeps the expensive half out of the polling
/// loop, which runs hundreds of times per attempt.
///
/// # Panics
///
/// When every attempt has been spent, naming `label`, the attempt count, the
/// budget and the report `describe` produced for the subject given up on.
pub(crate) async fn start_until_ready<T>(
    label: &str,
    attempts: u32,
    budget: Duration,
    make: impl AsyncFn() -> T,
    is_ready: impl AsyncFn(&T) -> bool,
    describe: impl AsyncFn(&T) -> String,
) -> T {
    let mut given_up_on: Option<T> = None;
    for _ in 0..attempts {
        // Let go of the previous attempt's subject before building its
        // replacement, so two containers are never alive at the same time.
        // Only the last one is kept, and only to be described.
        drop(given_up_on.take());

        let subject = make().await;
        let became_ready = tokio::time::timeout(budget, async {
            while !is_ready(&subject).await {
                tokio::time::sleep(READINESS_POLL_INTERVAL).await;
            }
        })
        .await
        .is_ok();

        if became_ready {
            return subject;
        }
        given_up_on = Some(subject);
    }

    let report = match given_up_on.as_ref() {
        Some(subject) => describe(subject).await,
        None => String::from("no attempt was made: the policy was given no attempts to spend"),
    };
    panic!(
        "{label} never reported itself ready within {budget:?}, over {attempts} attempts, each \
         building a new one from scratch.\n{report}"
    );
}

/// Start a fresh RabbitMQ container and resolve its AMQP URI.
///
/// The only place in this crate that builds a plaintext broker. Every test
/// that needs one comes through here, so the bounded retry below covers all
/// of them at once.
pub(crate) async fn start_rabbitmq() -> RunningBroker {
    let container = start_until_ready(
        "the RabbitMQ broker",
        BROKER_START_ATTEMPTS,
        BROKER_BOOT_BUDGET,
        async || {
            RabbitMq::default()
                .start()
                .await
                .expect("rabbitmq container must start")
        },
        async |container: &ContainerAsync<RabbitMq>| broker_log_reports_ready(container).await,
        async |container: &ContainerAsync<RabbitMq>| container_streams(container).await,
    )
    .await;
    let host = container
        .get_host()
        .await
        .expect("rabbitmq container must expose a host")
        .to_string();
    let port = container
        .get_host_port_ipv4(5672)
        .await
        .expect("rabbitmq container must expose AMQP port");
    let uri = format!("amqp://{host}:{port}");
    RunningBroker {
        container,
        host,
        port,
        uri,
    }
}

/// Select the rustls crypto provider for the whole test process.
///
/// `rustls` refuses to pick a provider on its own when its crate features name
/// more than one, and panics inside lapin's io loop at the first handshake
/// instead of returning an error. This test binary is exactly that case: the
/// production graph resolves `aws-lc-rs` alone through `tcp-stream`, but
/// `testcontainers` pulls `bollard`, which adds `ring`. The ambiguity is an
/// artefact of the dev-dependencies, not of the crate under test, so the tests
/// resolve it explicitly and pick the provider production would have used.
///
/// Safe to call from every test: `install_default` wins once per process and
/// the losers of the race are ignored.
fn install_test_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Start a broker that serves only AMQPS and requires mutual TLS.
pub(crate) async fn start_tls_rabbitmq() -> RunningTlsBroker {
    // `loopback_users = none` restores a default this file overwrites. The
    // official image ships its own `rabbitmq.conf` so that `guest` can log in
    // from outside the container; copying ours over it takes that away, and
    // `guest` reverts to loopback-only. The connection then arrives from the
    // Docker gateway, is refused with ACCESS_REFUSED, and the failure looks
    // like a TLS problem while TLS in fact succeeded.
    const RABBITMQ_TLS_CONFIG: &str = r"
listeners.tcp = none
listeners.ssl.default = 5671

loopback_users = none

ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
ssl_options.certfile = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile = /etc/rabbitmq/tls/server-key.pem
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = true
";

    install_test_crypto_provider();

    // `WaitFor::seconds` hands the container back straight away; readiness is
    // then established by the start policy, which reads the broker's own log
    // under a bounded budget. Delegating the wait to testcontainers would give
    // away the container, leaving nothing to inspect when a broker refuses its
    // configuration and never reports itself ready.
    let container = start_until_ready(
        "the TLS RabbitMQ broker",
        BROKER_START_ATTEMPTS,
        BROKER_BOOT_BUDGET,
        async || {
            GenericImage::new("rabbitmq", "3.8.22-management")
                .with_exposed_port(5671.tcp())
                .with_wait_for(WaitFor::seconds(1))
                .with_copy_to(
                    "/etc/rabbitmq/rabbitmq.conf",
                    RABBITMQ_TLS_CONFIG.as_bytes().to_vec(),
                )
                .with_copy_to(
                    "/etc/rabbitmq/tls/ca.pem",
                    include_bytes!("fixtures/tls/ca.pem").to_vec(),
                )
                .with_copy_to(
                    "/etc/rabbitmq/tls/server.pem",
                    include_bytes!("fixtures/tls/server.pem").to_vec(),
                )
                .with_copy_to(
                    CopyTargetOptions::new("/etc/rabbitmq/tls/server-key.pem").with_mode(0o644),
                    include_bytes!("fixtures/tls/server-key.pem").to_vec(),
                )
                .start()
                .await
                .expect("TLS RabbitMQ container must start")
        },
        async |container: &ContainerAsync<GenericImage>| broker_log_reports_ready(container).await,
        async |container: &ContainerAsync<GenericImage>| container_streams(container).await,
    )
    .await;
    let host = container
        .get_host()
        .await
        .expect("TLS RabbitMQ container must expose a host");
    let port = container
        .get_host_port_ipv4(5671)
        .await
        .expect("TLS RabbitMQ container must expose AMQPS port");

    let uri = format!("amqps://guest:guest@{host}:{port}/%2f");

    RunningTlsBroker {
        _container: container,
        uri,
    }
}

/// How long a broker is given to finish booting, on any one attempt.
///
/// Generous enough for a cold image on a loaded CI runner, and short enough
/// that a broker which will never come up fails with a diagnosis rather than
/// hanging until the job is cancelled.
const BROKER_BOOT_BUDGET: Duration = Duration::from_secs(120);

/// The line RabbitMQ prints once every listener is accepting connections.
///
/// Deliberately a prefix. The full line ends with the plugin count
/// ("Server startup complete; 4 plugins started."), and matching that count
/// couples the suite to the image: any change to the enabled plugins would
/// leave the wait hanging with no usable message.
const BROKER_READY_MARKER: &str = "Server startup complete";

/// Whether a broker's log says every listener is accepting connections.
///
/// Split out from the container it is read from so the rule can be put under
/// test without Docker. A broker that has printed nothing but its banner is
/// not ready, and that distinction is the whole point: a log is never empty
/// for long, so "has written something" would report readiness within
/// milliseconds of a container that is still booting.
pub(crate) fn log_reports_broker_ready(log: &str) -> bool {
    log.contains(BROKER_READY_MARKER)
}

/// Whether a container's own log says its broker is ready.
///
/// Readiness is read from the container's log, not from a TCP probe. Probing
/// the published port proves nothing under Docker: the port is bound by the
/// daemon's proxy and accepts connections while the service inside is still
/// booting, so a probe returns immediately and the first TLS handshake dies
/// against a broker that is not listening yet. That false positive is exactly
/// what made the suite fail in six seconds against a healthy broker.
///
/// Reading the log ourselves, rather than delegating to `WaitFor`, keeps the
/// container in hand: a broker that rejects its own configuration and never
/// prints the marker can be described and replaced instead of hanging the job.
async fn broker_log_reports_ready<I: Image>(container: &ContainerAsync<I>) -> bool {
    let stdout = container.stdout_to_vec().await.unwrap_or_default();
    log_reports_broker_ready(&String::from_utf8_lossy(&stdout))
}

/// Both of a container's output streams, for reporting a broker that never
/// came up.
///
/// Read once, by the start policy, on the last container it gave up on.
/// `stderr` is deliberately not read while polling: it costs a round trip to
/// the daemon and says nothing until there is a failure to explain.
async fn container_streams<I: Image>(container: &ContainerAsync<I>) -> String {
    let stdout = container.stdout_to_vec().await.unwrap_or_default();
    let stderr = container.stderr_to_vec().await.unwrap_or_default();
    format!(
        "It never printed {BROKER_READY_MARKER:?}.\n--- container stdout ---\n{}\n\
         --- container stderr ---\n{}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr),
    )
}

/// TLS material accepted by [`start_tls_rabbitmq`].
///
/// The fixtures are disposable and rebuilt wholesale by
/// `tests/fixtures/tls/regenerate.sh`. Its `README.md` records why an
/// unencrypted private key and the bundle password below belong in a public
/// repository, and which subjects the broker configuration depends on.
pub(crate) fn client_tls_config() -> OwnedTLSConfig {
    OwnedTLSConfig {
        identity: Some(OwnedIdentity::PKCS12 {
            der: include_bytes!("fixtures/tls/client.p12").to_vec(),
            password: "hexeract-test".to_owned(),
        }),
        cert_chain: Some(include_str!("fixtures/tls/ca.pem").to_owned()),
    }
}

/// Publish `envelope` to `routing_key` on the default exchange.
///
/// Stamps the AMQP `type`, `correlation_id` and header properties from
/// the envelope, following the same conventions
/// `hexeract_bus_rabbitmq::worker::delivery_to_envelope` reconstructs
/// from, so a test can publish a reply by hand and have the reply inbox
/// consumer decode it back into an equivalent [`BusEnvelope`].
pub(crate) async fn publish_to_default_exchange(
    channel: &Channel,
    routing_key: &str,
    envelope: &BusEnvelope,
) {
    let mut headers = FieldTable::default();
    for (key, value) in &envelope.headers {
        headers.insert(
            ShortString::from(key.as_str()),
            AMQPValue::LongString(value.as_str().into()),
        );
    }
    let properties = BasicProperties::default()
        .with_message_id(envelope.message_id.to_string().into())
        .with_correlation_id(envelope.correlation_id.to_string().into())
        .with_type(envelope.message_type.as_str().into())
        .with_headers(headers);

    channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(routing_key),
            BasicPublishOptions::default(),
            &envelope.payload,
            properties,
        )
        .await
        .expect("publish to inbox must succeed")
        .await
        .expect("publish confirmation must resolve");
}

/// Publish `payload` to `routing_key` on the default exchange, using
/// `properties` exactly as given.
///
/// Lower-level than [`publish_to_default_exchange`]: that helper always
/// derives the AMQP properties from a [`BusEnvelope`]'s own fields, which
/// cannot represent a foreign producer that signs for one destination and
/// publishes on another, omits the signature headers entirely, or tampers
/// with the payload after signing it. This is the extension point for
/// exactly that: the caller builds `properties` by hand, security headers
/// forged, mismatched or omitted at will, and picks `routing_key`
/// independently of whatever destination those headers, if any, claim.
pub(crate) async fn publish_with_properties(
    channel: &Channel,
    routing_key: &str,
    properties: BasicProperties,
    payload: &[u8],
) {
    channel
        .basic_publish(
            ShortString::from(""),
            ShortString::from(routing_key),
            BasicPublishOptions::default(),
            payload,
            properties,
        )
        .await
        .expect("publish must succeed")
        .await
        .expect("publish confirmation must resolve");
}
