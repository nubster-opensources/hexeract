# RabbitMQ Configurable TLS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support a private CA and optional mTLS client identity in every RabbitMQ connection path without changing the current platform-trust-store default.

**Architecture:** One redaction-safe `RabbitMqConnectionConfig` owns the optional Lapin TLS configuration. A single connection primitive receives it for every attempt; transport and request/reply propagate it through publisher recovery, initial inbox setup, and inbox reconnection.

**Tech Stack:** Rust 2024, Tokio, lapin 4, rustls selected by lapin, testcontainers RabbitMQ, rustdoc.

**Spec:** `docs/superpowers/specs/2026-08-31-rabbitmq-configurable-tls-design.md`

## Global Constraints

- Preserve existing URI-only constructors and their platform-trust-store defaults.
- `amqps://` remains the TLS selector; #449's plaintext policy is not part of this work.
- Never render TLS configuration, certificate bytes, private keys, passwords, or raw credential-bearing URIs in diagnostics.
- Clone TLS configuration for every retry, publisher probe, recovering session, and inbox reconnect.
- Preserve `BusError::Connection` mapping and existing retry classification.
- Do not alter untracked `.claude/` files.

---

## File structure

| File | Responsibility |
| --- | --- |
| `crates/hexeract-bus-rabbitmq/src/connection.rs` | TLS config, safe debug rendering, and common connect attempt. |
| `crates/hexeract-bus-rabbitmq/src/lib.rs` | Re-export public TLS inputs and the connection config. |
| `crates/hexeract-bus-rabbitmq/src/transport.rs` | Config-aware publisher constructors. |
| `crates/hexeract-bus-rabbitmq/src/request_client.rs` | Carry connection config through both RPC connections. |
| `crates/hexeract-bus-rabbitmq/tests/tls.rs` | Ignored private-CA/mTLS integration test. |
| `docs/reference/hexeract-bus-rabbitmq.md` | TLS constructor reference. |
| `docs/operations/production-checklist.md` | Private-CA and mTLS deployment guidance. |

### Task 1: Add the redaction-safe TLS configuration

**Files:**

- Modify: `crates/hexeract-bus-rabbitmq/src/connection.rs:1-30, 150-420, tests module`
- Modify: `crates/hexeract-bus-rabbitmq/src/lib.rs:45-61`

**Interfaces:**

- Produces `pub struct RabbitMqConnectionConfig` with `Default`, `Clone`, and `with_tls_config(OwnedTLSConfig) -> Self`.
- Produces crate-root re-exports of `lapin::tcp::{OwnedIdentity, OwnedTLSConfig}`.
- Keeps secrets out of `Debug` with a manual formatter reporting only `custom_tls_configured`.

- [ ] **Step 1: Write the failing configuration test**

```rust
#[test]
fn custom_tls_config_is_retained_without_debugging_its_secret() {
    let config = RabbitMqConnectionConfig::default().with_tls_config(OwnedTLSConfig {
        cert_chain: Some("private-ca-pem".to_owned()),
        identity: None,
    });

    assert_eq!(config.tls_config().cert_chain.as_deref(), Some("private-ca-pem"));
    assert!(!format!("{config:?}").contains("private-ca-pem"));
}
```

- [ ] **Step 2: Run it and verify the initial failure**

Run: `cargo test -p hexeract-bus-rabbitmq --lib custom_tls_config_is_retained_without_debugging_its_secret`

Expected: FAIL because `RabbitMqConnectionConfig` does not exist.

- [ ] **Step 3: Implement the additive public type**

```rust
#[derive(Clone, Default)]
pub struct RabbitMqConnectionConfig {
    tls_config: Option<OwnedTLSConfig>,
}

impl RabbitMqConnectionConfig {
    #[must_use]
    pub fn with_tls_config(mut self, tls_config: OwnedTLSConfig) -> Self {
        self.tls_config = Some(tls_config);
        self
    }

    fn tls_config(&self) -> OwnedTLSConfig {
        self.tls_config.clone().unwrap_or_default()
    }

    pub(crate) fn has_custom_tls_config(&self) -> bool {
        self.tls_config.is_some()
    }
}
```

Implement `Debug` manually; do not derive it because Lapin's TLS config can hold secrets. Re-export the TLS types from `lib.rs`.

- [ ] **Step 4: Verify the foundation**

Run: `cargo test -p hexeract-bus-rabbitmq --lib connection::tests`

Expected: PASS, including existing sanitized-URI tests.

Run: `cargo fmt --check`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add crates/hexeract-bus-rabbitmq/src/connection.rs crates/hexeract-bus-rabbitmq/src/lib.rs
git commit -m "feat(bus-rabbitmq): add TLS connection configuration"
```

### Task 2: Apply the configuration to every connection attempt

**Files:**

- Modify: `crates/hexeract-bus-rabbitmq/src/connection.rs:connect, connect_with_retry_inner, connect_recovering_within`

**Interfaces:**

- Consumes `RabbitMqConnectionConfig`.
- Produces `RabbitMqConnection::connect_with_config(uri, &config)` and `connect_with_retry_with_config(uri, attempts, base_delay, &config)`.
- Produces a config-aware private recovering connector for publisher paths.

- [ ] **Step 1: Write failing propagation tests with a private attempt seam**

Extract the direct Lapin call behind a private attempt function. Its test form records whether a custom certificate chain was supplied without formatting it:

```rust
#[tokio::test]
async fn recovering_connect_reuses_custom_tls_for_probe_and_session() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    connect_recovering_with_attempt(&private_ca_config(), recording_attempt(Arc::clone(&seen))).await;
    assert_eq!(seen.lock().unwrap().as_slice(), [true, true]);
}
```

Also cover a supervised retry so a later retry gets a fresh cloned configuration.

- [ ] **Step 2: Run the propagation test and verify it fails**

Run: `cargo test -p hexeract-bus-rabbitmq --lib recovering_connect_reuses_custom_tls_for_probe_and_session`

Expected: FAIL because the retry and recovery paths call `Connection::connect` directly.

- [ ] **Step 3: Implement the common TLS-aware attempt**

```rust
async fn connect_once(
    uri: &str,
    properties: ConnectionProperties,
    config: &RabbitMqConnectionConfig,
) -> lapin::Result<Connection> {
    Connection::builder()
        .with_uri(uri)
        .with_connection_properties(properties)
        .with_tls_config(config.tls_config())
        .build()
        .await
}
```

Thread `&RabbitMqConnectionConfig` into the retry loop and both probe/session phases. Make old URI-only methods delegate to `RabbitMqConnectionConfig::default()`.

- [ ] **Step 4: Verify connection behaviour**

Run: `cargo test -p hexeract-bus-rabbitmq --lib connection::tests`

Expected: PASS; unreachable-host errors remain redacted `BusError::Connection` values.

Run: `cargo clippy -p hexeract-bus-rabbitmq --lib --all-features -- -D warnings`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add crates/hexeract-bus-rabbitmq/src/connection.rs
git commit -m "feat(bus-rabbitmq): apply TLS configuration to retries"
```

### Task 3: Carry the configuration into transport and request/reply

**Files:**

- Modify: `crates/hexeract-bus-rabbitmq/src/transport.rs:68-171, tests module`
- Modify: `crates/hexeract-bus-rabbitmq/src/request_client.rs:46-190, 342-395, tests module`

**Interfaces:**

- Produces `RabbitMqTransport::new_with_config(uri, &config)` and `with_exchange_with_config(uri, exchange, &config)`.
- Extends `RabbitMqRequestClientConfig` with `connection_config: RabbitMqConnectionConfig`.
- Produces `RabbitMqRequestClientConfigBuilder::connection_config(config)`.

- [ ] **Step 1: Write failing request-client config carriage tests**

```rust
#[test]
fn request_client_builder_keeps_the_selected_connection_configuration() {
    let config = RabbitMqRequestClientConfigBuilder::new()
        .connection_config(private_ca_config())
        .build();

    assert!(config.connection_config.has_custom_tls_config());
}
```

Add private factory-seam tests proving `new_with_config` and `with_exchange_with_config` pass the configuration to the recovering connection. Add one request-client test that observes the same configuration at initial inbox setup and inbox reconnect.

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `cargo test -p hexeract-bus-rabbitmq --lib request_client::tests::request_client_builder_keeps_the_selected_connection_configuration`

Expected: FAIL because the builder has no connection configuration setter.

- [ ] **Step 3: Implement additive propagation**

Make URI-only transport methods delegate to the config-aware variants. Store the connection configuration in `RabbitMqRequestClientConfig`; use it for the recovering publisher, initial supervised inbox connection, and an `Arc<RabbitMqConnectionConfig>` captured by `spawn_reply_inbox_supervisor` for `reconnect_reply_inbox`.

```rust
let transport = Arc::new(RabbitMqTransport::new_with_config(uri, &config.connection_config).await?);
let connection = RabbitMqConnection::connect_with_retry_with_config(
    uri, DEFAULT_RETRY_ATTEMPTS, DEFAULT_RETRY_BASE_DELAY, &config.connection_config,
).await?;
```

Do not add a second TLS argument to `connect_request_client_with_config`; that existing configuration object remains its sole options surface.

- [ ] **Step 4: Verify crate-wide behaviour**

Run: `cargo test -p hexeract-bus-rabbitmq --lib --all-features`

Expected: PASS, including default in-flight, reconnect, and cancellation tests.

Run: `cargo test -p hexeract-bus-rabbitmq --doc --all-features`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add crates/hexeract-bus-rabbitmq/src/transport.rs crates/hexeract-bus-rabbitmq/src/request_client.rs
git commit -m "feat(bus-rabbitmq): configure TLS for transport and RPC"
```

### Task 4: Prove private-CA mTLS and document it

**Files:**

- Create: `crates/hexeract-bus-rabbitmq/tests/tls.rs`
- Create: `crates/hexeract-bus-rabbitmq/tests/fixtures/tls/ca.pem`
- Create: `crates/hexeract-bus-rabbitmq/tests/fixtures/tls/server.pem`
- Create: `crates/hexeract-bus-rabbitmq/tests/fixtures/tls/server-key.pem`
- Create: `crates/hexeract-bus-rabbitmq/tests/fixtures/tls/client.p12`
- Modify: `docs/reference/hexeract-bus-rabbitmq.md:11-36`
- Modify: `docs/operations/production-checklist.md:46-51`

**Interfaces:**

- Consumes crate-root `OwnedTLSConfig`, `OwnedIdentity`, `RabbitMqConnectionConfig`, transport and request-client APIs.
- Produces an ignored testcontainers regression test used by the existing Docker integration job.

- [ ] **Step 1: Write the failing ignored mTLS test**

Configure the RabbitMQ test container with a test-only CA, server certificate/key, peer verification, and a client-certificate requirement. Construct:

```rust
let config = RabbitMqConnectionConfig::default().with_tls_config(OwnedTLSConfig {
    cert_chain: Some(include_str!("fixtures/tls/ca.pem").to_owned()),
    identity: Some(OwnedIdentity::PKCS12 {
        der: include_bytes!("fixtures/tls/client.p12").to_vec(),
        password: "hexeract-test".to_owned(),
    }),
});
```

Use `amqps://`; assert one publish/consume round trip and one request/reply round trip. Do not emit certificate material or test passwords.

- [ ] **Step 2: Run it and verify it fails before TLS broker setup exists**

Run: `cargo test -p hexeract-bus-rabbitmq --test tls private_ca_and_mtls_cover_transport_and_request_client -- --ignored --nocapture`

Expected: FAIL until fixture copy/mount, broker TLS configuration, and public propagation are complete.

- [ ] **Step 3: Add non-production fixture chain and container setup**

Generate a test-only CA, server certificate/key, and PKCS#12 client identity. Copy or mount those fixtures with testcontainers before startup; configure RabbitMQ to listen on TLS, trust the CA, and require peer verification. Keep all files under `tests/fixtures/tls`.

- [ ] **Step 4: Add public and operational documentation**

Document a `RabbitMqConnectionConfig` built from a CA PEM and PKCS#12 identity and passed to `RabbitMqTransport::new_with_config`. State that `amqps://` selects TLS, the default uses the platform trust store, and applications load secret files through their own secret-management system. Mirror these constraints in the production checklist.

- [ ] **Step 5: Run full verification**

Run: `cargo fmt --check`

Expected: PASS.

Run: `cargo test -p hexeract-bus-rabbitmq --lib --all-features`

Expected: PASS.

Run: `cargo test -p hexeract-bus-rabbitmq --test tls -- --ignored`

Expected: PASS with Docker available.

Run: `cargo clippy -p hexeract-bus-rabbitmq --all-targets --all-features -- -D warnings`

Expected: PASS.

Run: `cargo doc --workspace --all-features --no-deps`

Expected: PASS without rustdoc warnings.

- [ ] **Step 6: Commit**

```powershell
git add crates/hexeract-bus-rabbitmq/tests docs/reference/hexeract-bus-rabbitmq.md docs/operations/production-checklist.md
git commit -m "test(bus-rabbitmq): cover private CA mutual TLS"
```

## Plan self-review

- **Spec coverage:** Tasks 1–3 implement configuration, compatibility, safe diagnostics, and every connection path. Task 4 proves private-CA/mTLS behaviour and documents it. #449 and envelope authentication stay out of scope.
- **Placeholder scan:** Every task names files, methods, tests, commands, and expected outcomes.
- **Type consistency:** `RabbitMqConnectionConfig` is the shared input; `RabbitMqRequestClientConfig.connection_config` is the single request-client carrier; `OwnedTLSConfig` and `OwnedIdentity` are re-exported inputs.
