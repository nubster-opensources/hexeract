# RabbitMQ Secure-Default TLS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject plaintext RabbitMQ connections outside an explicit local-development boundary.

**Architecture:** One URI-security gate runs before every connection attempt. It accepts AMQPS, permits only syntactically loopback AMQP by default, and makes remote plaintext require an explicit insecure setting on `RabbitMqConnectionConfig`; all transport and RPC paths already converge on this gate.

**Tech Stack:** Rust, lapin, tokio, testcontainers, rustdoc.

**Spec:** `docs/superpowers/specs/2026-09-01-rabbitmq-secure-default-tls-design.md`

## Global Constraints

- Do not resolve DNS to classify a host; only `localhost`, `127.0.0.0/8`, and `::1` are loopback.
- Never render a raw connection URI, credentials, certificate bytes, or passwords.
- `amqps://` keeps the #350 private-CA and mTLS behaviour unchanged.
- Remote plaintext needs the explicitly named `allow_insecure_plaintext_transport()` opt-in.
- Preserve the existing loopback `amqp://` testcontainers and example path without an opt-in.

---

### Task 1: Add one common plaintext security gate

**Files:**
- Modify: `crates/hexeract-bus-rabbitmq/src/connection.rs:272-414, 701-728`
- Test: `crates/hexeract-bus-rabbitmq/src/connection.rs:771-1118`

**Interfaces:**
- Produces `RabbitMqConnectionConfig::allow_insecure_plaintext_transport(self) -> Self`.
- Produces private `ensure_transport_security(uri: &str, config: &RabbitMqConnectionConfig) -> Result<(), BusError>`.
- Consumes existing `redact_uri`, `connection_error_with_kind`, `connect_once`, and both public/recovering connection paths.

- [ ] **Step 1: Write failing policy tests**

```rust
#[tokio::test]
async fn plaintext_loopback_forms_remain_usable() {
    for uri in ["amqp://localhost:1", "amqp://127.0.0.1:1", "amqp://[::1]:1"] {
        let error = RabbitMqConnection::connect(uri).await.expect_err("closed loopback port");
        assert_eq!(error.is_retryable_connection(), Some(true));
    }
}

#[tokio::test]
async fn plaintext_remote_host_is_refused_before_connecting() {
    let error = RabbitMqConnection::connect("amqp://user:secret@broker.example:5672")
        .await.expect_err("remote plaintext must be refused");
    assert_eq!(error.is_retryable_connection(), Some(false));
    assert!(!error.to_string().contains("secret"));
}
```

- [ ] **Step 2: Run the new tests and confirm the remote case fails**

Run: `cargo test -p hexeract-bus-rabbitmq --lib connection::tests::plaintext_remote_host_is_refused_before_connecting`

Expected: FAIL because current default policy attempts the remote plaintext connection.

- [ ] **Step 3: Implement the policy and the explicit override**

```rust
pub fn allow_insecure_plaintext_transport(mut self) -> Self {
    self.allows_plaintext_transport = true;
    self
}

fn ensure_transport_security(uri: &str, config: &RabbitMqConnectionConfig) -> Result<(), BusError> {
    // Parse only the scheme and host without resolving DNS.
    // Allow amqps; allow amqp only for localhost, 127/8, or ::1 unless the
    // explicit override is set; preserve the #350 TLS-material mismatch guard.
}

fn is_plaintext_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<std::net::IpAddr>().is_ok_and(|address| address.is_loopback())
}
```

Call the gate before `connect_once` in `connect_with_config` and before the
retry loop in `connect_with_retry_inner`, replacing the narrower
`ensure_tls_material_is_honoured`. Rename its existing public opt-out to
`allow_insecure_plaintext_transport` and update every rustdoc link and unit
test. Use `std::net::IpAddr` for literal addresses and reject unrecognised
schemes through `connection_error_with_kind` with a fixed reason.

- [ ] **Step 4: Add coverage for all branches**

```rust
#[tokio::test]
async fn explicit_insecure_opt_in_allows_remote_plaintext() {
    let config = RabbitMqConnectionConfig::default().allow_insecure_plaintext_transport();
    assert!(ensure_transport_security("amqp://broker.example:5672", &config).is_ok());
}

#[tokio::test]
async fn tls_material_still_requires_amqps_without_the_override() {
    let config = RabbitMqConnectionConfig::default().with_tls_config(mutual_tls_config());
    let error = ensure_transport_security("amqp://localhost:5672", &config).expect_err("TLS material must not be ignored");
    assert_eq!(error.is_retryable_connection(), Some(false));
}

#[test]
fn plaintext_host_classification_never_uses_dns() {
    assert!(!is_plaintext_loopback_host("dev-broker"));
}
```

Assert the retrying and recovering paths reject remote plaintext before any
attempt, retain the credential-redaction assertion, and use the new method
name in the existing #350 test.

- [ ] **Step 5: Verify Task 1**

Run: `cargo test -p hexeract-bus-rabbitmq --lib connection::tests`

Expected: all connection tests pass.

- [ ] **Step 6: Commit Task 1**

```bash
git add crates/hexeract-bus-rabbitmq/src/connection.rs
git commit -m "fix(bus-rabbitmq): require TLS for remote brokers"
```

### Task 2: Prove propagation and document the migration

**Files:**
- Modify: `crates/hexeract-bus-rabbitmq/tests/integration.rs`
- Modify: `docs/operations/production-checklist.md:45-55`
- Create: `docs/operations/migration-v0.6-v0.7.md`

**Interfaces:**
- Consumes the Task 1 security gate through the existing transport and RPC constructors.
- Produces a documented v0.7 migration path for remote plaintext users.

- [ ] **Step 1: Write an integration regression test for loopback plaintext**

```rust
#[tokio::test]
#[ignore = "requires Docker"]
async fn plaintext_loopback_transport_still_publishes() {
    let broker = harness::start_rabbitmq().await;
    let transport = RabbitMqTransport::new(broker.uri()).await.expect("loopback plaintext remains available");
    drop(transport);
}
```

- [ ] **Step 2: Run it against Docker**

Run: `cargo test -p hexeract-bus-rabbitmq --test integration plaintext_loopback_transport_still_publishes -- --ignored`

Expected: PASS on a Docker-capable host.

- [ ] **Step 3: Document the secure default**

Add checklist guidance stating that remote `amqp://` now fails before the
network call, `amqps://` is the production fix, and the insecure override is
development-only. Create the v0.6-to-v0.7 migration note with a before/after
example using `RabbitMqConnectionConfig::allow_insecure_plaintext_transport()`
only for a deliberately remote development broker; do not recommend it for
production.

- [ ] **Step 4: Verify Task 2 and the complete change**

Run:

```bash
cargo fmt --all -- --check
cargo clippy -p hexeract-bus-rabbitmq --all-targets --all-features -- -D warnings
cargo check -p hexeract-bus-rabbitmq --all-targets --all-features
cargo test -p hexeract-bus-rabbitmq --doc --all-features
git diff --check
```

Expected: all commands exit successfully. Run the ignored Docker test in CI if
Docker is unavailable locally, and record that limitation in the PR.

- [ ] **Step 5: Commit Task 2**

```bash
git add crates/hexeract-bus-rabbitmq/tests/integration.rs docs/operations/production-checklist.md docs/operations/migration-v0.6-v0.7.md
git commit -m "docs(bus-rabbitmq): explain secure TLS default"
```
