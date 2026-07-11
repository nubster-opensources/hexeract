# Migration v0.5.0 to v0.6.0

v0.6.0 is the scheduler release. It ships the durable message scheduler as two new crates, `hexeract-scheduler` and `hexeract-scheduler-sql`, and adds a scheduler operator surface to `hexeract-cli`. Every change to a previously published crate is additive: no breaking API changes. Most of this guide is therefore about new CLI behavior to be aware of, not source edits you are forced to make.

If you are still on v0.4.x, apply the [v0.4 to v0.5 guide](migration-v0.4-v0.5.md) first.

## What changed

| Area | Change | Action required |
| --- | --- | --- |
| New crates | `hexeract-scheduler` and `hexeract-scheduler-sql` (PostgreSQL, MySQL, SQLite backends) | None; opt in when you want durable scheduling |
| `hexeract-cli` | Scheduler operator surface: `scheduler schema`, `list`, `inspect`, `dead-letter list`, `dead-letter replay` | None |
| `hexeract-cli` / `outbox apply` and `outbox check` | TLS is now required by default against PostgreSQL | Set `sslmode=disable` explicitly if you intentionally connect in plaintext (a warning is logged when you do) |
| `hexeract-cli` PostgreSQL TLS stack | Switched from the system TLS library to `rustls` with the OS trust store | Revalidate any enterprise or internal CA chain against the OS trust store |
| `hexeract-cli` / `bus peek` | Payloads are truncated to 1 KiB by default | Pass `--max-bytes N` or `--raw` if you rely on seeing the full payload |

## 1. Bump the crates

```toml
hexeract = { version = "0.6", features = ["mediator", "bus-rabbitmq", "outbox-sql-postgres"] }
```

Or, if you depend on the individual crates, bump each one you already use to `0.6`. No feature was renamed or removed, so this step is mechanical.

## 2. Adopting the scheduler (optional)

The scheduler is entirely new in v0.6.0 and does not require any change to code that does not use it. To start scheduling delayed or recurring messages, follow the [scheduler quick start](../getting-started/scheduler-quick-start.md), which walks through adding `hexeract-scheduler` and `hexeract-scheduler-sql`, applying the schema, and wiring a `SchedulerWorker`.

## 3. CLI behavior changes to know about

None of these are source-level breaks. They change what the `hexeract` binary does by default, so review them before you deploy v0.6.0 in a script or a CI pipeline that shells out to it.

### 3a. `outbox apply` and `outbox check` now require TLS by default

Both commands parse the `sslmode` of the connection string with `tokio_postgres::Config` (instead of a naive substring match) and upgrade every mode other than an explicit `disable` to `require`, connecting via `rustls`. A connection string with no `sslmode` at all, or `sslmode=prefer`, now negotiates TLS and fails the connection outright if the server declines it, rather than silently falling back to a cleartext session.

If you intentionally run these two commands against a database that does not offer TLS (a local development database, for instance), opt out explicitly:

```text
postgres://user:pass@localhost/db?sslmode=disable
```

A `tracing::warn` is logged whenever `sslmode=disable` takes effect, so the opt-out is visible in your logs rather than silent.

This does not apply to the `scheduler` admin commands (`list`, `inspect`, `dead-letter list`, `dead-letter replay`): they open their PostgreSQL pool through `sqlx` directly, which defaults to `sslmode=prefer` and can still fall back to plaintext. Set `sslmode=require` explicitly in `DATABASE_URL` for those commands if you need the same guarantee; see the [production checklist](production-checklist.md).

### 3b. The PostgreSQL TLS stack moved to `rustls`

The CLI's PostgreSQL connections (`outbox apply`, `outbox check`) now negotiate TLS through `rustls` with the `ring` crypto provider, validating the server certificate against the operating system's trust store (loaded via `rustls-native-certs`), instead of the previous system TLS library (`native-tls`/OpenSSL). Certificate-chain and hostname verification are `rustls`'s default behavior and are never disabled.

If your PostgreSQL server presents a certificate issued by an internal or enterprise CA, confirm that CA is present in the operating system's trust store on any machine that runs `hexeract outbox apply`/`check`: a CA that was only trusted through an application-level OpenSSL configuration may not carry over automatically.

### 3c. `bus peek` truncates payloads by default

`hexeract bus peek` now caps each printed payload at 1 KiB by default, appending a truncation marker when it cuts a message short. This closes a leak path where a peeked payload containing secrets or personal data would previously be dumped in full to a terminal, CI log or pipe.

- Pass `--max-bytes N` to raise the cap.
- Pass `--raw` to print the full, untruncated payload (only when every reader of the output is trusted with the whole message body).

Connection strings passed to any `bus` subcommand were already redacted before v0.6.0 and remain so: `--conn` is never echoed back in argv rendering, `Debug` output or connection error messages.

## Verification checklist

After the bump:

- [ ] `cargo build --workspace` succeeds.
- [ ] `cargo test --workspace --all-features` succeeds.
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` succeeds.
- [ ] Any script that calls `hexeract outbox apply`/`check` against a non-TLS database sets `sslmode=disable` explicitly, and you have reviewed the resulting warning.
- [ ] Any internal or enterprise CA used by a PostgreSQL server behind `outbox apply`/`check` is present in the OS trust store of the machine running the CLI.
- [ ] Any tooling that parses `hexeract bus peek` output for full payload bytes passes `--raw` or a large enough `--max-bytes`.
- [ ] `DATABASE_URL` used with `scheduler` admin commands sets `sslmode=require` explicitly if TLS is required in your environment.
