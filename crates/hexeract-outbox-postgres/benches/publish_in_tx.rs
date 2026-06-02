#![allow(missing_docs)]
#![allow(deprecated)]

//! Criterion benchmark for [`PgOutboxPublisher::publish_in_tx`].
//!
//! Spins up a single PostgreSQL container via `testcontainers` and
//! measures the latency of one `publish_in_tx` call (insert inside an
//! already opened transaction, plus the commit on a fresh transaction
//! per iteration). The numbers should validate the v0.1.0 target of
//! `publish_in_tx p99 < 5 ms` on a developer machine.
//!
//! Requires a running Docker daemon. Run with:
//!
//! ```sh
//! cargo bench -p hexeract-outbox-postgres --bench publish_in_tx
//! ```

use std::sync::OnceLock;
use std::time::Duration;

use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use deadpool_postgres::Config;
use deadpool_postgres::Pool;
use deadpool_postgres::Runtime;
use hexeract_outbox::Event;
use hexeract_outbox::OutboxPublisher;
use hexeract_outbox_postgres::PgOutboxPublisher;
use hexeract_outbox_postgres::ensure_schema;
use serde::Deserialize;
use serde::Serialize;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::runtime::Builder as TokioBuilder;
use tokio_postgres::NoTls;
use uuid::Uuid;

const TABLE: &str = "audit_outbox";

#[derive(Debug, Serialize, Deserialize)]
struct BenchEvent {
    user_id: Uuid,
    email: String,
}

impl Event for BenchEvent {
    const EVENT_TYPE: &'static str = "bench.event";
}

struct Fixture {
    _container: ContainerAsync<Postgres>,
    pool: Pool,
    publisher: PgOutboxPublisher,
}

static FIXTURE: OnceLock<Fixture> = OnceLock::new();

async fn build_fixture() -> Fixture {
    let container = Postgres::default()
        .start()
        .await
        .expect("docker daemon must be running for this benchmark");
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let mut cfg = Config::new();
    cfg.host = Some(host);
    cfg.port = Some(port);
    cfg.user = Some("postgres".to_owned());
    cfg.password = Some("postgres".to_owned());
    cfg.dbname = Some("postgres".to_owned());

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    ensure_schema(&pool, TABLE).await.expect("schema apply");

    let publisher = PgOutboxPublisher::new(pool.clone(), TABLE).unwrap();

    Fixture {
        _container: container,
        pool,
        publisher,
    }
}

fn bench_publish_in_tx(c: &mut Criterion) {
    let rt = TokioBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let fixture = FIXTURE.get_or_init(|| rt.block_on(build_fixture()));

    let mut group = c.benchmark_group("publish_in_tx");
    group.sample_size(200);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("single", |b| {
        b.to_async(&rt).iter(|| async {
            let event = BenchEvent {
                user_id: Uuid::new_v4(),
                email: "bench@example.com".to_owned(),
            };
            let mut client = fixture.pool.get().await.unwrap();
            let mut tx = client.transaction().await.unwrap();
            let _event_id = fixture
                .publisher
                .publish_in_tx(&mut tx, &event)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_publish_in_tx);
criterion_main!(benches);
