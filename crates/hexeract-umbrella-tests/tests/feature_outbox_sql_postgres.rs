//! The `outbox-sql-postgres` facade feature must re-export the symbols named below through
//! the `hexeract` umbrella crate. A compilation is the whole check: removing a
//! re-export, or the feature that gates it, breaks this file.

#![cfg(feature = "outbox-sql-postgres")]

use hexeract_umbrella_tests::resolve_types;

#[test]
fn outbox_sql_postgres_feature_reexports_its_symbols() {
    resolve_types!(
        hexeract::outbox_sql::Dialect,
        hexeract::outbox_sql::PgOutboxStore,
        hexeract::outbox_sql::PgOutboxPublisher,
        hexeract::outbox_sql::PgOutboxWorkerBuilder,
    );
    let _: &str = hexeract::outbox_sql::DEFAULT_TABLE_NAME;
}
