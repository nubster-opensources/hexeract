//! The `outbox-sql-sqlite` facade feature must re-export the symbols named below through
//! the `hexeract` umbrella crate. A compilation is the whole check: removing a
//! re-export, or the feature that gates it, breaks this file.

#![cfg(feature = "outbox-sql-sqlite")]

use hexeract_umbrella_tests::resolve_types;

#[test]
fn outbox_sql_sqlite_feature_reexports_its_symbols() {
    resolve_types!(
        hexeract::outbox_sql::Dialect,
        hexeract::outbox_sql::SqliteOutboxStore,
        hexeract::outbox_sql::SqliteOutboxPublisher,
        hexeract::outbox_sql::SqliteOutboxWorkerBuilder,
    );
    let _: &str = hexeract::outbox_sql::DEFAULT_TABLE_NAME;
}
