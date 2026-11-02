//! The `scheduler-sql-sqlite` facade feature must re-export the symbols named below through
//! the `hexeract` umbrella crate. A compilation is the whole check: removing a
//! re-export, or the feature that gates it, breaks this file.

use hexeract_umbrella_tests::resolve_types;

#[test]
fn scheduler_sql_sqlite_feature_reexports_its_symbols() {
    resolve_types!(
        hexeract::scheduler_sql::Dialect,
        hexeract::scheduler_sql::SqliteScheduleStore,
    );
    let _: &str = hexeract::scheduler_sql::DEFAULT_TABLE_NAME;
}
