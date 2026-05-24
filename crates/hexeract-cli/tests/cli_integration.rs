//! CLI integration tests.
//!
//! The `patch` test runs without external dependencies. The `apply` and
//! `check` tests require a running Docker daemon for `testcontainers`
//! and are therefore marked `#[ignore]`. To run them:
//!
//! ```sh
//! cargo test -p hexeract-cli -- --ignored
//! ```

use assert_cmd::Command;
use predicates::str::contains;

#[test]
fn patch_prints_canonical_schema_to_stdout() {
    Command::cargo_bin("hexeract")
        .unwrap()
        .args(["outbox", "patch", "--table", "audit_outbox"])
        .assert()
        .success()
        .stdout(contains("CREATE TABLE IF NOT EXISTS audit_outbox"));
}

#[test]
fn patch_with_invalid_table_name_fails() {
    Command::cargo_bin("hexeract")
        .unwrap()
        .args(["outbox", "patch", "--table", "bad name"])
        .assert()
        .failure();
}

#[test]
fn apply_without_confirmation_flag_refuses_with_exit_code_2() {
    Command::cargo_bin("hexeract")
        .unwrap()
        .args([
            "outbox",
            "apply",
            "--conn",
            "postgres://nobody@127.0.0.1:1/none",
            "--table",
            "audit_outbox",
        ])
        .assert()
        .failure()
        .code(2)
        .stderr(contains("--yes-i-know"));
}
