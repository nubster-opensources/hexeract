//! Compile-fail UI tests for the `#[handler]` macro driven by `trybuild`.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/fail_missing_kind.rs");
    t.compile_fail("tests/ui/fail_unknown_kind.rs");
    t.compile_fail("tests/ui/fail_trait_impl.rs");
    t.compile_fail("tests/ui/fail_non_async_impl.rs");
    t.compile_fail("tests/ui/fail_non_async_free.rs");
    t.compile_fail("tests/ui/fail_wrong_arity.rs");
    t.compile_fail("tests/ui/fail_no_result_return.rs");
    t.compile_fail("tests/ui/fail_notification_non_unit.rs");
    t.compile_fail("tests/ui/fail_notification_not_arc.rs");
    t.compile_fail("tests/ui/fail_notification_arc_non_std.rs");
    t.compile_fail("tests/ui/fail_notification_arc_ref.rs");
    t.compile_fail("tests/ui/fail_notification_arc_arity.rs");
    t.pass("tests/ui/pass_notification_impl_arc.rs");
    t.compile_fail("tests/ui/fail_wrong_output_type.rs");
    t.compile_fail("tests/ui/fail_generic_impl_handler.rs");
    t.compile_fail("tests/ui/fail_lifetime_handler.rs");
    t.compile_fail("tests/ui/fail_reference_message.rs");
    t.compile_fail("tests/ui/fail_tuple_message.rs");
    t.compile_fail("tests/ui/fail_ctx_wrong_type.rs");
    t.compile_fail("tests/ui/fail_ctx_mut.rs");
    // #234: generated public struct must not trigger missing_docs in user crates
    t.pass("tests/ui/pass_pub_handler_missing_docs_allowed.rs");
}
