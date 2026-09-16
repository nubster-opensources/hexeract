//! Compile-fail UI test proving a transport cannot hand
//! [`hexeract_bus::RequestRegistry::resolve`] a shape rejection.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/fail_transport_passes_shape_rejection.rs");
}
