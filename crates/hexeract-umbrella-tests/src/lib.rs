//! Internal, unpublished crate whose integration tests verify the `hexeract`
//! umbrella crate feature by feature: each facade feature, enabled alone,
//! must compile and re-export the symbols it promises.

/// Resolve each listed type path, failing the compilation when one is missing.
///
/// A path that the `hexeract` facade no longer re-exports under the enabled
/// feature is a compile error at the call site, which is the whole check.
#[macro_export]
macro_rules! resolve_types {
    ($($path:ty),+ $(,)?) => {{
        $( let _: &'static str = ::std::any::type_name::<$path>(); )+
    }};
}
