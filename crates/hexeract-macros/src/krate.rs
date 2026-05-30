//! Resolution of the crate path under which `hexeract-core` items are
//! reachable from the `#[handler]` macro call site.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::Ident;

/// Resolves the path prefix under which `hexeract-core` items are reachable
/// from the caller.
///
/// Two dependency layouts are supported:
///
/// - direct: the caller depends on `hexeract-core`, so items live at
///   `::hexeract_core::...`;
/// - umbrella: the caller depends only on the `hexeract` facade, which
///   re-exports the core crate as `hexeract::core`, so items live at
///   `::hexeract::core::...`.
///
/// The direct layout is preferred when both crates are present. When neither
/// is found in the caller manifest the function falls back to `::hexeract_core`,
/// preserving the historical behaviour.
pub(crate) fn core_root() -> TokenStream {
    if let Ok(found) = crate_name("hexeract-core") {
        return match found {
            FoundCrate::Itself => quote!(crate),
            FoundCrate::Name(name) => {
                let ident = Ident::new(&name, Span::call_site());
                quote!(::#ident)
            }
        };
    }
    if let Ok(found) = crate_name("hexeract") {
        return match found {
            FoundCrate::Itself => quote!(crate::core),
            FoundCrate::Name(name) => {
                let ident = Ident::new(&name, Span::call_site());
                quote!(::#ident::core)
            }
        };
    }
    quote!(::hexeract_core)
}
