//! Procedural macros for the Hexeract messaging framework.
//!
//! This crate is a placeholder. The full implementation ships in v0.1.0.

extern crate proc_macro;
use proc_macro::TokenStream;

/// Placeholder for the `#[handler]` attribute macro.
///
/// The full implementation ships in v0.1.0 and will generate
/// `CommandHandler` / `QueryHandler` implementations and auto-register
/// the handler via `inventory`.
#[proc_macro_attribute]
pub fn handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
