//! Procedural macros for the Hexeract messaging framework.
//!
//! Exposes the `#[handler]` attribute that generates `CommandHandler`,
//! `QueryHandler` or `NotificationHandler` implementations and submits
//! a `HandlerRegistration` entry to the `inventory` collector for
//! `MediatorBuilder::verify_handlers`.

extern crate proc_macro;

use proc_macro::TokenStream;

mod expand;
mod krate;
mod parse;

/// Attribute macro that wires a handler into the Hexeract registry.
///
/// # Syntax
///
/// ```ignore
/// #[handler(command)]
/// impl MyHandler {
///     async fn handle(&self, cmd: CreateUser, ctx: &HandlerContext) -> Result<UserId, MyError> { ... }
/// }
///
/// #[handler(query)]
/// async fn list_users(q: ListUsers, ctx: &HandlerContext) -> Result<Vec<User>, MyError> { ... }
///
/// #[handler(notification)]
/// async fn audit(n: UserCreated, ctx: &HandlerContext) -> Result<(), MyError> { ... }
/// ```
///
/// The kind is mandatory and must be one of `command`, `query` or
/// `notification`. The macro generates the matching trait
/// implementation, plus a unit struct wrapper for the free function
/// variant, and submits a [`HandlerRegistration`] entry via
/// [`inventory::submit!`] for [`MediatorBuilder::verify_handlers`].
///
/// [`HandlerRegistration`]: hexeract_core::HandlerRegistration
/// [`MediatorBuilder::verify_handlers`]: hexeract_mediator::MediatorBuilder::verify_handlers
/// [`inventory::submit!`]: https://docs.rs/inventory/latest/inventory/macro.submit.html
#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    match try_handler(attr.into(), item.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn try_handler(
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let kind = parse::parse_kind(attr)?;
    let parsed = parse::parse_handler_item(kind, item)?;
    Ok(expand::expand(parsed))
}
