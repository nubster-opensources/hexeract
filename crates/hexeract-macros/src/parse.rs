//! Parsing the `#[handler(kind)]` attribute and the annotated item.

use proc_macro2::{Span, TokenStream};
use syn::{
    FnArg, GenericArgument, GenericParam, Generics, Ident, ImplItem, ImplItemFn, ItemFn, ItemImpl,
    PathArguments, ReturnType, Signature, Type,
};

/// Kind argument parsed from `#[handler(command|query|notification)]`.
#[derive(Debug, Clone, Copy)]
pub(crate) enum HandlerKindArg {
    Command,
    Query,
    Notification,
}

impl HandlerKindArg {
    pub(crate) fn variant_ident(self) -> Ident {
        let s = match self {
            Self::Command => "Command",
            Self::Query => "Query",
            Self::Notification => "Notification",
        };
        Ident::new(s, Span::call_site())
    }

    pub(crate) fn trait_ident(self) -> Ident {
        let s = match self {
            Self::Command => "CommandHandler",
            Self::Query => "QueryHandler",
            Self::Notification => "NotificationHandler",
        };
        Ident::new(s, Span::call_site())
    }

    /// Identifier of the message marker trait carrying the `Output` associated
    /// type (`Command` or `Query`); `Notification` carries no output.
    pub(crate) fn marker_trait_ident(self) -> Ident {
        let s = match self {
            Self::Command => "Command",
            Self::Query => "Query",
            Self::Notification => "Notification",
        };
        Ident::new(s, Span::call_site())
    }

    pub(crate) fn is_notification(self) -> bool {
        matches!(self, Self::Notification)
    }
}

/// Annotated item, either an inherent `impl` block or a free `async fn`.
pub(crate) enum HandlerItem {
    Impl(HandlerImpl),
    FreeFn(HandlerFreeFn),
}

pub(crate) struct HandlerImpl {
    pub(crate) kind: HandlerKindArg,
    pub(crate) item: ItemImpl,
    pub(crate) message_ty: Type,
    pub(crate) error_ty: Type,
}

pub(crate) struct HandlerFreeFn {
    pub(crate) kind: HandlerKindArg,
    pub(crate) item: ItemFn,
    pub(crate) handler_struct_ident: Ident,
    pub(crate) message_ty: Type,
    pub(crate) error_ty: Type,
}

pub(crate) fn parse_kind(attr: TokenStream) -> syn::Result<HandlerKindArg> {
    if attr.is_empty() {
        return Err(syn::Error::new(
            Span::call_site(),
            "#[handler] requires a kind argument: #[handler(command)], #[handler(query)] or #[handler(notification)]",
        ));
    }
    let ident: Ident = syn::parse2(attr)?;
    match ident.to_string().as_str() {
        "command" => Ok(HandlerKindArg::Command),
        "query" => Ok(HandlerKindArg::Query),
        "notification" => Ok(HandlerKindArg::Notification),
        other => Err(syn::Error::new(
            ident.span(),
            format!(
                "unknown handler kind `{other}`; expected `command`, `query` or `notification`"
            ),
        )),
    }
}

pub(crate) fn parse_handler_item(
    kind: HandlerKindArg,
    item: TokenStream,
) -> syn::Result<HandlerItem> {
    if let Ok(item_impl) = syn::parse2::<ItemImpl>(item.clone()) {
        return parse_impl(kind, item_impl).map(HandlerItem::Impl);
    }
    let item_fn: ItemFn = syn::parse2(item).map_err(|err| {
        syn::Error::new(
            err.span(),
            "#[handler] must annotate an inherent `impl` block or a free `async fn`",
        )
    })?;
    parse_free_fn(kind, item_fn).map(HandlerItem::FreeFn)
}

fn parse_impl(kind: HandlerKindArg, item_impl: ItemImpl) -> syn::Result<HandlerImpl> {
    if item_impl.trait_.is_some() {
        return Err(syn::Error::new_spanned(
            &item_impl,
            "#[handler] must annotate a bare inherent impl, not a trait implementation",
        ));
    }
    reject_generics(&item_impl.generics)?;
    let handle_fn = item_impl
        .items
        .iter()
        .find_map(|i| match i {
            ImplItem::Fn(f) if f.sig.ident == "handle" => Some(f),
            _ => None,
        })
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &item_impl,
                "#[handler] impl block must contain an `async fn handle(&self, msg: M, ctx: &HandlerContext) -> Result<T, E>` method",
            )
        })?;

    if handle_fn.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &handle_fn.sig,
            "`handle` must be `async`",
        ));
    }
    let (message_ty, error_ty) = extract_method_signature(kind, handle_fn)?;
    Ok(HandlerImpl {
        kind,
        item: item_impl,
        message_ty,
        error_ty,
    })
}

fn parse_free_fn(kind: HandlerKindArg, item_fn: ItemFn) -> syn::Result<HandlerFreeFn> {
    if item_fn.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &item_fn.sig,
            "#[handler] free function must be `async`",
        ));
    }
    let (message_ty, error_ty) = extract_free_signature(kind, &item_fn.sig)?;
    let handler_struct_ident = pascal_case_handler(&item_fn.sig.ident);
    Ok(HandlerFreeFn {
        kind,
        item: item_fn,
        handler_struct_ident,
        message_ty,
        error_ty,
    })
}

fn extract_method_signature(
    kind: HandlerKindArg,
    handle_fn: &ImplItemFn,
) -> syn::Result<(Type, Type)> {
    let sig = &handle_fn.sig;
    reject_generics(&sig.generics)?;
    let mut inputs = sig.inputs.iter();
    match inputs.next() {
        Some(FnArg::Receiver(r)) if r.reference.is_some() && r.mutability.is_none() => {}
        Some(other) => {
            return Err(syn::Error::new_spanned(
                other,
                "`handle` must take `&self` as first argument",
            ));
        }
        None => {
            return Err(syn::Error::new_spanned(
                sig,
                "`handle` must take `&self` as first argument",
            ));
        }
    }
    let msg_arg = inputs.next().ok_or_else(|| {
        syn::Error::new_spanned(sig, "`handle` must take a message argument `msg: M`")
    })?;
    let message_ty = message_type(kind, msg_arg)?;
    let ctx_arg = inputs
        .next()
        .ok_or_else(|| syn::Error::new_spanned(sig, "`handle` must take `ctx: &HandlerContext`"))?;
    validate_ctx_arg(ctx_arg)?;
    if inputs.next().is_some() {
        return Err(syn::Error::new_spanned(
            sig,
            "`handle` must take exactly 3 arguments",
        ));
    }
    let error_ty = extract_result_error(kind, sig)?;
    Ok((message_ty, error_ty))
}

fn extract_free_signature(kind: HandlerKindArg, sig: &Signature) -> syn::Result<(Type, Type)> {
    reject_generics(&sig.generics)?;
    let mut inputs = sig.inputs.iter();
    let msg_arg = inputs.next().ok_or_else(|| {
        syn::Error::new_spanned(
            sig,
            "function must take `msg: M` and `ctx: &HandlerContext`",
        )
    })?;
    let message_ty = message_type(kind, msg_arg)?;
    let ctx_arg = inputs.next().ok_or_else(|| {
        syn::Error::new_spanned(sig, "function must take a `ctx: &HandlerContext` argument")
    })?;
    validate_ctx_arg(ctx_arg)?;
    if inputs.next().is_some() {
        return Err(syn::Error::new_spanned(
            sig,
            "function must take exactly 2 arguments",
        ));
    }
    let error_ty = extract_result_error(kind, sig)?;
    Ok((message_ty, error_ty))
}

fn typed_arg_type(arg: &FnArg, expected: &str) -> syn::Result<Type> {
    match arg {
        FnArg::Typed(t) => Ok((*t.ty).clone()),
        FnArg::Receiver(_) => Err(syn::Error::new_spanned(
            arg,
            format!("expected typed argument `{expected}`"),
        )),
    }
}

/// Rejects any generic parameters or where-clause: the macro generates a
/// separate trait impl that does not carry the user's generics, so generic
/// handlers cannot be expanded correctly. Generic handler support is a future
/// feature, out of scope for this macro. A lifetime-only handler is reported
/// with a dedicated message since the failure mode is identical but less
/// obvious.
fn reject_generics(generics: &Generics) -> syn::Result<()> {
    if generics.lt_token.is_some() || !generics.params.is_empty() {
        let only_lifetimes = !generics.params.is_empty()
            && generics
                .params
                .iter()
                .all(|p| matches!(p, GenericParam::Lifetime(_)));
        let message = if only_lifetimes {
            "#[handler] does not support handlers with lifetime parameters: the generated trait impl cannot carry them"
        } else {
            "#[handler] does not support generic handlers"
        };
        return Err(syn::Error::new_spanned(generics, message));
    }
    if let Some(where_clause) = &generics.where_clause {
        return Err(syn::Error::new_spanned(
            where_clause,
            "#[handler] does not support generic handlers",
        ));
    }
    Ok(())
}

/// Extracts the message type from the message argument. Only an owned,
/// path-named message type can implement `Command`/`Query`/`Notification`, so a
/// reference (`&T`) is reported as such and any other shape (tuple, array,
/// slice) is reported as not being a named type.
fn owned_message_type(arg: &FnArg) -> syn::Result<Type> {
    let ty = typed_arg_type(arg, "msg: M")?;
    match &ty {
        Type::Path(_) => Ok(ty),
        Type::Reference(_) => Err(syn::Error::new_spanned(
            &ty,
            "the message argument must be an owned message type, not a reference",
        )),
        other => Err(syn::Error::new_spanned(
            other,
            "the message argument must be an owned, named message type",
        )),
    }
}

/// Extracts the message type the generated impl is parameterized over. Command
/// and query handlers receive the message by value, so the type is the argument
/// itself. Notification handlers receive the notification shared as `Arc<N>`,
/// so the message type is the single generic argument of that `Arc`.
fn message_type(kind: HandlerKindArg, arg: &FnArg) -> syn::Result<Type> {
    if kind.is_notification() {
        notification_message_type(arg)
    } else {
        owned_message_type(arg)
    }
}

/// Extracts `N` from a notification handler argument `Arc<N>`. The mediator
/// shares the notification across handlers, so the argument must be an `Arc` of
/// a named notification type.
fn notification_message_type(arg: &FnArg) -> syn::Result<Type> {
    let ty = typed_arg_type(arg, "msg: Arc<N>")?;
    let Type::Path(tp) = &ty else {
        return Err(syn::Error::new_spanned(
            &ty,
            "a notification handler argument must be `Arc<N>`, since the notification is shared across handlers",
        ));
    };
    let last = tp.path.segments.last().ok_or_else(|| {
        syn::Error::new_spanned(&ty, "a notification handler argument must be `Arc<N>`")
    })?;
    if last.ident != "Arc" {
        return Err(syn::Error::new_spanned(
            &ty,
            "a notification handler argument must be `Arc<N>`, since the notification is shared across handlers",
        ));
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else {
        return Err(syn::Error::new_spanned(
            &ty,
            "`Arc` must have exactly one type argument: `Arc<N>`",
        ));
    };
    let mut generics = args.args.iter();
    let (Some(GenericArgument::Type(inner)), None) = (generics.next(), generics.next()) else {
        return Err(syn::Error::new_spanned(
            &ty,
            "`Arc` must have exactly one type argument: `Arc<N>`",
        ));
    };
    match inner {
        Type::Path(_) => Ok(inner.clone()),
        other => Err(syn::Error::new_spanned(
            other,
            "the notification type inside `Arc<...>` must be a named type",
        )),
    }
}

/// Validates that the context argument is `ctx: &HandlerContext`: a shared
/// reference to a path type whose final segment is `HandlerContext`. A mutable
/// reference is rejected with a dedicated message, since the generated trait
/// signature takes the context by shared reference.
fn validate_ctx_arg(arg: &FnArg) -> syn::Result<()> {
    let ty = typed_arg_type(arg, "ctx: &HandlerContext")?;
    if let Type::Reference(reference) = &ty {
        if let Type::Path(path) = &*reference.elem {
            if path
                .path
                .segments
                .last()
                .is_some_and(|seg| seg.ident == "HandlerContext")
            {
                if reference.mutability.is_some() {
                    return Err(syn::Error::new_spanned(
                        &ty,
                        "the context must be taken by shared reference `&HandlerContext`, not `&mut`",
                    ));
                }
                return Ok(());
            }
        }
    }
    Err(syn::Error::new_spanned(
        &ty,
        "the second argument must be `ctx: &HandlerContext`",
    ))
}

fn extract_result_error(kind: HandlerKindArg, sig: &Signature) -> syn::Result<Type> {
    let return_ty = match &sig.output {
        ReturnType::Type(_, ty) => &**ty,
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                sig,
                "handler must return `Result<Output, Error>`",
            ));
        }
    };
    let Type::Path(tp) = return_ty else {
        return Err(syn::Error::new_spanned(
            return_ty,
            "expected `Result<Output, Error>`",
        ));
    };
    let Some(last_seg) = tp.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            return_ty,
            "expected `Result<Output, Error>`",
        ));
    };
    if last_seg.ident != "Result" {
        return Err(syn::Error::new_spanned(
            return_ty,
            "return type must be a `Result`",
        ));
    }
    let PathArguments::AngleBracketed(args) = &last_seg.arguments else {
        return Err(syn::Error::new_spanned(
            return_ty,
            "`Result` must have two type arguments",
        ));
    };
    if args.args.len() != 2 {
        return Err(syn::Error::new_spanned(
            return_ty,
            "`Result` must have exactly two type arguments",
        ));
    }
    let mut g = args.args.iter();
    let GenericArgument::Type(output_ty) = g.next().expect("len was checked") else {
        return Err(syn::Error::new_spanned(
            return_ty,
            "first `Result` type argument must be a type",
        ));
    };
    let GenericArgument::Type(error_ty) = g.next().expect("len was checked") else {
        return Err(syn::Error::new_spanned(
            return_ty,
            "second `Result` type argument must be a type",
        ));
    };
    if kind.is_notification() && !is_unit_type(output_ty) {
        return Err(syn::Error::new_spanned(
            output_ty,
            "a notification handler must return `Result<(), Error>`",
        ));
    }
    Ok(error_ty.clone())
}

fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(t) if t.elems.is_empty())
}

fn pascal_case_handler(ident: &Ident) -> Ident {
    let s = ident.to_string();
    let mut pascal = String::with_capacity(s.len() + 7);
    let mut next_upper = true;
    for c in s.chars() {
        if c == '_' {
            next_upper = true;
        } else if next_upper {
            pascal.extend(c.to_uppercase());
            next_upper = false;
        } else {
            pascal.push(c);
        }
    }
    pascal.push_str("Handler");
    Ident::new(&pascal, ident.span())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn parse_kind_rejects_empty_attr() {
        let err = parse_kind(quote!()).expect_err("empty attr must fail");
        assert!(err.to_string().contains("requires a kind argument"));
    }

    #[test]
    fn parse_kind_accepts_command_query_notification() {
        assert!(matches!(
            parse_kind(quote!(command)).unwrap(),
            HandlerKindArg::Command
        ));
        assert!(matches!(
            parse_kind(quote!(query)).unwrap(),
            HandlerKindArg::Query
        ));
        assert!(matches!(
            parse_kind(quote!(notification)).unwrap(),
            HandlerKindArg::Notification
        ));
    }

    #[test]
    fn parse_kind_rejects_unknown_word() {
        let err = parse_kind(quote!(event)).expect_err("unknown kind must fail");
        assert!(err.to_string().contains("unknown handler kind"));
    }

    #[test]
    fn parse_handler_item_accepts_inherent_impl_with_async_handle() {
        let item = quote! {
            impl GreetHandler {
                async fn handle(&self, cmd: Greet, ctx: &HandlerContext) -> Result<String, MyError> {
                    Ok(format!("hello {}", cmd.name))
                }
            }
        };
        let parsed = match parse_handler_item(HandlerKindArg::Command, item) {
            Ok(p) => p,
            Err(err) => panic!("must parse: {err}"),
        };
        match parsed {
            HandlerItem::Impl(_) => {}
            HandlerItem::FreeFn(_) => panic!("must be parsed as Impl"),
        }
    }

    #[test]
    fn parse_handler_item_accepts_free_async_fn() {
        let item = quote! {
            async fn list_users(q: ListUsers, ctx: &HandlerContext) -> Result<Vec<User>, MyError> {
                Ok(Vec::new())
            }
        };
        let parsed = match parse_handler_item(HandlerKindArg::Query, item) {
            Ok(p) => p,
            Err(err) => panic!("must parse: {err}"),
        };
        match parsed {
            HandlerItem::FreeFn(f) => {
                assert_eq!(f.handler_struct_ident.to_string(), "ListUsersHandler");
            }
            HandlerItem::Impl(_) => panic!("must be parsed as FreeFn"),
        }
    }

    fn expect_parse_err(
        kind: HandlerKindArg,
        item: proc_macro2::TokenStream,
        msg: &str,
    ) -> syn::Error {
        match parse_handler_item(kind, item) {
            Ok(_) => panic!("{msg}"),
            Err(err) => err,
        }
    }

    #[test]
    fn parse_handler_item_rejects_trait_impl() {
        let item = quote! {
            impl CommandHandler<Greet> for GreetHandler {
                type Error = MyError;
                async fn handle(&self, cmd: Greet, ctx: &HandlerContext) -> Result<String, MyError> {
                    Ok(String::new())
                }
            }
        };
        let err = expect_parse_err(
            HandlerKindArg::Command,
            item,
            "trait impls must be rejected",
        );
        assert!(err.to_string().contains("bare inherent impl"));
    }

    #[test]
    fn parse_handler_item_rejects_non_async_handle() {
        let item = quote! {
            impl H {
                fn handle(&self, cmd: C, ctx: &HandlerContext) -> Result<(), E> { Ok(()) }
            }
        };
        let err = expect_parse_err(HandlerKindArg::Command, item, "non-async handle must fail");
        assert!(err.to_string().contains("must be `async`"));
    }

    #[test]
    fn parse_handler_item_rejects_non_async_free_fn() {
        let item = quote! {
            fn list_users(q: ListUsers, ctx: &HandlerContext) -> Result<Vec<User>, MyError> {
                Ok(Vec::new())
            }
        };
        let err = expect_parse_err(HandlerKindArg::Query, item, "non-async free fn must fail");
        assert!(err.to_string().contains("must be `async`"));
    }

    #[test]
    fn parse_handler_item_rejects_wrong_arity_impl() {
        let item = quote! {
            impl H {
                async fn handle(&self, cmd: C) -> Result<(), E> { Ok(()) }
            }
        };
        let err = expect_parse_err(HandlerKindArg::Command, item, "missing ctx arg must fail");
        assert!(err.to_string().contains("HandlerContext"));
    }

    #[test]
    fn parse_handler_item_rejects_no_result_return() {
        let item = quote! {
            impl H {
                async fn handle(&self, cmd: C, ctx: &HandlerContext) -> Vec<u8> { vec![] }
            }
        };
        let err = expect_parse_err(HandlerKindArg::Command, item, "non-Result return must fail");
        assert!(err.to_string().contains("Result"));
    }

    #[test]
    fn parse_handler_item_rejects_notification_with_non_unit_output() {
        let item = quote! {
            impl H {
                async fn handle(&self, n: std::sync::Arc<N>, ctx: &HandlerContext) -> Result<i32, E> { Ok(0) }
            }
        };
        let err = expect_parse_err(
            HandlerKindArg::Notification,
            item,
            "notification must return Result<(), _>",
        );
        assert!(err.to_string().contains("notification handler must return"));
    }

    #[test]
    fn parse_handler_item_rejects_notification_message_not_wrapped_in_arc() {
        let item = quote! {
            impl H {
                async fn handle(&self, n: UserCreated, ctx: &HandlerContext) -> Result<(), E> { Ok(()) }
            }
        };
        let err = expect_parse_err(
            HandlerKindArg::Notification,
            item,
            "notification message must be Arc<N>",
        );
        assert!(err.to_string().contains("Arc<N>"));
    }

    #[test]
    fn parse_handler_item_accepts_notification_with_arc_message() {
        let item = quote! {
            async fn audit(n: std::sync::Arc<UserCreated>, ctx: &HandlerContext) -> Result<(), MyError> {
                Ok(())
            }
        };
        let parsed = match parse_handler_item(HandlerKindArg::Notification, item) {
            Ok(p) => p,
            Err(err) => panic!("must parse: {err}"),
        };
        match parsed {
            HandlerItem::FreeFn(f) => {
                assert_eq!(f.handler_struct_ident.to_string(), "AuditHandler")
            }
            HandlerItem::Impl(_) => panic!("must be parsed as FreeFn"),
        }
    }

    #[test]
    fn pascal_case_handler_converts_snake_case() {
        let id = Ident::new("create_user", Span::call_site());
        assert_eq!(pascal_case_handler(&id).to_string(), "CreateUserHandler");
        let id = Ident::new("list", Span::call_site());
        assert_eq!(pascal_case_handler(&id).to_string(), "ListHandler");
        let id = Ident::new("send_audit_log", Span::call_site());
        assert_eq!(pascal_case_handler(&id).to_string(), "SendAuditLogHandler");
    }
}
