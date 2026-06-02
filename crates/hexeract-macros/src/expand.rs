//! Expansion of a parsed `#[handler]` item into the trait implementation
//! and the `inventory::submit!` registration.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Type;

use crate::krate;
use crate::parse::{HandlerFreeFn, HandlerImpl, HandlerItem, HandlerKindArg};

pub(crate) fn expand(item: HandlerItem) -> TokenStream {
    match item {
        HandlerItem::Impl(i) => expand_impl(i),
        HandlerItem::FreeFn(f) => expand_free_fn(f),
    }
}

fn expand_impl(input: HandlerImpl) -> TokenStream {
    let HandlerImpl {
        kind,
        item,
        message_ty,
        error_ty,
    } = input;
    let root = krate::core_root();
    let trait_ident = kind.trait_ident();
    let kind_variant = kind.variant_ident();
    let self_ty = &item.self_ty;
    let output_ty = handler_output_ty(kind, &message_ty, &root);
    let param_ty = handler_param_ty(kind, &message_ty);
    let registration = quote! {
        #root::registration::__private::inventory::submit!(
            #root::HandlerRegistration {
                kind: #root::HandlerKind::#kind_variant,
                message_type_name: ::core::any::type_name::<#message_ty>,
                handler_type_name: ::core::any::type_name::<#self_ty>,
            }
        );
    };

    quote! {
        #item

        impl #root::#trait_ident<#message_ty> for #self_ty {
            type Error = #error_ty;
            async fn handle(
                &self,
                msg: #param_ty,
                ctx: &#root::HandlerContext,
            ) -> ::core::result::Result<#output_ty, #error_ty> {
                self.handle(msg, ctx).await
            }
        }

        #registration
    }
}

fn expand_free_fn(input: HandlerFreeFn) -> TokenStream {
    let HandlerFreeFn {
        kind,
        item,
        handler_struct_ident,
        message_ty,
        error_ty,
    } = input;
    let root = krate::core_root();
    let trait_ident = kind.trait_ident();
    let kind_variant = kind.variant_ident();
    let fn_ident = &item.sig.ident;
    let fn_vis = &item.vis;
    let output_ty = handler_output_ty(kind, &message_ty, &root);
    let param_ty = handler_param_ty(kind, &message_ty);
    let registration = quote! {
        #root::registration::__private::inventory::submit!(
            #root::HandlerRegistration {
                kind: #root::HandlerKind::#kind_variant,
                message_type_name: ::core::any::type_name::<#message_ty>,
                handler_type_name: ::core::any::type_name::<#handler_struct_ident>,
            }
        );
    };

    quote! {
        #item

        #fn_vis struct #handler_struct_ident;

        impl #root::#trait_ident<#message_ty> for #handler_struct_ident {
            type Error = #error_ty;
            async fn handle(
                &self,
                msg: #param_ty,
                ctx: &#root::HandlerContext,
            ) -> ::core::result::Result<#output_ty, #error_ty> {
                #fn_ident(msg, ctx).await
            }
        }

        #registration
    }
}

/// Computes the handler output type used in the generated trait method
/// signature: `()` for notifications, otherwise the canonical
/// `<M as Command>::Output` / `<M as Query>::Output` associated type so the
/// signature always matches the trait rather than the user-written type.
fn handler_output_ty(kind: HandlerKindArg, message_ty: &Type, root: &TokenStream) -> TokenStream {
    if kind.is_notification() {
        quote!(())
    } else {
        let marker = kind.marker_trait_ident();
        quote!(<#message_ty as #root::#marker>::Output)
    }
}

/// Computes the message argument type of the generated `handle` method:
/// `Arc<N>` for notifications, which the mediator shares across handlers, and
/// the owned message type otherwise.
fn handler_param_ty(kind: HandlerKindArg, message_ty: &Type) -> TokenStream {
    if kind.is_notification() {
        quote!(::std::sync::Arc<#message_ty>)
    } else {
        quote!(#message_ty)
    }
}
