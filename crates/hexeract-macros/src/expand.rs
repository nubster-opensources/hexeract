//! Expansion of a parsed `#[handler]` item into the trait implementation
//! and the `inventory::submit!` registration.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::Type;

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
        output_ty,
        error_ty,
    } = input;
    let trait_ident = kind.trait_ident();
    let kind_variant = kind.variant_ident();
    let self_ty = &item.self_ty;
    let return_inner = trait_return_inner(kind, &output_ty);
    let registration = quote! {
        ::hexeract_core::registration::__private::inventory::submit!(
            ::hexeract_core::HandlerRegistration {
                kind: ::hexeract_core::HandlerKind::#kind_variant,
                message_type_name: ::core::any::type_name::<#message_ty>,
                handler_type_name: ::core::any::type_name::<#self_ty>,
            }
        );
    };

    quote! {
        #item

        impl ::hexeract_core::#trait_ident<#message_ty> for #self_ty {
            type Error = #error_ty;
            async fn handle(
                &self,
                msg: #message_ty,
                ctx: &::hexeract_core::HandlerContext,
            ) -> ::core::result::Result<#return_inner, #error_ty> {
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
        output_ty,
        error_ty,
    } = input;
    let trait_ident = kind.trait_ident();
    let kind_variant = kind.variant_ident();
    let fn_ident = &item.sig.ident;
    let fn_vis = &item.vis;
    let return_inner = trait_return_inner(kind, &output_ty);
    let registration = quote! {
        ::hexeract_core::registration::__private::inventory::submit!(
            ::hexeract_core::HandlerRegistration {
                kind: ::hexeract_core::HandlerKind::#kind_variant,
                message_type_name: ::core::any::type_name::<#message_ty>,
                handler_type_name: ::core::any::type_name::<#handler_struct_ident>,
            }
        );
    };

    quote! {
        #item

        #fn_vis struct #handler_struct_ident;

        impl ::hexeract_core::#trait_ident<#message_ty> for #handler_struct_ident {
            type Error = #error_ty;
            async fn handle(
                &self,
                msg: #message_ty,
                ctx: &::hexeract_core::HandlerContext,
            ) -> ::core::result::Result<#return_inner, #error_ty> {
                #fn_ident(msg, ctx).await
            }
        }

        #registration
    }
}

fn trait_return_inner(kind: HandlerKindArg, output_ty: &Type) -> TokenStream {
    if kind.is_notification() {
        quote!(())
    } else {
        output_ty.to_token_stream()
    }
}
