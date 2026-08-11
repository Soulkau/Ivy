use heck::ToPascalCase;
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Expr, FnArg, Ident, ItemTrait, Pat, Result as SResult, ReturnType, Token, TraitItem, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

struct ActorMacroArgs {
    handle_ident: Ident,
}

impl Parse for ActorMacroArgs {
    fn parse(input: ParseStream) -> SResult<Self> {
        let handle_ident = input.parse()?;

        Ok(ActorMacroArgs { handle_ident })
    }
}

pub fn expand_handle(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let trait_item = parse_macro_input!(item as ItemTrait);
    let trait_name = &trait_item.ident;
    let command_name = format_ident!("{}Command", trait_name);
    let args = parse_macro_input!(attrs as ActorMacroArgs);
    let handle_name = args.handle_ident;

    let generics = &trait_item.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    struct MethodInfo {
        name: Ident,
        variant: Ident,
        args: Vec<(Ident, Type)>,
        ret: Type,
    }

    let methods: Vec<MethodInfo> = trait_item
        .items
        .iter()
        .filter_map(|item| match item {
            TraitItem::Fn(method) => Some(method),
            _ => None,
        })
        .map(|method| {
            let sig = &method.sig;
            let ret = match &sig.output {
                ReturnType::Default => syn::parse_quote!(()),
                ReturnType::Type(_, ty) => (**ty).clone(),
            };

            let args = sig
                .inputs
                .iter()
                .filter_map(|arg| match arg {
                    FnArg::Typed(pat) => match &*pat.pat {
                        Pat::Ident(id) => Some((id.ident.clone(), (*pat.ty).clone())),
                        _ => None,
                    },
                    _ => None,
                })
                .collect();

            MethodInfo {
                name: sig.ident.clone(),
                variant: format_ident!("{}", sig.ident.to_string().to_pascal_case()),
                args,
                ret,
            }
        })
        .collect();

    // 1. Generate Command Enum
    let command_enum = {
        let variants = methods.iter().map(|m| {
            let variant = &m.variant;
            let tys = m.args.iter().map(|(_, ty)| ty);
            let ret = &m.ret;

            quote! {
                #variant(
                    ::ivy_types::actor::ReplyConsumer<#ret>,
                    #(#tys,)*
                )
            }
        });

        quote! {
            pub enum #command_name #generics #where_clause {
                #(#variants),*
            }
        }
    };

    // 2. Generate Handle methods
    let handle_methods = methods.iter().map(|m| {
        let name = &m.name;
        let variant = &m.variant;
        let ret = &m.ret;
        let arg_names: Vec<_> = m.args.iter().map(|(n, _)| n).collect();
        let arg_types: Vec<_> = m.args.iter().map(|(_, t)| t).collect();

        quote! {
            pub async fn #name(&self #(, #arg_names: #arg_types)*) -> #ret {
                ::ivy_types::actor::rt::request(
                    &self.cmd_tx,
                    |c| #command_name::#variant(
                        c,
                        #(#arg_names,)*
                    )
                ).await
            }
        }
    });

    // 3. Generate Handle struct & ActorHandle impl
    let handle = quote! {
        #[derive(Clone)]
        pub struct #handle_name #generics #where_clause {
            cmd_tx: ::embassy_sync::channel::DynamicSender<
                'static,
                #command_name #ty_generics,
            >,
        }

        impl #impl_generics #handle_name #ty_generics #where_clause {


            #(#handle_methods)*
        }

        impl #impl_generics ::ivy_types::actor::ActorHandle
            for #handle_name #ty_generics
            #where_clause
        {
            type Cmd = #command_name #ty_generics;

            fn create(
                cmd_tx: ::embassy_sync::channel::DynamicSender<
                    'static,
                    #command_name #ty_generics,
                >,
            ) -> Self {
                Self {
                    cmd_tx,
                }
            }
        }
    };

    quote! {
        #command_enum
        #handle
    }
    .into()
}
