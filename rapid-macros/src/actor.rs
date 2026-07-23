use heck::{ToPascalCase, ToSnakeCase};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Expr, FnArg, Ident, ItemStruct, ItemTrait, Pat, Result as SResult, ReturnType, Token,
    TraitItem, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    token::Struct,
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

fn channel_fn_name() -> Ident {
    format_ident!("__actor_channel")
}

pub fn expand_handle(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let trait_item = parse_macro_input!(item as ItemTrait);
    let trait_name = &trait_item.ident;
    let command_name = format_ident!("{}Command", trait_name);
    let args = parse_macro_input!(attrs as ActorMacroArgs);

    let handle_name = args.handle_ident;
    let channel_fn_name = channel_fn_name();
    let lock_fn_name = format_ident!("__actor_call_lock");

    struct MethodInfo {
        name: syn::Ident,
        variant_name: syn::Ident,
        args: Vec<(syn::Ident, Type)>,
        ret: Type,
    }

    let mut methods = Vec::new();

    for trait_item in &trait_item.items {
        if let TraitItem::Fn(method) = trait_item {
            let sig = &method.sig;
            let name = sig.ident.clone();
            let variant_name = format_ident!("{}", &name.to_string().to_pascal_case());

            let ret = match &sig.output {
                ReturnType::Type(_, ty) => (**ty).clone(),
                ReturnType::Default => syn::parse_quote! { () },
            };

            let mut args = Vec::new();
            for input_arg in &sig.inputs {
                if let FnArg::Typed(pat_type) = input_arg {
                    if let Pat::Ident(pat_ident) = &*pat_type.pat {
                        args.push((pat_ident.ident.clone(), (*pat_type.ty).clone()));
                    }
                }
            }

            methods.push(MethodInfo {
                name,
                variant_name,
                args,
                ret,
            });
        }
    }

    // each variant carries a &'static Signal<...> instead of a Sender for the reply
    let enum_variants = methods.iter().map(|m| {
        let variant = &m.variant_name;
        let arg_types = m.args.iter().map(|(_, ty)| ty);
        let ret = &m.ret;
        quote! {
            #variant(
                #(#arg_types,)*
                ::rapid_types::ResponseConsumer<#ret>,
            )
        }
    });

    let handle_methods = methods.iter().map(|m| {
        let name = &m.name;
        let variant = &m.variant_name;
        let ret = &m.ret;
        let arg_names: Vec<_> = m.args.iter().map(|(n, _)| n).collect();
        let arg_types: Vec<_> = m.args.iter().map(|(_, t)| t).collect();
        let reply_static = format_ident!("REPLY_{}", m.name.to_string().to_uppercase());

        quote! {
            pub async fn #name(&self #(, #arg_names: #arg_types)*) -> #ret {
                static #reply_static: ::embassy_sync::signal::Signal<
                    ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex, #ret
                > = ::embassy_sync::signal::Signal::new();

                // serialize concurrent callers so they don't share the static reply slot
                let _guard = self.call_lock.lock().await;
                #reply_static.reset();

                self.cmd_tx.send(#command_name::#variant(#(#arg_names,)* ::rapid_types::ResponseConsumer::<#ret>(&#reply_static))).await;
                #reply_static.wait().await
            }
        }
    });

    let trait_expanded = quote! {
        #trait_item


        pub enum #command_name {
            #(#enum_variants),*
        }

        #[derive(Clone)]
        pub struct #handle_name {
            cmd_tx: ::embassy_sync::channel::Sender<'static, ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex, #command_name, 4>,
            call_lock: &'static ::embassy_sync::mutex::Mutex<::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex, ()>,
        }

        impl #handle_name {
            pub fn from_static_parts() -> Self {
                Self {
                    cmd_tx: Self::#channel_fn_name().sender(),
                    call_lock: Self::#lock_fn_name(),
                }
            }

            #(#handle_methods)*

            fn #channel_fn_name() -> &'static ::embassy_sync::channel::Channel<::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex, #command_name, 4> {
                static CHANNEL: ::embassy_sync::channel::Channel<
                ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex, #command_name, 4
                    > = ::embassy_sync::channel::Channel::new();
                &CHANNEL
            }

            fn #lock_fn_name() -> &'static ::embassy_sync::mutex::Mutex<::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex, ()> {
                static LOCK: ::embassy_sync::mutex::Mutex<
                ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex, ()
                > = ::embassy_sync::mutex::Mutex::new(());
                &LOCK
            }
        }

        impl ::rapid_types::ActorHandle for #handle_name {
            type Command = #command_name;
        }

    };

    trait_expanded.into()
}

pub fn expand_actor(attrs: TokenStream, input: TokenStream) -> TokenStream {
    // 1. Parse the input as an ItemStruct instead of a Type
    let struct_item = parse_macro_input!(input as ItemStruct);
    let handle_item = parse_macro_input!(attrs as Type);
    let channel_fn_name = channel_fn_name();

    // 2. Extract the struct's name and split its generics for the impl blocks
    let struct_ident = &struct_item.ident;
    let (impl_generics, ty_generics, where_clause) = struct_item.generics.split_for_impl();

    quote! {
        // 3. Re-emit the original struct so it doesn't get erased from your code
        #struct_item

        // 4. Use the identifier and split generics for the trait impl
        impl #impl_generics ::rapid_types::Actor for #struct_ident #ty_generics #where_clause {
            type Handle = #handle_item;
        }

        // 5. Use the same generics setup for the inherent impl
        impl #impl_generics #struct_ident #ty_generics #where_clause {
            pub async fn next_command() -> <#handle_item as ::rapid_types::ActorHandle>::Command {
                let receiver = #handle_item::#channel_fn_name().receiver();
                receiver.receive().await
            }
        }
    }
    .into()
}

struct SpawnInput {
    spawner: Expr,
    actor_type: Type,
    actor: Expr,
}

impl Parse for SpawnInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let spawner: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let actor_type: Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let actor: Expr = input.parse()?;

        Ok(SpawnInput {
            spawner,
            actor_type,
            actor,
        })
    }
}

pub fn expand_spawn_actor(input: TokenStream) -> TokenStream {
    let SpawnInput {
        spawner,
        actor_type,
        actor,
    } = parse_macro_input!(input as SpawnInput);

    let expanded = quote! {
        {
            let actor_inst = #actor;

            // Define an inline monomorphic task for this exact concrete type
            #[::embassy_executor::task]
            async fn __embassy_actor_task(mut instance: #actor_type) -> ! {
                // Fully qualified trait call: the compiler infers the concrete type for `_`
                // Swap out `SomeTrait` for your actual trait path (e.g., ::rapid_types::Actor)
                < _ as Runnable>::run(instance).await
            }

            // Spawn the task
            #spawner.must_spawn(__embassy_actor_task(actor_inst));

            // Return the handle type
            <#actor_type as Actor>::Handle::from_static_parts()
        }
    };

    expanded.into()
}
