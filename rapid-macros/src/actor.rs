use heck::{ToPascalCase, ToSnakeCase};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Expr, Fields, FnArg, Ident, ItemStruct, ItemTrait, Pat, Result as SResult, ReturnType, Token,
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
    let signals_name = format_ident!("{}Signals", handle_name);

    let generics = &trait_item.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    struct MethodInfo {
        name: Ident,
        variant: Ident,
        signal_field: Ident,
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
                signal_field: format_ident!("reply_{}", sig.ident),
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
                    #(#tys,)*
                    ::rapid_types::ResponseConsumer<#ret>,
                )
            }
        });

        quote! {
            pub enum #command_name #generics #where_clause {
                #(#variants),*
            }
        }
    };

    // 2. Generate Signals Struct (holds actual Signal values)
    let signals_struct = {
        let fields = methods.iter().map(|m| {
            let field_name = &m.signal_field;
            let ret = &m.ret;
            quote! {
                pub #field_name: ::embassy_sync::signal::Signal<
                    ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex,
                    #ret
                >
            }
        });

        let inits = methods.iter().map(|m| {
            let field_name = &m.signal_field;
            quote! { #field_name: ::embassy_sync::signal::Signal::new() }
        });

        quote! {
            pub struct #signals_name #generics #where_clause {
                #(#fields,)*
            }

            impl #impl_generics #signals_name #ty_generics #where_clause {
                pub const fn new() -> Self {
                    Self {
                        #(#inits,)*
                    }
                }
            }
        }
    };

    // 3. Generate Handle methods
    let handle_methods = methods.iter().map(|m| {
        let name = &m.name;
        let variant = &m.variant;
        let ret = &m.ret;
        let signal_field = &m.signal_field;
        let arg_names: Vec<_> = m.args.iter().map(|(n, _)| n).collect();
        let arg_types: Vec<_> = m.args.iter().map(|(_, t)| t).collect();

        quote! {
            pub async fn #name(&self #(, #arg_names: #arg_types)*) -> #ret {
                let _guard = self.call_lock.lock().await;
                self.signals.#signal_field.reset();

                self.cmd_tx
                    .send(#command_name::#variant(
                        #(#arg_names,)*
                        ::rapid_types::ResponseConsumer::<#ret>(&self.signals.#signal_field)
                    ))
                    .await;

                self.signals.#signal_field.wait().await
            }
        }
    });

    // 4. Generate Handle struct & ActorHandle impl
    let handle = quote! {
        #[derive(Clone)]
        pub struct #handle_name #generics #where_clause {
            cmd_tx: ::embassy_sync::channel::Sender<
                'static,
                ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex,
                #command_name #ty_generics,
                4,
            >,
            call_lock: &'static ::embassy_sync::mutex::Mutex<
                ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex,
                (),
            >,
            signals: &'static #signals_name #ty_generics,
        }

        impl #impl_generics #handle_name #ty_generics #where_clause {
            pub(crate) fn new(
                cmd_tx: ::embassy_sync::channel::Sender<
                    'static,
                    ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex,
                    #command_name #ty_generics,
                    4,
                >,
                call_lock: &'static ::embassy_sync::mutex::Mutex<
                    ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex,
                    (),
                >,
                signals: &'static #signals_name #ty_generics,
            ) -> Self {
                Self {
                    cmd_tx,
                    call_lock,
                    signals,
                }
            }

            #(#handle_methods)*
        }

        impl #impl_generics ::rapid_types::ActorHandle
            for #handle_name #ty_generics
            #where_clause
        {
            type Command = #command_name #ty_generics;
            type Signals = #signals_name #ty_generics;
        }
    };

    quote! {
        #command_enum
        #signals_struct
        #handle
    }
    .into()
}

pub fn expand_actor(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let mut struct_item = parse_macro_input!(input as ItemStruct);
    let handle_item = parse_macro_input!(attrs as Type);

    let (struct_field_names, struct_field_types) = match struct_item.fields {
        Fields::Named(ref fields) => {
            let names: Vec<_> = fields
                .named
                .iter()
                .map(|f| f.ident.clone().unwrap())
                .collect();
            let types: Vec<_> = fields.named.iter().map(|f| f.ty.clone()).collect();
            (names, types)
        }
        _ => panic!("#[actor] can only be used on structs with named fields"),
    };

    if let Fields::Named(ref mut fields) = struct_item.fields {
        // Inject channel
        fields.named.push(syn::parse_quote! {
            pub(crate) cmd_channel: ::embassy_sync::channel::Channel<
                ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex,
                <#handle_item as ::rapid_types::ActorHandle>::Command,
                4,
            >
        });

        // Inject mutex lock
        fields.named.push(syn::parse_quote! {
            pub(crate) call_lock: ::embassy_sync::mutex::Mutex<
                ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex,
                (),
            >
        });

        // Inject signals struct
        fields.named.push(syn::parse_quote! {
            pub(crate) signals: <#handle_item as ::rapid_types::ActorHandle>::Signals
        });
    }

    let struct_ident = &struct_item.ident;
    let (impl_generics, ty_generics, where_clause) = struct_item.generics.split_for_impl();

    quote! {
        #struct_item

        impl #impl_generics ::rapid_types::Actor for #struct_ident #ty_generics #where_clause {
            type Handle = #handle_item;
        }

        impl #impl_generics #struct_ident #ty_generics #where_clause {
            pub fn new(#(#struct_field_names: #struct_field_types),*) -> Self {
                Self {
                    #(#struct_field_names,)*
                    cmd_channel: ::embassy_sync::channel::Channel::new(),
                    call_lock: ::embassy_sync::mutex::Mutex::new(()),
                    signals: <<#handle_item as ::rapid_types::ActorHandle>::Signals>::new(),
                }
            }

            pub fn handle(&'static self) -> #handle_item {
                <#handle_item>::new(
                    self.cmd_channel.sender(),
                    &self.call_lock,
                    &self.signals,
                )
            }

            pub async fn next_command(&self) -> <#handle_item as ::rapid_types::ActorHandle>::Command {
                self.cmd_channel.receive().await
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
