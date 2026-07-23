use proc_macro::TokenStream;

mod actor;

#[proc_macro_attribute]
pub fn actor_handle(attr: TokenStream, item: TokenStream) -> TokenStream {
    actor::expand_handle(attr, item)
}

#[proc_macro_attribute]
pub fn actor(attrs: TokenStream, input: TokenStream) -> TokenStream {
    actor::expand_actor(attrs, input)
}

#[proc_macro]
pub fn spawn_actor(input: TokenStream) -> TokenStream {
    actor::expand_spawn_actor(input)
}
