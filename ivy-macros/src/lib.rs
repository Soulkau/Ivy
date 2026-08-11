use proc_macro::TokenStream;

mod actor;

#[proc_macro_attribute]
pub fn actor_handle(attr: TokenStream, item: TokenStream) -> TokenStream {
    actor::expand_handle(attr, item)
}
