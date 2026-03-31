use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, Type, parse_macro_input};

use crate::util;

/// Derives `config_values(&self) -> Vec<String>` on a struct.
///
/// Returns one string per field, in declaration order, formatted for use as
/// an environment variable value:
/// - `PathBuf`  → `self.field.display().to_string()`
/// - `Vec<_>`   → `"[item1, item2, ...]"` (elements via `Display`)
/// - everything else → `self.field.to_string()` (requires `Display`)
pub fn derive_config_values(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_impl(&input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

fn derive_impl(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = &input.ident;
    let named_fields = util::extract_named_fields(input, "ConfigValues")?;

    let value_exprs: Vec<TokenStream2> = named_fields
        .iter()
        .filter_map(|field| {
            let ident = field.ident.as_ref()?;
            let ty = &field.ty;
            Some(if is_type_named(ty, "Vec") {
                quote! {
                    format!(
                        "[{}]",
                        self.#ident
                            .iter()
                            .map(|x| x.to_string())
                            .collect::<::std::vec::Vec<_>>()
                            .join(", ")
                    )
                }
            } else if is_type_named(ty, "PathBuf") {
                quote! { self.#ident.display().to_string() }
            } else {
                quote! { self.#ident.to_string() }
            })
        })
        .collect();

    Ok(quote! {
        impl #struct_name {
            /// Returns the current value of each configuration field as a string, in declaration order.
            pub fn config_values(&self) -> ::std::vec::Vec<::std::string::String> {
                vec![#(#value_exprs),*]
            }
        }
    })
}

fn is_type_named(ty: &Type, name: &str) -> bool {
    if let Type::Path(tp) = ty
        && let Some(last) = tp.path.segments.last()
    {
        return last.ident == name;
    }
    false
}
