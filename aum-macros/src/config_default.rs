use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, Type, parse_macro_input};

use crate::util;

/// Derives `impl Default` on a struct using the `#[config_default = "..."]` attributes.
///
/// Each field must have either:
/// - `#[config_default = "..."]` — value string parsed according to the field's type, or
/// - `#[config_default_expr = "..."]` — a raw Rust expression emitted verbatim (takes precedence).
///
/// Type handling for `#[config_default]`:
/// - `String`, `PathBuf` → `.into()` from the string literal
/// - `bool` → `true` / `false` literal
/// - `u8`, `u16`, `u32`, `u64`, `usize` → suffixed integer literal
/// - `i8`, `i16`, `i32`, `i64`, `isize` → suffixed integer literal
/// - `f32`, `f64` → suffixed float literal
/// - `Vec<_>` → `Vec::new()` (value string is ignored)
/// - all other types (enums, etc.) → `<Type as Default>::default()`
pub fn derive_config_default(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_impl(&input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

fn derive_impl(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = &input.ident;
    let named_fields = util::extract_named_fields(input, "ConfigDefault")?;

    let mut field_inits = Vec::new();

    for field in named_fields {
        let Some(ident) = field.ident.as_ref() else {
            return Err(syn::Error::new_spanned(
                struct_name,
                "ConfigDefault: unnamed field in named struct",
            ));
        };
        let field_name_str = ident.to_string();
        let ty = &field.ty;

        let mut default_value: Option<String> = None;
        let mut default_expr: Option<String> = None;

        for attr in &field.attrs {
            if let Some(v) = util::get_attr_str(attr, "config_default_expr") {
                default_expr = Some(v);
            } else if let Some(v) = util::get_attr_str(attr, "config_default") {
                default_value = Some(v);
            }
        }

        let init = if let Some(expr_str) = default_expr {
            let tokens: TokenStream2 = expr_str.parse().unwrap_or_else(|e| {
                panic!("field `{field_name_str}`: invalid config_default_expr: {e}")
            });
            quote! { #ident: #tokens }
        } else if let Some(val) = default_value {
            let expr = make_default_expr(ty, &val, &field_name_str);
            quote! { #ident: #expr }
        } else {
            return Err(syn::Error::new_spanned(
                ident,
                format!(
                    "field `{field_name_str}` is missing #[config_default = \"...\"] attribute"
                ),
            ));
        };

        field_inits.push(init);
    }

    Ok(quote! {
        impl Default for #struct_name {
            fn default() -> Self {
                Self {
                    #(#field_inits),*
                }
            }
        }
    })
}

fn make_default_expr(ty: &Type, val: &str, field_name: &str) -> TokenStream2 {
    macro_rules! parse_num {
        ($t:ty, $lit_fn:ident) => {{
            let n: $t = val.parse().unwrap_or_else(|_| {
                panic!(
                    "field `{field_name}`: cannot parse {val:?} as {}",
                    stringify!($t)
                )
            });
            let lit = proc_macro2::Literal::$lit_fn(n);
            quote! { #lit }
        }};
    }

    let Type::Path(type_path) = ty else {
        return quote! { <#ty as ::std::default::Default>::default() };
    };

    let Some(last) = type_path.path.segments.last() else {
        return quote! { <#ty as ::std::default::Default>::default() };
    };
    match last.ident.to_string().as_str() {
        "String" => quote! { #val.into() },
        "PathBuf" => quote! { ::std::path::PathBuf::from(#val) },
        "bool" => {
            let b: bool = val
                .parse()
                .unwrap_or_else(|_| panic!("field `{field_name}`: cannot parse {val:?} as bool"));
            quote! { #b }
        }
        "u8" => parse_num!(u8, u8_suffixed),
        "u16" => parse_num!(u16, u16_suffixed),
        "u32" => parse_num!(u32, u32_suffixed),
        "u64" => parse_num!(u64, u64_suffixed),
        "usize" => parse_num!(usize, usize_suffixed),
        "i8" => parse_num!(i8, i8_suffixed),
        "i16" => parse_num!(i16, i16_suffixed),
        "i32" => parse_num!(i32, i32_suffixed),
        "i64" => parse_num!(i64, i64_suffixed),
        "isize" => parse_num!(isize, isize_suffixed),
        "f32" => parse_num!(f32, f32_suffixed),
        "f64" => parse_num!(f64, f64_suffixed),
        "Vec" => quote! { ::std::vec::Vec::new() },
        _ => quote! { <#ty as ::std::default::Default>::default() },
    }
}
