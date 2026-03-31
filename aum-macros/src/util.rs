//! Shared utilities for the aum proc macro implementations.

use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{Attribute, Data, DeriveInput, Expr, Fields, Lit, Meta};

/// Extracts the named fields from a struct `DeriveInput`, or returns a `syn::Error`.
pub fn extract_named_fields<'a>(
    input: &'a DeriveInput,
    macro_name: &str,
) -> syn::Result<&'a Punctuated<syn::Field, Comma>> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => Ok(&fields.named),
            _ => Err(syn::Error::new_spanned(
                &input.ident,
                format!("{macro_name} only supports structs with named fields"),
            )),
        },
        _ => Err(syn::Error::new_spanned(
            &input.ident,
            format!("{macro_name} only supports structs"),
        )),
    }
}

/// Extracts the string value from a `#[name = "value"]` style attribute.
pub fn get_attr_str(attr: &Attribute, name: &str) -> Option<String> {
    if attr.path().is_ident(name)
        && let Meta::NameValue(nv) = &attr.meta
        && let Expr::Lit(expr_lit) = &nv.value
        && let Lit::Str(s) = &expr_lit.lit
    {
        Some(s.value())
    } else {
        None
    }
}
