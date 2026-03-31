use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

use crate::util;

/// Derives `config_docs() -> &'static [ConfigDoc]` on a struct.
///
/// Each field must have:
/// - A `///` doc comment, used as the description.
/// - A `#[config_default = "..."]` attribute with the string representation of the default value.
///
/// The struct may optionally carry `#[config_section = "sectionname"]`. When present:
/// - `env_var` is generated as `AUM_{SECTION}__{FIELD}` (e.g. `AUM_MEILISEARCH__URL`)
/// - `section` is populated with the section name string
///
/// Without `#[config_section]`, `env_var` is `AUM_{FIELD}` and section is `""`.
pub fn derive_config_docs(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_impl(&input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

fn derive_impl(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = &input.ident;

    let section = input
        .attrs
        .iter()
        .find_map(|a| util::get_attr_str(a, "config_section"));
    let named_fields = util::extract_named_fields(input, "ConfigDocs")?;

    let section_str = section.as_deref().unwrap_or("");
    let mut entries = Vec::new();

    for field in named_fields {
        let Some(ident) = field.ident.as_ref() else {
            return Err(syn::Error::new_spanned(
                struct_name,
                "ConfigDocs: unnamed field in named struct",
            ));
        };
        let field_name = ident.to_string();

        let env_var = match &section {
            Some(sec) => format!("AUM_{}__{}", sec.to_uppercase(), field_name.to_uppercase()),
            None => format!("AUM_{}", field_name.to_uppercase()),
        };

        let mut description_parts: Vec<String> = Vec::new();
        let mut default_value: Option<String> = None;

        for attr in &field.attrs {
            if let Some(doc) = util::get_attr_str(attr, "doc") {
                let trimmed = doc.trim().to_string();
                if !trimmed.is_empty() {
                    description_parts.push(trimmed);
                }
            } else if let Some(v) = util::get_attr_str(attr, "config_default") {
                default_value = Some(v);
            }
        }

        let Some(default_value) = default_value else {
            return Err(syn::Error::new_spanned(
                ident,
                format!("field `{field_name}` is missing #[config_default = \"...\"] attribute"),
            ));
        };

        let description = description_parts.join(" ");
        entries.push(quote! {
            ConfigDoc {
                name: #field_name,
                env_var: #env_var,
                default: #default_value,
                description: #description,
                section: #section_str,
            }
        });
    }

    Ok(quote! {
        impl #struct_name {
            /// Returns documentation for all configuration fields in this section.
            pub fn config_docs() -> &'static [ConfigDoc] {
                const DOCS: &[ConfigDoc] = &[#(#entries),*];
                DOCS
            }
        }
    })
}
