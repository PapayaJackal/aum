//! Procedural macros for aum configuration structs.

use proc_macro::TokenStream;

mod config_default;
mod config_docs;
mod config_values;
mod util;

/// Derives `config_docs() -> &'static [ConfigDoc]` for a configuration section struct.
///
/// See `config_docs` module for full documentation.
#[proc_macro_derive(ConfigDocs, attributes(config_default, config_section))]
pub fn derive_config_docs(input: TokenStream) -> TokenStream {
    config_docs::derive_config_docs(input)
}

/// Derives `impl Default` for a configuration section struct using `#[config_default]` attributes.
///
/// See `config_default` module for full documentation.
#[proc_macro_derive(ConfigDefault, attributes(config_default, config_default_expr))]
pub fn derive_config_default(input: TokenStream) -> TokenStream {
    config_default::derive_config_default(input)
}

/// Derives `config_values(&self) -> Vec<String>` for a configuration section struct.
///
/// See `config_values` module for full documentation.
#[proc_macro_derive(ConfigValues)]
pub fn derive_config_values(input: TokenStream) -> TokenStream {
    config_values::derive_config_values(input)
}
