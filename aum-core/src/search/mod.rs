//! Search backend abstraction: types, constants, utilities, and backend
//! implementations.

pub mod backend;
pub mod constants;
pub mod dispatch;
pub mod meta;

#[cfg(feature = "meilisearch")]
pub mod meilisearch;

#[cfg(feature = "opensearch")]
pub mod opensearch;

pub mod types;
pub mod utils;

pub use backend::SearchBackend;
pub use dispatch::AumBackend;

#[cfg(feature = "meilisearch")]
pub use meilisearch::MeilisearchBackend;

#[cfg(feature = "opensearch")]
pub use opensearch::OpenSearchBackend;

pub use types::{
    BatchIndexResult, FacetMap, FilterMap, SearchError, SearchResult, SortSpec, TruncationRecord,
};
pub use utils::{alias_mimetype, extract_email, normalize_message_id};
