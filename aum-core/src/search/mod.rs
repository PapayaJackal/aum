//! Search backend abstraction: types, constants, utilities, and backend
//! implementations.
//!
//! The primary entry point for most code is [`MeilisearchBackend`], which
//! implements [`SearchBackend`], [`BatchSink`], and [`ExistenceChecker`].

pub mod backend;
pub mod constants;
pub mod meilisearch;
pub mod types;
pub mod utils;

pub use backend::SearchBackend;
pub use meilisearch::MeilisearchBackend;
pub use types::{
    BatchIndexResult, FacetMap, FilterMap, SearchError, SearchResult, SortSpec, TruncationRecord,
};
pub use utils::{alias_mimetype, extract_email, normalize_message_id};
