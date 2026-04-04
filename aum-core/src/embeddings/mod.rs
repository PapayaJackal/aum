//! Embedding pipeline: backends, chunking, and orchestration.
//!
//! # Overview
//!
//! - [`Embedder`] — async trait implemented by each backend
//! - [`OllamaEmbedder`] / [`OpenAiEmbedder`] — concrete HTTP clients
//! - [`chunk_text`] — paragraph/sentence-aware text splitter
//! - [`EmbedPipeline`] — streams unembedded docs, chunks, embeds, and writes vectors back
//! - [`EmbedSnapshot`] / [`EmbedProgressTx`] — live progress reporting

pub mod backend;
pub mod chunking;
pub mod error;
pub mod ollama;
pub mod openai;
pub mod pipeline;

pub use backend::{Embedder, l2_normalize, l2_normalize_batch};
pub use chunking::chunk_text;
pub use error::EmbedError;
pub use ollama::OllamaEmbedder;
pub use openai::OpenAiEmbedder;
pub use pipeline::{EmbedPipeline, EmbedPipelineError, EmbedProgressTx, EmbedSnapshot};
