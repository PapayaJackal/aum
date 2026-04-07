-- Store the full embedding configuration used per index so hybrid search can
-- reconstruct the correct embedder without relying on the global config.
ALTER TABLE index_embeddings ADD COLUMN context_length INTEGER NOT NULL DEFAULT 0;
ALTER TABLE index_embeddings ADD COLUMN query_prefix TEXT NOT NULL DEFAULT '';
