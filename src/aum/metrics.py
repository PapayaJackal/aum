from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, Info

# Ingest
DOCS_INGESTED = Counter(
    "aum_documents_ingested_total",
    "Total documents successfully ingested",
    ["backend"],
)
DOCS_FAILED = Counter(
    "aum_documents_failed_total",
    "Total documents that failed ingestion",
    ["error_type"],
)
INGEST_DURATION = Histogram(
    "aum_ingest_duration_seconds",
    "Time to process a single document",
    ["stage"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)
INGEST_JOBS_ACTIVE = Gauge(
    "aum_ingest_jobs_active",
    "Number of currently running ingest jobs",
)

# Index management
INDEXES_RECREATED = Counter(
    "aum_indexes_recreated_total",
    "Number of times an index was recreated due to stale mapping",
    ["index"],
)

# Search
SEARCH_REQUESTS = Counter(
    "aum_search_requests_total",
    "Total search requests",
    ["type"],
)
SEARCH_LATENCY = Histogram(
    "aum_search_latency_seconds",
    "Search request latency",
    ["type", "backend"],
)

# Extraction
EXTRACTION_DURATION = Histogram(
    "aum_extraction_duration_seconds",
    "Time to extract text from a document",
)
EXTRACTION_ERRORS = Counter(
    "aum_extraction_errors_total",
    "Extraction errors by type",
    ["error_type"],
)

# Embedding
EMBEDDING_DURATION = Histogram(
    "aum_embedding_duration_seconds",
    "Time to embed a batch",
    ["backend"],
    buckets=[0.1, 0.5, 1, 2, 5, 10],
)
EMBEDDING_REQUESTS = Counter(
    "aum_embedding_requests_total",
    "Total embedding API requests",
    ["backend"],
)
EMBEDDING_DOCS_PROCESSED = Counter(
    "aum_embedding_docs_processed_total",
    "Total documents successfully embedded",
)
EMBEDDING_DOCS_FAILED = Counter(
    "aum_embedding_docs_failed_total",
    "Total documents that failed embedding",
)
EMBEDDING_JOBS_ACTIVE = Gauge(
    "aum_embedding_jobs_active",
    "Number of currently running embedding jobs",
)

# Auth
AUTH_REQUESTS = Counter(
    "aum_auth_requests_total",
    "Total auth requests",
    ["method"],
)
AUTH_FAILURES = Counter(
    "aum_auth_failures_total",
    "Auth failures",
    ["reason"],
)

AUTH_RATE_LIMITED = Counter(
    "aum_auth_rate_limited_total",
    "Login attempts rejected by rate limiter",
)

# Build info
BUILD_INFO = Info("aum_build", "Build and version info")
