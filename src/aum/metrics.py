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
DOCS_SKIPPED = Counter(
    "aum_documents_skipped_total",
    "Documents skipped during resume (already indexed)",
)
DOCS_TRUNCATED = Counter(
    "aum_documents_truncated_total",
    "Documents whose content was truncated to fit the search backend payload limit",
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

# Passkey auth
PASSKEY_LOGINS = Counter(
    "aum_passkey_logins_total",
    "Passkey login attempts",
    ["result"],
)
PASSKEY_ENROLLMENTS = Counter(
    "aum_passkey_enrollments_total",
    "Passkey enrollments completed",
)

# Invitations
INVITATIONS_REDEEMED = Counter(
    "aum_invitations_redeemed_total",
    "User invitations redeemed",
)

# Documents
DOCUMENT_VIEWS = Counter(
    "aum_document_views_total",
    "Total document detail views",
)
DOCUMENT_DOWNLOADS = Counter(
    "aum_document_downloads_total",
    "Total document downloads",
)
THREAD_LOOKUPS = Counter(
    "aum_thread_lookups_total",
    "Total email thread lookups",
)

# Instance pool
POOL_REQUESTS = Counter(
    "aum_pool_requests_total",
    "Total requests dispatched to service instances",
    ["service", "instance_url"],
)
POOL_ERRORS = Counter(
    "aum_pool_errors_total",
    "Errors from service instances",
    ["service", "instance_url", "error_type"],
)
POOL_LATENCY = Histogram(
    "aum_pool_request_duration_seconds",
    "Request latency per service instance",
    ["service", "instance_url"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)
POOL_INSTANCE_HEALTHY = Gauge(
    "aum_pool_instance_healthy",
    "Whether a service instance is healthy (1) or not (0)",
    ["service", "instance_url"],
)
POOL_IN_FLIGHT = Gauge(
    "aum_pool_instance_in_flight",
    "Current in-flight requests per instance",
    ["service", "instance_url"],
)

# Build info
BUILD_INFO = Info("aum_build", "Build and version info")
