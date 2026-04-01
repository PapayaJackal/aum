-- One row per ingest or embed job.
CREATE TABLE IF NOT EXISTS jobs (
    job_id      TEXT    PRIMARY KEY,
    source_dir  TEXT    NOT NULL,
    index_name  TEXT    NOT NULL DEFAULT 'aum',
    status      TEXT    NOT NULL DEFAULT 'pending',
    total_files INTEGER NOT NULL DEFAULT 0,
    extracted   INTEGER NOT NULL DEFAULT 0,
    processed   INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    empty       INTEGER NOT NULL DEFAULT 0,
    skipped     INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL,
    finished_at TEXT,
    job_type    TEXT    NOT NULL DEFAULT 'ingest'
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_created
    ON jobs (status, created_at DESC);

-- One row per file that failed within a job.
-- (job_id, file_path, error_type) is the natural unique key; no surrogate id needed.
CREATE TABLE IF NOT EXISTS job_errors (
    job_id     TEXT NOT NULL REFERENCES jobs (job_id),
    file_path  TEXT NOT NULL,
    error_type TEXT NOT NULL,
    message    TEXT NOT NULL,
    timestamp  TEXT NOT NULL,
    PRIMARY KEY (job_id, file_path, error_type)
);

-- One row per search index recording which embedding model was used.
CREATE TABLE IF NOT EXISTS index_embeddings (
    index_name TEXT    PRIMARY KEY,
    model      TEXT    NOT NULL,
    backend    TEXT    NOT NULL DEFAULT 'ollama',
    dimension  INTEGER NOT NULL,
    updated_at TEXT    NOT NULL
);
