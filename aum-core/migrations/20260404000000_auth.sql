-- Users table for local authentication.
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    UNIQUE NOT NULL,
    password_hash TEXT,
    is_admin      INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT    NOT NULL
);

-- Server-side sessions (opaque tokens replacing JWTs).
CREATE TABLE IF NOT EXISTS sessions (
    token      TEXT    PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TEXT    NOT NULL,
    expires_at TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);

-- One-time invitation tokens for user onboarding.
CREATE TABLE IF NOT EXISTS invitations (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    token      TEXT    NOT NULL UNIQUE,
    username   TEXT    NOT NULL,
    is_admin   INTEGER NOT NULL DEFAULT 0,
    created_at TEXT    NOT NULL,
    expires_at TEXT    NOT NULL,
    used_at    TEXT
);

-- Per-index access control.
CREATE TABLE IF NOT EXISTS user_index_permissions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    index_name TEXT    NOT NULL,
    UNIQUE(user_id, index_name)
);
