from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

import structlog

log = structlog.get_logger()

AUTH_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT,
    is_admin INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS oauth_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    provider_user_id TEXT NOT NULL,
    email TEXT,
    UNIQUE(provider, provider_user_id)
);

CREATE INDEX IF NOT EXISTS idx_oauth_accounts_user_id ON oauth_accounts(user_id);

CREATE TABLE IF NOT EXISTS user_index_permissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    index_name TEXT NOT NULL,
    UNIQUE(user_id, index_name)
);

CREATE TABLE IF NOT EXISTS webauthn_credentials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    credential_id BLOB NOT NULL UNIQUE,
    public_key BLOB NOT NULL,
    sign_count INTEGER NOT NULL DEFAULT 0,
    transports TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    name TEXT NOT NULL DEFAULT 'passkey'
);
CREATE INDEX IF NOT EXISTS idx_webauthn_user_id ON webauthn_credentials(user_id);

CREATE TABLE IF NOT EXISTS invitations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token TEXT NOT NULL UNIQUE,
    username TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT NOT NULL,
    used_at TEXT
);
"""


@dataclass
class User:
    id: int
    username: str
    password_hash: str | None
    is_admin: bool


@dataclass
class IndexPermission:
    user_id: int
    index_name: str


@dataclass
class WebAuthnCredential:
    id: int
    user_id: int
    credential_id: bytes
    public_key: bytes
    sign_count: int
    transports: list[str] | None
    created_at: str
    name: str


@dataclass
class Invitation:
    id: int
    token: str
    username: str
    is_admin: bool
    created_at: str
    expires_at: str
    used_at: str | None


_initialized_connections: set[int] = set()


def init_auth_tables(conn: sqlite3.Connection) -> None:
    conn_id = id(conn)
    if conn_id in _initialized_connections:
        return
    # The schema uses CREATE TABLE IF NOT EXISTS, so re-running on a
    # connection whose id() was recycled is safe — just a no-op.
    conn.executescript(AUTH_SCHEMA)
    conn.commit()
    _initialized_connections.add(conn_id)
    log.debug("auth tables initialized")


def row_to_user(row: sqlite3.Row) -> User:
    return User(
        id=row["id"],
        username=row["username"],
        password_hash=row["password_hash"],
        is_admin=bool(row["is_admin"]),
    )


def row_to_webauthn_credential(row: sqlite3.Row) -> WebAuthnCredential:
    transports_raw = row["transports"]
    transports = json.loads(transports_raw) if transports_raw else None
    return WebAuthnCredential(
        id=row["id"],
        user_id=row["user_id"],
        credential_id=row["credential_id"],
        public_key=row["public_key"],
        sign_count=row["sign_count"],
        transports=transports,
        created_at=row["created_at"],
        name=row["name"],
    )


def row_to_invitation(row: sqlite3.Row) -> Invitation:
    return Invitation(
        id=row["id"],
        token=row["token"],
        username=row["username"],
        is_admin=bool(row["is_admin"]),
        created_at=row["created_at"],
        expires_at=row["expires_at"],
        used_at=row["used_at"],
    )
