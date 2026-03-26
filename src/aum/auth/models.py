from __future__ import annotations

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


_initialized_connections: set[int] = set()


def init_auth_tables(conn: sqlite3.Connection) -> None:
    conn_id = id(conn)
    if conn_id in _initialized_connections:
        return
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
