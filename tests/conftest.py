from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from aum.auth.local import LocalAuth
from aum.auth.models import _initialized_connections
from aum.auth.permissions import PermissionManager
from aum.auth.tokens import TokenManager
from aum.config import AumConfig
from aum.ingest.tracker import JobTracker

_TEST_JWT_SECRET = "test-jwt-secret-for-testing-only"


@pytest.fixture(autouse=True)
def _reset_auth_init():
    """Clear cached connection IDs so init_auth_tables runs on fresh connections."""
    _initialized_connections.clear()


@pytest.fixture
def tmp_db(tmp_path: Path) -> str:
    return str(tmp_path / "test.db")


@pytest.fixture
def db_conn(tmp_db: str) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_db, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@pytest.fixture
def tracker(tmp_db: str) -> JobTracker:
    return JobTracker(db_path=tmp_db)


@pytest.fixture
def local_auth(db_conn: sqlite3.Connection) -> LocalAuth:
    return LocalAuth(db_conn)


@pytest.fixture
def permissions(db_conn: sqlite3.Connection) -> PermissionManager:
    return PermissionManager(db_conn)


@pytest.fixture
def config(tmp_db: str) -> AumConfig:
    return AumConfig(data_dir=str(Path(tmp_db).parent), log_format="console", jwt_secret=_TEST_JWT_SECRET)


@pytest.fixture
def token_manager() -> TokenManager:
    return TokenManager(secret=_TEST_JWT_SECRET)


@pytest.fixture
def sample_files(tmp_path: Path) -> Path:
    """Create a directory with sample text files for testing."""
    docs = tmp_path / "docs"
    docs.mkdir()
    for i in range(5):
        (docs / f"doc_{i}.txt").write_text(f"This is test document number {i}. It has some content.")
    return docs
