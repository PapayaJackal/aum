from __future__ import annotations

import re
import sqlite3

import structlog
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

from aum.auth.models import User, init_auth_tables, row_to_user
from aum.metrics import AUTH_FAILURES, AUTH_REQUESTS

log = structlog.get_logger()

_hasher = PasswordHasher()

_DEFAULT_MIN_PASSWORD_LENGTH = 8


class AuthError(Exception):
    """Raised on authentication failure."""


class PasswordPolicyError(ValueError):
    """Raised when a password does not meet the policy requirements."""


def validate_password(password: str, min_length: int = _DEFAULT_MIN_PASSWORD_LENGTH) -> None:
    """Enforce the password policy.

    Raises PasswordPolicyError if the password is too weak.
    """
    errors: list[str] = []
    if len(password) < min_length:
        errors.append(f"at least {min_length} characters")
    if not re.search(r"[a-z]", password):
        errors.append("at least one lowercase letter")
    if not re.search(r"[A-Z]", password):
        errors.append("at least one uppercase letter")
    if not re.search(r"[0-9]", password):
        errors.append("at least one digit")
    if not re.search(r"[^a-zA-Z0-9]", password):
        errors.append("at least one special character")
    if errors:
        raise PasswordPolicyError("Password must contain: " + ", ".join(errors))


class LocalAuth:
    """Local username/password authentication backed by SQLite."""

    def __init__(self, conn: sqlite3.Connection, password_min_length: int = _DEFAULT_MIN_PASSWORD_LENGTH) -> None:
        self._conn = conn
        self._conn.row_factory = sqlite3.Row
        self._password_min_length = password_min_length
        init_auth_tables(self._conn)

    def create_user(self, username: str, password: str, is_admin: bool = False) -> User:
        validate_password(password, min_length=self._password_min_length)
        password_hash = _hasher.hash(password)
        cursor = self._conn.execute(
            "INSERT INTO users (username, password_hash, is_admin) VALUES (?, ?, ?)",
            (username, password_hash, int(is_admin)),
        )
        self._conn.commit()
        log.info("user created", username=username, is_admin=is_admin)
        return User(
            id=cursor.lastrowid,  # type: ignore[arg-type]
            username=username,
            password_hash=password_hash,
            is_admin=is_admin,
        )

    def authenticate(self, username: str, password: str) -> User:
        AUTH_REQUESTS.labels(method="local").inc()
        row = self._conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()

        if row is None:
            AUTH_FAILURES.labels(reason="user_not_found").inc()
            raise AuthError("Invalid username or password")

        if row["password_hash"] is None:
            AUTH_FAILURES.labels(reason="no_local_password").inc()
            raise AuthError("This account uses OAuth login only")

        try:
            _hasher.verify(row["password_hash"], password)
        except VerifyMismatchError:
            AUTH_FAILURES.labels(reason="bad_password").inc()
            raise AuthError("Invalid username or password")

        # Rehash if argon2 params have changed
        if _hasher.check_needs_rehash(row["password_hash"]):
            new_hash = _hasher.hash(password)
            self._conn.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_hash, row["id"]))
            self._conn.commit()

        return row_to_user(row)

    def get_user(self, user_id: int) -> User | None:
        row = self._conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return row_to_user(row) if row else None

    def get_user_by_username(self, username: str) -> User | None:
        row = self._conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        return row_to_user(row) if row else None

    def list_users(self) -> list[User]:
        rows = self._conn.execute("SELECT * FROM users ORDER BY username").fetchall()
        return [row_to_user(row) for row in rows]

    def delete_user(self, username: str) -> bool:
        cursor = self._conn.execute("DELETE FROM users WHERE username = ?", (username,))
        self._conn.commit()
        deleted = cursor.rowcount > 0
        if deleted:
            log.info("user deleted", username=username)
        return deleted

    def set_password(self, username: str, new_password: str) -> bool:
        validate_password(new_password, min_length=self._password_min_length)
        new_hash = _hasher.hash(new_password)
        cursor = self._conn.execute(
            "UPDATE users SET password_hash = ? WHERE username = ?",
            (new_hash, username),
        )
        self._conn.commit()
        updated = cursor.rowcount > 0
        if updated:
            log.info("user password changed", username=username)
        return updated

    def set_admin(self, username: str, is_admin: bool) -> bool:
        cursor = self._conn.execute(
            "UPDATE users SET is_admin = ? WHERE username = ?",
            (int(is_admin), username),
        )
        self._conn.commit()
        updated = cursor.rowcount > 0
        if updated:
            log.info("user admin status changed", username=username, is_admin=is_admin)
        return updated
