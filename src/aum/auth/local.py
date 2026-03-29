from __future__ import annotations

import re
import secrets
import sqlite3
from datetime import UTC, datetime, timedelta

import structlog
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

from aum.auth.models import Invitation, User, init_auth_tables, row_to_invitation, row_to_user
from aum.metrics import AUTH_FAILURES, AUTH_REQUESTS, INVITATIONS_REDEEMED

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

    def create_user_without_password(self, username: str, is_admin: bool = False) -> User:
        """Create a user with no password (passkey-only or OAuth-only)."""
        cursor = self._conn.execute(
            "INSERT INTO users (username, password_hash, is_admin) VALUES (?, NULL, ?)",
            (username, int(is_admin)),
        )
        self._conn.commit()
        log.info("user created (no password)", username=username, is_admin=is_admin)
        return User(
            id=cursor.lastrowid,  # type: ignore[arg-type]
            username=username,
            password_hash=None,
            is_admin=is_admin,
        )

    # -- Invitations --

    def create_invitation(
        self,
        username: str,
        is_admin: bool = False,
        expires_hours: int = 48,
    ) -> Invitation:
        """Create a one-time invitation token for a new user."""
        token = secrets.token_urlsafe(32)
        expires_at = (datetime.now(UTC) + timedelta(hours=expires_hours)).isoformat()
        cursor = self._conn.execute(
            "INSERT INTO invitations (token, username, is_admin, expires_at) VALUES (?, ?, ?, ?)",
            (token, username, int(is_admin), expires_at),
        )
        self._conn.commit()
        log.info("invitation created", username=username, is_admin=is_admin, expires_at=expires_at)
        return Invitation(
            id=cursor.lastrowid,  # type: ignore[arg-type]
            token=token,
            username=username,
            is_admin=is_admin,
            created_at=datetime.now(UTC).isoformat(),
            expires_at=expires_at,
            used_at=None,
        )

    def get_invitation(self, token: str) -> Invitation | None:
        """Get a valid (unused, non-expired) invitation."""
        row = self._conn.execute(
            "SELECT * FROM invitations WHERE token = ?",
            (token,),
        ).fetchone()
        if row is None:
            return None
        invitation = row_to_invitation(row)
        if invitation.used_at is not None:
            return None
        if datetime.fromisoformat(invitation.expires_at) < datetime.now(UTC):
            return None
        return invitation

    def redeem_invitation(self, token: str, password: str | None = None) -> User:
        """Redeem an invitation: create the user and mark it used.

        If password is provided, the user gets a local password.
        Otherwise the user is created without a password (passkey-only).
        """
        invitation = self.get_invitation(token)
        if invitation is None:
            raise AuthError("Invalid or expired invitation")

        # Check username not already taken
        if self.get_user_by_username(invitation.username):
            raise AuthError(f"Username '{invitation.username}' is already taken")

        if password:
            user = self.create_user(invitation.username, password, is_admin=invitation.is_admin)
        else:
            user = self.create_user_without_password(invitation.username, is_admin=invitation.is_admin)

        self._conn.execute(
            "UPDATE invitations SET used_at = ? WHERE id = ?",
            (datetime.now(UTC).isoformat(), invitation.id),
        )
        self._conn.commit()
        INVITATIONS_REDEEMED.inc()
        log.info("invitation redeemed", username=user.username, token_id=invitation.id)
        return user

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
