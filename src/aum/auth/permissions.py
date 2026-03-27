from __future__ import annotations

import sqlite3

import structlog

from aum.auth.models import User, init_auth_tables

log = structlog.get_logger()


class PermissionDeniedError(Exception):
    """Raised when a user lacks access to a resource."""


class PermissionManager:
    """Manages user-to-index access control."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._conn.row_factory = sqlite3.Row
        init_auth_tables(self._conn)

    def grant(self, username: str, index_name: str) -> bool:
        """Grant a user access to an index. Returns True if newly granted."""
        row = self._conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        if row is None:
            raise ValueError(f"User not found: {username}")

        try:
            self._conn.execute(
                "INSERT INTO user_index_permissions (user_id, index_name) VALUES (?, ?)",
                (row["id"], index_name),
            )
            self._conn.commit()
            log.info("permission granted", username=username, index=index_name)
            return True
        except sqlite3.IntegrityError:
            return False  # already granted

    def revoke(self, username: str, index_name: str) -> bool:
        """Revoke a user's access to an index. Returns True if was revoked."""
        row = self._conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        if row is None:
            raise ValueError(f"User not found: {username}")

        cursor = self._conn.execute(
            "DELETE FROM user_index_permissions WHERE user_id = ? AND index_name = ?",
            (row["id"], index_name),
        )
        self._conn.commit()
        revoked = cursor.rowcount > 0
        if revoked:
            log.info("permission revoked", username=username, index=index_name)
        return revoked

    def check(self, user: User, index_name: str) -> bool:
        """Check if a user has access to an index. Admins always have access."""
        if user.is_admin:
            return True
        row = self._conn.execute(
            "SELECT 1 FROM user_index_permissions WHERE user_id = ? AND index_name = ?",
            (user.id, index_name),
        ).fetchone()
        return row is not None

    def require(self, user: User, index_name: str) -> None:
        """Raise PermissionDenied if the user lacks access."""
        if not self.check(user, index_name):
            raise PermissionDeniedError(f"User '{user.username}' does not have access to index '{index_name}'")

    def list_user_indices(self, user: User) -> list[str]:
        """Return the list of index names a user can access."""
        if user.is_admin:
            return ["*"]  # admin sees all
        rows = self._conn.execute(
            "SELECT index_name FROM user_index_permissions WHERE user_id = ?",
            (user.id,),
        ).fetchall()
        return [row["index_name"] for row in rows]

    def list_index_users(self, index_name: str) -> list[str]:
        """Return usernames that have access to an index."""
        rows = self._conn.execute(
            """SELECT u.username FROM users u
               JOIN user_index_permissions p ON u.id = p.user_id
               WHERE p.index_name = ?
               ORDER BY u.username""",
            (index_name,),
        ).fetchall()
        return [row["username"] for row in rows]
