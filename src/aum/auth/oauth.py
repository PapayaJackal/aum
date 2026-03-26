from __future__ import annotations

import sqlite3

import structlog
from authlib.integrations.starlette_client import OAuth

from aum.auth.models import User, init_auth_tables, row_to_user
from aum.config import OAuthProvider
from aum.metrics import AUTH_REQUESTS

log = structlog.get_logger()


class OAuthManager:
    """Manages OAuth2 provider integration and user linking."""

    def __init__(self, conn: sqlite3.Connection, providers: list[OAuthProvider]) -> None:
        self._conn = conn
        self._conn.row_factory = sqlite3.Row
        init_auth_tables(self._conn)

        self.oauth = OAuth()
        self._providers: dict[str, OAuthProvider] = {}

        for provider in providers:
            self._providers[provider.name] = provider
            self.oauth.register(
                name=provider.name,
                client_id=provider.client_id,
                client_secret=provider.client_secret,
                server_metadata_url=provider.server_metadata_url,
                client_kwargs={"scope": "openid email profile"},
            )
            log.info("registered oauth provider", provider=provider.name)

    @property
    def provider_names(self) -> list[str]:
        return list(self._providers.keys())

    def get_client(self, provider_name: str):  # noqa: ANN201
        """Get the authlib OAuth client for a provider."""
        if provider_name not in self._providers:
            raise ValueError(f"Unknown OAuth provider: {provider_name}")
        return getattr(self.oauth, provider_name)

    def get_or_create_user(self, provider_name: str, userinfo: dict) -> User:
        """Find or create a user from OAuth provider userinfo.

        Links the OAuth account to an existing user if the email matches,
        or creates a new user.
        """
        AUTH_REQUESTS.labels(method=f"oauth_{provider_name}").inc()

        provider_user_id = userinfo.get("sub") or userinfo.get("id", "")
        email = userinfo.get("email", "")
        name = userinfo.get("name") or userinfo.get("preferred_username") or email

        # Check if this OAuth account is already linked
        row = self._conn.execute(
            """SELECT u.* FROM users u
               JOIN oauth_accounts oa ON u.id = oa.user_id
               WHERE oa.provider = ? AND oa.provider_user_id = ?""",
            (provider_name, str(provider_user_id)),
        ).fetchone()

        if row:
            return row_to_user(row)

        # Check if a user with this email already exists (link accounts)
        user_row = None
        if email:
            user_row = self._conn.execute(
                """SELECT u.* FROM users u
                   JOIN oauth_accounts oa ON u.id = oa.user_id
                   WHERE oa.email = ?""",
                (email,),
            ).fetchone()

        if user_row:
            user_id = user_row["id"]
        else:
            # Create new user
            username = self._unique_username(name)
            cursor = self._conn.execute(
                "INSERT INTO users (username, password_hash, is_admin) VALUES (?, NULL, 0)",
                (username,),
            )
            user_id = cursor.lastrowid
            log.info("created oauth user", username=username, provider=provider_name)

        # Link OAuth account
        self._conn.execute(
            "INSERT INTO oauth_accounts (user_id, provider, provider_user_id, email) VALUES (?, ?, ?, ?)",
            (user_id, provider_name, str(provider_user_id), email),
        )
        self._conn.commit()

        row = self._conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return row_to_user(row)

    def _unique_username(self, base: str) -> str:
        """Generate a unique username from a base name."""
        # Sanitize: lowercase, replace spaces
        username = base.lower().replace(" ", "_").strip("_")
        if not username:
            username = "user"

        candidate = username
        counter = 1
        while True:
            row = self._conn.execute(
                "SELECT 1 FROM users WHERE username = ?", (candidate,)
            ).fetchone()
            if row is None:
                return candidate
            candidate = f"{username}_{counter}"
            counter += 1

