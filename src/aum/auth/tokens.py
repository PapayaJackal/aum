from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt

from aum.auth.models import User


class TokenError(Exception):
    """Raised when a token is invalid or expired."""


class TokenManager:
    """Issue and verify JWT access and refresh tokens."""

    def __init__(
        self,
        secret: str,
        algorithm: str = "HS256",
        access_expire_minutes: int = 30,
        refresh_expire_days: int = 7,
    ) -> None:
        self._secret = secret
        self._algorithm = algorithm
        self._access_expire = timedelta(minutes=access_expire_minutes)
        self._refresh_expire = timedelta(days=refresh_expire_days)

    def create_access_token(self, user: User) -> str:
        now = datetime.now(UTC)
        payload = {
            "sub": str(user.id),
            "username": user.username,
            "is_admin": user.is_admin,
            "type": "access",
            "iat": now,
            "exp": now + self._access_expire,
        }
        return jwt.encode(payload, self._secret, algorithm=self._algorithm)

    def create_refresh_token(self, user: User) -> str:
        now = datetime.now(UTC)
        payload = {
            "sub": str(user.id),
            "type": "refresh",
            "iat": now,
            "exp": now + self._refresh_expire,
        }
        return jwt.encode(payload, self._secret, algorithm=self._algorithm)

    def create_api_token(self, user: User, expire_days: int = 365) -> str:
        """Create a long-lived API token for programmatic access."""
        now = datetime.now(UTC)
        payload = {
            "sub": str(user.id),
            "username": user.username,
            "is_admin": user.is_admin,
            "type": "access",
            "iat": now,
            "exp": now + timedelta(days=expire_days),
        }
        return jwt.encode(payload, self._secret, algorithm=self._algorithm)

    def verify_access_token(self, token: str) -> dict:
        """Verify and decode an access token. Returns the payload dict.

        Raises TokenError if invalid, expired, or wrong type.
        """
        try:
            payload = jwt.decode(token, self._secret, algorithms=[self._algorithm])
        except jwt.ExpiredSignatureError:
            raise TokenError("Token has expired")
        except jwt.InvalidTokenError as exc:
            raise TokenError(f"Invalid token: {exc}")

        if payload.get("type") != "access":
            raise TokenError("Not an access token")

        return payload

    def create_passkey_enroll_token(self, user: User) -> str:
        """Token for forced passkey enrollment after password login."""
        now = datetime.now(UTC)
        return jwt.encode(
            {"sub": str(user.id), "type": "passkey_enroll", "iat": now, "exp": now + timedelta(minutes=10)},
            self._secret,
            algorithm=self._algorithm,
        )

    def verify_passkey_enroll_token(self, token: str) -> dict:
        """Verify a passkey enrollment token."""
        try:
            payload = jwt.decode(token, self._secret, algorithms=[self._algorithm])
        except jwt.ExpiredSignatureError:
            raise TokenError("Passkey enrollment token expired")
        except jwt.InvalidTokenError as exc:
            raise TokenError(f"Invalid passkey enrollment token: {exc}")
        if payload.get("type") != "passkey_enroll":
            raise TokenError("Not a passkey enrollment token")
        return payload

    def verify_refresh_token(self, token: str) -> dict:
        """Verify and decode a refresh token. Returns the payload dict."""
        try:
            payload = jwt.decode(token, self._secret, algorithms=[self._algorithm])
        except jwt.ExpiredSignatureError:
            raise TokenError("Refresh token has expired")
        except jwt.InvalidTokenError as exc:
            raise TokenError(f"Invalid token: {exc}")

        if payload.get("type") != "refresh":
            raise TokenError("Not a refresh token")

        return payload
