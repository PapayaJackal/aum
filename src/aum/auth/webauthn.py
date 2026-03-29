from __future__ import annotations

import json
import secrets
import sqlite3
import time

import structlog
import webauthn
from webauthn.helpers import base64url_to_bytes
from webauthn.helpers.structs import (
    AuthenticatorSelectionCriteria,
    PublicKeyCredentialDescriptor,
    ResidentKeyRequirement,
    UserVerificationRequirement,
)

from aum.auth.models import User, WebAuthnCredential, init_auth_tables, row_to_user, row_to_webauthn_credential

log = structlog.get_logger()

_CHALLENGE_TTL_SECONDS = 300  # 5 minutes


class WebAuthnError(Exception):
    """Raised on WebAuthn registration or authentication failure."""


class WebAuthnManager:
    """Manage WebAuthn passkey credentials backed by SQLite."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        rp_id: str,
        rp_name: str,
        origin: str,
    ) -> None:
        self._conn = conn
        self._conn.row_factory = sqlite3.Row
        self._rp_id = rp_id
        self._rp_name = rp_name
        self._origin = origin
        self._challenges: dict[int, tuple[bytes, float]] = {}
        self._passkey_sessions: dict[str, tuple[bytes, float]] = {}
        init_auth_tables(self._conn)

    # -- Challenge store (in-memory, keyed by user_id) --

    def _store_challenge(self, user_id: int, challenge: bytes) -> None:
        self._challenges[user_id] = (challenge, time.monotonic())

    def _pop_challenge(self, user_id: int) -> bytes | None:
        entry = self._challenges.pop(user_id, None)
        if entry is None:
            return None
        challenge, ts = entry
        if time.monotonic() - ts > _CHALLENGE_TTL_SECONDS:
            return None
        return challenge

    # -- Credential CRUD --

    def get_credentials(self, user_id: int) -> list[WebAuthnCredential]:
        rows = self._conn.execute(
            "SELECT * FROM webauthn_credentials WHERE user_id = ? ORDER BY created_at",
            (user_id,),
        ).fetchall()
        return [row_to_webauthn_credential(row) for row in rows]

    def has_credentials(self, user_id: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM webauthn_credentials WHERE user_id = ? LIMIT 1",
            (user_id,),
        ).fetchone()
        return row is not None

    def delete_credentials(self, user_id: int) -> int:
        cursor = self._conn.execute(
            "DELETE FROM webauthn_credentials WHERE user_id = ?",
            (user_id,),
        )
        self._conn.commit()
        count = cursor.rowcount
        if count:
            log.info("webauthn credentials deleted", user_id=user_id, count=count)
        return count

    # -- Registration --

    def generate_registration_options(self, user: User) -> tuple[str, bytes]:
        """Generate WebAuthn registration options.

        Returns (options_json, challenge) where options_json is ready to send
        to the browser.
        """
        existing = self.get_credentials(user.id)
        exclude = [PublicKeyCredentialDescriptor(id=c.credential_id) for c in existing]

        options = webauthn.generate_registration_options(
            rp_id=self._rp_id,
            rp_name=self._rp_name,
            user_name=user.username,
            user_id=user.username.encode(),
            exclude_credentials=exclude,
            authenticator_selection=AuthenticatorSelectionCriteria(
                resident_key=ResidentKeyRequirement.PREFERRED,
                user_verification=UserVerificationRequirement.PREFERRED,
            ),
        )

        challenge = options.challenge
        self._store_challenge(user.id, challenge)

        options_json = webauthn.options_to_json(options)
        return options_json, challenge

    def verify_registration(
        self,
        user: User,
        challenge: bytes,
        credential: dict | str,
    ) -> WebAuthnCredential:
        """Verify a registration response and store the new credential."""
        try:
            verification = webauthn.verify_registration_response(
                credential=credential,
                expected_challenge=challenge,
                expected_rp_id=self._rp_id,
                expected_origin=self._origin,
            )
        except Exception as exc:
            raise WebAuthnError(f"Registration verification failed: {exc}") from exc

        transports_json: str | None = None
        if isinstance(credential, dict):
            raw_response = credential.get("response", {})
            transports = raw_response.get("transports")
            if transports:
                transports_json = json.dumps(transports)

        cursor = self._conn.execute(
            """INSERT INTO webauthn_credentials
               (user_id, credential_id, public_key, sign_count, transports)
               VALUES (?, ?, ?, ?, ?)""",
            (
                user.id,
                verification.credential_id,
                verification.credential_public_key,
                verification.sign_count,
                transports_json,
            ),
        )
        self._conn.commit()

        cred = WebAuthnCredential(
            id=cursor.lastrowid,  # type: ignore[arg-type]
            user_id=user.id,
            credential_id=verification.credential_id,
            public_key=verification.credential_public_key,
            sign_count=verification.sign_count,
            transports=json.loads(transports_json) if transports_json else None,
            created_at="",
            name="passkey",
        )
        log.info("webauthn credential registered", username=user.username, credential_id_len=len(cred.credential_id))
        return cred

    # -- Authentication --

    def generate_authentication_options(self, user: User) -> tuple[str, bytes]:
        """Generate WebAuthn authentication options.

        Returns (options_json, challenge).
        """
        credentials = self.get_credentials(user.id)
        allow = [PublicKeyCredentialDescriptor(id=c.credential_id) for c in credentials]

        options = webauthn.generate_authentication_options(
            rp_id=self._rp_id,
            allow_credentials=allow,
            user_verification=UserVerificationRequirement.PREFERRED,
        )

        challenge = options.challenge
        self._store_challenge(user.id, challenge)

        options_json = webauthn.options_to_json(options)
        return options_json, challenge

    def verify_authentication(
        self,
        user: User,
        challenge: bytes,
        credential: dict | str,
    ) -> WebAuthnCredential:
        """Verify an authentication response and update the sign count."""
        credentials = self.get_credentials(user.id)
        if not credentials:
            raise WebAuthnError("No credentials registered for this user")

        # Find the matching credential by ID
        if isinstance(credential, str):
            credential = json.loads(credential)

        raw_id = credential.get("rawId", credential.get("id", ""))
        # raw_id from the browser is base64url-encoded
        from webauthn.helpers import base64url_to_bytes

        try:
            cred_id_bytes = base64url_to_bytes(raw_id)
        except Exception:
            cred_id_bytes = raw_id.encode() if isinstance(raw_id, str) else raw_id

        matched = None
        for c in credentials:
            if c.credential_id == cred_id_bytes:
                matched = c
                break

        if matched is None:
            raise WebAuthnError("Credential not recognized")

        try:
            verification = webauthn.verify_authentication_response(
                credential=credential,
                expected_challenge=challenge,
                expected_rp_id=self._rp_id,
                expected_origin=self._origin,
                credential_public_key=matched.public_key,
                credential_current_sign_count=matched.sign_count,
            )
        except Exception as exc:
            raise WebAuthnError(f"Authentication verification failed: {exc}") from exc

        # Update sign count
        self._conn.execute(
            "UPDATE webauthn_credentials SET sign_count = ? WHERE id = ?",
            (verification.new_sign_count, matched.id),
        )
        self._conn.commit()

        matched.sign_count = verification.new_sign_count
        log.info("webauthn authentication verified", username=user.username)
        return matched

    # -- Discoverable passkey login (no username required) --

    def generate_discoverable_authentication_options(self) -> tuple[str, str]:
        """Generate WebAuthn authentication options for discoverable credentials.

        No allowCredentials is set — the browser presents all available
        passkeys for this relying party.

        Returns (options_json, session_id).  The caller must send session_id
        back alongside the credential response so the server can retrieve the
        challenge.
        """
        options = webauthn.generate_authentication_options(
            rp_id=self._rp_id,
            user_verification=UserVerificationRequirement.PREFERRED,
        )

        session_id = secrets.token_urlsafe(32)
        self._passkey_sessions[session_id] = (options.challenge, time.monotonic())

        # Periodically prune expired sessions
        if len(self._passkey_sessions) > 100:
            cutoff = time.monotonic() - _CHALLENGE_TTL_SECONDS
            self._passkey_sessions = {k: v for k, v in self._passkey_sessions.items() if v[1] > cutoff}

        options_json = webauthn.options_to_json(options)
        return options_json, session_id

    def pop_passkey_session(self, session_id: str) -> bytes | None:
        """Retrieve and consume a passkey login challenge by session ID."""
        entry = self._passkey_sessions.pop(session_id, None)
        if entry is None:
            return None
        challenge, ts = entry
        if time.monotonic() - ts > _CHALLENGE_TTL_SECONDS:
            return None
        return challenge

    def verify_discoverable_authentication(
        self,
        challenge: bytes,
        credential: dict | str,
    ) -> tuple[User, WebAuthnCredential]:
        """Verify a discoverable passkey assertion and return (user, credential).

        Looks up the user from the credential ID in the response — no prior
        knowledge of the user is needed.
        """
        if isinstance(credential, str):
            credential = json.loads(credential)

        raw_id = credential.get("rawId", credential.get("id", ""))
        try:
            cred_id_bytes = base64url_to_bytes(raw_id)
        except Exception:
            cred_id_bytes = raw_id.encode() if isinstance(raw_id, str) else raw_id

        # Look up credential in DB
        row = self._conn.execute(
            "SELECT * FROM webauthn_credentials WHERE credential_id = ?",
            (cred_id_bytes,),
        ).fetchone()
        if row is None:
            raise WebAuthnError("Credential not recognized")

        matched = row_to_webauthn_credential(row)

        # Look up user
        user_row = self._conn.execute(
            "SELECT * FROM users WHERE id = ?",
            (matched.user_id,),
        ).fetchone()
        if user_row is None:
            raise WebAuthnError("User not found for credential")
        user = row_to_user(user_row)

        try:
            verification = webauthn.verify_authentication_response(
                credential=credential,
                expected_challenge=challenge,
                expected_rp_id=self._rp_id,
                expected_origin=self._origin,
                credential_public_key=matched.public_key,
                credential_current_sign_count=matched.sign_count,
            )
        except Exception as exc:
            raise WebAuthnError(f"Authentication verification failed: {exc}") from exc

        self._conn.execute(
            "UPDATE webauthn_credentials SET sign_count = ? WHERE id = ?",
            (verification.new_sign_count, matched.id),
        )
        self._conn.commit()

        matched.sign_count = verification.new_sign_count
        log.info("passkey login verified", username=user.username)
        return user, matched
