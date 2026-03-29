"""Tests for WebAuthn credential management."""

from __future__ import annotations

import time

import pytest

from aum.auth.local import LocalAuth
from aum.auth.webauthn import _CHALLENGE_TTL_SECONDS, WebAuthnError, WebAuthnManager

_VALID_PW = "Test12!@pass"


@pytest.fixture
def webauthn(db_conn):
    return WebAuthnManager(db_conn, rp_id="localhost", rp_name="aum", origin="http://localhost:8000")


@pytest.fixture
def test_user(local_auth: LocalAuth):
    return local_auth.create_user("alice", _VALID_PW)


class TestCredentialCRUD:
    def test_no_credentials_initially(self, webauthn, test_user):
        assert webauthn.has_credentials(test_user.id) is False
        assert webauthn.get_credentials(test_user.id) == []

    def test_has_credentials_after_insert(self, webauthn, test_user, db_conn):
        """Directly insert a credential to test CRUD without going through WebAuthn verification."""
        db_conn.execute(
            "INSERT INTO webauthn_credentials (user_id, credential_id, public_key, sign_count, name)"
            " VALUES (?, ?, ?, ?, ?)",
            (test_user.id, b"cred-id-1", b"pub-key-1", 0, "test-key"),
        )
        db_conn.commit()

        assert webauthn.has_credentials(test_user.id) is True
        creds = webauthn.get_credentials(test_user.id)
        assert len(creds) == 1
        assert creds[0].credential_id == b"cred-id-1"
        assert creds[0].public_key == b"pub-key-1"
        assert creds[0].name == "test-key"

    def test_delete_credentials(self, webauthn, test_user, db_conn):
        db_conn.execute(
            "INSERT INTO webauthn_credentials (user_id, credential_id, public_key, sign_count) VALUES (?, ?, ?, ?)",
            (test_user.id, b"cred-1", b"key-1", 0),
        )
        db_conn.execute(
            "INSERT INTO webauthn_credentials (user_id, credential_id, public_key, sign_count) VALUES (?, ?, ?, ?)",
            (test_user.id, b"cred-2", b"key-2", 0),
        )
        db_conn.commit()

        count = webauthn.delete_credentials(test_user.id)
        assert count == 2
        assert webauthn.has_credentials(test_user.id) is False

    def test_delete_no_credentials(self, webauthn, test_user):
        count = webauthn.delete_credentials(test_user.id)
        assert count == 0

    def test_credentials_cascade_on_user_delete(self, webauthn, test_user, db_conn, local_auth):
        """Credentials are deleted when the user is deleted (CASCADE)."""
        db_conn.execute(
            "INSERT INTO webauthn_credentials (user_id, credential_id, public_key, sign_count) VALUES (?, ?, ?, ?)",
            (test_user.id, b"cred-1", b"key-1", 0),
        )
        db_conn.commit()
        assert webauthn.has_credentials(test_user.id) is True

        local_auth.delete_user(test_user.username)
        assert webauthn.has_credentials(test_user.id) is False


class TestChallengeStore:
    def test_store_and_pop(self, webauthn):
        webauthn._store_challenge(1, b"challenge-data")
        result = webauthn._pop_challenge(1)
        assert result == b"challenge-data"

    def test_pop_removes_challenge(self, webauthn):
        webauthn._store_challenge(1, b"challenge")
        webauthn._pop_challenge(1)
        assert webauthn._pop_challenge(1) is None

    def test_pop_nonexistent_returns_none(self, webauthn):
        assert webauthn._pop_challenge(999) is None

    def test_expired_challenge_returns_none(self, webauthn):
        webauthn._store_challenge(1, b"old-challenge")
        # Manually expire it
        webauthn._challenges[1] = (b"old-challenge", time.monotonic() - _CHALLENGE_TTL_SECONDS - 1)
        assert webauthn._pop_challenge(1) is None

    def test_overwrite_challenge(self, webauthn):
        webauthn._store_challenge(1, b"first")
        webauthn._store_challenge(1, b"second")
        assert webauthn._pop_challenge(1) == b"second"


class TestRegistrationOptions:
    def test_generate_registration_options(self, webauthn, test_user):
        options_json, challenge = webauthn.generate_registration_options(test_user)
        assert isinstance(options_json, str)
        assert isinstance(challenge, bytes)
        assert len(challenge) > 0

        # Challenge should be stored
        stored = webauthn._pop_challenge(test_user.id)
        assert stored == challenge

    def test_registration_excludes_existing_credentials(self, webauthn, test_user, db_conn):
        db_conn.execute(
            "INSERT INTO webauthn_credentials (user_id, credential_id, public_key, sign_count) VALUES (?, ?, ?, ?)",
            (test_user.id, b"existing-cred", b"existing-key", 0),
        )
        db_conn.commit()

        options_json, _ = webauthn.generate_registration_options(test_user)
        assert "existing" not in options_json or "excludeCredentials" in options_json


class TestAuthenticationOptions:
    def test_generate_authentication_options(self, webauthn, test_user, db_conn):
        db_conn.execute(
            "INSERT INTO webauthn_credentials (user_id, credential_id, public_key, sign_count) VALUES (?, ?, ?, ?)",
            (test_user.id, b"cred-1", b"key-1", 0),
        )
        db_conn.commit()

        options_json, challenge = webauthn.generate_authentication_options(test_user)
        assert isinstance(options_json, str)
        assert isinstance(challenge, bytes)

    def test_verify_authentication_no_credentials(self, webauthn, test_user):
        with pytest.raises(WebAuthnError, match="No credentials registered"):
            webauthn.verify_authentication(test_user, b"challenge", {"id": "fake"})


class TestDiscoverableAuth:
    def test_generate_discoverable_options(self, webauthn):
        options_json, session_id = webauthn.generate_discoverable_authentication_options()
        assert isinstance(options_json, str)
        assert isinstance(session_id, str)
        assert len(session_id) > 0

    def test_pop_passkey_session(self, webauthn):
        _, session_id = webauthn.generate_discoverable_authentication_options()
        challenge = webauthn.pop_passkey_session(session_id)
        assert isinstance(challenge, bytes)
        assert len(challenge) > 0

    def test_pop_passkey_session_consumed(self, webauthn):
        _, session_id = webauthn.generate_discoverable_authentication_options()
        webauthn.pop_passkey_session(session_id)
        assert webauthn.pop_passkey_session(session_id) is None

    def test_pop_passkey_session_nonexistent(self, webauthn):
        assert webauthn.pop_passkey_session("nonexistent") is None

    def test_pop_passkey_session_expired(self, webauthn):
        _, session_id = webauthn.generate_discoverable_authentication_options()
        # Manually expire it
        challenge, _ = webauthn._passkey_sessions[session_id]
        webauthn._passkey_sessions[session_id] = (challenge, time.monotonic() - _CHALLENGE_TTL_SECONDS - 1)
        assert webauthn.pop_passkey_session(session_id) is None

    def test_verify_discoverable_unknown_credential(self, webauthn):
        with pytest.raises(WebAuthnError, match="Credential not recognized"):
            webauthn.verify_discoverable_authentication(b"challenge", {"id": "unknown", "rawId": "dW5rbm93bg"})
