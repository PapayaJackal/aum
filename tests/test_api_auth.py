"""Tests for auth API endpoints: login, refresh, rate limiting, providers."""

from __future__ import annotations

import pytest
from conftest import _TEST_JWT_SECRET
from fastapi.testclient import TestClient

from aum.api.app import create_app
from aum.api.deps import get_config, get_local_auth, get_oauth_manager, get_token_manager, get_webauthn_manager
from aum.api.routes.auth import _login_failures
from aum.auth.tokens import TokenManager
from aum.auth.webauthn import WebAuthnManager


@pytest.fixture(autouse=True)
def _clear_rate_limits():
    _login_failures.clear()
    yield
    _login_failures.clear()


@pytest.fixture
def auth_setup(config, local_auth, db_conn):
    """Create app with real auth dependencies and a test user."""
    tokens = TokenManager(secret=_TEST_JWT_SECRET)
    webauthn = WebAuthnManager(db_conn, rp_id="localhost", rp_name="aum", origin="http://localhost:8000")
    app = create_app(config)
    app.dependency_overrides[get_local_auth] = lambda: local_auth
    app.dependency_overrides[get_token_manager] = lambda: tokens
    app.dependency_overrides[get_oauth_manager] = lambda: None
    app.dependency_overrides[get_webauthn_manager] = lambda: webauthn
    app.dependency_overrides[get_config] = lambda: config
    user = local_auth.create_user("testuser", "Test1234!", is_admin=False)
    return TestClient(app), tokens, local_auth, user, webauthn


class TestLogin:
    def test_success(self, auth_setup):
        client, *_ = auth_setup
        res = client.post("/api/auth/login", json={"username": "testuser", "password": "Test1234!"})
        assert res.status_code == 200
        data = res.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["token_type"] == "bearer"

    def test_wrong_password(self, auth_setup):
        client, *_ = auth_setup
        res = client.post("/api/auth/login", json={"username": "testuser", "password": "Wrong1234!"})
        assert res.status_code == 401

    def test_unknown_user(self, auth_setup):
        client, *_ = auth_setup
        res = client.post("/api/auth/login", json={"username": "nobody", "password": "Test1234!"})
        assert res.status_code == 401

    def test_missing_fields(self, auth_setup):
        client, *_ = auth_setup
        res = client.post("/api/auth/login", json={"username": "testuser"})
        assert res.status_code == 422


class TestRateLimit:
    def test_blocks_after_max_failures(self, auth_setup):
        client, *_ = auth_setup
        for _ in range(5):
            client.post("/api/auth/login", json={"username": "testuser", "password": "wrong"})
        # 6th attempt should be blocked even with correct password
        res = client.post("/api/auth/login", json={"username": "testuser", "password": "Test1234!"})
        assert res.status_code == 429
        assert "Too many" in res.json()["detail"]

    def test_allows_before_limit(self, auth_setup):
        client, *_ = auth_setup
        for _ in range(4):
            client.post("/api/auth/login", json={"username": "testuser", "password": "wrong"})
        # 5th attempt with correct password should succeed (only 4 failures)
        res = client.post("/api/auth/login", json={"username": "testuser", "password": "Test1234!"})
        assert res.status_code == 200


class TestRefresh:
    def test_success(self, auth_setup):
        client, tokens, _, user, *_ = auth_setup
        refresh = tokens.create_refresh_token(user)
        res = client.post("/api/auth/refresh", json={"refresh_token": refresh})
        assert res.status_code == 200
        data = res.json()
        assert "access_token" in data
        assert "refresh_token" in data

    def test_invalid_token(self, auth_setup):
        client, *_ = auth_setup
        res = client.post("/api/auth/refresh", json={"refresh_token": "invalid.token.here"})
        assert res.status_code == 401

    def test_deleted_user(self, auth_setup):
        client, tokens, local_auth, user, *_ = auth_setup
        refresh = tokens.create_refresh_token(user)
        local_auth.delete_user("testuser")
        res = client.post("/api/auth/refresh", json={"refresh_token": refresh})
        assert res.status_code == 401


class TestProviders:
    def test_no_oauth_configured(self, auth_setup):
        client, *_ = auth_setup
        res = client.get("/api/auth/providers")
        assert res.status_code == 200
        data = res.json()
        assert data["providers"] == []
        assert data["passkey_required"] is False
        assert data["passkey_login_enabled"] is False
        assert data["public_mode"] is False


class TestPasskeyLogin:
    """Test passkey-first login flow."""

    def test_passkey_begin_returns_options(self, auth_setup, config):
        config.passkey_enabled = True
        client, *_ = auth_setup
        res = client.post("/api/auth/passkey/begin")
        assert res.status_code == 200
        data = res.json()
        assert "options" in data
        assert "session_id" in data

    def test_passkey_begin_disabled(self, auth_setup):
        client, *_ = auth_setup
        res = client.post("/api/auth/passkey/begin")
        assert res.status_code == 404

    def test_passkey_complete_invalid_session(self, auth_setup, config):
        config.passkey_enabled = True
        client, *_ = auth_setup
        res = client.post(
            "/api/auth/passkey/complete",
            json={"session_id": "nonexistent", "credential": {}},
        )
        assert res.status_code == 401
        assert "expired or invalid" in res.json()["detail"]

    def test_password_login_returns_tokens_directly(self, auth_setup, config):
        """Password login should return tokens directly (no MFA challenge)."""
        config.passkey_enabled = True
        client, tokens, local_auth, user, webauthn = auth_setup
        # Even with passkeys registered, password login returns tokens
        webauthn._conn.execute(
            "INSERT INTO webauthn_credentials (user_id, credential_id, public_key, sign_count) VALUES (?, ?, ?, ?)",
            (user.id, b"fake-cred-id", b"fake-pub-key", 0),
        )
        webauthn._conn.commit()

        res = client.post("/api/auth/login", json={"username": "testuser", "password": "Test1234!"})
        assert res.status_code == 200
        data = res.json()
        assert "access_token" in data
        assert "refresh_token" in data


class TestPasskeyEnrollment:
    """Test forced passkey enrollment when passkey_required=true."""

    def test_login_forces_enrollment_without_passkeys(self, auth_setup, config):
        client, *_ = auth_setup
        config.passkey_required = True

        res = client.post("/api/auth/login", json={"username": "testuser", "password": "Test1234!"})
        assert res.status_code == 200
        data = res.json()
        assert data["passkey_enrollment_required"] is True
        assert "enrollment_token" in data
        assert "access_token" not in data

    def test_login_skips_enrollment_with_passkeys(self, auth_setup, config):
        """Users who already have passkeys should get tokens directly."""
        client, tokens, local_auth, user, webauthn = auth_setup
        config.passkey_required = True
        webauthn._conn.execute(
            "INSERT INTO webauthn_credentials (user_id, credential_id, public_key, sign_count) VALUES (?, ?, ?, ?)",
            (user.id, b"fake-cred-id", b"fake-pub-key", 0),
        )
        webauthn._conn.commit()

        res = client.post("/api/auth/login", json={"username": "testuser", "password": "Test1234!"})
        assert res.status_code == 200
        data = res.json()
        assert "access_token" in data

    def test_enroll_begin_invalid_token(self, auth_setup):
        client, *_ = auth_setup
        res = client.post("/api/auth/passkey/enroll/begin", json={"enrollment_token": "bad"})
        assert res.status_code == 401

    def test_enroll_begin_valid_token(self, auth_setup):
        client, tokens, local_auth, user, webauthn = auth_setup
        enroll_token = tokens.create_passkey_enroll_token(user)
        res = client.post("/api/auth/passkey/enroll/begin", json={"enrollment_token": enroll_token})
        assert res.status_code == 200
        assert "options" in res.json()


class TestInvitationAPI:
    def test_validate_valid_invitation(self, auth_setup):
        client, _, local_auth, *_ = auth_setup
        inv = local_auth.create_invitation("newuser")
        res = client.get(f"/api/auth/invite/{inv.token}")
        assert res.status_code == 200
        assert res.json()["username"] == "newuser"

    def test_validate_invalid_token(self, auth_setup):
        client, *_ = auth_setup
        res = client.get("/api/auth/invite/nonexistent-token")
        assert res.status_code == 404

    def test_redeem_with_password(self, auth_setup):
        client, _, local_auth, *_ = auth_setup
        inv = local_auth.create_invitation("pwuser")
        res = client.post(f"/api/auth/invite/{inv.token}/redeem", json={"password": "NewPass1!@"})
        assert res.status_code == 200
        data = res.json()
        assert "access_token" in data
        assert "refresh_token" in data

    def test_redeem_no_credentials(self, auth_setup):
        client, _, local_auth, *_ = auth_setup
        inv = local_auth.create_invitation("emptyuser")
        res = client.post(f"/api/auth/invite/{inv.token}/redeem", json={})
        assert res.status_code == 400
        assert "Must provide" in res.json()["detail"]

    def test_redeem_double_use(self, auth_setup):
        client, _, local_auth, *_ = auth_setup
        inv = local_auth.create_invitation("onceuser")
        client.post(f"/api/auth/invite/{inv.token}/redeem", json={"password": "NewPass1!@"})
        res = client.post(f"/api/auth/invite/{inv.token}/redeem", json={"password": "NewPass1!@"})
        assert res.status_code == 400


class TestPublicMode:
    @pytest.fixture
    def public_setup(self, config, local_auth, db_conn):
        """App configured in public mode."""
        config.public_mode = True
        tokens = TokenManager(secret=_TEST_JWT_SECRET)
        webauthn = WebAuthnManager(db_conn, rp_id="localhost", rp_name="aum", origin="http://localhost:8000")
        app = create_app(config)
        app.dependency_overrides[get_local_auth] = lambda: local_auth
        app.dependency_overrides[get_token_manager] = lambda: tokens
        app.dependency_overrides[get_oauth_manager] = lambda: None
        app.dependency_overrides[get_webauthn_manager] = lambda: webauthn
        app.dependency_overrides[get_config] = lambda: config
        return TestClient(app), tokens, local_auth

    def test_providers_shows_public_mode(self, public_setup):
        client, *_ = public_setup
        res = client.get("/api/auth/providers")
        assert res.json()["public_mode"] is True
