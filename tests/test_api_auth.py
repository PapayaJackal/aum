"""Tests for auth API endpoints: login, refresh, rate limiting, providers."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from aum.api.app import create_app
from aum.api.deps import get_local_auth, get_oauth_manager, get_token_manager
from aum.api.routes.auth import _login_failures
from aum.auth.tokens import TokenManager

from conftest import _TEST_JWT_SECRET


@pytest.fixture(autouse=True)
def _clear_rate_limits():
    _login_failures.clear()
    yield
    _login_failures.clear()


@pytest.fixture
def auth_setup(config, local_auth):
    """Create app with real auth dependencies and a test user."""
    tokens = TokenManager(secret=_TEST_JWT_SECRET)
    app = create_app(config)
    app.dependency_overrides[get_local_auth] = lambda: local_auth
    app.dependency_overrides[get_token_manager] = lambda: tokens
    app.dependency_overrides[get_oauth_manager] = lambda: None
    user = local_auth.create_user("testuser", "Test1234!", is_admin=False)
    return TestClient(app), tokens, local_auth, user


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
        client, tokens, _, user = auth_setup
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
        client, tokens, local_auth, user = auth_setup
        refresh = tokens.create_refresh_token(user)
        local_auth.delete_user("testuser")
        res = client.post("/api/auth/refresh", json={"refresh_token": refresh})
        assert res.status_code == 401


class TestProviders:
    def test_no_oauth_configured(self, auth_setup):
        client, *_ = auth_setup
        res = client.get("/api/auth/providers")
        assert res.status_code == 200
        assert res.json() == {"providers": []}
