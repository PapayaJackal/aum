"""Tests for HTTP middleware: security headers."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from aum.api.app import create_app
from aum.api.deps import get_oauth_manager


class TestSecurityHeaders:
    @pytest.fixture(autouse=True)
    def setup(self, config):
        app = create_app(config)
        app.dependency_overrides[get_oauth_manager] = lambda: None
        self.client = TestClient(app)

    def test_x_frame_options(self):
        # /api/auth/providers is unauthenticated — good target for header checks
        res = self.client.get("/api/auth/providers")
        assert res.headers["X-Frame-Options"] == "DENY"
