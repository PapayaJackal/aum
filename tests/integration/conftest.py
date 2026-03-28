"""Shared fixtures for integration tests.

These tests require external services (Meilisearch, Tika, Ollama) and are
intended to run inside the docker-compose.test.yml environment.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn
from click.testing import CliRunner
from fastapi.testclient import TestClient

from aum.api.app import create_app
from aum.api.deps import get_config
from aum.cli import main as cli_main
from aum.config import AumConfig

TESTDATA_DIR = Path("/testdata")
EMAILS_DIR = TESTDATA_DIR / "Emails"
EMBED_TEST_DIR = TESTDATA_DIR / "Embed_Test"

# ---------------------------------------------------------------------------
# Module-level shared state for cross-test communication
# ---------------------------------------------------------------------------

# Populated by facet discovery tests, consumed by facet filter tests.
discovered_facets: dict[str, list[str]] = {}

# Populated by test_create_regular_user, consumed by permission tests.
reader1_creds: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def config() -> AumConfig:
    """Load AumConfig from environment variables (set by docker-compose)."""
    get_config.cache_clear()
    return AumConfig()


@pytest.fixture(scope="session")
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(scope="session")
def app(config: AumConfig):
    """Create the FastAPI app for API and Playwright testing."""
    return create_app(config)


@pytest.fixture(scope="session")
def api_client(app):
    """HTTPX-backed TestClient for the FastAPI app."""
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="session")
def admin_user(cli_runner: CliRunner) -> tuple[str, str]:
    """Create an admin user and return (username, password)."""
    result = cli_runner.invoke(
        cli_main,
        [
            "user",
            "create",
            "admin_test",
            "--admin",
            "--generate-password",
        ],
    )
    assert result.exit_code == 0, f"Failed to create admin user: {result.output}"
    password = None
    for line in result.output.splitlines():
        if "Generated password:" in line:
            password = line.split("Generated password:", 1)[1].strip()
    assert password is not None, f"Could not parse password from: {result.output}"
    return "admin_test", password


@pytest.fixture(scope="session")
def admin_token(api_client: TestClient, admin_user: tuple[str, str]) -> str:
    """Obtain a JWT access token for the admin user."""
    username, password = admin_user
    resp = api_client.post(
        "/api/auth/login",
        json={
            "username": username,
            "password": password,
        },
    )
    assert resp.status_code == 200, f"Login failed: {resp.text}"
    return resp.json()["access_token"]


@pytest.fixture(scope="session")
def auth_headers(admin_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {admin_token}"}


# ---------------------------------------------------------------------------
# Playwright / live server fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def live_server(app) -> str:
    """Start the FastAPI app in a background thread for browser tests."""
    host, port = "127.0.0.1", 18000
    server_config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(server_config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait until the server is accepting requests.
    base_url = f"http://{host}:{port}"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            httpx.get(f"{base_url}/api/auth/providers", timeout=2)
            break
        except Exception:
            time.sleep(0.5)
    else:
        raise RuntimeError("Live server did not start within 30 seconds")

    yield base_url
    server.should_exit = True


@pytest.fixture(scope="session")
def browser_context(live_server):
    """Provide a Playwright browser context (session-scoped for speed)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        pytest.skip("playwright not installed")

    frontend_dist = Path(__file__).parent.parent.parent / "frontend" / "dist"
    if not frontend_dist.is_dir():
        pytest.skip("frontend/dist not found — build the frontend or use docker-compose.test.yml")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context()
        yield context
        context.close()
        browser.close()


@pytest.fixture
def authenticated_page(browser_context, live_server, admin_user):
    """A Playwright page already logged in as admin."""
    page = browser_context.new_page()
    username, password = admin_user

    page.goto(f"{live_server}/#/login")
    page.locator("input[autocomplete='username']").fill(username)
    page.locator("input[autocomplete='current-password']").fill(password)
    page.get_by_role("button", name="Sign in").click()

    # Wait for redirect away from login page.
    page.wait_for_function(
        "() => !window.location.hash.includes('/login')",
        timeout=10_000,
    )

    yield page
    page.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def enable_embeddings() -> None:
    """Set embedding env vars and clear the config cache."""
    os.environ["AUM_EMBEDDINGS_ENABLED"] = "true"
    os.environ["AUM_EMBEDDINGS_MODEL"] = "snowflake-arctic-embed:xs"
    os.environ["AUM_EMBEDDINGS_DIMENSION"] = "384"
    os.environ["AUM_EMBEDDINGS_QUERY_PREFIX"] = "query: "
    get_config.cache_clear()


def disable_embeddings() -> None:
    """Restore embedding env vars and clear the config cache."""
    os.environ["AUM_EMBEDDINGS_ENABLED"] = "false"
    get_config.cache_clear()


def get_reader1_token(api_client: TestClient) -> str:
    """Login as reader1 and return an access token."""
    resp = api_client.post(
        "/api/auth/login",
        json={
            "username": reader1_creds["username"],
            "password": reader1_creds["password"],
        },
    )
    assert resp.status_code == 200, f"reader1 login failed: {resp.text}"
    return resp.json()["access_token"]


def reader1_headers(api_client: TestClient) -> dict[str, str]:
    """Authorization headers for reader1."""
    return {"Authorization": f"Bearer {get_reader1_token(api_client)}"}
