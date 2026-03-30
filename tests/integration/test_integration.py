"""Integration tests for aum — CLI, API, embeddings, and permissions.

Tests are ordered sequentially because later phases depend on state created by
earlier ones (e.g. search tests need documents to have been ingested first).
Run with ``pytest tests/integration/ -v --tb=short -x``.
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from aum.cli import main as cli_main
from aum.config import AumConfig

from .conftest import (
    EMAILS_DIR,
    EMBED_TEST_DIR,
    disable_embeddings,
    discovered_facets,
    enable_embeddings,
    reader1_creds,
    reader1_headers,
)

pytestmark = pytest.mark.integration


# ───────────────────────────────────────────────────────────────────────────
# Phase 1: Ingest without OCR
# ───────────────────────────────────────────────────────────────────────────


@pytest.mark.order(1)
def test_init_default_index(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli_main, ["init", "aum"])
    assert result.exit_code == 0, result.output
    assert "initialized" in result.output.lower()


@pytest.mark.order(2)
def test_ingest_emails_no_ocr(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(
        cli_main,
        [
            "ingest",
            "aum",
            str(EMAILS_DIR),
            "--no-ocr",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Indexed:" in result.output or "Processed:" in result.output


@pytest.mark.order(3)
def test_verify_document_count(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli_main, ["indices"])
    assert result.exit_code == 0, result.output
    assert "aum" in result.output
    # Parse the count from the table output (format: "aum<spaces>N")
    for line in result.output.splitlines():
        if line.strip().startswith("aum"):
            parts = line.split()
            if len(parts) >= 2:
                count = int(parts[-1])
                assert count > 0, f"Expected documents in index, got {count}"
                return
    pytest.fail(f"Could not find 'aum' index in output:\n{result.output}")


@pytest.mark.order(4)
def test_search_basic_text(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(
        cli_main,
        [
            "search",
            "aum",
            "Ukraine",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "results" in result.output.lower() or "0." not in result.output[:5]
    # Should find at least one result — multiple emails reference Ukraine.
    assert "No results found" not in result.output


# ───────────────────────────────────────────────────────────────────────────
# Phase 2: Reset index
# ───────────────────────────────────────────────────────────────────────────


@pytest.mark.order(5)
def test_reset_index(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli_main, ["reset", "aum", "--yes"])
    assert result.exit_code == 0, result.output
    assert "deleted" in result.output.lower()


@pytest.mark.order(6)
def test_verify_empty_after_reset(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli_main, ["indices"])
    assert result.exit_code == 0, result.output
    for line in result.output.splitlines():
        if line.strip().startswith("aum"):
            parts = line.split()
            if len(parts) >= 2:
                count = int(parts[-1])
                assert count == 0, f"Expected 0 documents after reset, got {count}"
                return
    # Index may not appear at all after reset (0 docs), that's fine.


# ───────────────────────────────────────────────────────────────────────────
# Phase 3: Ingest with OCR
# ───────────────────────────────────────────────────────────────────────────


@pytest.mark.order(7)
def test_ingest_emails_with_ocr(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(
        cli_main,
        [
            "ingest",
            "aum",
            str(EMAILS_DIR),
            "--ocr",
        ],
    )
    assert result.exit_code == 0, result.output


@pytest.mark.order(8)
def test_verify_count_after_ocr_ingest(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli_main, ["indices"])
    assert result.exit_code == 0, result.output
    for line in result.output.splitlines():
        if line.strip().startswith("aum"):
            parts = line.split()
            if len(parts) >= 2:
                count = int(parts[-1])
                assert count > 0, f"Expected documents after OCR ingest, got {count}"
                return
    pytest.fail("aum index not found after OCR ingest")


# ───────────────────────────────────────────────────────────────────────────
# Phase 4: Search via CLI with facets
# ───────────────────────────────────────────────────────────────────────────


@pytest.mark.order(9)
def test_search_cli_show_facets(cli_runner: CliRunner) -> None:
    """Search with --show-facets and discover available facet values."""
    result = cli_runner.invoke(
        cli_main,
        [
            "search",
            "aum",
            "Boris",
            "--show-facets",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "No results found" not in result.output
    assert "Available Facets" in result.output

    # Parse facet blocks from CLI output.
    current_facet: str | None = None
    for line in result.output.splitlines():
        stripped = line.strip()
        # Facet headings look like "  File Type:"
        if stripped.endswith(":") and not stripped.startswith("-"):
            current_facet = stripped.rstrip(":")
            discovered_facets[current_facet] = []
        elif stripped.startswith("- ") and current_facet is not None:
            discovered_facets[current_facet].append(stripped[2:])

    # We expect at least some of these facets from email data.
    assert len(discovered_facets) > 0, f"No facets parsed from output:\n{result.output}"

    # The Created (date) facet should be present with year-like values.
    assert "Created" in discovered_facets, (
        f"Expected 'Created' date facet in discovered facets, got: {list(discovered_facets.keys())}"
    )
    created_values = discovered_facets["Created"]
    assert len(created_values) > 0, "Created facet has no values"
    for year_val in created_values:
        assert year_val.isdigit() and len(year_val) == 4, f"Expected 4-digit year in Created facet, got: {year_val!r}"


@pytest.mark.order(10)
def test_search_cli_date_facet(cli_runner: CliRunner) -> None:
    """Date range filter via CLI should return results and show Created metadata."""
    created_values = discovered_facets.get("Created", [])
    if not created_values:
        pytest.skip("No Created facet values discovered in earlier test")

    # Use the discovered year range so we know results exist.
    min_year = min(created_values)
    max_year = max(created_values)

    result = cli_runner.invoke(
        cli_main,
        [
            "search",
            "aum",
            "Ukraine",
            "--created-from",
            min_year,
            "--created-to",
            max_year,
        ],
    )
    assert result.exit_code == 0, result.output
    assert "No results found" not in result.output

    # Verify that an impossibly narrow year range returns no results.
    result_empty = cli_runner.invoke(
        cli_main,
        [
            "search",
            "aum",
            "Ukraine",
            "--created-from",
            "1800",
            "--created-to",
            "1801",
        ],
    )
    assert result_empty.exit_code == 0, result_empty.output
    assert "No results found" in result_empty.output


@pytest.mark.order(11)
def test_search_cli_email_facet(cli_runner: CliRunner) -> None:
    emails = discovered_facets.get("Email Addresses", [])
    if not emails:
        pytest.skip("No email facets discovered in earlier test")
    result = cli_runner.invoke(
        cli_main,
        [
            "search",
            "aum",
            "meeting",
            "--email",
            emails[0],
        ],
    )
    assert result.exit_code == 0, result.output


@pytest.mark.order(12)
def test_search_cli_creator_facet(cli_runner: CliRunner) -> None:
    creators = discovered_facets.get("Creator", [])
    if not creators:
        pytest.skip("No creator facets discovered in earlier test")
    result = cli_runner.invoke(
        cli_main,
        [
            "search",
            "aum",
            "meeting",
            "--creator",
            creators[0],
        ],
    )
    assert result.exit_code == 0, result.output


@pytest.mark.order(13)
def test_search_cli_file_type_filter(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(
        cli_main,
        [
            "search",
            "aum",
            "Ukraine",
            "--file-type",
            "Email",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "No results found" not in result.output


# ───────────────────────────────────────────────────────────────────────────
# Phase 5: Search via API
# ───────────────────────────────────────────────────────────────────────────


@pytest.mark.order(14)
def test_search_api_text(api_client: TestClient, auth_headers: dict) -> None:
    resp = api_client.get(
        "/api/search",
        params={
            "q": "Ukraine",
            "index": "aum",
            "type": "text",
            "limit": 10,
        },
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] > 0
    assert len(data["results"]) > 0


@pytest.mark.order(15)
def test_search_api_facets(api_client: TestClient, auth_headers: dict) -> None:
    resp = api_client.get(
        "/api/search",
        params={
            "q": "Boris",
            "index": "aum",
            "type": "text",
            "limit": 10,
            "offset": 0,
        },
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    facets = data["facets"]
    assert facets is not None, "Expected facets on first page"
    # At least one facet category should be present.
    assert len(facets) > 0


@pytest.mark.order(16)
def test_search_api_date_facet(api_client: TestClient, auth_headers: dict) -> None:
    """Verify the Created date facet is returned with valid year values, and
    that date range filtering actually narrows results."""
    # Step 1: Fetch facets from a broad search.
    resp = api_client.get(
        "/api/search",
        params={
            "q": "Boris",
            "index": "aum",
            "type": "text",
            "limit": 10,
            "offset": 0,
        },
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    facets = data["facets"]
    assert facets is not None, "Expected facets in response"
    assert "Created" in facets, f"Expected 'Created' date facet, got keys: {list(facets.keys())}"

    created_years = facets["Created"]
    assert len(created_years) > 0, "Created facet has no values"
    for year_val in created_years:
        assert year_val.isdigit() and len(year_val) == 4, (
            f"Expected 4-digit year string in Created facet, got: {year_val!r}"
        )

    # Step 2: Filter with the discovered year range — should return results.
    min_year = min(created_years)
    max_year = max(created_years)
    filters = json.dumps({"Created": [min_year, max_year]})
    resp_filtered = api_client.get(
        "/api/search",
        params={
            "q": "Boris",
            "index": "aum",
            "type": "text",
            "filters": filters,
        },
        headers=auth_headers,
    )
    assert resp_filtered.status_code == 200
    filtered_data = resp_filtered.json()
    assert filtered_data["total"] > 0, "Date filter with valid range should return results"

    # Step 3: Filter with an impossible year range — should return zero results.
    impossible_filters = json.dumps({"Created": ["1800", "1801"]})
    resp_empty = api_client.get(
        "/api/search",
        params={
            "q": "Boris",
            "index": "aum",
            "type": "text",
            "filters": impossible_filters,
        },
        headers=auth_headers,
    )
    assert resp_empty.status_code == 200
    assert resp_empty.json()["total"] == 0, "Date filter with impossible range should return 0 results"


@pytest.mark.order(17)
def test_search_api_email_filter(api_client: TestClient, auth_headers: dict) -> None:
    # First discover an email address from facets.
    resp = api_client.get(
        "/api/search",
        params={
            "q": "Boris",
            "index": "aum",
            "type": "text",
            "limit": 5,
            "offset": 0,
        },
        headers=auth_headers,
    )
    assert resp.status_code == 200
    facets = resp.json().get("facets") or {}
    emails = facets.get("Email Addresses", [])
    if not emails:
        pytest.skip("No Email Addresses facet values returned")
    filters = json.dumps({"Email Addresses": [emails[0]]})
    resp2 = api_client.get(
        "/api/search",
        params={
            "q": "Boris",
            "index": "aum",
            "type": "text",
            "filters": filters,
        },
        headers=auth_headers,
    )
    assert resp2.status_code == 200


@pytest.mark.order(18)
def test_get_document_api(api_client: TestClient, auth_headers: dict) -> None:
    # Search to find a document ID.
    resp = api_client.get(
        "/api/search",
        params={
            "q": "Ukraine",
            "index": "aum",
            "type": "text",
            "limit": 1,
        },
        headers=auth_headers,
    )
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) > 0
    doc_id = results[0]["doc_id"]
    idx = results[0].get("index") or "aum"

    # Fetch the document detail.
    resp2 = api_client.get(f"/api/documents/{doc_id}", params={"index": idx}, headers=auth_headers)
    assert resp2.status_code == 200
    doc = resp2.json()
    assert doc["doc_id"] == doc_id
    assert doc["content"]  # non-empty


# ───────────────────────────────────────────────────────────────────────────
# Phase 6: API response shapes (GUI contract validation)
# ───────────────────────────────────────────────────────────────────────────


@pytest.mark.order(19)
def test_gui_list_indices(api_client: TestClient, auth_headers: dict) -> None:
    resp = api_client.get("/api/indices", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "indices" in data
    names = [idx["name"] for idx in data["indices"]]
    assert "aum" in names
    for idx in data["indices"]:
        assert "name" in idx
        assert "has_embeddings" in idx


@pytest.mark.order(20)
def test_gui_search_response_shape(api_client: TestClient, auth_headers: dict) -> None:
    resp = api_client.get(
        "/api/search",
        params={
            "q": "Johnson",
            "index": "aum",
            "type": "text",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "results" in data
    assert "total" in data
    assert "facets" in data
    if data["results"]:
        r = data["results"][0]
        for field in ("doc_id", "display_path", "display_path_highlighted", "score", "snippet", "metadata"):
            assert field in r, f"Missing field '{field}' in search result"


@pytest.mark.order(21)
def test_gui_document_response_shape(api_client: TestClient, auth_headers: dict) -> None:
    resp = api_client.get(
        "/api/search",
        params={
            "q": "meeting",
            "index": "aum",
            "type": "text",
            "limit": 1,
        },
        headers=auth_headers,
    )
    doc_id = resp.json()["results"][0]["doc_id"]
    resp2 = api_client.get(f"/api/documents/{doc_id}", params={"index": "aum"}, headers=auth_headers)
    assert resp2.status_code == 200
    doc = resp2.json()
    for field in ("doc_id", "display_path", "content", "metadata", "attachments", "extracted_from"):
        assert field in doc, f"Missing field '{field}' in document response"


# ───────────────────────────────────────────────────────────────────────────
# Phase 7: Embeddings
# ───────────────────────────────────────────────────────────────────────────


@pytest.mark.order(22)
def test_init_embed_test_index(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli_main, ["init", "embed_test"])
    assert result.exit_code == 0, result.output


@pytest.mark.order(23)
def test_ingest_embed_test_data(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(
        cli_main,
        [
            "ingest",
            "embed_test",
            str(EMBED_TEST_DIR),
            "--no-ocr",
        ],
    )
    assert result.exit_code == 0, result.output


@pytest.mark.order(24)
def test_embed_index(cli_runner: CliRunner) -> None:
    enable_embeddings()
    try:
        result = cli_runner.invoke(cli_main, ["embed", "embed_test"])
        assert result.exit_code == 0, result.output
        assert "completed" in result.output.lower() or "already have embeddings" in result.output.lower()
    finally:
        disable_embeddings()


@pytest.mark.order(25)
def test_search_hybrid_cli(cli_runner: CliRunner) -> None:
    enable_embeddings()
    try:
        result = cli_runner.invoke(
            cli_main,
            [
                "search",
                "embed_test",
                "TLS handshake",
                "--type",
                "hybrid",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "No results found" not in result.output
    finally:
        disable_embeddings()


@pytest.mark.order(26)
def test_search_hybrid_api(auth_headers: dict) -> None:
    enable_embeddings()
    try:
        # Need a fresh TestClient because get_config was cached with embeddings off.
        from aum.api.app import create_app

        config = AumConfig()
        app = create_app(config)
        with TestClient(app) as client:
            resp = client.get(
                "/api/search",
                params={
                    "q": "TLS handshake",
                    "index": "embed_test",
                    "type": "hybrid",
                },
                headers=auth_headers,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] > 0
    finally:
        disable_embeddings()


# ───────────────────────────────────────────────────────────────────────────
# Phase 8: User permissions
# ───────────────────────────────────────────────────────────────────────────


@pytest.mark.order(27)
def test_create_regular_user(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(
        cli_main,
        [
            "user",
            "create",
            "reader1",
            "--generate-password",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Generated password:" in result.output
    for line in result.output.splitlines():
        if "Generated password:" in line:
            password = line.split("Generated password:", 1)[1].strip()
            reader1_creds["username"] = "reader1"
            reader1_creds["password"] = password
            return
    pytest.fail("Could not parse reader1 password")


@pytest.mark.order(28)
def test_regular_user_no_access(api_client: TestClient) -> None:
    headers = reader1_headers(api_client)
    resp = api_client.get("/api/indices", headers=headers)
    assert resp.status_code == 200
    assert len(resp.json()["indices"]) == 0


@pytest.mark.order(29)
def test_regular_user_search_denied(api_client: TestClient) -> None:
    headers = reader1_headers(api_client)
    resp = api_client.get(
        "/api/search",
        params={
            "q": "test",
            "index": "aum",
            "type": "text",
        },
        headers=headers,
    )
    assert resp.status_code == 403


@pytest.mark.order(30)
def test_grant_user_aum_access(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli_main, ["user", "grant", "reader1", "aum"])
    assert result.exit_code == 0, result.output
    assert "Granted" in result.output


@pytest.mark.order(31)
def test_granted_user_can_search(api_client: TestClient) -> None:
    headers = reader1_headers(api_client)
    resp = api_client.get(
        "/api/search",
        params={
            "q": "Ukraine",
            "index": "aum",
            "type": "text",
        },
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["total"] > 0


@pytest.mark.order(32)
def test_granted_user_denied_other_index(api_client: TestClient) -> None:
    headers = reader1_headers(api_client)
    resp = api_client.get(
        "/api/search",
        params={
            "q": "TLS",
            "index": "embed_test",
            "type": "text",
        },
        headers=headers,
    )
    assert resp.status_code == 403


@pytest.mark.order(33)
def test_granted_user_sees_only_granted_indices(api_client: TestClient) -> None:
    headers = reader1_headers(api_client)
    resp = api_client.get("/api/indices", headers=headers)
    assert resp.status_code == 200
    names = [idx["name"] for idx in resp.json()["indices"]]
    assert "aum" in names
    assert "embed_test" not in names


@pytest.mark.order(34)
def test_revoke_user_access(cli_runner: CliRunner) -> None:
    result = cli_runner.invoke(cli_main, ["user", "revoke", "reader1", "aum"])
    assert result.exit_code == 0, result.output
    assert "Revoked" in result.output


@pytest.mark.order(35)
def test_revoked_user_denied_again(api_client: TestClient) -> None:
    headers = reader1_headers(api_client)
    resp = api_client.get(
        "/api/search",
        params={
            "q": "Ukraine",
            "index": "aum",
            "type": "text",
        },
        headers=headers,
    )
    assert resp.status_code == 403
