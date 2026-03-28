"""Playwright browser tests for the aum web UI.

These tests run against a live aum server started by the ``live_server``
fixture and exercise the Svelte SPA in a real Chromium browser.  They are
ordered to run after the CLI/API integration tests so that data is already
indexed and ready to search.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_FRONTEND_DIST = Path(__file__).parent.parent.parent / "frontend" / "dist"

pytestmark = [
    pytest.mark.integration,
]


if not _FRONTEND_DIST.is_dir():
    raise RuntimeError("frontend/dist not found — build the frontend before running integration tests")

# All browser tests run after the CLI/API tests (orders 1-35).
# The search index already has data from the email ingest, and
# embed_test has embeddings.


@pytest.mark.order(36)
def test_browser_login(browser_context, live_server, admin_user) -> None:
    """Login flow: fill form, submit, verify redirect to search page."""
    page = browser_context.new_page()
    username, password = admin_user

    try:
        page.goto(f"{live_server}/")
        # Should redirect to login.
        page.wait_for_url("**/#/login", timeout=10_000)

        page.locator("input[autocomplete='username']").fill(username)
        page.locator("input[autocomplete='current-password']").fill(password)
        page.get_by_role("button", name="Sign in").click()

        # Should redirect to the search page after login.
        page.wait_for_function(
            "() => !window.location.hash.includes('/login')",
            timeout=10_000,
        )
        assert "/login" not in page.url
    finally:
        page.close()


@pytest.mark.order(37)
def test_browser_search(authenticated_page) -> None:
    """Type a query, click search, verify results appear."""
    page = authenticated_page

    # Wait for the index selector to load and auto-select an index.
    page.wait_for_function(
        "() => !document.body.innerText.includes('Select dataset')",
        timeout=15_000,
    )

    search_input = page.get_by_placeholder("Search documents...")
    search_input.fill("Ukraine")
    page.get_by_role("button", name="Search").click()

    # Wait for result cards to render.
    page.locator("button.block.w-full.text-left").first.wait_for(timeout=15_000)

    body_text = page.locator("body").inner_text()
    assert "No results found" not in body_text


@pytest.mark.order(38)
def test_browser_facets(authenticated_page) -> None:
    """Verify the facet panel appears with filters after searching."""
    page = authenticated_page

    # Ensure a search has been performed.
    search_input = page.get_by_placeholder("Search documents...")
    search_input.fill("Boris")
    page.get_by_role("button", name="Search").click()

    # Wait for result cards to render.
    page.locator("button.block.w-full.text-left").first.wait_for(timeout=15_000)

    # The facet panel should show "Filters" heading.
    filters_heading = page.get_by_text("Filters")
    filters_heading.wait_for(timeout=5_000)
    assert filters_heading.is_visible()

    # There should be at least one facet checkbox.
    checkboxes = page.locator("input[type='checkbox']")
    assert checkboxes.count() > 0

    # Click the first checkbox to apply a facet filter.
    first_checkbox = checkboxes.first
    first_checkbox.click()

    # Results should update — wait for network activity to settle.
    page.wait_for_load_state("networkidle", timeout=10_000)

    # Click "Clear" to reset facets.
    clear_button = page.get_by_role("button", name="Clear")
    if clear_button.is_visible():
        clear_button.click()
        page.wait_for_load_state("networkidle", timeout=10_000)


@pytest.mark.order(39)
def test_browser_date_facet(authenticated_page) -> None:
    """Verify that date range sliders appear for the Created facet."""
    page = authenticated_page

    search_input = page.get_by_placeholder("Search documents...")
    search_input.fill("Boris")
    page.get_by_role("button", name="Search").click()

    # Wait for result cards to render.
    page.locator("button.block.w-full.text-left").first.wait_for(timeout=15_000)

    # Look for range inputs (date sliders).
    range_inputs = page.locator("input[type='range']")
    # If the Created facet has data, there should be two range sliders.
    if range_inputs.count() >= 2:
        # Verify they are visible.
        assert range_inputs.first.is_visible()


@pytest.mark.order(40)
def test_browser_document_detail(authenticated_page) -> None:
    """Click a result card, verify document sidebar opens with content."""
    page = authenticated_page

    search_input = page.get_by_placeholder("Search documents...")
    search_input.fill("Ukraine")
    page.get_by_role("button", name="Search").click()

    # Wait for result cards to render.
    page.locator("button.block.w-full.text-left").first.wait_for(timeout=15_000)
    page.locator("button.block.w-full.text-left").first.click()

    # The document detail sidebar should appear with a "Metadata" section.
    metadata_heading = page.get_by_text("Metadata")
    metadata_heading.wait_for(timeout=10_000)
    assert metadata_heading.is_visible()

    # Should also have a "Content" section.
    content_heading = page.get_by_text("Content")
    assert content_heading.is_visible()

    # Close the sidebar by clicking the close button.
    close_button = page.locator("button").filter(has_text="\u2715")
    if close_button.count() > 0:
        close_button.first.click()
        # Metadata heading should disappear.
        page.wait_for_timeout(500)


@pytest.mark.order(41)
def test_browser_search_type_toggle(authenticated_page) -> None:
    """Verify Full text / Hybrid toggle buttons exist."""
    page = authenticated_page

    # The search type buttons should be visible in the header.
    full_text_btn = page.get_by_role("button", name="Full text")
    full_text_btn.wait_for(timeout=5_000)
    assert full_text_btn.is_visible()

    hybrid_btn = page.get_by_role("button", name="Hybrid")
    assert hybrid_btn.is_visible()


@pytest.mark.order(42)
def test_browser_logout(browser_context, live_server, admin_user) -> None:
    """Logout should redirect to the login page."""
    page = browser_context.new_page()
    username, password = admin_user

    try:
        # Login first.
        page.goto(f"{live_server}/#/login")
        page.locator("input[autocomplete='username']").fill(username)
        page.locator("input[autocomplete='current-password']").fill(password)
        page.get_by_role("button", name="Sign in").click()
        page.wait_for_function(
            "() => !window.location.hash.includes('/login')",
            timeout=10_000,
        )

        # Now logout.
        logout_button = page.get_by_role("button", name="Logout")
        logout_button.wait_for(timeout=5_000)
        logout_button.click()

        # Should redirect back to login.
        page.wait_for_url("**/#/login", timeout=10_000)
        assert "/login" in page.url
    finally:
        page.close()
