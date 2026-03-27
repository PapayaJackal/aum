"""Tests for search, document, index listing, and job API endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from aum.api.app import create_app
from aum.api.deps import get_current_user, get_oauth_manager, require_admin
from aum.models import IngestJob, JobStatus, JobType
from aum.search.base import SearchResult

_SEARCH = "aum.api.routes.search"
_JOBS = "aum.api.routes.jobs"
_DEPS = "aum.api.deps"


def _make_result(**overrides):
    defaults = dict(
        doc_id="doc1",
        source_path="/tmp/test.txt",
        display_path="test.txt",
        display_path_highlighted="<mark>test</mark>.txt",
        score=1.0,
        snippet="test content",
        metadata={},
        index="idx1",
    )
    defaults.update(overrides)
    return SearchResult(**defaults)


def _make_job(**overrides):
    defaults = dict(
        job_id="test-job",
        source_dir=Path("/data/docs"),
        index_name="idx1",
        job_type=JobType.INGEST,
        status=JobStatus.COMPLETED,
        total_files=10,
        processed=10,
        failed=0,
        errors=[],
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
    )
    defaults.update(overrides)
    return IngestJob(**defaults)


@pytest.fixture
def mock_backend():
    backend = MagicMock()
    backend.list_indices.return_value = ["idx1", "idx2"]
    backend.search_text.return_value = ([_make_result()], 1, {"File Type": ["Plain Text"]})
    backend.get_document.return_value = _make_result()
    backend.find_attachments.return_value = []
    backend.find_by_display_path.return_value = None
    return backend


@pytest.fixture
def mock_tracker():
    tracker = MagicMock()
    tracker.get_embedding_model.return_value = None
    tracker.list_jobs.return_value = [_make_job()]
    tracker.get_job.return_value = _make_job()
    return tracker


class TestListIndices:
    @pytest.fixture(autouse=True)
    def setup(self, config, local_auth, permissions, mock_backend, mock_tracker):
        self.admin = local_auth.create_user("admin", "Admin1234!", is_admin=True)
        self.user = local_auth.create_user("viewer", "View1234!", is_admin=False)
        self.permissions = permissions

        app = create_app(config)
        app.dependency_overrides[get_current_user] = lambda: self.admin
        app.dependency_overrides[get_oauth_manager] = lambda: None
        self.app = app

        with (
            patch(f"{_SEARCH}.get_config", return_value=config),
            patch(f"{_SEARCH}.get_permission_manager", return_value=permissions),
            patch(f"{_SEARCH}.make_search_backend", return_value=mock_backend),
            patch(f"{_DEPS}.make_tracker", return_value=mock_tracker),
        ):
            self.client = TestClient(app)
            yield

    def test_admin_sees_all(self):
        res = self.client.get("/api/indices")
        assert res.status_code == 200
        names = [i["name"] for i in res.json()["indices"]]
        assert names == ["idx1", "idx2"]

    def test_non_admin_filtered_by_permissions(self):
        self.app.dependency_overrides[get_current_user] = lambda: self.user
        self.permissions.grant("viewer", "idx1")
        res = self.client.get("/api/indices")
        assert res.status_code == 200
        names = [i["name"] for i in res.json()["indices"]]
        assert names == ["idx1"]

    def test_non_admin_no_permissions(self):
        self.app.dependency_overrides[get_current_user] = lambda: self.user
        res = self.client.get("/api/indices")
        assert res.status_code == 200
        assert res.json()["indices"] == []


class TestSearch:
    @pytest.fixture(autouse=True)
    def setup(self, config, local_auth, permissions, mock_backend, mock_tracker):
        self.admin = local_auth.create_user("admin", "Admin1234!", is_admin=True)
        self.user = local_auth.create_user("viewer", "View1234!", is_admin=False)
        self.permissions = permissions
        self.mock_backend = mock_backend

        app = create_app(config)
        app.dependency_overrides[get_current_user] = lambda: self.admin
        app.dependency_overrides[get_oauth_manager] = lambda: None
        self.app = app

        with (
            patch(f"{_SEARCH}.get_config", return_value=config),
            patch(f"{_SEARCH}.get_permission_manager", return_value=permissions),
            patch(f"{_SEARCH}.make_search_backend", return_value=mock_backend),
            patch(f"{_DEPS}.make_tracker", return_value=mock_tracker),
        ):
            self.client = TestClient(app)
            yield

    def test_text_search(self):
        res = self.client.get("/api/search", params={"q": "hello", "index": "idx1"})
        assert res.status_code == 200
        data = res.json()
        assert data["total"] == 1
        assert len(data["results"]) == 1
        assert data["results"][0]["doc_id"] == "doc1"
        self.mock_backend.search_text.assert_called_once()

    def test_invalid_search_type(self):
        res = self.client.get("/api/search", params={"q": "hello", "index": "idx1", "type": "invalid"})
        assert res.status_code == 400
        assert "Unknown search type" in res.json()["detail"]

    def test_invalid_filters_json(self):
        res = self.client.get("/api/search", params={"q": "hello", "index": "idx1", "filters": "not-json"})
        assert res.status_code == 400
        assert "Invalid filters" in res.json()["detail"]

    def test_unauthorized_index(self):
        self.app.dependency_overrides[get_current_user] = lambda: self.user
        res = self.client.get("/api/search", params={"q": "hello", "index": "idx1"})
        assert res.status_code == 403

    def test_authorized_index(self):
        self.app.dependency_overrides[get_current_user] = lambda: self.user
        self.permissions.grant("viewer", "idx1")
        res = self.client.get("/api/search", params={"q": "hello", "index": "idx1"})
        assert res.status_code == 200

    def test_facets_included_on_first_page(self):
        res = self.client.get("/api/search", params={"q": "hello", "index": "idx1"})
        assert res.status_code == 200
        assert res.json()["facets"] is not None

    def test_empty_query_rejected(self):
        res = self.client.get("/api/search", params={"q": "", "index": "idx1"})
        assert res.status_code == 422


class TestGetDocument:
    @pytest.fixture(autouse=True)
    def setup(self, config, local_auth, permissions, mock_backend, mock_tracker):
        self.admin = local_auth.create_user("admin", "Admin1234!", is_admin=True)
        self.user = local_auth.create_user("viewer", "View1234!", is_admin=False)
        self.permissions = permissions
        self.mock_backend = mock_backend

        app = create_app(config)
        app.dependency_overrides[get_current_user] = lambda: self.admin
        app.dependency_overrides[get_oauth_manager] = lambda: None
        self.app = app

        with (
            patch(f"{_SEARCH}.get_config", return_value=config),
            patch(f"{_SEARCH}.get_permission_manager", return_value=permissions),
            patch(f"{_SEARCH}.make_search_backend", return_value=mock_backend),
        ):
            self.client = TestClient(app)
            yield

    def test_success(self):
        res = self.client.get("/api/documents/doc1", params={"index": "idx1"})
        assert res.status_code == 200
        assert res.json()["doc_id"] == "doc1"

    def test_not_found(self):
        self.mock_backend.get_document.return_value = None
        res = self.client.get("/api/documents/missing", params={"index": "idx1"})
        assert res.status_code == 404

    def test_unauthorized_index(self):
        self.app.dependency_overrides[get_current_user] = lambda: self.user
        res = self.client.get("/api/documents/doc1", params={"index": "idx1"})
        assert res.status_code == 403


class TestDownloadDocument:
    @pytest.fixture(autouse=True)
    def setup(self, config, local_auth, permissions, mock_backend, mock_tracker):
        self.admin = local_auth.create_user("admin", "Admin1234!", is_admin=True)
        self.mock_backend = mock_backend

        app = create_app(config)
        app.dependency_overrides[get_current_user] = lambda: self.admin
        app.dependency_overrides[get_oauth_manager] = lambda: None

        with (
            patch(f"{_SEARCH}.get_config", return_value=config),
            patch(f"{_SEARCH}.get_permission_manager", return_value=permissions),
            patch(f"{_SEARCH}.make_search_backend", return_value=mock_backend),
        ):
            self.client = TestClient(app)
            yield

    def test_success(self, tmp_path):
        f = tmp_path / "download.txt"
        f.write_text("file content")
        self.mock_backend.get_document.return_value = _make_result(source_path=str(f))
        res = self.client.get("/api/documents/doc1/download", params={"index": "idx1"})
        assert res.status_code == 200
        assert res.content == b"file content"

    def test_not_found(self):
        self.mock_backend.get_document.return_value = None
        res = self.client.get("/api/documents/missing/download", params={"index": "idx1"})
        assert res.status_code == 404

    def test_symlink_blocked(self, tmp_path):
        real = tmp_path / "real.txt"
        real.write_text("content")
        link = tmp_path / "link.txt"
        link.symlink_to(real)
        self.mock_backend.get_document.return_value = _make_result(source_path=str(link))
        res = self.client.get("/api/documents/doc1/download", params={"index": "idx1"})
        assert res.status_code == 403
        assert "symlink" in res.json()["detail"].lower()

    def test_source_file_missing(self):
        self.mock_backend.get_document.return_value = _make_result(source_path="/nonexistent/file.txt")
        res = self.client.get("/api/documents/doc1/download", params={"index": "idx1"})
        assert res.status_code == 404


class TestJobs:
    @pytest.fixture(autouse=True)
    def setup(self, config, local_auth, mock_tracker):
        self.admin = local_auth.create_user("admin", "Admin1234!", is_admin=True)
        self.user = local_auth.create_user("viewer", "View1234!", is_admin=False)
        self.mock_tracker = mock_tracker

        app = create_app(config)
        app.dependency_overrides[get_current_user] = lambda: self.admin
        app.dependency_overrides[require_admin] = lambda: self.admin
        app.dependency_overrides[get_oauth_manager] = lambda: None
        self.app = app

        with patch(f"{_JOBS}.get_tracker", return_value=mock_tracker):
            self.client = TestClient(app)
            yield

    def test_list_jobs(self):
        res = self.client.get("/api/jobs")
        assert res.status_code == 200
        jobs = res.json()
        assert len(jobs) == 1
        assert jobs[0]["job_id"] == "test-job"
        assert jobs[0]["status"] == "completed"

    def test_list_jobs_non_admin(self):
        # Remove admin override so real require_admin check runs
        del self.app.dependency_overrides[require_admin]
        self.app.dependency_overrides[get_current_user] = lambda: self.user
        res = self.client.get("/api/jobs")
        assert res.status_code == 403

    def test_get_job(self):
        res = self.client.get("/api/jobs/test-job")
        assert res.status_code == 200
        data = res.json()
        assert data["job_id"] == "test-job"
        assert data["errors"] == []

    def test_get_job_not_found(self):
        self.mock_tracker.get_job.return_value = None
        res = self.client.get("/api/jobs/nonexistent")
        assert res.status_code == 404
