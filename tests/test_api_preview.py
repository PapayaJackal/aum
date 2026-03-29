"""Tests for the document preview endpoint."""

from __future__ import annotations

import base64
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from unittest.mock import patch

import pytest

from aum.api.app import create_app
from aum.api.deps import get_oauth_manager, get_optional_user
from aum.search.base import SearchResult

_SEARCH = "aum.api.routes.search"


def _make_result(**overrides):
    defaults = dict(
        doc_id="doc1",
        source_path="/tmp/test.txt",
        display_path="test.txt",
        display_path_highlighted="test.txt",
        score=1.0,
        snippet="test content",
        metadata={},
        index="idx1",
    )
    defaults.update(overrides)
    return SearchResult(**defaults)


@pytest.fixture
def mock_backend():
    from unittest.mock import MagicMock

    backend = MagicMock()
    backend.list_indices.return_value = ["idx1"]
    return backend


class TestPreviewDocument:
    @pytest.fixture(autouse=True)
    def setup(self, config, local_auth, permissions, mock_backend):
        self.admin = local_auth.create_user("admin", "Admin1234!", is_admin=True)
        self.mock_backend = mock_backend

        app = create_app(config)
        app.dependency_overrides[get_optional_user] = lambda: self.admin
        app.dependency_overrides[get_oauth_manager] = lambda: None

        with (
            patch(f"{_SEARCH}.get_config", return_value=config),
            patch(f"{_SEARCH}.get_permission_manager", return_value=permissions),
            patch(f"{_SEARCH}.make_search_backend", return_value=mock_backend),
        ):
            from fastapi.testclient import TestClient

            self.client = TestClient(app)
            yield

    def test_preview_image(self, tmp_path):
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\xff\xd8\xff\xe0fake-jpeg")
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": "image/jpeg"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 200
        assert res.headers["content-type"] == "image/jpeg"
        assert res.headers["content-disposition"] == "inline"
        assert res.headers["x-content-type-options"] == "nosniff"
        assert "default-src 'none'" in res.headers["content-security-policy"]
        assert res.content == b"\xff\xd8\xff\xe0fake-jpeg"

    def test_preview_pdf(self, tmp_path):
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"%PDF-1.4 fake-pdf")
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": "application/pdf"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 200
        assert res.headers["content-type"] == "application/pdf"
        assert res.headers["content-disposition"] == "inline"

    def test_preview_png(self, tmp_path):
        f = tmp_path / "img.png"
        f.write_bytes(b"\x89PNG\r\n\x1a\nfake-png")
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": "image/png"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 200
        assert res.headers["content-type"] == "image/png"

    def test_svg_blocked(self, tmp_path):
        f = tmp_path / "img.svg"
        f.write_text("<svg></svg>")
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": "image/svg+xml"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 403
        assert "not permitted" in res.json()["detail"].lower()

    def test_non_previewable_type(self):
        self.mock_backend.get_document.return_value = _make_result(
            metadata={"Content-Type": "text/plain"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 415
        assert "not previewable" in res.json()["detail"].lower()

    def test_missing_content_type(self):
        self.mock_backend.get_document.return_value = _make_result(metadata={})
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 415

    def test_content_type_with_params(self, tmp_path):
        """Content-Type with parameters like charset should still match."""
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\xff\xd8\xff\xe0fake-jpeg")
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": "image/jpeg; charset=binary"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 200
        assert res.headers["content-type"] == "image/jpeg"

    def test_content_type_as_list(self, tmp_path):
        """Metadata Content-Type stored as list should use first element."""
        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\xff\xd8\xff\xe0fake-jpeg")
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": ["image/jpeg", "application/octet-stream"]},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 200

    def test_document_not_found(self):
        self.mock_backend.get_document.return_value = None
        res = self.client.get("/api/documents/missing/preview", params={"index": "idx1"})
        assert res.status_code == 404

    def test_symlink_blocked(self, tmp_path):
        real = tmp_path / "real.jpg"
        real.write_bytes(b"\xff\xd8\xff\xe0")
        link = tmp_path / "link.jpg"
        link.symlink_to(real)
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(link),
            metadata={"Content-Type": "image/jpeg"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 403
        assert "symlink" in res.json()["detail"].lower()

    def test_preview_html_file(self, tmp_path):
        f = tmp_path / "page.html"
        f.write_text("<html><body><p>Hello world</p></body></html>")
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": "text/html"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 200
        assert "text/html" in res.headers["content-type"]
        assert res.headers["content-disposition"] == "inline"
        csp = res.headers["content-security-policy"]
        assert "default-src 'none'" in csp
        assert "img-src data:" in csp
        assert b"<p>Hello world</p>" in res.content

    def test_preview_email(self, tmp_path):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Test"
        msg["From"] = "alice@example.com"
        msg.attach(MIMEText("plain fallback", "plain"))
        msg.attach(MIMEText("<html><body><b>Hi</b></body></html>", "html"))
        f = tmp_path / "test.eml"
        f.write_bytes(msg.as_bytes())
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": "message/rfc822"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 200
        assert "text/html" in res.headers["content-type"]
        csp = res.headers["content-security-policy"]
        assert "img-src data:" in csp
        assert b"<b>Hi</b>" in res.content

    def test_preview_email_with_inline_image(self, tmp_path):
        msg = MIMEMultipart("related")
        msg.attach(MIMEText('<html><body><img src="cid:img1"></body></html>', "html"))
        img_data = b"\x89PNG\r\n\x1a\nfake-png"
        img_part = MIMEImage(img_data, "png")
        img_part.add_header("Content-ID", "<img1>")
        msg.attach(img_part)
        f = tmp_path / "inline.eml"
        f.write_bytes(msg.as_bytes())
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": "message/rfc822"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 200
        expected_b64 = base64.b64encode(img_data).decode("ascii")
        assert f"data:image/png;base64,{expected_b64}".encode() in res.content
        assert b"cid:" not in res.content

    def test_preview_email_plain_text_fallback(self, tmp_path):
        msg = MIMEText("Just plain text", "plain")
        msg["Subject"] = "Plain"
        f = tmp_path / "plain.eml"
        f.write_bytes(msg.as_bytes())
        self.mock_backend.get_document.return_value = _make_result(
            source_path=str(f),
            metadata={"Content-Type": "message/rfc822"},
        )
        res = self.client.get("/api/documents/doc1/preview", params={"index": "idx1"})
        assert res.status_code == 200
        assert b"<pre>" in res.content
        assert b"Just plain text" in res.content
