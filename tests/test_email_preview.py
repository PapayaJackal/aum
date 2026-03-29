"""Unit tests for email HTML extraction."""

from __future__ import annotations

import base64
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pytest
from fastapi import HTTPException

from aum.api.email_preview import extract_email_html


def _write_eml(path: Path, msg) -> Path:
    eml = path / "test.eml"
    eml.write_bytes(msg.as_bytes())
    return eml


class TestExtractEmailHtml:
    def test_basic_html_extraction(self, tmp_path: Path):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Test"
        msg["From"] = "alice@example.com"
        msg.attach(MIMEText("Plain text fallback", "plain"))
        msg.attach(MIMEText("<html><body><b>Hello</b></body></html>", "html"))

        eml = _write_eml(tmp_path, msg)
        result = extract_email_html(eml).decode("utf-8")

        assert "<b>Hello</b>" in result
        assert "Plain text fallback" not in result

    def test_plain_text_fallback(self, tmp_path: Path):
        msg = MIMEText("Just plain text content", "plain")
        msg["Subject"] = "Plain"
        msg["From"] = "bob@example.com"

        eml = _write_eml(tmp_path, msg)
        result = extract_email_html(eml).decode("utf-8")

        assert "<pre>" in result
        assert "Just plain text content" in result

    def test_plain_text_html_escaping(self, tmp_path: Path):
        msg = MIMEText("1 < 2 & 3 > 0", "plain")
        msg["Subject"] = "Escape test"

        eml = _write_eml(tmp_path, msg)
        result = extract_email_html(eml).decode("utf-8")

        assert "&lt;" in result
        assert "&amp;" in result
        assert "&gt;" in result

    def test_cid_rewriting(self, tmp_path: Path):
        msg = MIMEMultipart("related")
        html_part = MIMEText(
            '<html><body><img src="cid:logo123"></body></html>',
            "html",
        )
        msg.attach(html_part)

        # Attach a small PNG-like image with a Content-ID.
        img_data = b"\x89PNG\r\n\x1a\nfake-image-data"
        img_part = MIMEImage(img_data, "png")
        img_part.add_header("Content-ID", "<logo123>")
        msg.attach(img_part)

        eml = _write_eml(tmp_path, msg)
        result = extract_email_html(eml).decode("utf-8")

        expected_b64 = base64.b64encode(img_data).decode("ascii")
        assert f"data:image/png;base64,{expected_b64}" in result
        assert "cid:logo123" not in result

    def test_cid_with_at_sign(self, tmp_path: Path):
        """Content-IDs often look like email addresses: <img001@example.com>."""
        msg = MIMEMultipart("related")
        html_part = MIMEText(
            '<html><body><img src="cid:img001@example.com"></body></html>',
            "html",
        )
        msg.attach(html_part)

        img_data = b"\xff\xd8\xff\xe0fake-jpeg"
        img_part = MIMEImage(img_data, "jpeg")
        img_part.add_header("Content-ID", "<img001@example.com>")
        msg.attach(img_part)

        eml = _write_eml(tmp_path, msg)
        result = extract_email_html(eml).decode("utf-8")

        assert "data:image/jpeg;base64," in result
        assert "cid:" not in result

    def test_unresolved_cid_removed(self, tmp_path: Path):
        """cid: references without matching Content-ID should be removed."""
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText('<html><body><img src="cid:missing"></body></html>', "html"))

        eml = _write_eml(tmp_path, msg)
        result = extract_email_html(eml).decode("utf-8")

        assert "cid:" not in result

    def test_empty_body_produces_fallback(self, tmp_path: Path):
        """Email with headers but empty body still produces output (plain text fallback)."""
        eml = tmp_path / "empty.eml"
        eml.write_bytes(b"From: nobody\nSubject: empty\n\n")

        # The stdlib parser treats this as text/plain with empty content.
        # Should not crash — produces a <pre> fallback.
        result = extract_email_html(eml).decode("utf-8")
        assert "<pre>" in result

    def test_malformed_email_does_not_crash(self, tmp_path: Path):
        """Binary garbage should not raise an unhandled exception."""
        eml = tmp_path / "bad.eml"
        eml.write_bytes(b"\x00\x01\x02\x03not-an-email")

        # The stdlib email parser is lenient and treats this as text/plain.
        result = extract_email_html(eml).decode("utf-8")
        assert "<pre>" in result

    def test_charset_handling(self, tmp_path: Path):
        """Email parts with non-UTF-8 charset should be decoded correctly."""
        msg = MIMEMultipart("alternative")
        # Create an HTML part with ISO-8859-1 encoding.
        html_body = "<html><body>Caf\xe9</body></html>"
        html_bytes = html_body.encode("iso-8859-1")
        html_part = MIMEText(html_bytes.decode("iso-8859-1"), "html", "iso-8859-1")
        msg.attach(html_part)

        eml = _write_eml(tmp_path, msg)
        result = extract_email_html(eml).decode("utf-8")

        assert "Caf" in result

    def test_multipart_mixed_with_html(self, tmp_path: Path):
        """Typical email: multipart/mixed with text/html and attachments."""
        msg = MIMEMultipart("mixed")
        msg["Subject"] = "Report"
        msg.attach(MIMEText("<html><body><h1>Report</h1></body></html>", "html"))
        # Add a non-image attachment.
        attachment = MIMEText("CSV data", "csv")
        attachment.add_header("Content-Disposition", "attachment", filename="data.csv")
        msg.attach(attachment)

        eml = _write_eml(tmp_path, msg)
        result = extract_email_html(eml).decode("utf-8")

        assert "<h1>Report</h1>" in result
