"""Extract HTML body from email (.eml) files for preview rendering.

Parses MIME structure, resolves ``cid:`` references to inline ``data:`` URIs,
and falls back to plain-text wrapped in ``<pre>`` when no HTML part exists.
"""

from __future__ import annotations

import base64
import email
import email.policy
import re
from pathlib import Path

import structlog
from fastapi import HTTPException

log = structlog.get_logger()

# Match cid: references in src/href attributes (with or without angle brackets
# on the Content-ID side).
_CID_RE = re.compile(r"cid:([^\s\"'><]+)", re.IGNORECASE)


def extract_email_html(file_path: Path) -> bytes:
    """Return an HTML preview of an ``.eml`` file as UTF-8 bytes.

    * Resolves ``cid:`` image references to inline ``data:`` URIs.
    * Falls back to ``<pre>``-wrapped plain text when no HTML part exists.

    Raises :class:`~fastapi.HTTPException` (422) if the email contains no
    viewable content at all.
    """

    raw = file_path.read_bytes()
    try:
        msg = email.message_from_bytes(raw, policy=email.policy.default)
    except Exception as exc:
        log.warning("email_parse_failed", path=str(file_path), error=str(exc))
        raise HTTPException(status_code=422, detail="Could not parse email file") from exc

    # Collect Content-ID → data-URI map for inline attachments.
    cid_map: dict[str, str] = {}
    html_part: str | None = None
    text_part: str | None = None

    for part in msg.walk():
        ct = part.get_content_type()
        content_id = part.get("Content-ID", "")

        # Build cid map from parts that have a Content-ID header.
        if content_id:
            payload = part.get_payload(decode=True)
            if payload:
                mime = ct or "application/octet-stream"
                b64 = base64.b64encode(payload).decode("ascii")
                # Strip angle brackets: <img001@example.com> → img001@example.com
                cid_key = content_id.strip("<>").strip()
                cid_map[cid_key] = f"data:{mime};base64,{b64}"

        # Capture the first text/html and text/plain parts.
        if ct == "text/html" and html_part is None:
            body = part.get_content()
            if isinstance(body, bytes):
                charset = part.get_content_charset() or "utf-8"
                body = body.decode(charset, errors="replace")
            html_part = body

        elif ct == "text/plain" and text_part is None:
            body = part.get_content()
            if isinstance(body, bytes):
                charset = part.get_content_charset() or "utf-8"
                body = body.decode(charset, errors="replace")
            text_part = body

    if html_part is None and text_part is None:
        raise HTTPException(
            status_code=422,
            detail="Could not extract viewable content from email",
        )

    if html_part is not None:
        # Rewrite cid: references to data: URIs.
        def _replace_cid(match: re.Match[str]) -> str:
            cid_key = match.group(1)
            data_uri = cid_map.get(cid_key)
            if data_uri:
                return data_uri
            # Leave unresolved cid: references removed (empty string)
            # to avoid broken image indicators.
            return ""

        result = _CID_RE.sub(_replace_cid, html_part)
    else:
        # Wrap plain text in minimal HTML.
        escaped = (
            text_part.replace("&", "&amp;")  # type: ignore[union-attr]
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        result = (
            "<!DOCTYPE html><html><head>"
            '<meta charset="utf-8">'
            "<style>body{font-family:monospace;white-space:pre-wrap;padding:1em;}</style>"
            "</head><body>"
            f"<pre>{escaped}</pre>"
            "</body></html>"
        )

    return result.encode("utf-8")
