"""Tests for shared search helpers in aum.search.base."""

import pytest

from aum.search.base import MIMETYPE_ALIASES, alias_mimetype, extract_email


class TestExtractEmail:
    """Tests for extract_email()."""

    def test_bare_address(self):
        assert extract_email("jane@example.com") == "jane@example.com"

    def test_name_and_angle_brackets(self):
        assert extract_email("John Doe <john@example.com>") == "john@example.com"

    def test_angle_brackets_only(self):
        assert extract_email("<Bob@Example.COM>") == "bob@example.com"

    def test_quoted_name_with_comma(self):
        assert extract_email('"Smith, Alice" <Alice.Smith@Corp.COM>') == "alice.smith@corp.com"

    def test_unquoted_last_first_with_comma(self):
        assert (
            extract_email("johnson, boris <boris.johnson.mp@parliament.uk>")
            == "boris.johnson.mp@parliament.uk"
        )

    def test_normalizes_to_lowercase(self):
        assert extract_email("USER@DOMAIN.COM") == "user@domain.com"

    def test_strips_whitespace(self):
        assert extract_email("  user@example.com  ") == "user@example.com"

    def test_no_at_sign_returns_none(self):
        assert extract_email("not-an-email") is None

    def test_undisclosed_recipients_returns_none(self):
        assert extract_email("undisclosed-recipients:;") is None

    def test_address_as_display_name(self):
        """Address repeated as both display name and angle-bracket addr."""
        assert (
            extract_email("alexanderbj64@googlemail.com <alexanderbj64@googlemail.com>")
            == "alexanderbj64@googlemail.com"
        )


class TestAliasMimetype:
    """Tests for alias_mimetype()."""

    def test_known_type(self):
        assert alias_mimetype("application/pdf") == "PDF"

    def test_strips_parameters(self):
        assert alias_mimetype("text/html; charset=UTF-8") == "HTML"

    def test_unknown_type_returned_as_is(self):
        assert alias_mimetype("application/x-custom-thing") == "application/x-custom-thing"

    def test_unknown_with_params_strips_params(self):
        assert alias_mimetype("application/x-foo; bar=1") == "application/x-foo"

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("application/msword", "Word"),
            ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "Word"),
            ("application/vnd.ms-excel", "Excel"),
            ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "Excel"),
            ("message/rfc822", "Email"),
            ("text/plain", "Plain Text"),
            ("image/png", "PNG Image"),
        ],
    )
    def test_common_aliases(self, raw: str, expected: str):
        assert alias_mimetype(raw) == expected
