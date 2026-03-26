"""Tests for text extraction helpers."""

from aum.extraction.tika import _condense_whitespace


class TestCondenseWhitespace:
    """Tests for _condense_whitespace()."""

    def test_plain_excess_newlines(self):
        assert _condense_whitespace("a\n\n\n\nb") == "a\n\nb"

    def test_two_newlines_preserved(self):
        assert _condense_whitespace("a\n\nb") == "a\n\nb"

    def test_single_newline_preserved(self):
        assert _condense_whitespace("a\nb") == "a\nb"

    def test_blank_lines_with_spaces(self):
        assert _condense_whitespace("a\n   \n   \n   \nb") == "a\n\nb"

    def test_blank_lines_with_tabs(self):
        assert _condense_whitespace("a\n\t\n\t\n\tb") == "a\n\n\tb"

    def test_blank_lines_with_mixed_whitespace(self):
        assert _condense_whitespace("a\n \t \n\t \n \t\nb") == "a\n\nb"

    def test_multiple_groups(self):
        text = "a\n\n\n\nb\n\n\n\nc"
        assert _condense_whitespace(text) == "a\n\nb\n\nc"

    def test_no_newlines(self):
        assert _condense_whitespace("hello world") == "hello world"

    def test_empty_string(self):
        assert _condense_whitespace("") == ""

    def test_exactly_three_blank_lines_condensed(self):
        assert _condense_whitespace("a\n\n\nb") == "a\n\nb"

    def test_whitespace_before_newlines(self):
        """Lines like 'text \\n \\n \\n' where spaces precede the newline."""
        assert _condense_whitespace("a \n \n \n \nb") == "a \n\nb"

    def test_trailing_spaces_on_blank_lines(self):
        """Mimics Tika output with ' \\n' blank lines (space before newline)."""
        text = "Betzy\n \n \n \nPolicy Advisor"
        assert _condense_whitespace(text) == "Betzy\n\nPolicy Advisor"

    def test_non_breaking_spaces(self):
        """Tika often emits non-breaking spaces (\\xa0) on blank lines."""
        text = "Hello\n\xa0\n\xa0\n\xa0\nWorld"
        assert _condense_whitespace(text) == "Hello\n\nWorld"

    def test_mixed_unicode_whitespace(self):
        """Blank lines with a mix of regular and non-breaking spaces."""
        text = "A\n \xa0 \n\t\xa0\n \nB"
        assert _condense_whitespace(text) == "A\n\nB"
