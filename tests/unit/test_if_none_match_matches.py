"""Tests for If-None-Match header comparison helper."""

import pytest

from hippius_s3.api.s3.common.headers import if_none_match_matches


@pytest.mark.parametrize(
    "header,md5_hash,expected",
    [
        ('"abc123"', "abc123", True),
        ('"abc123"', "def456", False),
        ("*", "abc123", True),
        (" * ", "abc123", True),
        ('"abc-5"', "abc-5", True),  # multipart ETag form
        ('W/"abc123"', "abc123", True),  # weak ETag
        ('"abc123", "def456"', "abc123", True),  # list
        ('"abc123", "def456"', "def456", True),  # list, second entry
        ('"abc123", "def456"', "ghi789", False),  # list, no match
        ("", "abc123", False),
        (None, "abc123", False),
        ('"abc123"', "", False),
        ("abc123", "abc123", True),  # unquoted (lenient)
        ('""', "abc123", False),
    ],
)
def test_if_none_match_matches(header: str | None, md5_hash: str, expected: bool) -> None:
    assert if_none_match_matches(header, md5_hash) is expected
