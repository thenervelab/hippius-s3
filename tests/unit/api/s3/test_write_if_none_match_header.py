"""parse_write_if_none_match: S3 supports only "*" on writes; anything else is NotImplemented."""

import pytest

from hippius_s3.api.s3.common import UnsupportedConditionalWrite
from hippius_s3.api.s3.common import parse_write_if_none_match


def test_absent_header_is_an_unconditional_write() -> None:
    assert parse_write_if_none_match(None) is False


@pytest.mark.parametrize("value", ["*", " * ", "*\t"])
def test_star_means_create_only(value: str) -> None:
    assert parse_write_if_none_match(value) is True


@pytest.mark.parametrize("value", ['"5d41402abc4b2a76b9719d911017c592"', "etag", "", "**", "*, *"])
def test_any_other_value_is_unsupported_not_ignored(value: str) -> None:
    with pytest.raises(UnsupportedConditionalWrite):
        parse_write_if_none_match(value)
