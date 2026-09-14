"""parse_content_md5: RFC 1864 Content-MD5 is the base64 of the 16-byte MD5 of the body."""

import base64
import hashlib

import pytest

from hippius_s3.api.s3.common import InvalidContentMD5
from hippius_s3.api.s3.common import parse_content_md5


BODY = b"hello world"
DIGEST = hashlib.md5(BODY).digest()


def test_absent_header_means_no_check() -> None:
    assert parse_content_md5(None) is None


def test_valid_header_decodes_to_the_raw_digest() -> None:
    assert parse_content_md5(base64.b64encode(DIGEST).decode()) == DIGEST


def test_surrounding_whitespace_is_ignored() -> None:
    assert parse_content_md5(f"  {base64.b64encode(DIGEST).decode()}  ") == DIGEST


@pytest.mark.parametrize(
    "value",
    [
        "",  # present but empty
        "not base64 at all!!",
        base64.b64encode(DIGEST[:15]).decode(),  # base64, but not 16 bytes
        base64.b64encode(DIGEST + b"\x00").decode(),
        # The classic client mistake: the hex digest instead of the base64 of the raw bytes. It is
        # valid base64, so only the length check catches it.
        hashlib.md5(BODY).hexdigest(),
    ],
)
def test_malformed_header_is_invalid_digest(value: str) -> None:
    with pytest.raises(InvalidContentMD5):
        parse_content_md5(value)
