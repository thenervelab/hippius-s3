"""`s3_error_response` duplicates its message into the `x-amz-error-message` header.

HTTP header values are latin-1 on the wire, but the message routinely carries client input — an
object key, a bucket name, a version id. A key outside latin-1 (CJK, Cyrillic, emoji, non-ASCII
digits) therefore raised UnicodeEncodeError while the response was being serialised, turning a
clean 404 into a 500 produced by the error path itself.

The XML body is UTF-8 and must still carry the message intact; only the convenience header
degrades.
"""

from __future__ import annotations

import pytest

from hippius_s3.api.s3.errors import s3_error_response


NON_LATIN1 = [
    pytest.param("日本語.txt", id="cjk"),
    pytest.param("файл.txt", id="cyrillic"),
    pytest.param("🔥.txt", id="emoji"),
    pytest.param("٤", id="arabic-indic-digit"),
    pytest.param("café-☕", id="mixed"),
]


@pytest.mark.parametrize("key", NON_LATIN1)
def test_non_latin1_message_does_not_raise(key: str) -> None:
    resp = s3_error_response("NoSuchKey", f"The specified key {key} does not exist", status_code=404)
    assert resp.status_code == 404


@pytest.mark.parametrize("key", NON_LATIN1)
def test_header_is_latin1_encodable(key: str) -> None:
    resp = s3_error_response("NoSuchKey", f"The specified key {key} does not exist", status_code=404)
    # Starlette encodes header values as latin-1 when writing the response; if this raises, the
    # request 500s after the handler has already decided on a clean error.
    resp.headers["x-amz-error-message"].encode("latin-1")


@pytest.mark.parametrize("key", NON_LATIN1)
def test_xml_body_preserves_the_original_text(key: str) -> None:
    """Degrading the header must not degrade the body — that is where clients read the message."""
    resp = s3_error_response("NoSuchKey", f"The specified key {key} does not exist", status_code=404)
    assert key.encode("utf-8") in resp.body


@pytest.mark.parametrize("key", NON_LATIN1)
def test_extra_xml_fields_accept_non_latin1(key: str) -> None:
    resp = s3_error_response("NoSuchKey", "missing", status_code=404, Key=key, BucketName=key)
    assert resp.status_code == 404
    assert key.encode("utf-8") in resp.body


def test_ascii_message_is_unchanged_in_the_header() -> None:
    msg = "The specified key does not exist"
    resp = s3_error_response("NoSuchKey", msg, status_code=404)
    assert resp.headers["x-amz-error-message"] == msg


def test_content_length_matches_the_utf8_body() -> None:
    """Content-Length is computed from the encoded XML; a mismatch truncates the body."""
    resp = s3_error_response("NoSuchKey", "ключ 日本語 🔥", status_code=404)
    assert int(resp.headers["Content-Length"]) == len(resp.body)
