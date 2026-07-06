"""Read-path key/crypto failures must map to a well-formed S3 error, never a bare 500.

Covers A3 (KMS brownout → retryable 503), A5 (v5 orphan → clean 500), and the A4 unwrap-failure
mapping (InvalidTag / malformed key → clean 500). Mirrors test_pool_saturation_response.py.
"""

from __future__ import annotations

import pytest
from cryptography.exceptions import InvalidTag

from hippius_s3.api.s3.errors import read_path_crypto_error_response


@pytest.mark.parametrize("marker", ["kms_unavailable", "kms_auth_failed", "kms_error"])
def test_kms_brownout_maps_to_retryable_503(marker: str) -> None:
    resp = read_path_crypto_error_response(RuntimeError(marker))
    assert resp is not None
    assert resp.status_code == 503
    assert resp.headers.get("x-amz-error-code") == "SlowDown"
    assert resp.headers.get("Retry-After") == "3"
    assert b"SlowDown" in bytes(resp.body)


def test_v5_missing_envelope_maps_to_clean_500() -> None:
    resp = read_path_crypto_error_response(RuntimeError("v5_missing_envelope_metadata"))
    assert resp is not None
    assert resp.status_code == 500
    assert resp.headers.get("x-amz-error-code") == "InternalError"
    # a proper S3 XML body, not a bare/body-less 500
    assert b"InternalError" in bytes(resp.body)


def test_local_unwrap_failed_maps_to_clean_500() -> None:
    resp = read_path_crypto_error_response(RuntimeError("local_unwrap_failed"))
    assert resp is not None
    assert resp.status_code == 500
    assert resp.headers.get("x-amz-error-code") == "InternalError"


def test_invalid_tag_maps_to_clean_500() -> None:
    """A DEK/KEK that fails AEAD authentication (the unwrap_dek InvalidTag) → clean 500, not 500-raw."""
    resp = read_path_crypto_error_response(InvalidTag())
    assert resp is not None
    assert resp.status_code == 500
    assert resp.headers.get("x-amz-error-code") == "InternalError"
    assert b"InternalError" in bytes(resp.body)


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("something_else"),
        RuntimeError("initial_stream_timeout"),  # handled elsewhere, not here
        ValueError("wrapped_dek_too_short"),  # a bare ValueError is NOT force-mapped here
        KeyError("k"),
        Exception("generic"),
    ],
)
def test_unrelated_exceptions_pass_through(exc: BaseException) -> None:
    assert read_path_crypto_error_response(exc) is None
