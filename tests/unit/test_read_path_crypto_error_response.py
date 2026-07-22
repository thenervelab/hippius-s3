"""Read-path key/crypto failures must map to a well-formed S3 error, never a bare 500.

Covers A3 (KMS brownout → retryable 503), A5 (v5 orphan → clean 500), and the A4 unwrap-failure
mapping (InvalidTag / malformed key → clean 500). Mirrors test_pool_saturation_response.py.
"""

from __future__ import annotations

import pytest
from cryptography.exceptions import InvalidTag

from hippius_s3.api.s3.errors import read_path_crypto_error_response


@pytest.mark.parametrize(
    "marker",
    ["kms_unavailable", "kms_auth_failed", "kms_error", "kek_database_unavailable"],
)
def test_transient_key_failures_map_to_retryable_503(marker: str) -> None:
    resp = read_path_crypto_error_response(RuntimeError(marker))
    assert resp is not None
    assert resp.status_code == 503
    assert resp.headers.get("x-amz-error-code") == "SlowDown"
    assert resp.headers.get("Retry-After") == "3"
    assert b"SlowDown" in bytes(resp.body)


@pytest.mark.parametrize("marker", ["v5_missing_envelope_metadata", "local_unwrap_failed", "kek_not_found"])
def test_unreadable_key_markers_map_to_clean_500(marker: str) -> None:
    resp = read_path_crypto_error_response(RuntimeError(marker))
    assert resp is not None
    assert resp.status_code == 500
    assert resp.headers.get("x-amz-error-code") == "InternalError"
    assert b"InternalError" in bytes(resp.body)


@pytest.mark.parametrize(
    "msg",
    [
        "KMS client not initialized. Call init_kms_client() at startup.",
        "KEK abc was wrapped with KMS key k1, but HIPPIUS_KMS_MODE=disabled. Cannot decrypt...",
    ],
)
def test_kms_misconfig_sentences_map_to_clean_500(msg: str) -> None:
    """A deploy-misconfig KEK error reachable on a cold GET → clean 500, not a bare body-less one."""
    resp = read_path_crypto_error_response(RuntimeError(msg))
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
        RuntimeError("initial_stream_timeout"),  # handled by map_read_path_exception, not here
        ValueError("wrapped_dek_too_short"),  # a bare ValueError is NOT force-mapped here
        KeyError("k"),
        Exception("generic"),
    ],
)
def test_unrelated_exceptions_pass_through(exc: BaseException) -> None:
    assert read_path_crypto_error_response(exc) is None


# --------------------------------------------------------------------------------------------------
# map_read_path_exception — the full chain the global handler calls (wiring coverage: the handler
# body was previously inline and untested, which is how a bare-500 regression slipped in before).
# --------------------------------------------------------------------------------------------------

from hippius_s3.api.s3.errors import map_read_path_exception  # noqa: E402
from hippius_s3.services.object_reader import DownloadNotReadyError  # noqa: E402
from hippius_s3.storage_version import UnsupportedStorageVersionError  # noqa: E402


@pytest.mark.parametrize(
    "exc,status,code",
    [
        (DownloadNotReadyError("Parts not ready"), 503, "SlowDown"),
        (RuntimeError("initial_stream_timeout"), 503, "SlowDown"),
        (RuntimeError("kms_unavailable"), 503, "SlowDown"),
        (RuntimeError("kek_database_unavailable"), 503, "SlowDown"),
        (RuntimeError("v5_missing_envelope_metadata"), 500, "InternalError"),
        (RuntimeError("kek_not_found"), 500, "InternalError"),
        (InvalidTag(), 500, "InternalError"),
        (UnsupportedStorageVersionError(4), 501, "NotImplemented"),
        (RuntimeError("unsupported_enc_suite_id:hip-enc/unknown"), 501, "NotImplemented"),
    ],
)
def test_map_read_path_exception_maps_each_arm(exc: BaseException, status: int, code: str) -> None:
    resp = map_read_path_exception(exc)
    assert resp is not None
    assert resp.status_code == status
    assert resp.headers.get("x-amz-error-code") == code


@pytest.mark.parametrize("exc", [ValueError("nope"), KeyError("k"), RuntimeError("totally_unrelated")])
def test_map_read_path_exception_passes_unrelated_through(exc: BaseException) -> None:
    assert map_read_path_exception(exc) is None
