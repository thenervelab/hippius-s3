"""Comprehensive coverage for the three classifiers in hippius_s3.workers.errors.

Exercises every rule layer (status code, boto error code, exception class,
OSError errno, keyword fallback, chained __cause__) and the 404 divergence
across upload / download / unpin paths.
"""

from __future__ import annotations

import asyncio
import errno
from types import SimpleNamespace
from typing import Any

import pytest

# Lazy imports for httpx / botocore so unit tests don't hard-depend on them.
try:
    import httpx
except Exception:  # pragma: no cover
    httpx = None  # type: ignore

try:
    from botocore.exceptions import (
        BotoCoreError,
        ClientError,
        ConnectTimeoutError,
        EndpointConnectionError,
        ReadTimeoutError,
    )
except Exception:  # pragma: no cover
    ClientError = None  # type: ignore
    BotoCoreError = None  # type: ignore

from hippius_s3.workers.errors import (
    classify_download_error,
    classify_error,
    classify_unpin_error,
    classify_upload_error,
    compute_backoff_ms,
    extract_boto_error_code,
    extract_http_status_code,
    is_billing_error,
    is_retryable,
)


# --------------------------------------------------------------------------- #
# Helpers — small fakes that match real exception shapes
# --------------------------------------------------------------------------- #

class _HTTPLikeError(Exception):
    """Mimics httpx.HTTPStatusError: ``error.response.status_code`` is set."""

    def __init__(self, status_code: int, message: str = "") -> None:
        super().__init__(message or f"HTTP {status_code}")
        self.response = SimpleNamespace(status_code=status_code)


def _client_error(
    http_status: int,
    code: str = "InternalError",
    message: str = "x",
    op: str = "PutObject",
) -> ClientError:
    """Realistic botocore ClientError shape."""
    if ClientError is None:  # pragma: no cover
        pytest.skip("botocore not installed")
    return ClientError(
        {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": http_status, "RetryAttempts": 0},
        },
        op,
    )


class _HippiusAPIError(Exception):
    """Mimics the production custom exception class hierarchy."""


class _HippiusAuthenticationError(_HippiusAPIError):
    pass


# =========================================================================== #
# extract_http_status_code
# =========================================================================== #

def test_extract_from_httpx_like() -> None:
    assert extract_http_status_code(_HTTPLikeError(503)) == "503"


def test_extract_from_botocore_client_error() -> None:
    assert extract_http_status_code(_client_error(429, code="Throttling")) == "429"


def test_extract_empty_for_plain_exception() -> None:
    assert extract_http_status_code(Exception("boom")) == ""


def test_extract_empty_when_response_lacks_status() -> None:
    """An object with `response` but no `status_code` attribute should yield ''."""
    err = Exception("x")
    err.response = SimpleNamespace()  # type: ignore[attr-defined]
    assert extract_http_status_code(err) == ""


def test_extract_walks_into_chained_cause() -> None:
    inner = _HTTPLikeError(502)
    try:
        try:
            raise inner
        except Exception as e:
            raise RuntimeError("wrapper") from e
    except RuntimeError as exc:
        assert extract_http_status_code(exc) == "502"


# =========================================================================== #
# extract_boto_error_code
# =========================================================================== #

def test_extract_boto_code_present() -> None:
    assert extract_boto_error_code(_client_error(503, code="SlowDown")) == "SlowDown"


def test_extract_boto_code_absent_on_httpx_like() -> None:
    assert extract_boto_error_code(_HTTPLikeError(500)) == ""


# =========================================================================== #
# is_billing_error
# =========================================================================== #

def test_is_billing_402_status() -> None:
    assert is_billing_error(_HTTPLikeError(402)) is True


def test_is_billing_payment_required_string() -> None:
    assert is_billing_error(Exception("402 Payment Required")) is True


def test_is_billing_false_on_other_codes() -> None:
    assert is_billing_error(_HTTPLikeError(403)) is False
    assert is_billing_error(Exception("connection refused")) is False


# =========================================================================== #
# classify_upload_error — status-code layer
# =========================================================================== #

@pytest.mark.parametrize(
    "error, expected",
    [
        # 5xx → transient
        (_HTTPLikeError(500), "transient"),
        (_HTTPLikeError(502), "transient"),
        (_HTTPLikeError(503), "transient"),
        (_HTTPLikeError(504), "transient"),
        # special codes
        (_HTTPLikeError(408), "transient"),   # request timeout
        (_HTTPLikeError(429), "transient"),   # too many requests
        (_HTTPLikeError(507), "permanent"),   # disk full upstream
        (_HTTPLikeError(402), "billing"),     # out of credit (upload-only bucket)
        # 4xx (non-special) → permanent
        (_HTTPLikeError(400), "permanent"),   # bad request
        (_HTTPLikeError(401), "permanent"),   # unauthorized
        (_HTTPLikeError(403), "permanent"),   # forbidden
        (_HTTPLikeError(404), "permanent"),   # bad URL on upload
        (_HTTPLikeError(409), "permanent"),
        (_HTTPLikeError(413), "permanent"),
        (_HTTPLikeError(415), "permanent"),
        (_HTTPLikeError(422), "permanent"),
    ],
)
def test_upload_status_codes(error: Exception, expected: str) -> None:
    assert classify_upload_error(error) == expected


# =========================================================================== #
# classify_upload_error — botocore error code layer
# =========================================================================== #

@pytest.mark.parametrize(
    "code, http_status, expected",
    [
        # Transient boto codes (S3 throttling family)
        ("SlowDown", 503, "transient"),
        ("Throttling", 400, "transient"),
        ("ThrottlingException", 400, "transient"),
        ("RequestTimeout", 400, "transient"),
        ("ServiceUnavailable", 503, "transient"),
        ("InternalError", 500, "transient"),
        ("RequestTimeTooSkewed", 403, "transient"),  # transient despite 403
        # Permanent boto codes
        ("NoSuchBucket", 404, "permanent"),
        ("NoSuchKey", 404, "permanent"),
        ("AccessDenied", 403, "permanent"),
        ("SignatureDoesNotMatch", 403, "permanent"),
        ("MalformedJSON", 400, "permanent"),
        ("MethodNotAllowed", 405, "permanent"),
        ("EntityTooLarge", 413, "permanent"),
    ],
)
def test_upload_boto_codes(code: str, http_status: int, expected: str) -> None:
    assert classify_upload_error(_client_error(http_status, code=code)) == expected


# =========================================================================== #
# classify_upload_error — exception class layer
# =========================================================================== #

def test_upload_asyncio_timeout_is_transient() -> None:
    assert classify_upload_error(asyncio.TimeoutError()) == "transient"


def test_upload_builtin_connection_error_is_transient() -> None:
    assert classify_upload_error(ConnectionError("dropped")) == "transient"


def test_upload_builtin_timeout_error_is_transient() -> None:
    assert classify_upload_error(TimeoutError("deadline")) == "transient"


@pytest.mark.parametrize("errno_value", [
    errno.ECONNRESET,
    errno.ECONNREFUSED,
    errno.ETIMEDOUT,
    errno.EHOSTUNREACH,
    errno.ENETUNREACH,
    errno.EPIPE,
    errno.EAGAIN,
])
def test_upload_oserror_errno_is_transient(errno_value: int) -> None:
    exc = OSError(errno_value, "broken")
    assert classify_upload_error(exc) == "transient"


def test_upload_oserror_unknown_errno_falls_through_to_keywords() -> None:
    # ENOSPC isn't on the transient list; with no transient-keyword match → unknown
    exc = OSError(errno.ENOSPC, "no space left on device")
    assert classify_upload_error(exc) == "unknown"


@pytest.mark.skipif(httpx is None, reason="httpx not installed")
def test_upload_httpx_classes_are_transient() -> None:
    assert classify_upload_error(httpx.ConnectError("refused")) == "transient"
    assert classify_upload_error(httpx.ReadError("eof")) == "transient"
    assert classify_upload_error(httpx.WriteError("eof")) == "transient"
    # TimeoutException requires a Request kwarg in newer httpx
    try:
        exc = httpx.TimeoutException("slow", request=httpx.Request("GET", "http://x"))
    except Exception:
        exc = httpx.TimeoutException("slow")  # type: ignore[call-arg]
    assert classify_upload_error(exc) == "transient"


@pytest.mark.skipif(ClientError is None, reason="botocore not installed")
def test_upload_botocore_transport_classes_are_transient() -> None:
    assert classify_upload_error(EndpointConnectionError(endpoint_url="https://example")) == "transient"
    assert classify_upload_error(ConnectTimeoutError(endpoint_url="https://example")) == "transient"
    assert classify_upload_error(ReadTimeoutError(endpoint_url="https://example")) == "transient"


# =========================================================================== #
# classify_upload_error — keyword fallback layer
# =========================================================================== #

@pytest.mark.parametrize("error, expected", [
    # Transient keywords
    (Exception("connection refused"), "transient"),
    (Exception("read timed out"), "transient"),
    (Exception("network is unreachable"), "transient"),
    (Exception("503 Service Unavailable"), "transient"),
    (Exception("429 Too Many Requests"), "transient"),
    (Exception("rate limit exceeded"), "transient"),
    (Exception("request was throttled"), "transient"),
    (Exception("could not connect to host"), "transient"),
    (Exception("connection pool is exhausted"), "transient"),
    (Exception("remote disconnected without sending response"), "transient"),
    (Exception("deadline exceeded"), "transient"),
    (Exception("part_meta_not_ready"), "transient"),
    (Exception("part_row_missing"), "transient"),
    # Permanent keywords
    (Exception("Malformed request body"), "permanent"),
    (Exception("invalid chunk index"), "permanent"),
    (Exception("validation error: bad field"), "permanent"),
    (Exception("integrity check failed"), "permanent"),
    (Exception("insufficient storage on device"), "permanent"),
    (Exception("SignatureDoesNotMatch"), "permanent"),
    (Exception("Hippius API rejected publish"), "permanent"),
    (Exception("forbidden by policy"), "permanent"),
    (Exception("unauthorized"), "permanent"),
    (Exception("access denied"), "permanent"),
    (Exception("filesystem cache missing"), "permanent"),  # FS evicted
    # Custom exception classes
    (_HippiusAPIError("publish failed"), "permanent"),
    (_HippiusAuthenticationError("token expired"), "permanent"),
    # Nothing matches → unknown (these go to DLQ flagged for review)
    (Exception("totally novel failure"), "unknown"),
    (Exception(""), "unknown"),
])
def test_upload_keyword_and_class_fallbacks(error: Exception, expected: str) -> None:
    assert classify_upload_error(error) == expected


# =========================================================================== #
# classify_upload_error — chained __cause__ walk-through
# =========================================================================== #

def test_upload_walks_chained_cause_for_transient() -> None:
    inner = ConnectionError("upstream dropped")
    try:
        try:
            raise inner
        except Exception as e:
            raise RuntimeError("wrapper that doesn't itself match any rule") from e
    except RuntimeError as exc:
        assert classify_upload_error(exc) == "transient"


def test_upload_walks_chained_cause_for_permanent() -> None:
    inner = _HippiusAPIError("publish failed")
    try:
        try:
            raise inner
        except Exception as e:
            raise RuntimeError("opaque wrapper") from e
    except RuntimeError as exc:
        assert classify_upload_error(exc) == "permanent"


# =========================================================================== #
# classify_upload_error — disk full 507 wins over the billing check
# =========================================================================== #

def test_507_wins_over_billing() -> None:
    # Pathological: a 507 with "Payment Required" in the message. 507 wins because
    # the status-code layer runs before the billing keyword fallback in is_billing_error.
    assert classify_upload_error(_HTTPLikeError(507, "Payment Required")) == "permanent"


# =========================================================================== #
# classify_error backward-compat alias
# =========================================================================== #

def test_classify_error_alias_matches_upload() -> None:
    # Several call sites still import classify_error; it must equal classify_upload_error.
    assert classify_error(_HTTPLikeError(503)) == classify_upload_error(_HTTPLikeError(503))
    assert classify_error(_HTTPLikeError(402)) == "billing"
    assert classify_error(_HTTPLikeError(404)) == "permanent"


# =========================================================================== #
# classify_download_error — the 404 divergence is the headline behaviour
# =========================================================================== #

@pytest.mark.parametrize(
    "error, expected",
    [
        # 404 on download = object missing = permanent (treat as miss)
        (_HTTPLikeError(404), "permanent"),
        (_client_error(404, code="NoSuchKey"), "permanent"),
        # 402 doesn't exist on download paths → not 'billing', not even special-cased
        (_HTTPLikeError(402), "permanent"),  # 4xx fallback
        # Everything else mirrors upload
        (_HTTPLikeError(503), "transient"),
        (_HTTPLikeError(403), "permanent"),
        (_HTTPLikeError(507), "permanent"),
        (_client_error(503, code="SlowDown"), "transient"),
        (ConnectionError("dropped"), "transient"),
        (Exception("totally novel failure"), "unknown"),
    ],
)
def test_download_classifier(error: Exception, expected: str) -> None:
    assert classify_download_error(error) == expected


def test_download_never_returns_billing() -> None:
    # Even a literal "402 Payment Required" exception classifies as permanent (4xx),
    # never billing — downloads don't trigger Arion's credit guard.
    assert classify_download_error(_HTTPLikeError(402)) == "permanent"
    assert classify_download_error(Exception("402 Payment Required")) != "billing"


# =========================================================================== #
# classify_unpin_error — 404 transient, everything else same as upload
# =========================================================================== #

def test_unpin_404_is_transient_not_permanent() -> None:
    """The headline difference from upload/download: 404 means the pin commit
    hasn't propagated upstream yet, so we keep retrying."""
    assert classify_unpin_error(_HTTPLikeError(404)) == "transient"
    assert classify_unpin_error(_client_error(404, code="NoSuchKey")) == "transient"
    assert classify_unpin_error(Exception("HTTP 404 not found")) == "transient"


def test_unpin_5xx_and_403_match_upload() -> None:
    assert classify_unpin_error(_HTTPLikeError(503)) == "transient"
    assert classify_unpin_error(_HTTPLikeError(403)) == "permanent"
    assert classify_unpin_error(_HTTPLikeError(507)) == "permanent"


def test_unpin_402_is_not_billing() -> None:
    # Unpin path can't trigger Arion's credit guard either.
    assert classify_unpin_error(_HTTPLikeError(402)) == "permanent"  # 4xx fallback


def test_three_classifiers_diverge_on_404() -> None:
    """Concise documentation: this is the *whole* reason we need three classifiers."""
    exc = _HTTPLikeError(404)
    assert classify_upload_error(exc) == "permanent"
    assert classify_download_error(exc) == "permanent"
    assert classify_unpin_error(exc) == "transient"


# =========================================================================== #
# is_retryable — wraps classify_upload_error + attempt budget
# =========================================================================== #

def test_retryable_true_for_transient_under_budget() -> None:
    assert is_retryable(_HTTPLikeError(503), attempts=0, max_attempts=5) is True
    assert is_retryable(_HTTPLikeError(503), attempts=3, max_attempts=5) is True


def test_retryable_false_when_budget_exhausted() -> None:
    # attempts=4 means this is attempt #5; next attempt would be #6, > max
    assert is_retryable(_HTTPLikeError(503), attempts=4, max_attempts=5) is False


def test_retryable_false_for_permanent_regardless_of_budget() -> None:
    assert is_retryable(_HTTPLikeError(403), attempts=0, max_attempts=5) is False
    assert is_retryable(_HippiusAPIError("publish"), attempts=0, max_attempts=5) is False


def test_retryable_false_for_billing() -> None:
    assert is_retryable(_HTTPLikeError(402), attempts=0, max_attempts=5) is False


# =========================================================================== #
# compute_backoff_ms — exponential with jitter, capped
# =========================================================================== #

@pytest.mark.parametrize("attempt, base_floor, base_ceiling", [
    (1, 1000, 1100),
    (2, 2000, 2200),
    (3, 4000, 4400),
])
def test_backoff_within_bounds(attempt: int, base_floor: int, base_ceiling: int) -> None:
    for _ in range(40):
        v = compute_backoff_ms(attempt, base_ms=1000, max_ms=10**9)
        assert base_floor <= v <= base_ceiling


def test_backoff_capped_at_max() -> None:
    for _ in range(20):
        assert compute_backoff_ms(20, base_ms=1000, max_ms=30000) == 30000.0


def test_backoff_grows_per_attempt() -> None:
    """Even with 10% jitter the windows never overlap: attempt N+1 always > attempt N."""
    assert compute_backoff_ms(1, base_ms=1000, max_ms=10**9) <= 1100
    assert compute_backoff_ms(2, base_ms=1000, max_ms=10**9) >= 2000
    assert compute_backoff_ms(3, base_ms=1000, max_ms=10**9) >= 4000


# =========================================================================== #
# Real-world DLQ-shaped failure messages (regression coverage for "unknown" leak)
# =========================================================================== #

@pytest.mark.parametrize("real_message, expected", [
    # ----- patterns observed in the 2026-06-04 prod arion_upload_requests:dlq sample -----
    ("all connection attempts failed", "transient"),                # 137× / 200 sample
    ("Server disconnected without sending a response.", "transient"),  # 22× / 200
    ("[Errno -5] No address associated with hostname", "transient"),  # 2× — DNS resolver blip
    (
        "Authentication failed: Client error '401 Unauthorized' for url 'https://arion.hippius.com/upload' "
        "For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/401",
        "permanent",
    ),
    # ----- additional real-world shapes (httpx, boto, generic transport) -----
    ("Connection reset by peer", "transient"),
    ("HTTPConnectionPool(host='arion.hippius.com', port=443): Max retries exceeded", "transient"),
    ("upstream request timeout", "transient"),
    ("HTTP/1.1 502 Bad Gateway", "transient"),
    ("Temporary failure in name resolution", "transient"),
    # Real arion-unpinner validation rejection (what we saw in the day-2 digest)
    ("Failed to unpin arion identifier=abc123: 1 validation error", "permanent"),
    # Real api STREAM fetch failed (the PR #177 territory)
    ("STREAM fetch failed object_id=... v=1 part=14 chunk=2", "unknown"),
    # boto SlowDown verbose form
    ("An error occurred (SlowDown) when calling the PutObject operation: Please reduce your request rate.", "transient"),
])
def test_real_world_error_messages(real_message: str, expected: str) -> None:
    assert classify_upload_error(Exception(real_message)) == expected
