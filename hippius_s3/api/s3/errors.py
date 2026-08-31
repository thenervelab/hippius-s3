"""Standard error responses for S3 API."""

import uuid

import asyncpg
from fastapi import Response
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.db_pool import PoolAcquireTimeout


# Non-standard status (nginx) for "client closed the request before we answered". Nothing is
# written to the socket on this path — the peer is already gone — so it exists only to classify
# the abort for our own logs and metrics instead of letting an exception escape as a 500.
# The gateway's ForwardService uses the same value for the same event at its hop.
CLIENT_CLOSED_REQUEST = 499


class S3Error(Exception):
    """Exception class for S3-specific errors."""

    def __init__(self, code: str, status_code: int = 400, message: str = ""):
        self.code = code
        self.status_code = status_code

        # Use a default message based on the error code if none provided
        if not message:
            if code == "NoSuchBucket":
                message = "The specified bucket does not exist"
            elif code == "NoSuchKey":
                message = "The specified key does not exist"
            else:
                message = f"S3 Error: {code}"

        self.message = message
        super().__init__(message)


def _latin1_safe(value: str) -> str:
    """Coerce a header value into something latin-1 can encode, losing only unencodable chars."""
    return value.encode("latin-1", errors="replace").decode("latin-1")


def s3_error_response(
    code: str,
    message: str,
    request_id: str = "",
    status_code: int = 400,
    *,
    extra_headers: dict[str, str] | None = None,
    **kwargs: str,
) -> Response:
    """Generate a standardized S3 error response in XML format.

    Args:
        code: The S3 error code (e.g., "NoSuchBucket", "InvalidRequest")
        message: The error message
        request_id: A unique ID for the request (generated if not provided)
        status_code: The HTTP status code
        **kwargs: Additional key-value pairs to include in the error response

    Returns:
        A properly formatted S3 XML error response
    """
    # Generate request ID if not provided
    if request_id == "":
        request_id = str(uuid.uuid4())

    # Create XML using lxml. Avoid XML namespace for maximum boto3 compatibility
    # (boto3 error parser expects <Error><Code>...</Code>...</Error> without a namespace)
    root = ET.Element("Error")

    code_elem = ET.SubElement(root, "Code")
    code_elem.text = code

    message_elem = ET.SubElement(root, "Message")
    message_elem.text = message

    req_id_elem = ET.SubElement(root, "RequestId")
    req_id_elem.text = request_id

    host_id_elem = ET.SubElement(root, "HostId")
    host_id_elem.text = "hippius-s3"

    # Add any additional elements
    for key, value in kwargs.items():
        elem = ET.SubElement(root, key)
        elem.text = str(value)

    # Generate proper XML with declaration
    xml_content = ET.tostring(
        root,
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=True,
    )

    # Set standard S3 headers for error responses
    headers = {
        "Content-Type": "application/xml; charset=utf-8",
        "x-amz-request-id": request_id,
        "Content-Length": str(len(xml_content)),
        # Help SDKs parse error type/message even when they don't parse XML body
        "x-amz-error-code": code,
        # Header values are latin-1 on the wire, but `message` routinely carries client input —
        # an object key, a bucket name, a version id. A key like "日本語.txt" or an emoji would
        # raise UnicodeEncodeError while starlette serialised the response, turning a clean 404
        # into a 500 from the error path itself. The XML body is UTF-8 and carries the message
        # in full; this header is a convenience duplicate, so degrade it rather than fail.
        "x-amz-error-message": _latin1_safe(message),
    }

    if extra_headers:
        headers.update({k: str(v) for k, v in extra_headers.items()})

    return Response(content=xml_content, media_type="application/xml", status_code=status_code, headers=headers)


def pool_saturation_response(exc: BaseException) -> Response | None:
    """Map a DB connection-pool acquire failure to a retryable 503 SlowDown.

    Matches only the dedicated ``PoolAcquireTimeout`` (raised by ``db_pool.acquire_with_timeout``
    when the acquire itself times out) and server-side ``TooManyConnectionsError``. It deliberately
    does NOT match a bare ``asyncio.TimeoutError``: asyncpg raises that for per-statement
    ``command_timeout``s too, so matching the type would relabel a slow/lock-blocked query as
    transient pool saturation and prompt a retry into the same contention. Returns ``None`` otherwise.
    """
    if isinstance(exc, (PoolAcquireTimeout, asyncpg.TooManyConnectionsError)):
        return s3_error_response(
            code="SlowDown",
            message="Server busy. Please retry.",
            status_code=503,
            extra_headers={"Retry-After": "3"},
        )
    return None


def listing_timeout_response() -> Response:
    """A listing that could not return a single row before the statement timeout.

    Deliberately NOT routed through ``pool_saturation_response``: that function refuses to match a
    bare ``asyncio.TimeoutError`` precisely because it cannot tell a saturated pool from a slow
    query, and calling a slow query "pool saturation" would be a lie that sends the client straight
    back into the same contention. This is the other branch — the caller has already established
    that the query itself timed out — so the label is accurate here.

    503 rather than the 500 this used to be: the request was well-formed and the bucket exists, the
    server simply could not answer in time. That is a capacity condition, which is the class every
    SDK already retries with backoff; a 500 is neither retryable by contract nor true.

    The longer Retry-After than the pool-saturation case is the point — a retry that comes straight
    back re-runs the same scan from the same offset and fails the same way. Callers that CAN make
    progress never reach here: ``_collect_page`` returns a short truncated page instead.
    """
    return s3_error_response(
        code="SlowDown",
        message="The listing could not be completed in time. Retry, or request fewer keys with max-keys.",
        status_code=503,
        extra_headers={"Retry-After": "10"},
    )


# Stable RuntimeError markers raised by kek_service on a KMS/key failure. TRANSIENT is a brownout
# (retry helps → 503); UNREADABLE means the object's key material is unusable (retry can't help →
# 500). We string-match because kek_service raises RuntimeError(<marker>) — same convention the
# global handler already uses for "initial_stream_timeout". Keep these in sync with the raises in
# hippius_s3/services/kek_service.py.
_KEY_TRANSIENT_MARKERS = frozenset({"kms_unavailable", "kms_auth_failed", "kms_error", "kek_database_unavailable"})
_KEY_UNREADABLE_MARKERS = frozenset({"v5_missing_envelope_metadata", "local_unwrap_failed", "kek_not_found"})
# Deploy-misconfig KEK errors raised as full sentences (KMS mode disabled with KMS-wrapped keys, or
# the client never initialized). Reachable on a cold GET of a misconfigured pod — still a permanent
# 500, but with a proper S3 body instead of a bare one. Matched by a stable substring.
_KEY_MISCONFIG_SUBSTRINGS = ("KMS client not initialized", "HIPPIUS_KMS_MODE=disabled")


def read_path_crypto_error_response(exc: BaseException) -> Response | None:
    """Map read-path key/crypto failures to a well-formed S3 error instead of a bare 500.

    Transient key/DB failures (KMS brownout, keystore-DB blip) → retryable 503; a genuinely
    unreadable object (missing envelope, lost KEK, bad wrapped key, AEAD `InvalidTag`, KMS
    deploy-misconfig) → 500 with a proper S3 body. Returns ``None`` for anything else (the caller
    re-raises). `unsupported_enc_suite_id` is handled by `map_read_path_exception` (→ 501), not here.
    """
    msg = str(exc)
    if isinstance(exc, RuntimeError) and msg in _KEY_TRANSIENT_MARKERS:
        return s3_error_response(
            code="SlowDown",
            message="Encryption key service is temporarily unavailable. Please retry.",
            status_code=503,
            extra_headers={"Retry-After": "3"},
        )
    # InvalidTag (AEAD auth failure from unwrap_dek) is matched by class name to avoid importing the
    # crypto exception type here; the two marker sets and the misconfig sentences are the key errors.
    if exc.__class__.__name__ == "InvalidTag" or (
        isinstance(exc, RuntimeError)
        and (msg in _KEY_UNREADABLE_MARKERS or any(s in msg for s in _KEY_MISCONFIG_SUBSTRINGS))
    ):
        return s3_error_response(
            code="InternalError",
            message="Object could not be decrypted (encryption metadata missing or key authentication failed).",
            status_code=500,
        )
    return None


def map_read_path_exception(exc: BaseException) -> Response | None:
    """The API's global exception-handler mapping, as a testable pure function.

    Returns a well-formed S3 ``Response`` for a recognized read-path failure, or ``None`` (the
    handler then re-raises so uvicorn returns a 500). Extracted from the inline handler body so the
    WHOLE chain is unit-tested — the handler was previously inline and untested, which is how the
    `v5_missing_envelope_metadata` bare-500 regression slipped in. Order matters: the first match
    wins. `DownloadNotReadyError` and `UnsupportedStorageVersionError` are matched by class name to
    avoid importing them here (they live in modules that would import this one).
    """
    # Not-ready (download pipeline / first-chunk bound) → retryable 503.
    if exc.__class__.__name__ == "DownloadNotReadyError" or str(exc) == "initial_stream_timeout":
        return s3_error_response(
            code="SlowDown",
            message="Object not ready for download yet. Please retry.",
            status_code=503,
        )
    pool_busy = pool_saturation_response(exc)
    if pool_busy is not None:
        return pool_busy
    crypto = read_path_crypto_error_response(exc)
    if crypto is not None:
        return crypto
    # Unsupported storage version (typed) OR unknown enc-suite (read-path RuntimeError) → 501.
    if exc.__class__.__name__ == "UnsupportedStorageVersionError":
        sv = getattr(exc, "storage_version", "?")
        return s3_error_response(
            code="NotImplemented",
            message=f"Object uses unsupported storage version (sv={sv}). Migrate object to v5.",
            status_code=501,
        )
    if isinstance(exc, RuntimeError) and str(exc).startswith("unsupported_enc_suite_id"):
        return s3_error_response(
            code="NotImplemented",
            message="Object uses an unsupported encryption suite and cannot be served.",
            status_code=501,
        )
    return None
