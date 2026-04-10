import hashlib
import hmac
import logging
import re
from urllib.parse import parse_qsl
from urllib.parse import quote
from urllib.parse import urlencode

from fastapi import Request

from gateway.config import get_config


config = get_config()
logger = logging.getLogger(__name__)


class AuthParsingError(Exception):
    pass


def extract_credential_from_auth_header(auth_header: str) -> tuple[str, str, str, str]:
    """
    Extract credential components from Authorization header.

    Returns:
        (credential_id, date_scope, region, service)
    """
    credential_match = re.search(
        r"Credential=([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^,]+)",
        auth_header,
    )
    if not credential_match:
        raise AuthParsingError("Credentials do not match")

    credential_id = credential_match.group(1)
    date_scope = credential_match.group(2)
    region = credential_match.group(3)
    service = credential_match.group(4)

    return credential_id, date_scope, region, service


def extract_signature_from_auth_header(auth_header: str) -> str:
    """Extract signature from Authorization header."""
    signature_match = re.search(r"Signature=([a-f0-9]+)", auth_header)
    if not signature_match:
        raise AuthParsingError("Signature does not match")
    return signature_match.group(1)


def extract_signed_headers(auth_header: str) -> list[str]:
    """Extract signed headers list from Authorization header."""
    signed_headers_match = re.search(r"SignedHeaders=([^,]+)", auth_header)
    if not signed_headers_match:
        raise AuthParsingError("Auth headers do not match")
    return signed_headers_match.group(1).split(";")


def canonical_path_from_scope(request: Request) -> str:
    """
    Derive the canonical request path from the ASGI scope.

    We require raw_path bytes so that we operate on the exact path that the
    client signed, including any percent-encoding of spaces and other
    characters. Falling back to request.url.path would risk subtle
    mismatches due to normalization or re-encoding in the framework stack.
    """
    raw_path = request.scope.get("raw_path")
    if not isinstance(raw_path, (bytes, bytearray)):
        logger.error(
            "ASGI scope missing 'raw_path' bytes; SigV4 canonicalization "
            "requires raw_path to match the client-signed request path."
        )
        raise RuntimeError("ASGI scope missing 'raw_path' for SigV4 canonical path")

    # raw_path is already percent-encoded on the wire; decode bytes to str
    # using ASCII, which is sufficient for HTTP path bytes. Non-ASCII should
    # raise rather than being silently altered.
    return raw_path.decode("ascii")


async def create_canonical_request(
    request: Request,
    signed_headers: list[str],
    method: str,
    path: str,
    query_string: str,
) -> str:
    """
    Create AWS SigV4 canonical request.

    Args:
        request: FastAPI request object
        signed_headers: List of header names that were signed
        method: HTTP method
        path: URL path
        query_string: Query string

    Returns:
        Canonical request string
    """
    logger.debug(f"Creating canonical request with signed headers: {signed_headers}")

    # Normalize header names to lowercase and sort for canonicalization
    lower_sorted_headers = sorted({h.lower() for h in signed_headers})
    canonical_headers = ""

    for header in lower_sorted_headers:
        if header == "host":
            value = (
                request.headers.get("x-forwarded-host")
                or request.headers.get("x-original-host")
                or request.headers.get("host", "")
            )
            logger.debug(
                f"Host header processing: x-forwarded-host={request.headers.get('x-forwarded-host')}, x-original-host={request.headers.get('x-original-host')}, host={request.headers.get('host')}, final_value='{value}'"
            )
        else:
            # Header lookup in Starlette is case-insensitive
            value = request.headers.get(header, "")

        value = " ".join((value or "").strip().split())
        canonical_headers += f"{header}:{value}\n"
        logger.debug(f"Canonical header: {header}:{value}")

    canonical_headers = canonical_headers.rstrip("\n")
    signed_headers_str = ";".join(lower_sorted_headers)

    canonical_query_string = canonicalize_query_string(query_string)

    # Detect presigned SigV4 via query parameters
    query_params = request.query_params
    is_presigned = (
        query_params.get("X-Amz-Algorithm") == "AWS4-HMAC-SHA256"
        and query_params.get("X-Amz-SignedHeaders") is not None
    )

    payload_hash = request.headers.get("x-amz-content-sha256", "")
    logger.debug(f"Payload hash from header: '{payload_hash}'")

    if not payload_hash:
        if is_presigned:
            logger.debug("No payload hash header found; treating presigned request as UNSIGNED-PAYLOAD")
            payload_hash = "UNSIGNED-PAYLOAD"
        else:
            logger.error("FAIL: Missing x-amz-content-sha256 header")
            raise AuthParsingError("Missing payload hash header")
    if payload_hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
        payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        logger.debug(f"Converted streaming payload to empty body hash: {payload_hash}")

    logger.debug("Final canonical request components:")
    logger.debug(f"  Method: {method}")
    logger.debug(f"  Path: {path}")
    logger.debug(f"  Query: {canonical_query_string}")
    logger.debug(f"  Signed headers: {signed_headers_str}")
    logger.debug(f"  Payload hash: {payload_hash}")

    return f"{method}\n{path}\n{canonical_query_string}\n{canonical_headers}\n\n{signed_headers_str}\n{payload_hash}"


def calculate_signature(
    string_to_sign: str,
    secret: str,
    date_stamp: str,
    region: str,
    service: str,
) -> str:
    """
    Calculate AWS SigV4 signature.

    Args:
        string_to_sign: The string to sign
        secret: Secret key (seed phrase or decrypted access key secret)
        date_stamp: Date in YYYYMMDD format
        region: AWS region
        service: AWS service name

    Returns:
        Hex-encoded signature
    """

    def sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_secret = ("AWS4" + secret).encode("utf-8")
    k_date = sign(k_secret, date_stamp)
    k_region = sign(k_date, region)
    k_service = sign(k_region, service)
    k_signing = sign(k_service, "aws4_request")

    return hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


def canonicalize_query_string(query_string: str) -> str:
    if not query_string:
        return ""

    # Parse into list of (key, value) tuples
    params = parse_qsl(query_string, keep_blank_values=True)

    # Sort by key name
    params.sort(key=lambda x: x[0])

    # Re-encode with proper AWS formatting
    # RFC 3986 unreserved characters: -_.~
    return urlencode(params, quote_via=quote, safe="-_.~")


def canonicalize_presigned_query_string(query_string: str) -> str:
    """
    Canonicalize query string for AWS SigV4 presigned URLs.

    This is similar to canonicalize_query_string but excludes X-Amz-Signature
    from the canonical representation, matching AWS S3 presigned URL rules.
    """
    if not query_string:
        return ""

    params = parse_qsl(query_string, keep_blank_values=True)

    # Exclude the signature itself from the canonical query string
    params = [(k, v) for (k, v) in params if k != "X-Amz-Signature"]

    # Sort by (name, value) to match AWS canonicalization for repeated keys
    params.sort(key=lambda x: (x[0], x[1]))

    # RFC 3986 unreserved characters: -_.~
    return urlencode(params, quote_via=quote, safe="-_.~")
