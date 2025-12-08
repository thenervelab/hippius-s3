import base64
import hashlib
import hmac
import logging
import re
from typing import Awaitable
from typing import Callable
from urllib.parse import parse_qsl
from urllib.parse import quote
from urllib.parse import urlencode

from fastapi import Request
from fastapi import Response
from starlette import status

from gateway.config import get_config
from gateway.utils.errors import s3_error_response


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


class SigV4Verifier:
    def __init__(
        self,
        request: Request,
    ):
        self.request = request
        self.auth_header = request.headers.get("authorization", "")
        self.amz_date = request.headers.get("x-amz-date", "")
        self.method = request.method

        # Use the raw_path from the ASGI scope so that we canonicalize *exactly*
        # the path that the client signed, including any percent-encoding of
        # spaces and special characters. If raw_path is missing, we treat this
        # as a configuration error rather than guessing, because any
        # re-encoding risks subtle signature mismatches.
        raw_path = request.scope.get("raw_path")
        if not isinstance(raw_path, (bytes, bytearray)):
            logger.error(
                "ASGI scope missing 'raw_path' bytes; SigV4Verifier requires "
                "raw_path to match the client-signed request path."
            )
            raise RuntimeError("ASGI scope missing 'raw_path' for SigV4 verification")

        # raw_path is already percent-encoded on the wire; decode bytes to str.
        # We assume ASCII for HTTP path bytes; if non-ASCII appears, this should
        # fail loudly rather than silently altering the path.
        self.path = raw_path.decode("ascii")
        self.query_string = request.url.query
        self.seed_phrase = ""
        self.region = config.validator_region
        self.service = "s3"

    def extract_auth_parts(self) -> bool:
        logger.debug(f"Request method: {self.method}")
        logger.debug(f"Request path: {self.path}")
        logger.debug(f"Request query: {self.query_string}")
        logger.debug(f"AMZ date header: {self.amz_date}")

        logger.debug("ALL REQUEST HEADERS:")
        for key, value in self.request.headers.items():
            logger.debug(f"  {key}: {value}")

        if not self.auth_header or not self.auth_header.startswith("AWS4-HMAC-SHA256"):
            logger.error("FAIL: Authorization header missing or invalid format")
            raise AuthParsingError("Credentials not found")

        logger.debug("SUCCESS: Authorization header format valid")

        encoded_seed_phrase, date_scope, self.region, self.service = extract_credential_from_auth_header(
            self.auth_header
        )

        logger.debug("Extracted credential parts:")
        logger.debug(f"  Date scope: '{date_scope}'")
        logger.debug(f"  Region: '{self.region}'")
        logger.debug(f"  Service: '{self.service}'")

        if not encoded_seed_phrase or len(encoded_seed_phrase.strip()) == 0:
            logger.error("FAIL: Access key is empty")
            raise AuthParsingError("Bad seed phrase format")

        padding_needed = len(encoded_seed_phrase) % 4
        if padding_needed:
            encoded_seed_phrase += "=" * (4 - padding_needed)

        seed_phrase_bytes = base64.b64decode(encoded_seed_phrase)

        if not seed_phrase_bytes:
            logger.error("FAIL: Seed phrase decoded to empty bytes")
            raise AuthParsingError("Bad seed phrase format")

        if not all(b < 128 for b in seed_phrase_bytes):
            logger.error("FAIL: Seed phrase contains non-ASCII bytes")
            raise AuthParsingError("Bad seed phrase format")

        self.seed_phrase = seed_phrase_bytes.decode("ascii")
        logger.debug("SUCCESS: Seed phrase decoded successfully")

        if not self.seed_phrase or len(self.seed_phrase.strip()) == 0:
            logger.error("FAIL: Seed phrase is empty after decoding")
            raise AuthParsingError("Bad seed phrase format")

        self.provided_signature = extract_signature_from_auth_header(self.auth_header)
        logger.debug(f"SUCCESS: Extracted signature: {self.provided_signature}")

        self.signed_headers = extract_signed_headers(self.auth_header)
        logger.debug(f"SUCCESS: Extracted signed headers: {self.signed_headers}")

        return True

    async def create_canonical_request(self, headers: list[str]) -> str:
        return await create_canonical_request(
            request=self.request,
            signed_headers=headers,
            method=self.method,
            path=self.path,
            query_string=self.query_string,
        )

    def create_string_to_sign(self, canonical_request: str) -> str:
        credential_scope = f"{self.amz_date[:8]}/{self.region}/{self.service}/aws4_request"
        hashed_canonical_request = hashlib.sha256(canonical_request.encode()).hexdigest()
        return f"AWS4-HMAC-SHA256\n{self.amz_date}\n{credential_scope}\n{hashed_canonical_request}"

    def calculate_signature(self, string_to_sign: str) -> str:
        date_stamp = self.amz_date[:8]
        return calculate_signature(
            string_to_sign=string_to_sign,
            secret=self.seed_phrase,
            date_stamp=date_stamp,
            region=self.region,
            service=self.service,
        )

    async def verify_signature(self) -> bool:
        try:
            self.extract_auth_parts()
            logger.debug("SUCCESS: Auth parts extraction completed")
        except AuthParsingError as e:
            logger.error(f"FAIL: Auth parsing failed: {e}")
            return False

        canonical_request = await self.create_canonical_request(self.signed_headers)
        logger.debug(f"Canonical request:\n{canonical_request}")

        string_to_sign = self.create_string_to_sign(canonical_request)
        logger.debug(f"String to sign:\n{string_to_sign}")

        calculated_signature = self.calculate_signature(string_to_sign)
        logger.debug(f"Calculated signature: {calculated_signature}")
        logger.debug(f"Provided signature:   {self.provided_signature}")

        signature_valid = hmac.compare_digest(calculated_signature, self.provided_signature)
        logger.debug(f"Signature verification: {'SUCCESS' if signature_valid else 'FAIL'}")

        return signature_valid


async def sigv4_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    AWS Signature V4 verification middleware for the gateway.

    Verifies the SigV4 signature and extracts the seed phrase from the request.
    Public read access is handled by ACL middleware, not here.
    """
    exempt_paths = ["/docs", "/openapi.json", "/user/", "/robots.txt", "/metrics", "/health"]

    if request.method == "OPTIONS":
        return await call_next(request)

    path = request.url.path

    if any(path.startswith(exempt_path) or path == exempt_path for exempt_path in exempt_paths):
        return await call_next(request)

    # Allow anonymous GET/HEAD requests for public bucket access
    # ACL middleware will handle authorization
    # Exclude ListBuckets (/) which always requires authentication
    if request.method in ["GET", "HEAD"]:
        auth_header = request.headers.get("authorization")
        if not auth_header and path != "/":
            # No authentication provided - continue as anonymous
            # request.state.seed_phrase will not be set
            return await call_next(request)

    verifier = SigV4Verifier(request)
    is_valid = await verifier.verify_signature()

    if not is_valid:
        return s3_error_response(
            code="SignatureDoesNotMatch",
            message="The request signature we calculated does not match the signature you provided",
            status_code=status.HTTP_403_FORBIDDEN,
        )

    request.state.seed_phrase = verifier.seed_phrase
    return await call_next(request)


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
