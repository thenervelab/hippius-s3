import base64
import contextlib
import hashlib
import hmac
import logging
import re
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Awaitable
from typing import Callable
from urllib.parse import parse_qsl
from urllib.parse import quote
from urllib.parse import urlencode

from fastapi import Request
from fastapi import Response
from starlette import status

from hippius_s3.api.middlewares.credit_check import HippiusAccount
from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.config import get_config
from hippius_s3.utils import is_public_bucket


config = get_config()
logger = logging.getLogger(__name__)


class AuthParsingError(Exception):
    pass


class SigV4Verifier:
    def __init__(
        self,
        request: Request,
    ):
        self.request = request
        self.auth_header = request.headers.get("authorization", "")
        self.amz_date = request.headers.get("x-amz-date", "")
        self.method = request.method
        # URI encode the path according to AWS Signature V4 specs (RFC3986)
        # Keep '/', and do not encode '-', '_', '.', '~'
        self.path = quote(request.url.path, safe="/-_.~")
        self.query_string = request.url.query
        self.seed_phrase = ""
        self.region = config.validator_region
        self.service = "s3"
        self.is_presigned = False
        self.credential_date = ""
        self.expires_seconds = 0

    def extract_auth_parts(self) -> bool:
        logger.debug(f"Request method: {self.method}")
        logger.debug(f"Request path: {self.path}")
        logger.debug(f"Request query: {self.query_string}")
        logger.debug(f"AMZ date header: {self.amz_date}")

        # Log ALL headers (debug only)
        logger.debug("ALL REQUEST HEADERS:")
        for key, value in self.request.headers.items():
            logger.debug(f"  {key}: {value}")

        # Check for presigned URL flow via query params
        q = self.request.query_params
        if "X-Amz-Algorithm" in q:
            algo = q.get("X-Amz-Algorithm", "")
            if algo != "AWS4-HMAC-SHA256":
                logger.error(f"FAIL: Unsupported X-Amz-Algorithm: {algo}")
                raise AuthParsingError("Unsupported algorithm")

            credential = q.get("X-Amz-Credential", "")
            signed_headers = q.get("X-Amz-SignedHeaders", "")
            signature = q.get("X-Amz-Signature", "")
            self.amz_date = q.get("X-Amz-Date", self.amz_date)
            expires_str = q.get("X-Amz-Expires", "0")

            if not credential or not signed_headers or not signature or not self.amz_date:
                logger.error("FAIL: Missing required X-Amz-* presign parameters")
                raise AuthParsingError("Missing presign params")

            try:
                self.expires_seconds = int(expires_str)
            except Exception:
                logger.error(f"FAIL: Invalid X-Amz-Expires: {expires_str}")
                raise AuthParsingError("Invalid presign expiry") from None

            # Credential format: <b64seed>/<yyyymmdd>/<region>/<service>/aws4_request
            m = re.match(r"([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^/]+)", credential)
            if not m:
                logger.error("FAIL: X-Amz-Credential regex did not match")
                raise AuthParsingError("Credentials do not match")
            encoded_seed_phrase = m.group(1)
            date_scope = m.group(2)
            self.region = m.group(3)
            self.service = m.group(4)
            scope_suffix = m.group(5)

            # Validate credential scope suffix and capture credential date for signing
            if scope_suffix != "aws4_request":
                raise AuthParsingError("Invalid credential scope suffix")
            self.credential_date = date_scope

            logger.debug("Extracted presign credential parts:")
            logger.debug(f"  Date scope: '{date_scope}'")
            logger.debug(f"  Region: '{self.region}'")
            logger.debug(f"  Service: '{self.service}'")

            padding_needed = len(encoded_seed_phrase) % 4
            if padding_needed:
                encoded_seed_phrase += "=" * (4 - padding_needed)

            try:
                seed_phrase_bytes = base64.b64decode(encoded_seed_phrase)
                self.seed_phrase = seed_phrase_bytes.decode("utf-8")
            except Exception as e:
                logger.error(f"FAIL: Base64 decode error for presign: {e}")
                raise AuthParsingError("Bad seed phrase format") from None

            if not self.seed_phrase or len(self.seed_phrase.strip()) == 0:
                logger.error("FAIL: Seed phrase is empty after decoding (presign)")
                raise AuthParsingError("Bad seed phrase format")

            self.signed_headers = signed_headers.split(";")
            if "host" not in [h.lower() for h in self.signed_headers]:
                raise AuthParsingError("X-Amz-SignedHeaders must include 'host'")
            self.provided_signature = signature
            self.is_presigned = True

            # Enforce expiration window
            try:
                dt = datetime.strptime(self.amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                # Sanity check: date in X-Amz-Date must match credential scope date
                if self.amz_date[:8] != self.credential_date:
                    raise AuthParsingError("Date scope mismatch")
                if datetime.now(timezone.utc) > dt + timedelta(seconds=self.expires_seconds):
                    logger.error("FAIL: Presigned URL has expired")
                    raise AuthParsingError("Signature expired")
            except ValueError:
                logger.error(f"FAIL: Invalid X-Amz-Date: {self.amz_date}")
                raise AuthParsingError("Invalid date") from None

            return True

        if not self.auth_header or not self.auth_header.startswith("AWS4-HMAC-SHA256"):
            logger.error("FAIL: Authorization header missing or invalid format")
            raise AuthParsingError("Credentials not found")

        logger.debug("SUCCESS: Authorization header format valid")

        credential_match = re.search(
            r"Credential=([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^,]+)",
            self.auth_header,
        )
        if not credential_match:
            logger.error("FAIL: Credential regex did not match")
            raise AuthParsingError("Credentials do not match")

        logger.debug("SUCCESS: Credential regex matched")
        encoded_seed_phrase = credential_match.group(1)
        date_scope = credential_match.group(2)
        self.region = credential_match.group(3)
        self.service = credential_match.group(4)

        logger.debug("Extracted credential parts:")
        logger.debug(f"  Date scope: '{date_scope}'")
        logger.debug(f"  Region: '{self.region}'")
        logger.debug(f"  Service: '{self.service}'")

        padding_needed = len(encoded_seed_phrase) % 4
        if padding_needed:
            encoded_seed_phrase += "=" * (4 - padding_needed)

        try:
            seed_phrase_bytes = base64.b64decode(encoded_seed_phrase)
            self.seed_phrase = seed_phrase_bytes.decode("utf-8")
            logger.debug("SUCCESS: Seed phrase decoded successfully")
        except Exception as e:
            logger.error(f"FAIL: Base64 decode error: {e}")
            raise AuthParsingError("Bad seed phrase format") from None

        if not self.seed_phrase or len(self.seed_phrase.strip()) == 0:
            logger.error("FAIL: Seed phrase is empty after decoding")
            raise AuthParsingError("Bad seed phrase format")

        signature_match = re.search(r"Signature=([a-f0-9]+)", self.auth_header)
        if not signature_match:
            logger.error("FAIL: Signature regex did not match")
            raise AuthParsingError("Signature does not match")
        provided_signature = signature_match.group(1)
        logger.debug(f"SUCCESS: Extracted signature: {provided_signature}")

        signed_headers_match = re.search(r"SignedHeaders=([^,]+)", self.auth_header)
        if not signed_headers_match:
            logger.error("FAIL: SignedHeaders regex did not match")
            raise AuthParsingError("Auth headers do not match")

        self.signed_headers = signed_headers_match.group(1).split(";")
        self.provided_signature = provided_signature
        logger.debug(f"SUCCESS: Extracted signed headers: {self.signed_headers}")

        return True

    async def create_canonical_request(self, headers: list[str]) -> str:
        logger.debug(f"Creating canonical request with signed headers: {headers}")
        canonical_headers = ""
        sorted_headers = sorted(headers, key=str.lower)

        for header in sorted_headers:
            if header.lower() == "host":
                value = (
                    self.request.headers.get("x-forwarded-host")
                    or self.request.headers.get("x-original-host")
                    or self.request.headers.get("host", "")
                )
                logger.debug(
                    f"Host header processing: x-forwarded-host={self.request.headers.get('x-forwarded-host')}, x-original-host={self.request.headers.get('x-original-host')}, host={self.request.headers.get('host')}, final_value='{value}'"
                )
            else:
                value = self.request.headers.get(header, "")

            value = " ".join((value or "").strip().split())
            canonical_headers += f"{header.lower()}:{value}\n"
            logger.debug(f"Canonical header: {header.lower()}:{value}")

        canonical_headers = canonical_headers.rstrip("\n")
        signed_headers_str = ";".join(sorted_headers)

        # For presigned URLs, exclude the X-Amz-Signature param when canonicalizing
        if self.is_presigned:
            canonical_query_string = canonicalize_query_string(self.query_string, exclude_keys={"X-Amz-Signature"})
        else:
            canonical_query_string = canonicalize_query_string(self.query_string)

        if self.is_presigned:
            payload_hash = "UNSIGNED-PAYLOAD"
        else:
            payload_hash = self.request.headers.get("x-amz-content-sha256", "")
            logger.debug(f"Payload hash from header: '{payload_hash}'")
            if not payload_hash:
                logger.error("FAIL: Missing x-amz-content-sha256 header")
                raise AuthParsingError("Missing payload hash header")
            if payload_hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
                payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                logger.debug(f"Converted streaming payload to empty body hash: {payload_hash}")

        logger.debug("Final canonical request components:")
        logger.debug(f"  Method: {self.method}")
        logger.debug(f"  Path: {self.path}")
        logger.debug(f"  Query: {canonical_query_string}")
        logger.debug(f"  Signed headers: {signed_headers_str}")
        logger.debug(f"  Payload hash: {payload_hash}")

        return f"{self.method}\n{self.path}\n{canonical_query_string}\n{canonical_headers}\n\n{signed_headers_str}\n{payload_hash}"

    def create_string_to_sign(self, canonical_request: str) -> str:
        date_scope = self.credential_date or self.amz_date[:8]
        credential_scope = f"{date_scope}/{self.region}/{self.service}/aws4_request"
        hashed_canonical_request = hashlib.sha256(canonical_request.encode()).hexdigest()
        return f"AWS4-HMAC-SHA256\n{self.amz_date}\n{credential_scope}\n{hashed_canonical_request}"

    def calculate_signature(self, string_to_sign: str) -> str:
        def sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        date_stamp = self.credential_date or self.amz_date[:8]
        k_secret = ("AWS4" + self.seed_phrase).encode("utf-8")
        k_date = sign(k_secret, date_stamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, self.service)
        k_signing = sign(k_service, "aws4_request")

        return hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

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

        signature_valid = calculated_signature == self.provided_signature
        logger.debug(f"Signature verification: {'SUCCESS' if signature_valid else 'FAIL'}")

        return signature_valid


async def verify_hmac_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    exempt_paths = ["/docs", "/openapi.json", "/robots.txt", "/metrics"]

    if request.method == "OPTIONS":
        return await call_next(request)

    path = request.url.path
    if any(path.startswith(exempt_path) for exempt_path in exempt_paths):
        return await call_next(request)

    # Check for anonymous public read bypass (GET/HEAD on public bucket objects)
    if config.enable_public_read and request.method in ["GET", "HEAD"]:
        anon_bypass = await _check_public_read_bypass(request)
        if anon_bypass:
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


async def _check_public_read_bypass(request: Request) -> bool:
    """Check if this is a valid anonymous public read that should bypass signature verification.

    Returns True if the request should proceed anonymously, False otherwise.
    """
    path = request.url.path

    # Reject if any auth markers are present (headers or presigned query params)
    if request.headers.get("authorization") or any(
        k in request.query_params for k in ["X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Signature"]
    ):
        return False

    # Parse path: /{bucket}/{object_key:path}
    # Must have at least /{bucket}/{object_key} - no bare bucket paths
    path_parts = path.strip("/").split("/")
    if len(path_parts) < 2 or not path_parts[0] or not path_parts[1]:
        return False

    bucket_name = path_parts[0]

    # Query param policy is enforced in public_router (whitelist). Middleware does not block by params.

    # Use helper to check public flag (cached)
    if not await is_public_bucket(request, bucket_name):
        return False

    # Set up anonymous access state
    request.state.access_mode = "anon"
    request.state.account = HippiusAccount(
        seed="",  # No seed phrase for anonymous access
        id="anon",
        main_account="public",  # Use "public" as main account placeholder
        has_credits=True,  # Bypass credit checks for public reads
        upload=False,
        delete=False,
    )
    # Do NOT set request.state.seed_phrase - this signals to credit middleware to skip checks

    # Rewrite path to /public/{bucket}/{object_key}
    original_path = request.scope["path"]
    new_path = f"/public{original_path}"
    request.scope["path"] = new_path
    with contextlib.suppress(Exception):
        request.scope["raw_path"] = new_path.encode("utf-8")

    return True


def canonicalize_query_string(query_string: str, exclude_keys: set[str] | None = None) -> str:
    if not query_string:
        return ""

    # Parse into list of (key, value) tuples
    params = parse_qsl(query_string, keep_blank_values=True)
    if exclude_keys:
        params = [(k, v) for (k, v) in params if k not in exclude_keys]

    # Sort by (key, value) per AWS SigV4 when duplicate keys exist
    params.sort(key=lambda kv: (kv[0], kv[1]))

    # Re-encode with proper AWS formatting per RFC3986 (do not encode -_.~)
    return urlencode(params, quote_via=quote, safe="-_.~")
