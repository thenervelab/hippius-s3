import base64
import hashlib
import hmac
import logging
import re
from typing import Callable

from fastapi import Request
from fastapi import Response
from starlette import status

from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.config import get_config


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
        self.path = request.url.path
        self.query_string = request.url.query
        self.seed_phrase = ""
        self.region = config.validator_region
        self.service = "s3"

    def extract_auth_parts(self) -> bool:
        logger.info("=== HMAC AUTH DEBUGGING ===")
        logger.info(f"Request method: {self.method}")
        logger.info(f"Request path: {self.path}")
        logger.info(f"Request query: {self.query_string}")
        logger.info(f"AMZ date header: {self.amz_date}")

        # Log ALL headers
        logger.info("ALL REQUEST HEADERS:")
        for key, value in self.request.headers.items():
            logger.info(f"  {key}: {value}")

        if not self.auth_header or not self.auth_header.startswith("AWS4-HMAC-SHA256"):
            logger.error("FAIL: Authorization header missing or invalid format")
            raise AuthParsingError("Credentials not found")

        logger.info("SUCCESS: Authorization header format valid")
        logger.info(f"Full authorization header: '{self.auth_header}'")

        credential_match = re.search(
            r"Credential=([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^,]+)",
            self.auth_header,
        )
        if not credential_match:
            logger.error("FAIL: Credential regex did not match")
            raise AuthParsingError("Credentials do not match")

        logger.info("SUCCESS: Credential regex matched")
        encoded_seed_phrase = credential_match.group(1)
        date_scope = credential_match.group(2)
        self.region = credential_match.group(3)
        self.service = credential_match.group(4)

        logger.info("Extracted credential parts:")
        logger.info(f"  Encoded seed phrase: '{encoded_seed_phrase}' (length: {len(encoded_seed_phrase)})")
        logger.info(f"  Date scope: '{date_scope}'")
        logger.info(f"  Region: '{self.region}'")
        logger.info(f"  Service: '{self.service}'")

        padding_needed = len(encoded_seed_phrase) % 4
        if padding_needed:
            encoded_seed_phrase += "=" * (4 - padding_needed)
            logger.info(f"Added base64 padding: '{encoded_seed_phrase}' (length: {len(encoded_seed_phrase)})")

        try:
            seed_phrase_bytes = base64.b64decode(encoded_seed_phrase)
            self.seed_phrase = seed_phrase_bytes.decode("utf-8")
            logger.info(
                f"SUCCESS: Decoded seed phrase: '{self.seed_phrase}' (length: {len(self.seed_phrase.split())} words)"
            )
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
        logger.info(f"SUCCESS: Extracted signature: {provided_signature}")

        signed_headers_match = re.search(r"SignedHeaders=([^,]+)", self.auth_header)
        if not signed_headers_match:
            logger.error("FAIL: SignedHeaders regex did not match")
            raise AuthParsingError("Auth headers do not match")

        self.signed_headers = signed_headers_match.group(1).split(";")
        self.provided_signature = provided_signature
        logger.info(f"SUCCESS: Extracted signed headers: {self.signed_headers}")

        return provided_signature

    async def create_canonical_request(self, headers) -> str:
        logger.info(f"Creating canonical request with signed headers: {headers}")
        canonical_headers = ""
        sorted_headers = sorted(headers, key=str.lower)

        for header in sorted_headers:
            if header.lower() == "host":
                value = (
                    self.request.headers.get("x-forwarded-host")
                    or self.request.headers.get("x-original-host")
                    or self.request.headers.get("host", "")
                )
                logger.info(
                    f"Host header processing: x-forwarded-host={self.request.headers.get('x-forwarded-host')}, x-original-host={self.request.headers.get('x-original-host')}, host={self.request.headers.get('host')}, final_value='{value}'"
                )
            else:
                value = self.request.headers.get(header, "")

            value = " ".join(value.strip().split())
            canonical_headers += f"{header.lower()}:{value}\n"
            logger.info(f"Canonical header: {header.lower()}:{value}")

        canonical_headers = canonical_headers.rstrip("\n")
        signed_headers_str = ";".join(sorted_headers)

        payload_hash = self.request.headers.get("x-amz-content-sha256", "")
        logger.info(f"Payload hash from header: '{payload_hash}'")

        if not payload_hash:
            logger.error("FAIL: Missing x-amz-content-sha256 header")
            raise AuthParsingError("Missing payload hash header")
        if payload_hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD":
            payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            logger.info(f"Converted streaming payload to empty body hash: {payload_hash}")

        logger.info("Final canonical request components:")
        logger.info(f"  Method: {self.method}")
        logger.info(f"  Path: {self.path}")
        logger.info(f"  Query: {self.query_string}")
        logger.info(f"  Signed headers: {signed_headers_str}")
        logger.info(f"  Payload hash: {payload_hash}")

        return f"{self.method}\n{self.path}\n{self.query_string}\n{canonical_headers}\n\n{signed_headers_str}\n{payload_hash}"

    def create_string_to_sign(self, canonical_request: str) -> str:
        credential_scope = f"{self.amz_date[:8]}/{self.region}/{self.service}/aws4_request"
        hashed_canonical_request = hashlib.sha256(canonical_request.encode()).hexdigest()
        return f"AWS4-HMAC-SHA256\n{self.amz_date}\n{credential_scope}\n{hashed_canonical_request}"

    def calculate_signature(self, string_to_sign: str) -> str:
        def sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        date_stamp = self.amz_date[:8]
        k_secret = ("AWS4" + self.seed_phrase).encode("utf-8")
        k_date = sign(k_secret, date_stamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, self.service)
        k_signing = sign(k_service, "aws4_request")

        return hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    async def verify_signature(self) -> bool:
        try:
            self.extract_auth_parts()
            logger.info("SUCCESS: Auth parts extraction completed")
        except AuthParsingError as e:
            logger.error(f"FAIL: Auth parsing failed: {e}")
            return False

        canonical_request = await self.create_canonical_request(self.signed_headers)
        logger.info(f"Canonical request:\n{canonical_request}")

        string_to_sign = self.create_string_to_sign(canonical_request)
        logger.info(f"String to sign:\n{string_to_sign}")

        calculated_signature = self.calculate_signature(string_to_sign)
        logger.info(f"Calculated signature: {calculated_signature}")
        logger.info(f"Provided signature:   {self.provided_signature}")

        signature_valid = calculated_signature == self.provided_signature
        logger.info(f"Signature verification: {'SUCCESS' if signature_valid else 'FAIL'}")

        return signature_valid


async def verify_hmac_middleware(
    request: Request,
    call_next: Callable,
) -> Response:
    exempt_paths = ["/docs", "/openapi.json"]

    if request.method == "OPTIONS":
        return await call_next(request)

    path = request.url.path
    if any(path.startswith(exempt_path) for exempt_path in exempt_paths):
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
