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

        logger.debug(f"HMAC verification for {self.method} {self.path}")
        logger.debug(f"Has auth header: {bool(self.auth_header)}")
        logger.debug(f"Has amz-date: {bool(self.amz_date)}")
        logger.debug(f"Query string: {self.query_string}")
        if self.auth_header:
            logger.debug(f"Auth header starts with AWS4-HMAC-SHA256: {self.auth_header.startswith('AWS4-HMAC-SHA256')}")

    def extract_auth_parts(self) -> bool:
        logger.debug("Starting auth header parsing")

        if not self.auth_header or not self.auth_header.startswith("AWS4-HMAC-SHA256"):
            logger.debug("Missing or invalid auth header format")
            raise AuthParsingError("Credentials not found")

        credential_match = re.search(
            r"Credential=([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^,]+)",
            self.auth_header,
        )
        if not credential_match:
            logger.debug("Credential regex failed to match")
            raise AuthParsingError("Credentials do not match")

        encoded_seed_phrase = credential_match.group(1)
        self.region = credential_match.group(3)
        self.service = credential_match.group(4)

        logger.debug(f"Extracted region: {self.region}, service: {self.service}")
        logger.debug(f"Encoded seed phrase length: {len(encoded_seed_phrase)}")

        padding_needed = len(encoded_seed_phrase) % 4
        if padding_needed:
            encoded_seed_phrase += "=" * (4 - padding_needed)
            logger.debug(f"Added {4 - padding_needed} padding characters")

        try:
            seed_phrase_bytes = base64.b64decode(encoded_seed_phrase)
            self.seed_phrase = seed_phrase_bytes.decode("utf-8")
            logger.debug(f"Successfully decoded seed phrase (length: {len(self.seed_phrase)})")
        except Exception as e:
            logger.debug(f"Failed to decode seed phrase: {e}")
            raise AuthParsingError("Bad seed phrase format") from None

        if not self.seed_phrase or len(self.seed_phrase.strip()) == 0:
            logger.debug("Seed phrase is empty after decoding")
            raise AuthParsingError("Bad seed phrase format")

        signature_match = re.search(r"Signature=([a-f0-9]+)", self.auth_header)
        if not signature_match:
            logger.debug("Signature regex failed to match")
            raise AuthParsingError("Signature does not match")
        provided_signature = signature_match.group(1)
        logger.debug(f"Extracted signature length: {len(provided_signature)}")

        signed_headers_match = re.search(r"SignedHeaders=([^,]+)", self.auth_header)
        if not signed_headers_match:
            logger.debug("SignedHeaders regex failed to match")
            raise AuthParsingError("Auth headers do not match")

        self.signed_headers = signed_headers_match.group(1).split(";")
        self.provided_signature = provided_signature
        logger.debug(f"Signed headers: {self.signed_headers}")

        return provided_signature

    async def create_canonical_request(self, headers) -> str:
        logger.debug("Creating canonical request")
        canonical_headers = ""
        for header in headers:
            value = self.request.headers.get(header, "")
            canonical_headers += f"{header.lower()}:{value}\n"
            logger.debug(f"Header {header}: present={bool(value)}")

        signed_headers_str = ";".join(headers)

        # Use client-provided payload hash (AWS SigV4 standard)
        # This is cryptographically verified by the signature itself
        payload_hash = self.request.headers.get("x-amz-content-sha256", "")

        if not payload_hash:
            logger.debug("Missing x-amz-content-sha256 header - rejecting request")
            raise AuthParsingError("Missing payload hash header")
        if payload_hash in ["UNSIGNED-PAYLOAD", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"]:
            logger.debug("Unsigned or streaming payload - using empty hash")
            payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"  # SHA256 of empty string
        else:
            logger.debug(f"Using client-provided payload hash: {payload_hash}")

        logger.debug(f"Payload hash: {payload_hash}")
        logger.debug(f"Signed headers string: {signed_headers_str}")

        canonical_request = f"{self.method}\n{self.path}\n{self.query_string}\n{canonical_headers}\n{signed_headers_str}\n{payload_hash}"
        logger.debug(f"Canonical request created (length: {len(canonical_request)})")
        return canonical_request

    def create_string_to_sign(self, canonical_request: str) -> str:
        logger.debug("Creating string to sign")
        credential_scope = f"{self.amz_date[:8]}/{self.region}/{self.service}/aws4_request"
        hashed_canonical_request = hashlib.sha256(canonical_request.encode()).hexdigest()
        string_to_sign = f"AWS4-HMAC-SHA256\n{self.amz_date}\n{credential_scope}\n{hashed_canonical_request}"

        logger.debug(f"Credential scope: {credential_scope}")
        logger.debug(f"Hashed canonical request: {hashed_canonical_request}")
        logger.debug(f"String to sign created (length: {len(string_to_sign)})")

        return string_to_sign

    def calculate_signature(self, string_to_sign: str) -> str:
        logger.debug("Calculating signature")

        def sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        date_stamp = self.amz_date[:8]
        k_secret = ("AWS4" + self.seed_phrase).encode("utf-8")
        k_date = sign(k_secret, date_stamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, self.service)
        k_signing = sign(k_service, "aws4_request")

        calculated_signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        logger.debug(f"Date stamp: {date_stamp}")
        logger.debug(f"Calculated signature: {calculated_signature}")

        return calculated_signature

    async def verify_signature(self) -> bool:
        logger.debug("Starting signature verification")
        try:
            self.extract_auth_parts()
            logger.debug("Auth parts extraction successful")
        except AuthParsingError as e:
            logger.debug(f"Auth parsing failed: {e}")
            return False
        else:
            canonical_request = await self.create_canonical_request(self.signed_headers)
            string_to_sign = self.create_string_to_sign(canonical_request)
            calculated_signature = self.calculate_signature(string_to_sign)

            signature_match = calculated_signature == self.provided_signature
            logger.debug(f"Signature verification result: {signature_match}")
            logger.debug(f"Provided signature:  {self.provided_signature}")
            logger.debug(f"Calculated signature: {calculated_signature}")

            return signature_match


async def verify_hmac_middleware(
    request: Request,
    call_next: Callable,
) -> Response:
    exempt_paths = ["/docs", "/openapi.json"]

    logger.debug(f"HMAC middleware processing: {request.method} {request.url.path}")

    if request.method == "OPTIONS":
        logger.debug("OPTIONS request - bypassing HMAC verification")
        return await call_next(request)

    path = request.url.path
    if any(path.startswith(exempt_path) for exempt_path in exempt_paths):
        logger.debug(f"Exempt path detected - bypassing HMAC verification: {path}")
        return await call_next(request)

    logger.debug("Starting HMAC verification process")
    verifier = SigV4Verifier(request)
    is_valid = await verifier.verify_signature()

    if not is_valid:
        logger.warning(f"HMAC verification failed for {request.method} {request.url.path}")
        logger.warning("Returning 403 SignatureDoesNotMatch")
        return s3_error_response(
            code="SignatureDoesNotMatch",
            message="The request signature we calculated does not match the signature you provided",
            status_code=status.HTTP_403_FORBIDDEN,
        )

    logger.debug(f"HMAC verification successful for {request.method} {request.url.path}")
    request.state.seed_phrase = verifier.seed_phrase
    return await call_next(request)
