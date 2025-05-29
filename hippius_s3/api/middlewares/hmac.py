import base64
import logging
import re
from typing import Callable

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials as BotocoreCredentials
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

        # Debug ALL headers to see what MinIO is actually sending
        logger.debug("=== ALL REQUEST HEADERS ===")
        for name, value in self.request.headers.items():
            logger.debug(f"  {name}: {value}")
        logger.debug("=== END HEADERS ===")

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

    async def verify_signature(self) -> bool:
        logger.debug("Starting signature verification using botocore")
        try:
            self.extract_auth_parts()
            logger.debug("Auth parts extraction successful")
        except AuthParsingError as e:
            logger.debug(f"Auth parsing failed: {e}")
            return False

        # Create botocore credentials using the seed phrase as secret key
        credentials = BotocoreCredentials(
            access_key="dummy",  # Access key not used in our case
            secret_key=self.seed_phrase,
        )

        # Create an AWS request object from the FastAPI request
        url = f"https://{self.request.headers.get('host', 'localhost')}{self.path}"
        if self.query_string:
            url += f"?{self.query_string}"

        # Prepare headers for botocore - only include the exact headers that MinIO signed
        headers_for_signing = {}
        for header_name in self.signed_headers:
            if header_name.lower() in self.request.headers:
                headers_for_signing[header_name] = self.request.headers[header_name.lower()]

        # Handle request body for signature calculation
        # If client provided content hash, we need to set body to None to trust the header
        content_sha256 = self.request.headers.get("x-amz-content-sha256", "")
        body = None
        if content_sha256 == "UNSIGNED-PAYLOAD":
            body = b""  # Empty body for unsigned payload
        # For other cases, set body to None so botocore uses the x-amz-content-sha256 header

        aws_request = AWSRequest(
            method=self.method,
            url=url,
            headers=headers_for_signing,
            data=body,
        )

        # Create SigV4Auth instance and calculate signature
        signer = SigV4Auth(credentials, self.service, self.region)

        # Preserve the original timestamp - botocore will try to override x-amz-date
        original_amz_date = aws_request.headers.get("x-amz-date")

        signer.add_auth(aws_request)

        # Restore the original timestamp if it was changed
        if original_amz_date and aws_request.headers.get("x-amz-date") != original_amz_date:
            logger.debug(f"Restoring original timestamp: {original_amz_date}")
            aws_request.headers["x-amz-date"] = original_amz_date
            # Need to recalculate with the correct timestamp
            signer.add_auth(aws_request)

        # Extract the calculated signature from the authorization header
        calculated_auth = aws_request.headers.get("Authorization", "")
        calculated_signature_match = re.search(r"Signature=([a-f0-9]+)", calculated_auth)

        if not calculated_signature_match:
            logger.debug("Failed to extract calculated signature from botocore")
            return False

        calculated_signature = calculated_signature_match.group(1)

        signature_match = calculated_signature == self.provided_signature
        logger.debug(f"Signature verification result: {signature_match}")
        logger.debug(f"Provided signature:  {self.provided_signature}")
        logger.debug(f"Calculated signature: {calculated_signature}")
        logger.debug(f"Calculated auth header: {calculated_auth}")

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
