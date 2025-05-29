import base64
import hashlib
import hmac
import re
from typing import Callable

from fastapi import Request
from fastapi import Response
from starlette import status

from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.config import get_config


config = get_config()


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
        if not self.auth_header or not self.auth_header.startswith("AWS4-HMAC-SHA256"):
            raise AuthParsingError("Credentials not found")

        credential_match = re.search(
            r"Credential=([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^,]+)",
            self.auth_header,
        )
        if not credential_match:
            raise AuthParsingError("Credentials do not match")

        encoded_seed_phrase = credential_match.group(1)
        self.region = credential_match.group(3)
        self.service = credential_match.group(4)

        padding_needed = len(encoded_seed_phrase) % 4
        if padding_needed:
            encoded_seed_phrase += "=" * (4 - padding_needed)

        seed_phrase_bytes = base64.b64decode(encoded_seed_phrase)
        self.seed_phrase = seed_phrase_bytes.decode("utf-8")

        if not self.seed_phrase or len(self.seed_phrase.strip()) == 0:
            raise AuthParsingError("Bad seed phrase format")

        signature_match = re.search(r"Signature=([a-f0-9]+)", self.auth_header)
        if not signature_match:
            raise AuthParsingError("Signature does not match")
        provided_signature = signature_match.group(1)

        signed_headers_match = re.search(r"SignedHeaders=([^,]+)", self.auth_header)
        if not signed_headers_match:
            raise AuthParsingError("Auth headers do not match")

        self.signed_headers = signed_headers_match.group(1).split(";")
        self.provided_signature = provided_signature

        return provided_signature

    async def create_canonical_request(self, headers) -> str:
        canonical_headers = ""
        for header in headers:
            value = self.request.headers.get(header, "")
            canonical_headers += f"{header.lower()}:{value}\n"

        signed_headers_str = ";".join(headers)
        body = await self.request.body()
        payload_hash = hashlib.sha256(body).hexdigest()

        return f"{self.method}\n{self.path}\n{self.query_string}\n{canonical_headers}\n{signed_headers_str}\n{payload_hash}"

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
        except AuthParsingError:
            return False
        else:
            canonical_request = await self.create_canonical_request(self.signed_headers)
            string_to_sign = self.create_string_to_sign(canonical_request)
            calculated_signature = self.calculate_signature(string_to_sign)

            return calculated_signature == self.provided_signature


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
