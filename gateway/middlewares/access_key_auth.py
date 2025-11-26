import hashlib
import hmac
import logging
import re

from fastapi import Request

from gateway.config import get_config
from gateway.middlewares.sigv4 import calculate_signature
from gateway.middlewares.sigv4 import create_canonical_request
from gateway.middlewares.sigv4 import extract_signature_from_auth_header
from gateway.middlewares.sigv4 import extract_signed_headers
from gateway.services.auth_service import decrypt_secret
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.services.hippius_api_service import HippiusAPIError


logger = logging.getLogger(__name__)
config = get_config()


class AccessKeyAuthError(Exception):
    pass


ALLOWED_TOKEN_TYPES = {"master", "sub"}
ACCESS_KEY_PATTERN = re.compile(r"^hip_[a-zA-Z0-9_-]{1,240}$")


async def verify_access_key_signature(
    request: Request,
    access_key: str,
) -> tuple[bool, str, str]:
    """
    Verify AWS SigV4 signature using access key authentication.

    Args:
        request: FastAPI request object
        access_key: Access key ID (starts with hip_)

    Returns:
        (is_valid, account_address, token_type)
    """
    if not access_key or not ACCESS_KEY_PATTERN.match(access_key):
        logger.warning(f"Invalid access key format: {access_key[:8] if access_key else 'empty'}***")
        raise AccessKeyAuthError("Invalid access key format")

    auth_header = request.headers.get("authorization", "")
    amz_date = request.headers.get("x-amz-date", "")

    if not auth_header or not amz_date:
        logger.error("Missing required auth headers for access key verification")
        raise AccessKeyAuthError("Missing required auth headers")

    provided_signature = extract_signature_from_auth_header(auth_header)
    signed_headers = extract_signed_headers(auth_header)

    try:
        async with HippiusApiClient() as api_client:
            token_response = await api_client.auth(access_key)
    except HippiusAPIError as e:
        logger.error(f"Hippius API error during auth: {e}")
        raise AccessKeyAuthError(f"API authentication failed: {e}") from e

    if not token_response.valid or token_response.status != "active":
        logger.warning(f"Invalid or inactive access key: {access_key[:8]}***, status={token_response.status}")
        return False, "", ""

    if not token_response.account_address:
        logger.error(f"API returned empty account_address for key: {access_key[:8]}***")
        raise AccessKeyAuthError("Invalid API response: missing account_address")

    if not re.match(r"^[1-9A-HJ-NP-Za-km-z]{47,48}$", token_response.account_address):
        logger.error(f"Invalid account address format: {token_response.account_address}")
        raise AccessKeyAuthError("Invalid account address format")

    if token_response.token_type not in ALLOWED_TOKEN_TYPES:
        logger.error(f"Invalid token type: {token_response.token_type}")
        raise AccessKeyAuthError(f"Invalid token type: {token_response.token_type}")

    decrypted_secret = decrypt_secret(
        token_response.encrypted_secret,
        token_response.nonce,
        config.hippius_secret_decryption_material,
    )

    credential_match = re.search(
        r"Credential=[^/]+/([^/]+)/([^/]+)/([^/]+)/",
        auth_header,
    )
    if not credential_match:
        raise AccessKeyAuthError("Invalid credential format")

    date_scope = credential_match.group(1)
    region = credential_match.group(2)
    service = credential_match.group(3)

    canonical_request = await create_canonical_request(
        request=request,
        signed_headers=signed_headers,
        method=request.method,
        path=request.url.path,
        query_string=request.url.query,
    )

    credential_scope = f"{date_scope}/{region}/{service}/aws4_request"
    hashed_canonical_request = hashlib.sha256(canonical_request.encode()).hexdigest()
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashed_canonical_request}"

    calculated_signature = calculate_signature(
        string_to_sign=string_to_sign,
        secret=decrypted_secret,
        date_stamp=date_scope,
        region=region,
        service=service,
    )

    is_valid = hmac.compare_digest(calculated_signature, provided_signature)

    if is_valid:
        logger.info(
            f"Access key auth successful: key={access_key[:8]}***, account={token_response.account_address}, type={token_response.token_type}"
        )
        if token_response.token_type == "master":
            logger.warning(
                f"AUDIT: Master token used: key={access_key[:8]}***, account={token_response.account_address}"
            )
        return True, token_response.account_address, token_response.token_type

    logger.warning(f"Signature mismatch for access key: {access_key[:8]}***")
    return False, "", ""
