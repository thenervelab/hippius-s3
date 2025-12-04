import datetime
import hashlib
import hmac
import logging
import re

from fastapi import Request

from gateway.config import get_config
from gateway.middlewares.sigv4 import calculate_signature
from gateway.middlewares.sigv4 import canonicalize_presigned_query_string
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

    # provided_signature is guaranteed non-empty by earlier validation
    is_valid = hmac.compare_digest(calculated_signature, provided_signature or "")

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


async def verify_access_key_presigned_url(
    request: Request,
    access_key: str,
) -> tuple[bool, str, str]:
    """
    Verify AWS SigV4 query-string (presigned URL) signature using access key authentication.

    Args:
        request: FastAPI request object
        access_key: Access key ID (starts with hip_)

    Returns:
        (is_valid, account_address, token_type)
    """
    if not access_key or not ACCESS_KEY_PATTERN.match(access_key):
        logger.warning(f"Invalid access key format in presigned URL: {access_key[:8] if access_key else 'empty'}***")
        raise AccessKeyAuthError("Invalid access key format")

    query = request.query_params

    algorithm = query.get("X-Amz-Algorithm")
    credential = query.get("X-Amz-Credential")
    amz_date = query.get("X-Amz-Date")
    expires_str = query.get("X-Amz-Expires")
    signed_headers_str = query.get("X-Amz-SignedHeaders")
    provided_signature = query.get("X-Amz-Signature")

    if not all([algorithm, credential, amz_date, expires_str, signed_headers_str, provided_signature]):
        logger.error("Missing required X-Amz-* query parameters for presigned URL verification")
        raise AccessKeyAuthError("Missing required presigned URL parameters")

    if algorithm != "AWS4-HMAC-SHA256":
        logger.error(f"Unsupported X-Amz-Algorithm in presigned URL: {algorithm}")
        raise AccessKeyAuthError("Unsupported signature algorithm")

    # Parse credential: <access_key>/<date>/<region>/<service>/aws4_request
    # At this point credential is guaranteed non-empty by the earlier all([...]) check
    credential_parts = credential.split("/")  # type: ignore[union-attr]
    if len(credential_parts) < 5:
        logger.error(f"Invalid X-Amz-Credential format: {credential}")
        raise AccessKeyAuthError("Invalid credential format")

    credential_id = credential_parts[0]
    date_scope = credential_parts[1]
    region = credential_parts[2]
    service = credential_parts[3]

    if credential_id != access_key:
        logger.error(f"Presigned URL credential ID mismatch: header={access_key[:8]}***, query={credential_id[:8]}***")
        raise AccessKeyAuthError("Credential does not match access key")

    # Ensure credential scope date matches X-Amz-Date date prefix (YYYYMMDD)
    if not amz_date or len(amz_date) < 8 or date_scope != amz_date[:8]:
        logger.error(f"X-Amz-Date ({amz_date}) and credential scope date ({date_scope}) mismatch in presigned URL")
        raise AccessKeyAuthError("Invalid credential scope")

    try:
        expires = int(expires_str)  # type: ignore[arg-type]
    except ValueError:
        logger.error(f"Invalid X-Amz-Expires value: {expires_str}")
        raise AccessKeyAuthError("Invalid expires value") from None

    if expires <= 0 or expires > 604800:
        logger.error(f"X-Amz-Expires out of allowed range (1-604800): {expires}")
        raise AccessKeyAuthError("Expires value out of range")

    try:
        signed_at = datetime.datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=datetime.timezone.utc)
    except ValueError:
        logger.error(f"Invalid X-Amz-Date format: {amz_date}")
        raise AccessKeyAuthError("Invalid date format") from None

    now = datetime.datetime.now(datetime.timezone.utc)
    expiry_time = signed_at + datetime.timedelta(seconds=expires)

    if now > expiry_time:
        logger.info(
            f"Presigned URL expired: signed_at={signed_at.isoformat()}, expires_in={expires}s, now={now.isoformat()}"
        )
        return False, "", ""

    # signed_headers_str is validated above in the all([...]) check
    signed_headers = signed_headers_str.split(";")  # type: ignore[union-attr]

    # Require host to be part of the signed headers, as per AWS SigV4 requirements
    if "host" not in signed_headers:
        logger.error("Presigned URL missing required 'host' header in X-Amz-SignedHeaders")
        raise AccessKeyAuthError("Invalid signed headers")

    try:
        async with HippiusApiClient() as api_client:
            token_response = await api_client.auth(access_key)
    except HippiusAPIError as e:
        logger.error(f"Hippius API error during presigned URL auth: {e}")
        raise AccessKeyAuthError(f"API authentication failed: {e}") from e

    if not token_response.valid or token_response.status != "active":
        logger.warning(
            f"Invalid or inactive access key for presigned URL: {access_key[:8]}***, status={token_response.status}"
        )
        return False, "", ""

    if not token_response.account_address:
        logger.error(f"API returned empty account_address for presigned key: {access_key[:8]}***")
        raise AccessKeyAuthError("Invalid API response: missing account_address")

    if not re.match(r"^[1-9A-HJ-NP-Za-km-z]{47,48}$", token_response.account_address):
        logger.error(f"Invalid account address format: {token_response.account_address}")
        raise AccessKeyAuthError("Invalid account address format")

    if token_response.token_type not in ALLOWED_TOKEN_TYPES:
        logger.error(f"Invalid token type for presigned URL: {token_response.token_type}")
        raise AccessKeyAuthError(f"Invalid token type: {token_response.token_type}")

    decrypted_secret = decrypt_secret(
        token_response.encrypted_secret,
        token_response.nonce,
        config.hippius_secret_decryption_material,
    )

    # Build canonical request with presigned query canonicalization
    canonical_query = canonicalize_presigned_query_string(request.url.query)

    canonical_request = await create_canonical_request(
        request=request,
        signed_headers=signed_headers,
        method=request.method,
        path=request.url.path,
        query_string=canonical_query,
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

    # provided_signature is validated above as non-empty
    is_valid = hmac.compare_digest(calculated_signature, str(provided_signature))

    if is_valid:
        logger.info(
            f"Presigned URL access key auth successful: key={access_key[:8]}***, "
            f"account={token_response.account_address}, type={token_response.token_type}"
        )
        if token_response.token_type == "master":
            logger.warning(
                f"AUDIT: Master token used via presigned URL: key={access_key[:8]}***, "
                f"account={token_response.account_address}"
            )
        return True, token_response.account_address, token_response.token_type

    logger.warning(f"Presigned URL signature mismatch for access key: {access_key[:8]}***")
    return False, "", ""
