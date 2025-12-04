import logging
from dataclasses import dataclass
from typing import Any
from typing import Literal

from fastapi import Request
from fastapi import Response
from starlette import status

from gateway.middlewares.access_key_auth import AccessKeyAuthError
from gateway.middlewares.access_key_auth import verify_access_key_presigned_url
from gateway.middlewares.access_key_auth import verify_access_key_signature
from gateway.middlewares.sigv4 import AuthParsingError
from gateway.middlewares.sigv4 import SigV4Verifier
from gateway.middlewares.sigv4 import extract_credential_from_auth_header
from gateway.utils.errors import s3_error_response
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.services.hippius_api_service import HippiusAPIError
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


logger = logging.getLogger(__name__)

AuthMethod = Literal["access_key", "seed_phrase", "anonymous", "bearer_access_key"]


@dataclass
class AuthResult:
    is_valid: bool = False
    auth_method: AuthMethod = "anonymous"
    access_key: str | None = None
    account_address: str | None = None
    token_type: str | None = None
    seed_phrase: str | None = None
    account_id: str | None = None
    error_response: Response | None = None


async def authenticate_request(request: Request) -> AuthResult:
    """
    Determine authentication method and verify credentials.

    Flow:
    1. Check for Presigned URL (query params) -> Access Key Auth
    2. Check for Authorization Header
       - If starts with "Bearer " -> Bearer Access Key Auth
       - If starts with "hip_" -> Access Key Auth
       - Else -> Seed Phrase Auth (SigV4)
    3. No credentials -> Anonymous (if GET/HEAD)
    """
    ray_id = getattr(request.state, "ray_id", "no-ray-id")
    logger = get_logger_with_ray_id(__name__, ray_id)

    # 1. Presigned URL Detection
    query_params = request.query_params
    if (
        query_params.get("X-Amz-Algorithm") == "AWS4-HMAC-SHA256"
        and query_params.get("X-Amz-Credential")
        and query_params.get("X-Amz-Signature")
    ):
        return await _authenticate_presigned_url(request, logger)

    # 2. Authorization Header Detection
    auth_header = request.headers.get("authorization", "")
    if not auth_header:

        if request.method in ["GET", "HEAD"] and request.url.path != "/":
            return AuthResult(is_valid=True, auth_method="anonymous")

        return AuthResult(
            error_response=s3_error_response(
                code="InvalidAccessKeyId",
                message='Please provide a valid "hip_*" access key. See https://docs.hippius.com/storage/s3/integration#authentication for more information.',  # noqa: Q003
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )

    # 3. Bearer Token Detection
    if auth_header.startswith("Bearer "):
        return await _authenticate_bearer(request, auth_header, logger)

    try:
        credential, _, _, _ = extract_credential_from_auth_header(auth_header)
    except AuthParsingError as e:
        logger.error(f"Failed to extract credential: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="InvalidAccessKeyId",
                message="Invalid credential format",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )

    # 4. Access Key vs Seed Phrase Branching
    if credential.startswith("hip_"):
        return await _authenticate_access_key_header(request, credential, logger)

    return await _authenticate_seed_phrase(request, logger)


async def _authenticate_presigned_url(request: Request, logger: Any) -> AuthResult:
    credential_param = request.query_params["X-Amz-Credential"]
    credential_id = credential_param.split("/", 1)[0]

    if not credential_id.startswith("hip_"):
        logger.warning(f"Presigned URL credential is not a hip_ access key: {credential_id[:8]}***")
        return AuthResult(
            error_response=s3_error_response(
                code="InvalidAccessKeyId",
                message="The AWS Access Key Id you provided does not exist in our records.",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )

    logger.debug(f"Detected presigned URL access key authentication: {credential_id[:8]}***")

    try:
        is_valid, account_address, token_type = await verify_access_key_presigned_url(
            request=request,
            access_key=credential_id,
        )
    except AccessKeyAuthError as e:
        logger.warning(f"Presigned URL access key auth error: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="SignatureDoesNotMatch",
                message="The request signature we calculated does not match the signature you provided",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )
    except HippiusAPIError as e:
        logger.error(f"Hippius API error during presigned URL auth: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="ServiceUnavailable",
                message="Authentication service temporarily unavailable",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        )
    except Exception as e:
        logger.exception(f"Unexpected presigned URL auth error: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="InternalError",
                message="An internal error occurred",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        )

    if not is_valid:
        return AuthResult(
            error_response=s3_error_response(
                code="SignatureDoesNotMatch",
                message="The request signature we calculated does not match the signature you provided",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )

    return AuthResult(
        is_valid=True,
        auth_method="access_key",
        access_key=credential_id,
        account_address=account_address,
        token_type=token_type,
    )


async def _authenticate_bearer(request: Request, auth_header: str, logger: Any) -> AuthResult:
    token = auth_header.replace("Bearer ", "").strip()

    if not token.startswith("hip_"):
        return AuthResult(
            error_response=s3_error_response(
                code="InvalidAccessKeyId",
                message="Bearer token must be a valid access key starting with 'hip_'",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )

    try:
        async with HippiusApiClient() as api_client:
            token_response = await api_client.auth(token)

        if not token_response.valid or token_response.status != "active":
            logger.warning(f"Invalid or inactive Bearer access key: {token[:8]}***, status={token_response.status}")
            return AuthResult(
                error_response=s3_error_response(
                    code="InvalidAccessKeyId",
                    message="Invalid or inactive access key",
                    status_code=status.HTTP_403_FORBIDDEN,
                )
            )

        if not token_response.account_address:
            logger.error(f"API returned empty account_address for Bearer key: {token[:8]}***")
            return AuthResult(
                error_response=s3_error_response(
                    code="InvalidAccessKeyId",
                    message="Invalid API response",
                    status_code=status.HTTP_403_FORBIDDEN,
                )
            )

        logger.info(
            f"Bearer access key auth successful: {token[:8]}***, account={token_response.account_address}, type={token_response.token_type}"
        )
        return AuthResult(
            is_valid=True,
            auth_method="bearer_access_key",
            access_key=token,
            account_address=token_response.account_address,
            account_id=token_response.account_address,
            token_type=token_response.token_type,
        )

    except HippiusAPIError as e:
        logger.error(f"Hippius API error during Bearer auth: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="ServiceUnavailable",
                message="Authentication service temporarily unavailable",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        )
    except Exception as e:
        logger.exception(f"Unexpected Bearer auth error: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="InternalError",
                message="An internal error occurred",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        )


async def _authenticate_access_key_header(request: Request, credential: str, logger: Any) -> AuthResult:
    logger.debug(f"Detected access key authentication: {credential[:8]}***")

    try:
        is_valid, account_address, token_type = await verify_access_key_signature(
            request=request,
            access_key=credential,
        )
    except AccessKeyAuthError as e:
        logger.warning(f"Access key auth error: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="SignatureDoesNotMatch",
                message="The request signature we calculated does not match the signature you provided",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )
    except HippiusAPIError as e:
        logger.error(f"Hippius API error during auth: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="ServiceUnavailable",
                message="Authentication service temporarily unavailable",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        )
    except Exception as e:
        logger.exception(f"Unexpected auth error: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="InternalError",
                message="An internal error occurred",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        )

    if not is_valid:
        return AuthResult(
            error_response=s3_error_response(
                code="SignatureDoesNotMatch",
                message="The request signature we calculated does not match the signature you provided",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )

    return AuthResult(
        is_valid=True,
        auth_method="access_key",
        access_key=credential,
        account_address=account_address,
        token_type=token_type,
    )


async def _authenticate_seed_phrase(request: Request, logger: Any) -> AuthResult:
    logger.debug("Attempting seed phrase authentication")

    verifier = SigV4Verifier(request)
    try:
        is_valid = await verifier.verify_signature()
    except Exception as e:
        # Catch parsing errors that might slip through or other surprises
        logger.warning(f"Seed phrase auth verification failed: {e}")
        return AuthResult(
            error_response=s3_error_response(
                code="SignatureDoesNotMatch",
                message="The request signature we calculated does not match the signature you provided",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )

    if not is_valid:
        return AuthResult(
            error_response=s3_error_response(
                code="SignatureDoesNotMatch",
                message="The request signature we calculated does not match the signature you provided",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        )

    logger.info("Seed phrase auth successful")
    return AuthResult(
        is_valid=True,
        auth_method="seed_phrase",
        seed_phrase=verifier.seed_phrase,
    )
