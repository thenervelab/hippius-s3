from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response
from starlette import status

from gateway.middlewares.access_key_auth import AccessKeyAuthError
from gateway.middlewares.access_key_auth import verify_access_key_signature
from gateway.middlewares.sigv4 import AuthParsingError
from gateway.middlewares.sigv4 import SigV4Verifier
from gateway.middlewares.sigv4 import extract_credential_from_auth_header
from gateway.utils.errors import s3_error_response
from hippius_s3.services.hippius_api_service import HippiusAPIError
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


async def auth_router_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    Route authentication to appropriate handler based on credential format.

    Detects auth type:
    - Access key: credential starts with "hip_"
    - Seed phrase: credential is base64-encoded seed phrase

    Sets request.state fields:
    - For seed phrase: seed_phrase, auth_method="seed_phrase"
    - For access key: access_key, account_address, token_type, auth_method="access_key"
    - For anonymous: auth_method="anonymous"
    """
    ray_id = getattr(request.state, "ray_id", "no-ray-id")
    logger = get_logger_with_ray_id(__name__, ray_id)

    exempt_paths = ["/docs", "/openapi.json", "/user/", "/robots.txt", "/metrics", "/health"]

    if request.method == "OPTIONS":
        return await call_next(request)

    path = request.url.path
    if any(path.startswith(exempt_path) or path == exempt_path for exempt_path in exempt_paths):
        return await call_next(request)

    if request.method in ["GET", "HEAD"]:
        auth_header = request.headers.get("authorization")
        if not auth_header:
            request.state.auth_method = "anonymous"
            return await call_next(request)

    auth_header = request.headers.get("authorization", "")
    if not auth_header:
        return s3_error_response(
            code="InvalidAccessKeyId",
            message="The AWS Access Key Id you provided does not exist in our records.",
            status_code=status.HTTP_403_FORBIDDEN,
        )

    try:
        credential, date_scope, region, service = extract_credential_from_auth_header(auth_header)
    except AuthParsingError as e:
        logger.error(f"Failed to extract credential: {e}")
        return s3_error_response(
            code="InvalidAccessKeyId",
            message="Invalid credential format",
            status_code=status.HTTP_403_FORBIDDEN,
        )

    if credential.startswith("hip_"):
        logger.debug(f"Detected access key authentication: {credential[:8]}***")

        try:
            is_valid, account_address, token_type = await verify_access_key_signature(
                request=request,
                access_key=credential,
            )
        except AccessKeyAuthError as e:
            logger.warning(f"Access key auth error: {e}")
            return s3_error_response(
                code="SignatureDoesNotMatch",
                message="The request signature we calculated does not match the signature you provided",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        except HippiusAPIError as e:
            logger.error(f"Hippius API error during auth: {e}")
            return s3_error_response(
                code="ServiceUnavailable",
                message="Authentication service temporarily unavailable",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except Exception as e:
            logger.exception(f"Unexpected auth error: {e}")
            return s3_error_response(
                code="InternalError",
                message="An internal error occurred",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        if not is_valid:
            return s3_error_response(
                code="SignatureDoesNotMatch",
                message="The request signature we calculated does not match the signature you provided",
                status_code=status.HTTP_403_FORBIDDEN,
            )

        request.state.access_key = credential
        request.state.account_address = account_address
        request.state.token_type = token_type
        request.state.auth_method = "access_key"
    else:
        logger.debug("Attempting seed phrase authentication")

        verifier = SigV4Verifier(request)
        is_valid = await verifier.verify_signature()

        if not is_valid:
            return s3_error_response(
                code="SignatureDoesNotMatch",
                message="The request signature we calculated does not match the signature you provided",
                status_code=status.HTTP_403_FORBIDDEN,
            )

        request.state.seed_phrase = verifier.seed_phrase
        request.state.auth_method = "seed_phrase"

        logger.info("Seed phrase auth successful")

    return await call_next(request)
