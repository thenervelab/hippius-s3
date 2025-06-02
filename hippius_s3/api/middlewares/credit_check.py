"""Credit checking middleware for all S3 operations."""

import logging
import re
from typing import Callable

from fastapi import Request
from fastapi import Response
from starlette import status

from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.dependencies import check_account_has_credit


logger = logging.getLogger(__name__)


async def check_credit_for_all_operations(request: Request, call_next: Callable) -> Response:
    """
    Middleware to check if the account has enough credit for any S3 operation.

    This middleware intercepts all requests that modify state (PUT, POST, DELETE)
    and checks if the account associated with the seed phrase has enough credit.
    If not, it returns a 402 Payment Required response.

    Read operations (GET, HEAD) are allowed without credit checks.
    """
    # Only check credit for operations that modify state
    if request.method in ["PUT", "POST", "DELETE"]:
        path = request.url.path
        logger.info(f"Checking credit for {request.method} operation: {path}")

        # Check if we have a seed phrase in request.state (set by HMAC middleware)
        if hasattr(request.state, "seed_phrase"):
            seed_phrase = request.state.seed_phrase

            try:
                has_credit = await check_account_has_credit(seed_phrase)

                if not has_credit:
                    logger.warning(f"Account does not have credit for {request.method} operation: {path}")

                    # Extract bucket name for better error response
                    bucket_name = None
                    bucket_match = re.match(r"^/([^/]+)", path)
                    if bucket_match:
                        bucket_name = bucket_match.group(1)

                    return s3_error_response(
                        code="InsufficientAccountCredit",
                        message="The account does not have sufficient credit to perform this operation",
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        BucketName=bucket_name if bucket_name else "",
                    )

            except Exception as e:
                logger.exception(f"Error in credit verification for {request.method} {path}: {e}")

                # Extract bucket name for better error response
                bucket_name = None
                bucket_match = re.match(r"^/([^/]+)", path)
                if bucket_match:
                    bucket_name = bucket_match.group(1)

                return s3_error_response(
                    code="CreditVerificationError",
                    message="Unable to verify account credit. Please try again later.",
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    BucketName=bucket_name if bucket_name else "",
                )

    # Continue with the request
    response: Response = await call_next(request)
    return response
