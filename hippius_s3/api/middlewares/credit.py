"""Credit checking middleware for the application."""

import logging
import re
from typing import Callable

from fastapi import Request
from fastapi import Response
from starlette import status

from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.dependencies import check_account_has_credit


logger = logging.getLogger(__name__)


async def check_credit_for_bucket_creation(request: Request, call_next: Callable) -> Response:
    """
    Middleware to check if the account has enough credit for bucket creation.

    This middleware intercepts PUT requests to the bucket endpoint (e.g. PUT /{bucket_name})
    and checks if the account associated with the seed phrase has enough credit.
    If not, it returns a 402 Payment Required response.
    """
    # Only check credit for PUT requests to the bucket endpoint
    path = request.url.path

    # Check if this is a bucket creation request (PUT /{bucket_name})
    # This matches PUT /{bucket_name} but not PUT /{bucket_name}/{object_key}
    is_bucket_creation = (
        request.method == "PUT"
        and re.match(r"^/[^/]+$", path)
        and "tagging" not in request.query_params
        and "lifecycle" not in request.query_params
    )

    if is_bucket_creation:
        logger.info(f"Checking credit for bucket creation request: {path}")

        # Check if we have a seed phrase in request.state
        if hasattr(request.state, "seed_phrase"):
            seed_phrase = request.state.seed_phrase
            bucket_name = path.lstrip("/")

            try:
                has_credit = await check_account_has_credit(seed_phrase)

                if not has_credit:
                    logger.warning(f"Account does not have credit for bucket creation: {bucket_name}")

                    return s3_error_response(
                        code="InsufficientAccountCredit",
                        message="The account does not have sufficient credit to create a bucket",
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        BucketName=bucket_name,
                    )

            except Exception:
                return s3_error_response(
                    code="CreditVerificationError",
                    message="Unable to verify account credit. Please try again later.",
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    BucketName=bucket_name,
                )

    response: Response = await call_next(request)
    return response
