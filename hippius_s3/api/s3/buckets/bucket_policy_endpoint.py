from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.repositories.buckets import BucketRepository


logger = logging.getLogger(__name__)


async def get_bucket_policy(bucket_name: str, db: Any, main_account_id: str) -> Response:
    try:
        bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
        if not bucket:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )
        if not bucket["is_public"]:
            return errors.s3_error_response(
                "NoSuchBucketPolicy",
                "The bucket policy does not exist",
                status_code=404,
                BucketName=bucket_name,
            )
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
                }
            ],
        }
        return Response(content=json.dumps(policy, indent=2), media_type="application/json", status_code=200)
    except Exception as e:
        logger.exception(f"Error getting bucket policy: {e}")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


async def set_bucket_policy(bucket_name: str, request: Request, db: Any) -> Response:
    try:
        bucket = await BucketRepository(db).get_by_name(bucket_name)
        if not bucket:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )
        if bucket["is_public"]:
            return errors.s3_error_response(
                "PolicyAlreadyExists",
                "The bucket policy already exists and bucket is public",
                status_code=409,
                BucketName=bucket_name,
            )
        body = await request.body()
        if not body:
            return errors.s3_error_response("MalformedPolicy", "Policy document is empty", status_code=400)
        try:
            policy_json = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            return errors.s3_error_response("MalformedPolicy", "Policy document is not valid JSON", status_code=400)
        if not _validate_public_policy(policy_json, bucket_name):
            return errors.s3_error_response(
                "InvalidPolicyDocument",
                "Policy document is invalid or not a public read policy",
                status_code=400,
            )
        await db.fetchrow("UPDATE buckets SET is_public = TRUE WHERE bucket_id = $1", bucket["bucket_id"])
        return Response(status_code=204)
    except Exception as e:
        logger.exception(f"Error setting bucket policy: {e}")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


def _validate_public_policy(policy: dict, bucket_name: str) -> bool:
    try:
        if policy.get("Version") != "2012-10-17":
            return False
        statements = policy.get("Statement", [])
        if not statements or not isinstance(statements, list):
            return False
        expected_resource = f"arn:aws:s3:::{bucket_name}/*"
        for statement in statements:
            if (
                statement.get("Effect") == "Allow"
                and statement.get("Principal") == "*"
                and "s3:GetObject" in statement.get("Action", [])
                and expected_resource in statement.get("Resource", [])
            ):
                return True
        return False
    except Exception:
        logger.exception("Error validating policy")
        return False
