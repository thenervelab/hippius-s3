from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.config import get_config
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
config = get_config()


async def handle_delete_object(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
    redis_client: Any,
) -> Response:
    # Abort multipart upload path is handled in the router before delegating to us
    try:
        user = await UserRepository(db).ensure_by_main_account(request.state.account.main_account)
        bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, user["main_account_id"])
        if not bucket:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]
        result = await ObjectRepository(db).get_by_path(bucket_id, object_key)
        if not result:
            return Response(status_code=204)

        # Permission-aware delete
        deleted_object = await db.fetchrow(get_query("delete_object"), bucket_id, object_key, user["main_account_id"])
        if not deleted_object:
            return errors.s3_error_response(
                "AccessDenied",
                f"You do not have permission to delete object {object_key}",
                status_code=403,
                Key=object_key,
            )

        # Cleanup provisional multipart uploads
        try:
            await db.execute(
                "DELETE FROM multipart_uploads WHERE bucket_id = $1 AND object_key = $2 AND is_completed = FALSE",
                bucket_id,
                object_key,
            )
        except Exception:
            logger.debug("Failed to cleanup provisional multipart uploads on object delete", exc_info=True)

        # Enqueue unpin if CID exists
        cid = deleted_object.get("ipfs_cid") or ""
        if cid and cid.strip():
            await enqueue_unpin_request(
                payload=UnpinChainRequest(
                    substrate_url=config.substrate_url,
                    ipfs_node=config.ipfs_store_url,
                    address=request.state.account.main_account,
                    subaccount=request.state.account.main_account,
                    subaccount_seed_phrase=request.state.seed_phrase,
                    bucket_name=bucket_name,
                    object_key=object_key,
                    should_encrypt=not bucket["is_public"],
                    cid=cid,
                    object_id=str(deleted_object["object_id"]),
                ),
                redis_client=redis_client,
            )
        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting object")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )
