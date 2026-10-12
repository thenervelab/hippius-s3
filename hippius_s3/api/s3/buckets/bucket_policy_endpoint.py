from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.repositories.acl_repository import ACLRepository
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.services.public_prefix_policy import MAX_POLICY_BYTES
from hippius_s3.services.public_prefix_policy import PolicyDocumentError
from hippius_s3.services.public_prefix_policy import PolicyKind
from hippius_s3.services.public_prefix_policy import bucket_grants_anonymous_read
from hippius_s3.services.public_prefix_policy import parse_bucket_policy
from hippius_s3.services.public_prefix_policy import policy_document
from hippius_s3.services.public_prefix_policy import without_anonymous_read
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


async def get_bucket_policy(bucket_name: str, db: Any, main_account_id: str) -> Response:
    bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
    if not bucket:
        return errors.s3_error_response(
            "NoSuchBucket",
            f"The specified bucket {bucket_name} does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    acl = await ACLRepository(db).get_bucket_acl(bucket_name)
    bucket_id = bucket["bucket_id"]
    rows = await db.fetch(get_query("list_bucket_public_prefixes"), bucket_id)
    document = policy_document(
        bucket_name,
        public_acl=bucket_grants_anonymous_read(acl),
        prefixes=[str(row["prefix"]) for row in rows],
        public_objects=await _bucket_public_objects(db, bucket_id),
    )
    if document is None:
        return errors.s3_error_response(
            "NoSuchBucketPolicy",
            "The bucket policy does not exist",
            status_code=404,
            BucketName=bucket_name,
        )
    return Response(content=json.dumps(document, indent=2), media_type="application/json", status_code=200)


async def set_bucket_policy(bucket_name: str, request: Request, db: Any) -> Response:
    bucket = await BucketRepository(db).get_by_name(bucket_name)
    if not bucket:
        return errors.s3_error_response(
            "NoSuchBucket",
            f"The specified bucket {bucket_name} does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    parsed_or_error = _read_policy(await request.body(), bucket_name)
    if isinstance(parsed_or_error, Response):
        return parsed_or_error
    parsed = parsed_or_error

    bucket_id = bucket["bucket_id"]
    owner_id = str(bucket["main_account_id"])
    acl_repo = ACLRepository(db)
    conflict = False
    acl_changed = False
    async with db.transaction():
        locked = await db.fetchrow(get_query("lock_bucket_by_id"), bucket_id)
        if locked is None:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )
        # Re-read under the bucket lock. A prefix document must not be stored while
        # anonymous read of every key is already in force, whether that is the bucket
        # ACL or the whole-bucket object-read marker: the prefix would look like it
        # narrowed a grant that is still public.
        #
        # A whole-bucket GetObject document is that marker, not AllUsers READ. READ on
        # the bucket ACL is also ListBucket. Re-putting the document on a bucket that
        # already has AllUsers READ drops only that grant and stores the marker, so
        # listing stops and object GET stays. FULL_CONTROL still implies READ; storing
        # the marker beside it would show a narrower policy than the listing it allows,
        # so that put writes nothing.
        acl = await acl_repo.get_bucket_acl(bucket_name)
        anonymous_read = bucket_grants_anonymous_read(acl)
        if parsed.kind is PolicyKind.PREFIX and (anonymous_read or await _bucket_public_objects(db, bucket_id)):
            conflict = True
        elif parsed.kind is PolicyKind.PREFIX:
            await db.execute(get_query("delete_bucket_public_prefixes"), bucket_id)
            await db.execute(get_query("delete_bucket_public_objects"), bucket_id)
            await db.execute(get_query("insert_bucket_public_prefixes"), bucket_id, list(parsed.prefixes))
        else:
            stripped = without_anonymous_read(acl) if acl is not None else None
            if anonymous_read and (stripped is None or bucket_grants_anonymous_read(stripped)):
                conflict = True
            else:
                if stripped is not None and stripped != acl:
                    await acl_repo.set_bucket_acl(bucket_name, owner_id, stripped)
                    acl_changed = True
                await db.execute(get_query("delete_bucket_public_prefixes"), bucket_id)
                await db.execute(get_query("insert_bucket_public_objects"), bucket_id)

    if conflict:
        if parsed.kind is PolicyKind.PREFIX:
            message = "The bucket is already public, so a prefix policy would not narrow it"
        else:
            message = "The bucket policy already exists and bucket is public"
        return errors.s3_error_response(
            "PolicyAlreadyExists",
            message,
            status_code=409,
            BucketName=bucket_name,
        )

    await _invalidate_policy_caches(request, bucket_name, str(bucket_id), acl_changed=acl_changed)
    logger.info(
        "Set bucket policy for '%s' kind=%s prefixes=%d",
        bucket_name,
        parsed.kind.value,
        len(parsed.prefixes),
    )
    return Response(status_code=204)


async def delete_bucket_policy(bucket_name: str, db: Any, request: Request) -> Response:
    bucket = await BucketRepository(db).get_by_name(bucket_name)
    if not bucket:
        return errors.s3_error_response(
            "NoSuchBucket",
            f"The specified bucket {bucket_name} does not exist",
            status_code=404,
            BucketName=bucket_name,
        )
    # Prefixes and the whole-bucket object-read marker. The bucket ACL is a separate
    # grant, and clearing it here would make DeleteBucketPolicy a silent PutBucketAcl
    # private, including for a bucket made public with PutBucketAcl. While that grant
    # is what Get still returns, a 204 would tell the client the policy is gone. Refuse
    # instead, and write nothing, so a retry stays a refusal until PutBucketAcl private.
    # The same bucket-row lock as Put: without it a put that has already deleted the
    # old rows and not yet inserted the new ones commits after this delete, and the
    # 204 leaves the prefix published.
    bucket_id = bucket["bucket_id"]
    refused = False
    async with db.transaction():
        locked = await db.fetchrow(get_query("lock_bucket_by_id"), bucket_id)
        if locked is None:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )
        acl = await ACLRepository(db).get_bucket_acl(bucket_name)
        if bucket_grants_anonymous_read(acl):
            refused = True
        else:
            await db.execute(get_query("delete_bucket_public_prefixes"), bucket_id)
            await db.execute(get_query("delete_bucket_public_objects"), bucket_id)
    if refused:
        return errors.s3_error_response(
            "InvalidBucketState",
            "The bucket ACL still grants anonymous read. "
            "DeleteBucketPolicy would leave that grant in place. "
            "Make the bucket private with PutBucketAcl first.",
            status_code=409,
            BucketName=bucket_name,
        )
    await _invalidate_policy_caches(request, bucket_name, str(bucket_id), acl_changed=False)
    return Response(status_code=204)


def _read_policy(body: bytes, bucket_name: str) -> Any:
    if len(body) > MAX_POLICY_BYTES:
        return errors.s3_error_response("MalformedPolicy", "Policy document is too large", status_code=400)
    if not body:
        return errors.s3_error_response("MalformedPolicy", "Policy document is empty", status_code=400)
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return errors.s3_error_response("MalformedPolicy", "Policy document is not valid JSON", status_code=400)
    try:
        policy_json = json.loads(text)
    except json.JSONDecodeError:
        return errors.s3_error_response("MalformedPolicy", "Policy document is not valid JSON", status_code=400)
    try:
        return parse_bucket_policy(policy_json, bucket_name)
    except PolicyDocumentError as exc:
        return errors.s3_error_response(exc.code, str(exc), status_code=400)


async def _bucket_public_objects(db: Any, bucket_id: Any) -> bool:
    row = await db.fetchrow(get_query("bucket_has_public_objects"), bucket_id)
    return row is not None


async def _invalidate_policy_caches(request: Request, bucket_name: str, bucket_id: str, *, acl_changed: bool) -> None:
    acl_service = getattr(request.app.state, "acl_service", None)
    if acl_service is None:
        return
    # After commit. A reader that loaded the empty set before this write can still
    # SETEX the negative entry back; that window is an under-grant bounded by the
    # prefix-cache TTL. Allow lists are not cached, so a revoke is not served from Redis.
    await acl_service.invalidate_public_prefixes(bucket_id)
    if acl_changed:
        await acl_service.invalidate_cache(bucket_name)
