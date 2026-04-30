from __future__ import annotations

import base64
import binascii
import logging
from datetime import datetime
from urllib.parse import quote as urlquote

import asyncpg
from fastapi import Response
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3 import errors
from hippius_s3.dependencies import RequestContext
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

MAX_KEYS_LIMIT = 1000
DEFAULT_MAX_KEYS = 1000


def _format_s3_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _encode_continuation_token(last_key: str) -> str:
    return base64.urlsafe_b64encode(last_key.encode("utf-8")).decode("ascii")


def _decode_continuation_token(token: str) -> str | None:
    if not token:
        return None
    return base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")


def _maybe_url_encode(value: str, encoding_type: str | None) -> str:
    if encoding_type and encoding_type.lower() == "url":
        return urlquote(value, safe="")
    return value


async def handle_list_objects(
    bucket_name: str,
    ctx: RequestContext,
    pool: asyncpg.Pool,
    *,
    prefix: str | None,
    start_after: str | None,
    continuation_token: str | None,
    max_keys: str | None,
    encoding_type: str | None,
    delimiter: str | None,
) -> Response:
    if delimiter:
        logger.info("ListObjectsV2 delimiter=%r ignored (CommonPrefixes not implemented yet)", delimiter)

    if max_keys is None or max_keys == "":
        effective_max_keys = DEFAULT_MAX_KEYS
    else:
        try:
            effective_max_keys = int(max_keys)
        except ValueError:
            return errors.s3_error_response(
                code="InvalidArgument",
                message="max-keys must be an integer",
                status_code=400,
            )
        if effective_max_keys < 0:
            return errors.s3_error_response(
                code="InvalidArgument",
                message="max-keys must be non-negative",
                status_code=400,
            )
        effective_max_keys = min(effective_max_keys, MAX_KEYS_LIMIT)

    cursor: str | None
    if continuation_token:
        try:
            cursor = _decode_continuation_token(continuation_token)
        except (binascii.Error, UnicodeDecodeError, ValueError):
            return errors.s3_error_response(
                code="InvalidArgument",
                message="The continuation token provided is incorrect",
                status_code=400,
            )
    else:
        cursor = start_after or None

    bucket = await pool.fetchrow(
        get_query("get_bucket_by_name"),
        bucket_name,
    )
    if not bucket:
        return errors.s3_error_response(
            code="NoSuchBucket",
            message=f"The specified bucket {bucket_name} does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    bucket_id = bucket["bucket_id"]

    # Fetch one extra row so we can detect truncation without a separate count query.
    fetch_limit = effective_max_keys + 1 if effective_max_keys > 0 else 0
    rows = await pool.fetch(get_query("list_objects"), bucket_id, prefix, cursor, fetch_limit)

    is_truncated = len(rows) > effective_max_keys
    if is_truncated:
        rows = rows[:effective_max_keys]

    root = ET.Element("ListBucketResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    ET.SubElement(root, "Name").text = bucket_name
    ET.SubElement(root, "Prefix").text = _maybe_url_encode(prefix or "", encoding_type)
    if delimiter:
        ET.SubElement(root, "Delimiter").text = _maybe_url_encode(delimiter, encoding_type)
    if start_after is not None:
        ET.SubElement(root, "StartAfter").text = _maybe_url_encode(start_after, encoding_type)
    if continuation_token:
        ET.SubElement(root, "ContinuationToken").text = continuation_token
    ET.SubElement(root, "KeyCount").text = str(len(rows))
    ET.SubElement(root, "MaxKeys").text = str(effective_max_keys)
    if encoding_type:
        ET.SubElement(root, "EncodingType").text = encoding_type
    ET.SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"
    if is_truncated and rows:
        ET.SubElement(root, "NextContinuationToken").text = _encode_continuation_token(rows[-1]["object_key"])

    for obj in rows:
        content = ET.SubElement(root, "Contents")
        ET.SubElement(content, "Key").text = _maybe_url_encode(obj["object_key"], encoding_type)
        ET.SubElement(content, "LastModified").text = _format_s3_timestamp(obj["created_at"])
        # ETag is a quoted hex string per S3 spec; SDKs round-trip the quotes.
        md5 = obj.get("md5_hash") or ""
        ET.SubElement(content, "ETag").text = f'"{md5}"'
        ET.SubElement(content, "Size").text = str(obj["size_bytes"])
        ET.SubElement(content, "StorageClass").text = "STANDARD"
        owner = ET.SubElement(content, "Owner")
        ET.SubElement(owner, "ID").text = ctx.main_account_id
        ET.SubElement(owner, "DisplayName").text = ctx.main_account_id

    xml_content = ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)

    total_objects = len(rows)
    objects_with_cid = sum(1 for obj in rows if obj.get("arion_file_hash"))
    status_counts: dict[str, int] = {}
    for obj in rows:
        status = obj.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    headers = {
        "x-hippius-total-objects": str(total_objects),
        "x-hippius-objects-with-cid": str(objects_with_cid),
        "x-hippius-status-counts": ",".join(f"{k}:{v}" for k, v in status_counts.items()),
    }

    return Response(
        content=xml_content,
        media_type="application/xml",
        status_code=200,
        headers=headers,
    )
