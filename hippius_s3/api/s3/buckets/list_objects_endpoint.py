from __future__ import annotations

import base64
import binascii
import logging
from urllib.parse import quote as urlquote

import asyncpg
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.common import format_s3_timestamp
from hippius_s3.dependencies import RequestContext
from hippius_s3.utils import get_query
from hippius_s3.xml_helpers import add_subelement
from hippius_s3.xml_helpers import create_element
from hippius_s3.xml_helpers import to_xml_bytes


logger = logging.getLogger(__name__)

MAX_KEYS_LIMIT = 1000
DEFAULT_MAX_KEYS = 1000
# Cap continuation token length to avoid an attacker forcing a giant DB cursor bind.
MAX_CONTINUATION_TOKEN_LEN = 4096


def _invalid_arg(message: str) -> Response:
    return errors.s3_error_response(code="InvalidArgument", message=message, status_code=400)


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

    # FastAPI hands "?prefix=" / "?start-after=" through as "" — treat as absent.
    prefix = prefix or None
    start_after = start_after or None

    if max_keys is None or max_keys == "":
        effective_max_keys = DEFAULT_MAX_KEYS
    else:
        try:
            effective_max_keys = int(max_keys)
        except ValueError:
            return _invalid_arg("max-keys must be an integer")
        if effective_max_keys < 0:
            return _invalid_arg("max-keys must be non-negative")
        effective_max_keys = min(effective_max_keys, MAX_KEYS_LIMIT)

    cursor: str | None = start_after
    if continuation_token:
        if len(continuation_token) > MAX_CONTINUATION_TOKEN_LEN:
            return _invalid_arg("The continuation token provided is incorrect")
        try:
            cursor = _decode_continuation_token(continuation_token)
        except (binascii.Error, UnicodeDecodeError, ValueError):
            return _invalid_arg("The continuation token provided is incorrect")

    bucket = await pool.fetchrow(get_query("get_bucket_by_name"), bucket_name)
    if not bucket:
        return errors.s3_error_response(
            code="NoSuchBucket",
            message=f"The specified bucket {bucket_name} does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    bucket_id = bucket["bucket_id"]
    bucket_owner = bucket["main_account_id"] or ctx.main_account_id

    # Fetch one extra row so we can detect truncation without a separate count query.
    fetch_limit = effective_max_keys + 1 if effective_max_keys > 0 else 0
    rows = await pool.fetch(get_query("list_objects"), bucket_id, prefix, cursor, fetch_limit)

    is_truncated = len(rows) > effective_max_keys
    if is_truncated:
        rows = rows[:effective_max_keys]

    # Element order follows the AWS ListObjectsV2 response schema so strict XML
    # validators (some Java SDKs, S3-spec test suites) accept it.
    root = create_element("ListBucketResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    add_subelement(root, "Name", bucket_name)
    add_subelement(root, "Prefix", _maybe_url_encode(prefix or "", encoding_type))
    if continuation_token:
        add_subelement(root, "ContinuationToken", continuation_token)
    if start_after:
        add_subelement(root, "StartAfter", _maybe_url_encode(start_after, encoding_type))
    add_subelement(root, "MaxKeys", str(effective_max_keys))
    if delimiter:
        add_subelement(root, "Delimiter", _maybe_url_encode(delimiter, encoding_type))
    add_subelement(root, "IsTruncated", "true" if is_truncated else "false")
    if encoding_type:
        add_subelement(root, "EncodingType", encoding_type)
    add_subelement(root, "KeyCount", str(len(rows)))
    if is_truncated and rows:
        add_subelement(root, "NextContinuationToken", _encode_continuation_token(rows[-1]["object_key"]))

    for obj in rows:
        content = add_subelement(root, "Contents")
        add_subelement(content, "Key", _maybe_url_encode(obj["object_key"], encoding_type))
        add_subelement(content, "LastModified", format_s3_timestamp(obj["created_at"]))
        # ETag is a quoted hex string per S3 spec; SDKs round-trip the quotes.
        add_subelement(content, "ETag", f'"{obj["md5_hash"] or ""}"')
        add_subelement(content, "Size", str(obj["size_bytes"]))
        add_subelement(content, "StorageClass", "STANDARD")
        owner = add_subelement(content, "Owner")
        add_subelement(owner, "ID", bucket_owner)
        add_subelement(owner, "DisplayName", bucket_owner)

    return Response(
        content=to_xml_bytes(root, pretty_print=False),
        media_type="application/xml",
        status_code=200,
    )
