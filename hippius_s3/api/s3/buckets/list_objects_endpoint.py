from __future__ import annotations

import base64
import binascii
import logging
from typing import Any
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
# Marks tokens whose payload is an inclusive resume boundary (delimiter-aware).
# Legacy tokens (delimiter never worked) carried a bare content key used with a ">" cursor.
TOKEN_V2 = b"\x02"


def _invalid_arg(message: str) -> Response:
    return errors.s3_error_response(code="InvalidArgument", message=message, status_code=400)


def _content_resume(key: str) -> str:
    # Smallest representable text strictly greater than key (TEXT cannot hold U+0000),
    # so the inclusive ">= cursor" scan resumes just past this key.
    return key + "\x01"


def _prefix_resume(p: str) -> str:
    # Lexicographic successor that skips every key starting with p, under C/byte collation
    # (UTF-8 byte order == code-point order, so a code-point bump is a valid byte-wise successor).
    # Bump the last code point that can be bumped, dropping the tail; jumps past the collapsed
    # group in one index seek. The loop only guards the pathological U+10FFFF tail (a crafted
    # delimiter) from raising in chr(); for the real delimiter ('/') it bumps the last char.
    for i in range(len(p) - 1, -1, -1):
        if ord(p[i]) < 0x10FFFF:
            return p[:i] + chr(ord(p[i]) + 1)
    return p


def _encode_continuation_token(resume_inclusive: str) -> str:
    return base64.urlsafe_b64encode(TOKEN_V2 + resume_inclusive.encode("utf-8")).decode("ascii")


def _decode_continuation_token(token: str) -> str | None:
    if not token:
        return None
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    if raw[:1] == TOKEN_V2:
        return raw[1:].decode("utf-8")
    # Legacy bare-key token: reproduce the old "> key" semantics via an inclusive boundary.
    return raw.decode("utf-8") + "\x01"


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
    # FastAPI hands "?prefix=" / "?start-after=" / "?delimiter=" through as "" — treat as absent.
    prefix = prefix or None
    start_after = start_after or None
    delimiter = delimiter or None

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

    # The SQL cursor ($3) is an inclusive ">= boundary": a content key resumes at key+'\x01'
    # (== old "> key"), start-after at start_after+'\x01', and a common prefix jumps past its
    # whole collapsed group via _prefix_resume.
    cursor: str | None = _content_resume(start_after) if start_after else None
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

    # A common prefix is emitted only if it sorts strictly after StartAfter (AWS rule); the
    # cursor jump already keeps it from recurring across continuation pages, so the floor is
    # only meaningful for an explicit start-after (ignored when a continuation token resumes).
    cp_floor = None if continuation_token else start_after
    items, is_truncated, next_cursor = await _collect_page(
        pool,
        bucket_id,
        prefix=prefix,
        delimiter=delimiter,
        cursor=cursor,
        target=effective_max_keys,
        cp_floor=cp_floor,
    )
    contents = [row for kind, row in items if kind == "content"]
    common_prefixes = [payload for kind, payload in items if kind == "prefix"]

    # Element order follows the AWS ListObjectsV2 response schema so strict XML
    # validators (some Java SDKs, S3-spec test suites) accept it. CommonPrefixes come
    # after Contents, matching the AWS sample response.
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
    add_subelement(root, "KeyCount", str(len(items)))
    if is_truncated and next_cursor is not None:
        add_subelement(root, "NextContinuationToken", _encode_continuation_token(next_cursor))

    for obj in contents:
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

    for common_prefix in common_prefixes:
        cp = add_subelement(root, "CommonPrefixes")
        add_subelement(cp, "Prefix", _maybe_url_encode(common_prefix, encoding_type))

    return Response(
        content=to_xml_bytes(root, pretty_print=False),
        media_type="application/xml",
        status_code=200,
    )


async def _collect_page(
    pool: asyncpg.Pool,
    bucket_id: Any,
    *,
    prefix: str | None,
    delimiter: str | None,
    cursor: str | None,
    target: int,
    cp_floor: str | None,
) -> tuple[list[tuple[str, Any]], bool, str | None]:
    """Skip-scan the keyset query, rolling delimiter groups into CommonPrefixes.

    Returns the ordered page items (``("content", row)`` | ``("prefix", str)``), whether more
    results exist, and the inclusive boundary to resume from. Each common prefix counts as one
    item toward ``target``; once a group is seen its whole collapsed key range is skipped by
    advancing the cursor to ``_prefix_resume`` so the next fetch seeks past it in one index
    descent (a folder of N objects costs at most one extra round trip, not N rows of output).
    ``cp_floor`` (StartAfter) suppresses any common prefix not lexicographically greater than it.
    """
    if target == 0:
        # AWS returns an empty, non-truncated page for max-keys=0 (degenerate probe).
        return [], False, None

    plen = len(prefix or "")
    dlen = len(delimiter or "")
    # One extra item past the target proves more exist (truncation) without a count query.
    batch_limit = target + 1
    items: list[tuple[str, Any]] = []
    seen_prefixes: set[str] = set()
    query = get_query("list_objects")

    while True:
        batch = await pool.fetch(query, bucket_id, prefix, cursor, batch_limit)
        if not batch:
            return items, False, None

        for row in batch:
            key = row["object_key"]
            if cursor is not None and key < cursor:
                # Row sorts before a collapsed-group boundary we already jumped — skip cheaply.
                continue

            di = key.find(delimiter, plen) if delimiter else -1
            if di == -1:
                items.append(("content", row))
                cursor = _content_resume(key)
            else:
                common_prefix = key[: di + dlen]
                cursor = _prefix_resume(common_prefix)
                if common_prefix in seen_prefixes:
                    continue
                seen_prefixes.add(common_prefix)
                if cp_floor is not None and common_prefix <= cp_floor:
                    continue
                items.append(("prefix", common_prefix))

            if len(items) > target:
                # The (target+1)th item proves truncation; resume just past the last KEPT item.
                kind, payload = items[target - 1]
                next_cursor = _prefix_resume(payload) if kind == "prefix" else _content_resume(payload["object_key"])
                return items[:target], True, next_cursor

        if len(batch) < batch_limit:
            return items, False, None
