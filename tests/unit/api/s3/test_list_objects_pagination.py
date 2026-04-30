"""Unit tests for ListObjectsV2 keyset pagination behaviour."""

from __future__ import annotations

import base64
from datetime import datetime
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3.buckets import list_objects_endpoint
from hippius_s3.api.s3.buckets.list_objects_endpoint import _decode_continuation_token
from hippius_s3.api.s3.buckets.list_objects_endpoint import _encode_continuation_token
from hippius_s3.api.s3.buckets.list_objects_endpoint import _maybe_url_encode
from hippius_s3.api.s3.buckets.list_objects_endpoint import handle_list_objects
from hippius_s3.dependencies import RequestContext


S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
SAMPLE_TS = datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)


def _row(key: str, *, size: int = 100, md5: str = "deadbeef", multipart: bool = False) -> dict[str, Any]:
    return {
        "object_id": f"id-{key}",
        "object_key": key,
        "size_bytes": size,
        "md5_hash": md5,
        "created_at": SAMPLE_TS,
        "multipart": multipart,
        "status": "uploaded",
        "arion_file_hash": None,
        "ipfs_cid": None,
    }


def _make_pool(bucket_row: dict[str, Any] | None, list_rows: list[dict[str, Any]]) -> Any:
    pool = AsyncMock()
    pool.fetchrow = AsyncMock(return_value=bucket_row)
    pool.fetch = AsyncMock(return_value=list_rows)
    return pool


def _ctx() -> RequestContext:
    return RequestContext(main_account_id="5HWAJ-test-account", seed_phrase="")


def _parse(xml_bytes: bytes) -> ET._Element:
    return ET.fromstring(xml_bytes)


def _text(root: ET._Element, tag: str) -> str | None:
    el = root.find(f"{S3_NS}{tag}")
    return el.text if el is not None else None


def _all(root: ET._Element, tag: str) -> list[ET._Element]:
    return list(root.iterfind(f"{S3_NS}{tag}"))


# ---- pure helper round-trips --------------------------------------------------


def test_continuation_token_round_trip() -> None:
    for key in ["a/b.txt", "ünicode/é.bam", "with space/and+plus", "0", "z" * 200]:
        token = _encode_continuation_token(key)
        assert _decode_continuation_token(token) == key


def test_continuation_token_decodes_empty_to_none() -> None:
    assert _decode_continuation_token("") is None


def test_url_encode_only_when_requested() -> None:
    assert _maybe_url_encode("a/b c", None) == "a/b c"
    assert _maybe_url_encode("a/b c", "url") == "a%2Fb%20c"
    assert _maybe_url_encode("é", "URL") == "%C3%A9"


# ---- endpoint behaviour -------------------------------------------------------


@pytest.mark.asyncio
async def test_returns_404_for_unknown_bucket() -> None:
    pool = _make_pool(bucket_row=None, list_rows=[])
    resp = await handle_list_objects(
        "missing", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    assert resp.status_code == 404
    assert b"NoSuchBucket" in resp.body


@pytest.mark.asyncio
async def test_empty_bucket_returns_zero_keycount_not_truncated() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    assert resp.status_code == 200
    root = _parse(resp.body)
    assert _text(root, "KeyCount") == "0"
    assert _text(root, "IsTruncated") == "false"
    assert _text(root, "MaxKeys") == "1000"
    assert root.find(f"{S3_NS}NextContinuationToken") is None
    assert _all(root, "Contents") == []


@pytest.mark.asyncio
async def test_returns_keys_within_max_keys_no_truncation() -> None:
    rows = [_row(f"k{i:03d}") for i in range(50)]
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=rows)
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys="50", encoding_type=None, delimiter=None,
    )
    root = _parse(resp.body)
    assert _text(root, "KeyCount") == "50"
    assert _text(root, "IsTruncated") == "false"
    assert root.find(f"{S3_NS}NextContinuationToken") is None
    # SQL gets max_keys+1 to detect truncation
    pool.fetch.assert_awaited_once()
    call_args = pool.fetch.call_args.args
    assert call_args[-1] == 51


@pytest.mark.asyncio
async def test_truncated_when_more_rows_than_max_keys() -> None:
    rows = [_row(f"k{i:03d}") for i in range(101)]  # 101 rows for max_keys=100
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=rows)
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys="100", encoding_type=None, delimiter=None,
    )
    root = _parse(resp.body)
    assert _text(root, "KeyCount") == "100"
    assert _text(root, "IsTruncated") == "true"
    next_tok = _text(root, "NextContinuationToken")
    assert next_tok is not None
    # Token decodes back to the 100th (last returned) key
    assert _decode_continuation_token(next_tok) == "k099"
    # 101st row should not appear in response
    contents_keys = [c.find(f"{S3_NS}Key").text for c in _all(root, "Contents")]
    assert "k100" not in contents_keys
    assert len(contents_keys) == 100


@pytest.mark.asyncio
async def test_continuation_token_resumes_from_last_key() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    last_key = "deep/nested/key.bam"
    token = _encode_continuation_token(last_key)
    await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=token,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    # SQL positional args: 0=SQL_text, 1=bucket_id, 2=prefix, 3=cursor, 4=limit
    call_args = pool.fetch.call_args.args
    assert call_args[3] == last_key


@pytest.mark.asyncio
async def test_continuation_token_takes_precedence_over_start_after() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    token = _encode_continuation_token("from-token")
    await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after="from-start-after", continuation_token=token,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    call_args = pool.fetch.call_args.args
    assert call_args[3] == "from-token"


@pytest.mark.asyncio
async def test_start_after_used_when_no_continuation_token() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after="explicit-start", continuation_token=None,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    call_args = pool.fetch.call_args.args
    assert call_args[3] == "explicit-start"


@pytest.mark.asyncio
async def test_invalid_continuation_token_returns_400() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token="not!base64!@@@",
        max_keys=None, encoding_type=None, delimiter=None,
    )
    assert resp.status_code == 400
    assert b"InvalidArgument" in resp.body
    # No DB fetch should happen on bad token (we never reach get_bucket_by_name)
    pool.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_continuation_token_with_invalid_utf8_returns_400() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    bad_token = base64.urlsafe_b64encode(b"\xff\xfe\x80").decode("ascii")
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=bad_token,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_max_keys_above_1000_is_clamped() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys="5000", encoding_type=None, delimiter=None,
    )
    root = _parse(resp.body)
    assert _text(root, "MaxKeys") == "1000"
    # SQL also called with the clamped value (+1 for truncation detection)
    assert pool.fetch.call_args.args[-1] == 1001


@pytest.mark.asyncio
async def test_max_keys_zero_returns_empty() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys="0", encoding_type=None, delimiter=None,
    )
    assert resp.status_code == 200
    root = _parse(resp.body)
    assert _text(root, "MaxKeys") == "0"
    assert _text(root, "KeyCount") == "0"


@pytest.mark.asyncio
async def test_max_keys_negative_returns_400() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys="-5", encoding_type=None, delimiter=None,
    )
    assert resp.status_code == 400
    assert b"InvalidArgument" in resp.body


@pytest.mark.asyncio
async def test_max_keys_non_integer_returns_400() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys="lots", encoding_type=None, delimiter=None,
    )
    assert resp.status_code == 400
    assert b"InvalidArgument" in resp.body


@pytest.mark.asyncio
async def test_etag_is_double_quoted() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[_row("only", md5="abc123")])
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    root = _parse(resp.body)
    etag = root.find(f"{S3_NS}Contents/{S3_NS}ETag").text
    assert etag == '"abc123"'


@pytest.mark.asyncio
async def test_storage_class_is_uppercase_standard_for_all_objects() -> None:
    rows = [_row("simple", multipart=False), _row("mpu", multipart=True)]
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=rows)
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    root = _parse(resp.body)
    classes = [c.text for c in root.iterfind(f"{S3_NS}Contents/{S3_NS}StorageClass")]
    assert classes == ["STANDARD", "STANDARD"]


@pytest.mark.asyncio
async def test_delimiter_silently_ignored_returns_flat_list() -> None:
    # Pre-fix this returned 501 NotImplemented; we now log and ignore so aws s3 ls works.
    rows = [_row("a/x"), _row("a/y"), _row("b/z")]
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=rows)
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys=None, encoding_type=None, delimiter="/",
    )
    assert resp.status_code == 200
    root = _parse(resp.body)
    keys = [c.find(f"{S3_NS}Key").text for c in _all(root, "Contents")]
    assert keys == ["a/x", "a/y", "b/z"]
    # Delimiter is echoed back even though we don't fold CommonPrefixes yet.
    assert _text(root, "Delimiter") == "/"


@pytest.mark.asyncio
async def test_encoding_type_url_percent_encodes_keys_and_prefix() -> None:
    rows = [_row("path/with space/é.bam")]
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=rows)
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix="path/", start_after=None, continuation_token=None,
        max_keys=None, encoding_type="url", delimiter=None,
    )
    root = _parse(resp.body)
    assert _text(root, "Prefix") == "path%2F"
    assert _text(root, "EncodingType") == "url"
    encoded_key = root.find(f"{S3_NS}Contents/{S3_NS}Key").text
    assert encoded_key == "path%2Fwith%20space%2F%C3%A9.bam"


@pytest.mark.asyncio
async def test_owner_id_and_display_name_are_main_account() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[_row("k")])
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=None,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    root = _parse(resp.body)
    owner_id = root.find(f"{S3_NS}Contents/{S3_NS}Owner/{S3_NS}ID").text
    owner_dn = root.find(f"{S3_NS}Contents/{S3_NS}Owner/{S3_NS}DisplayName").text
    assert owner_id == "5HWAJ-test-account"
    assert owner_dn == "5HWAJ-test-account"


@pytest.mark.asyncio
async def test_request_echoes_continuation_token_when_provided() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    token = _encode_continuation_token("k50")
    resp = await handle_list_objects(
        "b", _ctx(), pool,
        prefix=None, start_after=None, continuation_token=token,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    root = _parse(resp.body)
    assert _text(root, "ContinuationToken") == token


@pytest.mark.asyncio
async def test_prefix_is_passed_to_sql() -> None:
    pool = _make_pool(bucket_row={"bucket_id": "bid"}, list_rows=[])
    await handle_list_objects(
        "b", _ctx(), pool,
        prefix="genomes/HG001/", start_after=None, continuation_token=None,
        max_keys=None, encoding_type=None, delimiter=None,
    )
    # SQL positional args: 0=SQL_text, 1=bucket_id, 2=prefix, 3=cursor, 4=limit
    call_args = pool.fetch.call_args.args
    assert call_args[2] == "genomes/HG001/"


def test_constants_match_aws_spec() -> None:
    # AWS hard cap for ListObjectsV2 page size.
    assert list_objects_endpoint.MAX_KEYS_LIMIT == 1000
    assert list_objects_endpoint.DEFAULT_MAX_KEYS == 1000
