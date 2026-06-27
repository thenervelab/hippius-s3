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
        "content_type": "application/octet-stream",
        "created_at": SAMPLE_TS,
        "multipart": multipart,
        "status": "uploaded",
    }


def _bucket(*, owner: str = "5BUCKET-OWNER") -> dict[str, Any]:
    return {"bucket_id": "bid", "main_account_id": owner}


def _make_pool(
    bucket_row: dict[str, Any] | None,
    list_rows: list[dict[str, Any]] | None = None,
    *,
    fetch_batches: list[list[dict[str, Any]]] | None = None,
) -> Any:
    pool = AsyncMock()
    pool.fetchrow = AsyncMock(return_value=bucket_row)
    if fetch_batches is not None:
        # Skip-scan issues multiple fetches; hand back a different batch per call.
        pool.fetch = AsyncMock(side_effect=fetch_batches)
    else:
        pool.fetch = AsyncMock(return_value=list_rows or [])
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


def _keys(root: ET._Element) -> list[str | None]:
    return [c.find(f"{S3_NS}Key").text for c in _all(root, "Contents")]


def _cps(root: ET._Element) -> list[str | None]:
    return [cp.find(f"{S3_NS}Prefix").text for cp in _all(root, "CommonPrefixes")]


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


def test_prefix_resume_is_a_tight_successor() -> None:
    # Successor must exceed every key under the prefix but nothing else.
    assert list_objects_endpoint._prefix_resume("photos/") == "photos0"
    assert "photos/zzz" < list_objects_endpoint._prefix_resume("photos/") <= "photos0"
    # Multi-char delimiter: bump the last char of the group.
    assert list_objects_endpoint._prefix_resume("a::") == "a:;"


def test_prefix_resume_does_not_crash_on_max_codepoint() -> None:
    # A crafted delimiter ending at U+10FFFF must not raise from chr(); fall back to bumping
    # an earlier code point and dropping the tail.
    p = "a" + chr(0x10FFFF)
    out = list_objects_endpoint._prefix_resume(p)
    assert out == "b"
    assert p < out


# ---- endpoint behaviour -------------------------------------------------------


@pytest.mark.asyncio
async def test_returns_404_for_unknown_bucket() -> None:
    pool = _make_pool(bucket_row=None, list_rows=[])
    resp = await handle_list_objects(
        "missing",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    assert resp.status_code == 404
    assert b"NoSuchBucket" in resp.body


@pytest.mark.asyncio
async def test_empty_bucket_returns_zero_keycount_not_truncated() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
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
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys="50",
        encoding_type=None,
        delimiter=None,
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
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys="100",
        encoding_type=None,
        delimiter=None,
    )
    root = _parse(resp.body)
    assert _text(root, "KeyCount") == "100"
    assert _text(root, "IsTruncated") == "true"
    next_tok = _text(root, "NextContinuationToken")
    assert next_tok is not None
    # Token decodes to the inclusive boundary just past the 100th (last returned) key.
    assert _decode_continuation_token(next_tok) == "k099\x01"
    # 101st row should not appear in response
    contents_keys = [c.find(f"{S3_NS}Key").text for c in _all(root, "Contents")]
    assert "k100" not in contents_keys
    assert len(contents_keys) == 100


@pytest.mark.asyncio
async def test_continuation_token_resumes_from_last_key() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    last_key = "deep/nested/key.bam"
    token = _encode_continuation_token(last_key)
    await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=token,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    # SQL positional args: 0=SQL_text, 1=bucket_id, 2=prefix, 3=cursor, 4=limit
    call_args = pool.fetch.call_args.args
    assert call_args[3] == last_key


@pytest.mark.asyncio
async def test_continuation_token_takes_precedence_over_start_after() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    token = _encode_continuation_token("from-token")
    await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after="from-start-after",
        continuation_token=token,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    call_args = pool.fetch.call_args.args
    assert call_args[3] == "from-token"


@pytest.mark.asyncio
async def test_start_after_used_when_no_continuation_token() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after="explicit-start",
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    # start-after becomes an inclusive ">= boundary" just past the given key.
    call_args = pool.fetch.call_args.args
    assert call_args[3] == "explicit-start\x01"


@pytest.mark.asyncio
async def test_invalid_continuation_token_returns_400() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token="not!base64!@@@",
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    assert resp.status_code == 400
    assert b"InvalidArgument" in resp.body
    # No DB fetch should happen on bad token (we never reach get_bucket_by_name)
    pool.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_continuation_token_with_invalid_utf8_returns_400() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    bad_token = base64.urlsafe_b64encode(b"\xff\xfe\x80").decode("ascii")
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=bad_token,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_max_keys_above_1000_is_clamped() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys="5000",
        encoding_type=None,
        delimiter=None,
    )
    root = _parse(resp.body)
    assert _text(root, "MaxKeys") == "1000"
    # SQL also called with the clamped value (+1 for truncation detection)
    assert pool.fetch.call_args.args[-1] == 1001


@pytest.mark.asyncio
async def test_max_keys_zero_returns_empty() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys="0",
        encoding_type=None,
        delimiter=None,
    )
    assert resp.status_code == 200
    root = _parse(resp.body)
    assert _text(root, "MaxKeys") == "0"
    assert _text(root, "KeyCount") == "0"


@pytest.mark.asyncio
async def test_max_keys_negative_returns_400() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys="-5",
        encoding_type=None,
        delimiter=None,
    )
    assert resp.status_code == 400
    assert b"InvalidArgument" in resp.body


@pytest.mark.asyncio
async def test_max_keys_non_integer_returns_400() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys="lots",
        encoding_type=None,
        delimiter=None,
    )
    assert resp.status_code == 400
    assert b"InvalidArgument" in resp.body


@pytest.mark.asyncio
async def test_etag_is_double_quoted() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[_row("only", md5="abc123")])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    root = _parse(resp.body)
    etag = root.find(f"{S3_NS}Contents/{S3_NS}ETag").text
    assert etag == '"abc123"'


@pytest.mark.asyncio
async def test_storage_class_is_uppercase_standard_for_all_objects() -> None:
    # Rows arrive in object_key order (the query's ORDER BY); keep the fixture sorted.
    rows = [_row("mpu", multipart=True), _row("simple", multipart=False)]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    root = _parse(resp.body)
    classes = [c.text for c in root.iterfind(f"{S3_NS}Contents/{S3_NS}StorageClass")]
    assert classes == ["STANDARD", "STANDARD"]


@pytest.mark.asyncio
async def test_delimiter_rolls_up_common_prefix_after_contents() -> None:
    # AWS doc example: sample.jpg stays in Contents; the photos/* keys collapse to photos/.
    rows = [
        _row("photos/2006/february/sample2.jpg"),
        _row("photos/2006/january/sample.jpg"),
        _row("sample.jpg"),
    ]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter="/",
    )
    assert resp.status_code == 200
    root = _parse(resp.body)
    assert _keys(root) == ["sample.jpg"]
    assert _cps(root) == ["photos/"]
    # Each common prefix counts as one item toward KeyCount.
    assert _text(root, "KeyCount") == "2"
    assert _text(root, "Delimiter") == "/"
    # CommonPrefixes must follow Contents in the document.
    tags = [ET.QName(c).localname for c in root]
    assert max(i for i, t in enumerate(tags) if t == "Contents") < min(
        i for i, t in enumerate(tags) if t == "CommonPrefixes"
    )


@pytest.mark.asyncio
async def test_delimiter_with_prefix_returns_only_common_prefixes() -> None:
    # AWS doc example: prefix=photos/2006/ + delimiter=/ yields the two sub-directories only.
    rows = [
        _row("photos/2006/february/sample2.jpg"),
        _row("photos/2006/february/sample3.jpg"),
        _row("photos/2006/january/sample.jpg"),
    ]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix="photos/2006/",
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter="/",
    )
    root = _parse(resp.body)
    assert _keys(root) == []
    assert _cps(root) == ["photos/2006/february/", "photos/2006/january/"]
    assert _text(root, "KeyCount") == "2"


@pytest.mark.asyncio
async def test_key_without_delimiter_after_prefix_goes_to_contents() -> None:
    # A delimiter that occurs only inside the prefix region must not roll up; the key
    # (and one exactly equal to the prefix) lands in Contents.
    rows = [_row("photos/2006/"), _row("photos/2006/cover.jpg")]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix="photos/2006/",
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter="/",
    )
    root = _parse(resp.body)
    assert _keys(root) == ["photos/2006/", "photos/2006/cover.jpg"]
    assert _cps(root) == []


@pytest.mark.asyncio
async def test_no_delimiter_emits_no_common_prefixes() -> None:
    rows = [_row("a/x"), _row("a/y"), _row("b/z")]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    root = _parse(resp.body)
    assert _keys(root) == ["a/x", "a/y", "b/z"]
    assert _cps(root) == []
    assert _text(root, "KeyCount") == "3"


@pytest.mark.asyncio
async def test_common_prefix_counts_as_one_toward_max_keys() -> None:
    # max-keys=2 over one standalone key plus two folders: page holds the key + one folder,
    # the second folder is the lookahead that trips truncation.
    rows = [_row("a.txt"), _row("docs/1"), _row("docs/2"), _row("img/1")]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys="2",
        encoding_type=None,
        delimiter="/",
    )
    root = _parse(resp.body)
    assert _keys(root) == ["a.txt"]
    assert _cps(root) == ["docs/"]
    assert _text(root, "KeyCount") == "2"
    assert _text(root, "IsTruncated") == "true"
    # Resume boundary jumps past the whole docs/ group.
    assert _decode_continuation_token(_text(root, "NextContinuationToken")) == "docs0"


@pytest.mark.asyncio
async def test_continuation_resumes_past_collapsed_common_prefix() -> None:
    # First page collapses docs/* and truncates; the next request must resume past the
    # entire docs/ group (no interior key reappears), landing on img/1.
    page1 = [_row("a.txt"), _row("docs/1"), _row("docs/2")]
    page2 = [_row("img/1")]
    pool = _make_pool(bucket_row=_bucket(), fetch_batches=[page1, page1, page2])
    resp1 = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys="2",
        encoding_type=None,
        delimiter="/",
    )
    root1 = _parse(resp1.body)
    token = _text(root1, "NextContinuationToken")
    assert _decode_continuation_token(token) == "docs0"

    pool2 = _make_pool(bucket_row=_bucket(), fetch_batches=[page2])
    resp2 = await handle_list_objects(
        "b",
        _ctx(),
        pool2,
        prefix=None,
        start_after=None,
        continuation_token=token,
        max_keys="2",
        encoding_type=None,
        delimiter="/",
    )
    # The second fetch is bound with the decoded boundary so the DB skips docs/*.
    assert pool2.fetch.call_args_list[0].args[3] == "docs0"
    root2 = _parse(resp2.body)
    assert _cps(root2) == ["img/"]
    assert _text(root2, "IsTruncated") == "false"


@pytest.mark.asyncio
async def test_start_after_filters_common_prefix_not_greater() -> None:
    # start-after past docs/ means the scan never returns docs/* keys, so docs/ is not formed;
    # only the later img/ folder rolls up.
    pool = _make_pool(bucket_row=_bucket(), list_rows=[_row("img/1")])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after="docs/zzz",
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter="/",
    )
    # Scan boundary is strictly past start-after.
    assert pool.fetch.call_args.args[3] == "docs/zzz\x01"
    root = _parse(resp.body)
    assert _cps(root) == ["img/"]


@pytest.mark.asyncio
async def test_start_after_equal_to_common_prefix_suppresses_it() -> None:
    # AWS: a common prefix is filtered out if it is NOT lexicographically greater than
    # StartAfter. With start-after="abc/", the abc/* keys form "abc/" which equals (not >)
    # StartAfter and must be dropped; only def/ (which is > "abc/") survives.
    rows = [_row("abc/file1"), _row("abc/file2"), _row("def/1")]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after="abc/",
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter="/",
    )
    root = _parse(resp.body)
    assert _keys(root) == []
    assert _cps(root) == ["def/"]
    assert _text(root, "KeyCount") == "1"


@pytest.mark.asyncio
async def test_multiple_folders_collapse_in_a_single_fetch() -> None:
    # Regression guard: distinct sibling folders must roll up within ONE fetch (the cursor
    # jump skips each group's interior in-batch) rather than one DB round trip per folder.
    rows = [_row("d1/a"), _row("d1/b"), _row("d2/a"), _row("d2/b"), _row("e1/a")]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter="/",
    )
    root = _parse(resp.body)
    assert _cps(root) == ["d1/", "d2/", "e1/"]
    assert _keys(root) == []
    pool.fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_encoding_type_url_encodes_common_prefix() -> None:
    rows = [_row("photos 2006/é/x.jpg")]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type="url",
        delimiter="/",
    )
    root = _parse(resp.body)
    assert _cps(root) == ["photos%202006%2F"]
    assert _text(root, "EncodingType") == "url"


@pytest.mark.asyncio
async def test_encoding_type_url_percent_encodes_keys_and_prefix() -> None:
    rows = [_row("path/with space/é.bam")]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix="path/",
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type="url",
        delimiter=None,
    )
    root = _parse(resp.body)
    assert _text(root, "Prefix") == "path%2F"
    assert _text(root, "EncodingType") == "url"
    encoded_key = root.find(f"{S3_NS}Contents/{S3_NS}Key").text
    assert encoded_key == "path%2Fwith%20space%2F%C3%A9.bam"


@pytest.mark.asyncio
async def test_owner_is_bucket_owner_not_requestor() -> None:
    # Even though the requestor is "5HWAJ-test-account", Owner must report the
    # bucket's main_account_id so that listings against shared/public buckets
    # don't impersonate the caller.
    pool = _make_pool(bucket_row=_bucket(owner="5BUCKET-OWNER"), list_rows=[_row("k")])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    root = _parse(resp.body)
    owner_id = root.find(f"{S3_NS}Contents/{S3_NS}Owner/{S3_NS}ID").text
    owner_dn = root.find(f"{S3_NS}Contents/{S3_NS}Owner/{S3_NS}DisplayName").text
    assert owner_id == "5BUCKET-OWNER"
    assert owner_dn == "5BUCKET-OWNER"


@pytest.mark.asyncio
async def test_owner_falls_back_to_requestor_when_bucket_owner_missing() -> None:
    # Defensive: if the buckets row is somehow missing main_account_id (legacy
    # data?), fall back to the requestor rather than emitting <ID/> with None.
    pool = _make_pool(bucket_row={"bucket_id": "bid", "main_account_id": None}, list_rows=[_row("k")])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    root = _parse(resp.body)
    owner_id = root.find(f"{S3_NS}Contents/{S3_NS}Owner/{S3_NS}ID").text
    assert owner_id == "5HWAJ-test-account"


@pytest.mark.asyncio
async def test_request_echoes_continuation_token_when_provided() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    token = _encode_continuation_token("k50")
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=token,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    root = _parse(resp.body)
    assert _text(root, "ContinuationToken") == token


@pytest.mark.asyncio
async def test_prefix_is_passed_to_sql() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix="genomes/HG001/",
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    # SQL positional args: 0=SQL_text, 1=bucket_id, 2=prefix, 3=cursor, 4=limit
    call_args = pool.fetch.call_args.args
    assert call_args[2] == "genomes/HG001/"


@pytest.mark.asyncio
async def test_oversized_continuation_token_returns_400_without_db_call() -> None:
    # Cap token length so an attacker can't force a multi-megabyte DB cursor bind.
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    huge = "A" * (list_objects_endpoint.MAX_CONTINUATION_TOKEN_LEN + 1)
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after=None,
        continuation_token=huge,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    assert resp.status_code == 400
    assert b"InvalidArgument" in resp.body
    pool.fetchrow.assert_not_called()
    pool.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_empty_prefix_query_string_normalized_to_none() -> None:
    # FastAPI hands "?prefix=" through as "" — that should NOT be turned into
    # `LIKE '' || '%'` (which matches everything but adds planner cost). Verify
    # the SQL receives None.
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix="",
        start_after=None,
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    call_args = pool.fetch.call_args.args
    assert call_args[2] is None


@pytest.mark.asyncio
async def test_empty_start_after_normalized_and_not_echoed() -> None:
    pool = _make_pool(bucket_row=_bucket(), list_rows=[])
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after="",
        continuation_token=None,
        max_keys=None,
        encoding_type=None,
        delimiter=None,
    )
    # Cursor passed to SQL is None, not ""
    assert pool.fetch.call_args.args[3] is None
    # XML must not echo an empty <StartAfter></StartAfter>
    root = _parse(resp.body)
    assert root.find(f"{S3_NS}StartAfter") is None


@pytest.mark.asyncio
async def test_xml_element_order_matches_aws_spec() -> None:
    # AWS schema order:
    #   Name, Prefix, ContinuationToken, StartAfter, MaxKeys, Delimiter,
    #   IsTruncated, EncodingType, KeyCount, NextContinuationToken, Contents..., CommonPrefixes...
    # boto3 doesn't care about order, but stricter XML-validating SDKs do.
    # One standalone key plus a folder so both Contents and CommonPrefixes appear, and a
    # lookahead row to force truncation (NextContinuationToken present).
    rows = [_row("a.txt"), _row("docs/1"), _row("img/1")]
    pool = _make_pool(bucket_row=_bucket(), list_rows=rows)
    # Resume boundary sorts before the rows so the (stateless) mock yields real progress.
    token = _encode_continuation_token("0")
    resp = await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=None,
        start_after="s",
        continuation_token=token,
        max_keys="2",
        encoding_type="url",
        delimiter="/",
    )
    root = _parse(resp.body)
    children = [ET.QName(child).localname for child in root]
    scalars = [t for t in children if t not in ("Contents", "CommonPrefixes")]
    assert scalars == [
        "Name",
        "Prefix",
        "ContinuationToken",
        "StartAfter",
        "MaxKeys",
        "Delimiter",
        "IsTruncated",
        "EncodingType",
        "KeyCount",
        "NextContinuationToken",
    ]
    # Contents must precede CommonPrefixes.
    assert max(i for i, t in enumerate(children) if t == "Contents") < min(
        i for i, t in enumerate(children) if t == "CommonPrefixes"
    )


def test_constants_match_aws_spec() -> None:
    # AWS hard cap for ListObjectsV2 page size.
    assert list_objects_endpoint.MAX_KEYS_LIMIT == 1000
    assert list_objects_endpoint.DEFAULT_MAX_KEYS == 1000
