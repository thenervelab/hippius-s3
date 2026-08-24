from __future__ import annotations

from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from typing import Any

import lxml.etree as ET
import pytest

from hippius_s3.api.s3.buckets import list_object_versions_endpoint as mod
from hippius_s3.utils import get_query


TS = datetime(2026, 8, 22, 9, 32, 42, tzinfo=timezone.utc)


class _FakePool:
    def __init__(self, *, bucket: dict[str, Any] | None, rows: list[dict[str, Any]]) -> None:
        self.bucket = bucket
        self.rows = rows
        self.fetch_args: list[tuple[Any, ...]] = []

    async def fetchrow(self, query: str, *args: Any) -> Any:
        return self.bucket

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self.fetch_args.append(args)
        return self.rows


def _bucket(versioning_status: str | None = "Enabled") -> dict[str, Any]:
    return {
        "bucket_id": "bkt-1",
        "bucket_name": "b",
        "main_account_id": "owner-1",
        "versioning_status": versioning_status,
    }


def _row(
    key: str,
    version: int,
    *,
    current: int,
    marker: bool = False,
    size: int = 11,
    md5: str = "abc",
    body_blake3: str | None = None,
) -> dict[str, Any]:
    return {
        "object_key": key,
        "object_version": version,
        "is_delete_marker": marker,
        "size_bytes": size,
        "md5_hash": md5,
        "body_blake3": body_blake3,
        "last_modified": TS,
        "current_object_version": current,
    }


def _ctx() -> Any:
    return SimpleNamespace(main_account_id="owner-1")


async def _call(pool: Any, **kw: Any) -> Any:
    params: dict[str, Any] = {
        "prefix": None,
        "key_marker": None,
        "version_id_marker": None,
        "max_keys": None,
        "encoding_type": None,
        "delimiter": None,
    }
    params.update(kw)
    return await mod.handle_list_object_versions("b", _ctx(), pool, **params)


def _children(root: Any, local: str) -> list[Any]:
    return root.xpath(f"./*[local-name()='{local}']")


def _text(node: Any, local: str) -> str | None:
    """Element text, or None when the element is ABSENT.

    An empty element serialises as `<Prefix/>` with `.text is None`, so coerce that to "" —
    otherwise "present but empty" and "missing entirely" are indistinguishable, and the
    DeleteMarker assertions depend on that distinction.
    """
    found = node.xpath(f"./*[local-name()='{local}']")
    if not found:
        return None
    return str(found[0].text) if found[0].text is not None else ""


@pytest.mark.asyncio
async def test_result_root_and_scalar_fields() -> None:
    pool = _FakePool(bucket=_bucket(), rows=[_row("a.txt", 1, current=1)])
    resp = await _call(pool, max_keys="10")

    assert resp.status_code == 200
    assert resp.media_type == "application/xml"
    root = ET.fromstring(resp.body)
    assert ET.QName(root).localname == "ListVersionsResult"
    assert _text(root, "Name") == "b"
    assert _text(root, "Prefix") == ""
    assert _text(root, "MaxKeys") == "10"
    assert _text(root, "IsTruncated") == "false"


@pytest.mark.asyncio
async def test_version_entry_field_set() -> None:
    pool = _FakePool(bucket=_bucket(), rows=[_row("a.txt", 2, current=2, size=17, md5="d41d8")])
    resp = await _call(pool)

    version = _children(ET.fromstring(resp.body), "Version")[0]
    assert _text(version, "Key") == "a.txt"
    assert _text(version, "VersionId") == "2"
    assert _text(version, "IsLatest") == "true"
    assert _text(version, "ETag") == '"d41d8"'
    assert _text(version, "Size") == "17"
    assert _text(version, "StorageClass") == "STANDARD"
    assert _text(version.xpath("./*[local-name()='Owner']")[0], "ID") == "owner-1"
    assert _text(version, "LastModified") is not None


@pytest.mark.asyncio
async def test_delete_marker_entry_field_set() -> None:
    """A DeleteMarker carries no ETag, Size or StorageClass."""
    pool = _FakePool(bucket=_bucket(), rows=[_row("a.txt", 3, current=3, marker=True, size=0, md5="")])
    resp = await _call(pool)

    root = ET.fromstring(resp.body)
    assert _children(root, "Version") == []
    marker = _children(root, "DeleteMarker")[0]
    assert _text(marker, "Key") == "a.txt"
    assert _text(marker, "VersionId") == "3"
    assert _text(marker, "IsLatest") == "true"
    assert _text(marker, "LastModified") is not None
    assert _text(marker, "ETag") is None
    assert _text(marker, "Size") is None
    assert _text(marker, "StorageClass") is None


@pytest.mark.asyncio
async def test_is_latest_only_for_current_version() -> None:
    pool = _FakePool(
        bucket=_bucket(),
        rows=[_row("a.txt", 3, current=3), _row("a.txt", 2, current=3), _row("a.txt", 1, current=3)],
    )
    resp = await _call(pool)

    versions = _children(ET.fromstring(resp.body), "Version")
    assert [_text(v, "VersionId") for v in versions] == ["3", "2", "1"]
    assert [_text(v, "IsLatest") for v in versions] == ["true", "false", "false"]


@pytest.mark.asyncio
async def test_markers_and_versions_interleave_in_key_then_version_order() -> None:
    pool = _FakePool(
        bucket=_bucket(),
        rows=[
            _row("a.txt", 2, current=2, marker=True),
            _row("a.txt", 1, current=2),
            _row("b.txt", 1, current=1),
        ],
    )
    resp = await _call(pool)
    root = ET.fromstring(resp.body)

    ordered = [
        (ET.QName(el).localname, _text(el, "Key"), _text(el, "VersionId"))
        for el in root
        if ET.QName(el).localname in ("Version", "DeleteMarker")
    ]
    assert ordered == [
        ("DeleteMarker", "a.txt", "2"),
        ("Version", "a.txt", "1"),
        ("Version", "b.txt", "1"),
    ]


@pytest.mark.asyncio
async def test_truncation_emits_next_markers() -> None:
    rows = [_row("a.txt", 2, current=2), _row("a.txt", 1, current=2), _row("b.txt", 1, current=1)]
    pool = _FakePool(bucket=_bucket(), rows=rows)
    resp = await _call(pool, max_keys="2")

    root = ET.fromstring(resp.body)
    assert _text(root, "IsTruncated") == "true"
    assert len(_children(root, "Version")) == 2
    # Resume points at the first entry NOT returned.
    assert _text(root, "NextKeyMarker") == "b.txt"
    assert _text(root, "NextVersionIdMarker") == "1"


@pytest.mark.asyncio
async def test_markers_echoed_back() -> None:
    pool = _FakePool(bucket=_bucket(), rows=[])
    resp = await _call(pool, key_marker="a.txt", version_id_marker="2")

    root = ET.fromstring(resp.body)
    assert _text(root, "KeyMarker") == "a.txt"
    assert _text(root, "VersionIdMarker") == "2"
    assert _text(root, "IsTruncated") == "false"


@pytest.mark.asyncio
async def test_delimiter_rolls_keys_into_common_prefixes() -> None:
    pool = _FakePool(
        bucket=_bucket(),
        rows=[
            _row("photos/1.jpg", 1, current=1),
            _row("photos/2.jpg", 1, current=1),
            _row("top.txt", 1, current=1),
        ],
    )
    resp = await _call(pool, delimiter="/")

    root = ET.fromstring(resp.body)
    assert _text(root, "Delimiter") == "/"
    prefixes = [_text(cp, "Prefix") for cp in _children(root, "CommonPrefixes")]
    assert prefixes == ["photos/"]
    assert [_text(v, "Key") for v in _children(root, "Version")] == ["top.txt"]


@pytest.mark.asyncio
async def test_encoding_type_url_encodes_keys() -> None:
    pool = _FakePool(bucket=_bucket(), rows=[_row("a b.txt", 1, current=1)])
    resp = await _call(pool, encoding_type="url", prefix="a ")

    root = ET.fromstring(resp.body)
    assert _text(root, "EncodingType") == "url"
    assert _text(root, "Prefix") == "a%20"
    assert _text(_children(root, "Version")[0], "Key") == "a%20b.txt"


@pytest.mark.asyncio
async def test_unversioned_bucket_lists_only_current_versions() -> None:
    """Never-versioned buckets must not surface their accumulated overwrite history."""
    pool = _FakePool(bucket=_bucket(None), rows=[_row("a.txt", 4, current=4)])
    resp = await _call(pool)

    root = ET.fromstring(resp.body)
    assert [_text(v, "VersionId") for v in _children(root, "Version")] == ["4"]
    # The query must have been asked for current-only (trailing `current_only` flag).
    assert pool.fetch_args, "expected the listing query to run"
    assert pool.fetch_args[0][-1] is True


@pytest.mark.asyncio
async def test_missing_bucket_404() -> None:
    pool = _FakePool(bucket=None, rows=[])
    resp = await _call(pool)

    assert resp.status_code == 404
    assert b"NoSuchBucket" in resp.body


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["abc", "-1"])
async def test_invalid_max_keys_rejected(bad: str) -> None:
    pool = _FakePool(bucket=_bucket(), rows=[])
    resp = await _call(pool, max_keys=bad)

    assert resp.status_code == 400
    assert b"InvalidArgument" in resp.body


# ---------------------------------------------------------------------------
# Arion file hash in Owner.ID — must match ListObjects exactly, or the console
# shows a digest for a key in one listing and an SS58 for the same key in the other.
# ---------------------------------------------------------------------------

BLAKE3_HEX = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"


def _owner_id(entry: Any) -> str | None:
    return _text(entry.xpath("./*[local-name()='Owner']")[0], "ID")


@pytest.mark.asyncio
async def test_version_owner_id_is_the_digest_when_present() -> None:
    pool = _FakePool(bucket=_bucket(), rows=[_row("a.txt", 2, current=2, body_blake3=BLAKE3_HEX)])
    resp = await _call(pool)

    version = _children(ET.fromstring(resp.body), "Version")[0]
    assert _owner_id(version) == BLAKE3_HEX
    assert _text(version.xpath("./*[local-name()='Owner']")[0], "DisplayName") == "owner-1"


@pytest.mark.asyncio
async def test_version_owner_id_falls_back_to_the_account_without_a_digest() -> None:
    pool = _FakePool(bucket=_bucket(), rows=[_row("a.txt", 2, current=2)])
    resp = await _call(pool)

    assert _owner_id(_children(ET.fromstring(resp.body), "Version")[0]) == "owner-1"


@pytest.mark.asyncio
async def test_delete_marker_owner_id_is_always_the_account() -> None:
    """A marker has no content to hash, so it must never carry a digest — even a stale one."""
    row = _row("a.txt", 3, current=3, marker=True, size=0, md5="", body_blake3=BLAKE3_HEX)
    pool = _FakePool(bucket=_bucket(), rows=[row])
    resp = await _call(pool)

    assert _owner_id(_children(ET.fromstring(resp.body), "DeleteMarker")[0]) == "owner-1"


@pytest.mark.asyncio
async def test_each_version_carries_its_own_digest() -> None:
    """Per-version digests, not the current version's — an overwrite changes the content."""
    older = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
    pool = _FakePool(
        bucket=_bucket(),
        rows=[
            _row("a.txt", 2, current=2, body_blake3=BLAKE3_HEX),
            _row("a.txt", 1, current=2, body_blake3=older),
        ],
    )
    resp = await _call(pool)

    versions = _children(ET.fromstring(resp.body), "Version")
    assert [_owner_id(v) for v in versions] == [BLAKE3_HEX, older]


def test_the_query_projects_the_digest() -> None:
    """Without this column the endpoint KeyErrors — the two listings must read the same column."""
    assert "body_blake3" in get_query("list_object_versions")
