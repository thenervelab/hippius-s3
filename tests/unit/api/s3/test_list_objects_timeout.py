"""A listing that outruns the statement timeout must not become a 500.

`_collect_page` skip-scans in batches over one pooled connection. Any of those `conn.fetch` calls
can hit the pool's `command_timeout` (`API_DB_POOL_COMMAND_TIMEOUT`, 30s in prod) and asyncpg raises
a bare `asyncio.TimeoutError`. Nothing caught it, so a well-formed request against an existing
bucket returned `500 Internal Server Error` — the wrong class, and not retryable by contract. It was
doing so continuously in production on the largest buckets.

Two behaviours are pinned here, and the split between them is the whole design:

  items already collected  -> a SHORT TRUNCATED PAGE. AWS permits returning fewer keys than
                              max-keys, so this is a valid answer, and it is better than any error:
                              the client keeps the keys it earned and resumes past them rather than
                              re-running a scan that just proved too slow from the same offset.

  nothing collected        -> 503 SlowDown. There is no progress to report, and an empty page would
                              read as end-of-listing — a silent truncation of the caller's view of
                              the bucket, which is worse than an error.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3.buckets import list_objects_endpoint
from hippius_s3.api.s3.buckets.list_objects_endpoint import _collect_page
from hippius_s3.api.s3.buckets.list_objects_endpoint import handle_list_objects
from hippius_s3.dependencies import RequestContext


S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
SAMPLE_TS = datetime(2026, 4, 30, 12, 0, 0, tzinfo=timezone.utc)


def _row(key: str) -> dict[str, Any]:
    return {
        "object_id": f"id-{key}",
        "object_key": key,
        "size_bytes": 100,
        "md5_hash": "deadbeef",
        "content_type": "application/octet-stream",
        "created_at": SAMPLE_TS,
        "multipart": False,
        "status": "uploaded",
        "body_blake3": None,
    }


def _bucket() -> dict[str, Any]:
    return {"bucket_id": "bid", "main_account_id": "5BUCKET-OWNER"}


def _make_pool(*, fetch_side_effect: list[Any]) -> Any:
    """`side_effect` entries may be row-lists OR exceptions, which is how the timeout is injected."""
    pool = AsyncMock()
    pool.fetchrow = AsyncMock(return_value=_bucket())
    pool.fetch = AsyncMock(side_effect=fetch_side_effect)

    class _Acq:
        async def __aenter__(self_: Any) -> Any:
            return pool

        async def __aexit__(self_: Any, *_a: Any) -> bool:
            return False

    pool.acquire = MagicMock(return_value=_Acq())
    return pool


def _ctx() -> RequestContext:
    return RequestContext(main_account_id="5HWAJ-test-account")


def _parse(b: bytes) -> ET._Element:
    return ET.fromstring(b)


def _text(root: ET._Element, tag: str) -> str | None:
    el = root.find(f"{S3_NS}{tag}")
    return el.text if el is not None else None


def _keys(root: ET._Element) -> list[str | None]:
    return [c.find(f"{S3_NS}Key").text for c in root.iterfind(f"{S3_NS}Contents")]


async def _list(pool: Any, **kw: Any) -> Any:
    return await handle_list_objects(
        "b",
        _ctx(),
        pool,
        prefix=kw.get("prefix"),
        start_after=None,
        continuation_token=kw.get("continuation_token"),
        max_keys=kw.get("max_keys"),
        encoding_type=None,
        delimiter=kw.get("delimiter"),
    )


# --- progress was made: short truncated page ------------------------------------------------


# A second fetch is only issued when the previous batch came back FULL — `_collect_page` treats
# `len(batch) < batch_limit` as end-of-listing. Without a delimiter a full batch yields target+1
# items and returns at the truncation check, so the timeout can never be reached with items in hand.
# The partial-page path therefore exists ONLY where rows collapse into common prefixes, which is
# exactly the shape of the production listings that were failing (deeply nested machine-generated
# keys read with a delimiter). Every test below forces a full batch on purpose; one that does not
# passes without ever raising the timeout.
MAX_KEYS = 5
BATCH_LIMIT = MAX_KEYS + 1


def _full_collapsing_batch(prefix: str = "dir") -> list[dict[str, Any]]:
    """Exactly `batch_limit` rows that all fold into one CommonPrefix — full batch, one item."""
    return [_row(f"{prefix}/{i:03d}") for i in range(BATCH_LIMIT)]


@pytest.mark.asyncio
async def test_timeout_after_collecting_rows_returns_a_short_truncated_page() -> None:
    """The first batch lands full, the second times out. The earned item must survive."""
    pool = _make_pool(fetch_side_effect=[_full_collapsing_batch(), TimeoutError()])

    resp = await _list(pool, max_keys=MAX_KEYS, delimiter="/")

    assert resp.status_code == 200
    root = _parse(resp.body)
    assert [cp.find(f"{S3_NS}Prefix").text for cp in root.iterfind(f"{S3_NS}CommonPrefixes")] == ["dir/"]
    assert _text(root, "KeyCount") == "1"


@pytest.mark.asyncio
async def test_that_short_page_is_marked_truncated_with_a_resume_token() -> None:
    """A short page without IsTruncated=true would tell the client the bucket ends here — the
    silent-data-loss failure mode this whole change exists to avoid."""
    pool = _make_pool(fetch_side_effect=[_full_collapsing_batch(), TimeoutError()])

    resp = await _list(pool, max_keys=MAX_KEYS, delimiter="/")
    root = _parse(resp.body)

    assert _text(root, "IsTruncated") == "true"
    assert _text(root, "NextContinuationToken") is not None


@pytest.mark.asyncio
async def test_the_resume_point_loses_no_rows() -> None:
    """The contract that makes the short page safe: resuming from the returned cursor yields
    exactly what was not delivered — no gap, no duplicate."""
    partial, truncated, cursor = await _collect_page(
        _make_pool(fetch_side_effect=[_full_collapsing_batch("dir"), TimeoutError()]),
        "bid",
        prefix=None,
        delimiter="/",
        cursor=None,
        target=MAX_KEYS,
        cp_floor=None,
    )
    assert truncated is True
    assert partial == [("prefix", "dir/")]

    # Resume: the server re-issues from `cursor`; the DB returns everything after the dir/ group.
    rest, _t, _c = await _collect_page(
        _make_pool(fetch_side_effect=[[_row("zeta")], []]),
        "bid",
        prefix=None,
        delimiter="/",
        cursor=cursor,
        target=MAX_KEYS,
        cp_floor=None,
    )

    delivered = [p for _k, p in partial] + [r["object_key"] for _k, r in rest]
    assert delivered == ["dir/", "zeta"]
    assert len(delivered) == len(set(delivered))


@pytest.mark.asyncio
async def test_the_cursor_skips_the_whole_collapsed_group() -> None:
    """Rows folded into a CommonPrefix advance the cursor without appending an item per row. A
    resume point taken from the last ITEM rather than the loop cursor would re-deliver the group."""
    items, truncated, cursor = await _collect_page(
        _make_pool(fetch_side_effect=[_full_collapsing_batch(), TimeoutError()]),
        "bid",
        prefix=None,
        delimiter="/",
        cursor=None,
        target=MAX_KEYS,
        cp_floor=None,
    )

    assert items == [("prefix", "dir/")]
    assert truncated is True
    # Strictly past every key in the group, so the resumed page cannot re-emit dir/…
    assert cursor is not None and cursor > "dir/999"


# --- no progress: retryable 503, never 500 --------------------------------------------------


@pytest.mark.asyncio
async def test_timeout_on_the_very_first_fetch_is_a_retryable_503() -> None:
    pool = _make_pool(fetch_side_effect=[TimeoutError()])

    resp = await _list(pool, max_keys=1000)

    assert resp.status_code == 503
    assert b"SlowDown" in resp.body
    assert resp.headers.get("Retry-After") == "10"


@pytest.mark.asyncio
async def test_the_no_progress_case_is_never_a_500() -> None:
    """The regression. This returned `500 Internal Server Error` for a well-formed request against
    an existing bucket."""
    pool = _make_pool(fetch_side_effect=[TimeoutError()])

    resp = await _list(pool, max_keys=1000)

    assert resp.status_code != 500


@pytest.mark.asyncio
async def test_the_sql_rollup_path_also_yields_503_not_500(monkeypatch: pytest.MonkeyPatch) -> None:
    """`_collect_page_sql` is a single fetch with no partial state, so it has no short page to fall
    back on — but it must still not surface a 500."""
    monkeypatch.setattr(list_objects_endpoint, "_sql_rollup_enabled", lambda: True)
    pool = _make_pool(fetch_side_effect=[TimeoutError()])

    resp = await _list(pool, max_keys=1000)

    assert resp.status_code == 503
    assert b"SlowDown" in resp.body


# --- the happy path must not have moved ------------------------------------------------------


@pytest.mark.asyncio
async def test_a_listing_that_does_not_time_out_is_unchanged() -> None:
    pool = _make_pool(fetch_side_effect=[[_row(f"k{i:03d}") for i in range(5)], []])

    resp = await _list(pool, max_keys=1000)
    root = _parse(resp.body)

    assert resp.status_code == 200
    assert _keys(root) == [f"k{i:03d}" for i in range(5)]
    assert _text(root, "IsTruncated") == "false"
    assert root.find(f"{S3_NS}NextContinuationToken") is None


@pytest.mark.asyncio
async def test_ordinary_truncation_still_reports_the_full_page() -> None:
    """Truncation by max-keys and truncation by timeout must stay distinguishable: this one fills
    the page, so it must return `target` items, not the short-page path."""
    pool = _make_pool(fetch_side_effect=[[_row(f"k{i:03d}") for i in range(11)]])

    resp = await _list(pool, max_keys=10)
    root = _parse(resp.body)

    assert _text(root, "KeyCount") == "10"
    assert _text(root, "IsTruncated") == "true"
    assert _text(root, "NextContinuationToken") is not None
