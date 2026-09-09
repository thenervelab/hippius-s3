"""The chunked storage count.

The point of chunking is that no single STATEMENT gets long enough to hit either 30s ceiling (our
asyncpg timeout, and the replica's max_standby_streaming_delay). The bytes must be identical to the
single-statement form -- so what these pin is that the keyset walk visits every page exactly once,
stops at the right moment, and sums the same total.
"""

from __future__ import annotations

from typing import Any

import pytest

from hippius_s3.services import usage_service


PAGE = 100


class FakeDb:
    """Serves buckets from a dict of {bucket_id: [(object_key, size_bytes), ...]}, in key order."""

    def __init__(self, buckets: dict[str, list[tuple[str, int]]], account_buckets: list[str] | None = None) -> None:
        self.buckets = buckets
        self.account_buckets = account_buckets if account_buckets is not None else list(buckets)
        self.page_calls: list[tuple[str, str]] = []

    async def fetch(self, _query: str, _account_id: str, timeout: float | None = None) -> list[dict[str, Any]]:
        return [{"bucket_id": b} for b in self.account_buckets]

    async def fetchrow(
        self, _query: str, bucket_id: str, cursor: str, page_size: int, timeout: float | None = None
    ) -> dict[str, Any]:
        self.page_calls.append((bucket_id, cursor))
        rows = [r for r in sorted(self.buckets.get(bucket_id, [])) if r[0] > cursor][:page_size]
        return {
            "rows_seen": len(rows),
            "last_key": rows[-1][0] if rows else None,
            "bytes_used": sum(size for _, size in rows),
        }


def _objects(n: int, size: int) -> list[tuple[str, int]]:
    return [(f"key-{i:08d}", size) for i in range(n)]


async def _count(db: FakeDb, page_size: int = PAGE) -> int:
    return await usage_service.get_account_storage_bytes(db, "5Acct", timeout=30.0, page_size=page_size)


@pytest.mark.asyncio
async def test_an_account_with_no_buckets_is_zero_not_an_error() -> None:
    assert await _count(FakeDb({})) == 0


@pytest.mark.asyncio
async def test_a_bucket_smaller_than_one_page_takes_a_single_round_trip() -> None:
    db = FakeDb({"b1": _objects(10, 5)})

    assert await _count(db) == 50
    assert len(db.page_calls) == 1, "a short page ends the walk immediately"


@pytest.mark.asyncio
async def test_a_bucket_spanning_many_pages_sums_every_one() -> None:
    """The case the chunking exists for. 250 objects at page size 100 is 3 pages: 100, 100, 50."""
    db = FakeDb({"b1": _objects(250, 7)})

    assert await _count(db) == 250 * 7
    assert len(db.page_calls) == 3
    assert [c for _, c in db.page_calls] == ["", "key-00000099", "key-00000199"], "keyset resumes exactly"


@pytest.mark.asyncio
async def test_a_bucket_that_is_an_exact_multiple_of_the_page_takes_one_more_trip() -> None:
    """A full final page is indistinguishable from "there may be more", so the walk must probe once
    more and get an empty page. Off-by-one here would silently drop a whole bucket's tail."""
    db = FakeDb({"b1": _objects(200, 3)})

    assert await _count(db) == 600
    assert len(db.page_calls) == 3, "100, 100, then an empty page to learn it is done"


@pytest.mark.asyncio
async def test_every_bucket_in_the_account_is_counted() -> None:
    db = FakeDb({"b1": _objects(5, 100), "b2": _objects(3, 1000), "b3": []})

    assert await _count(db) == 500 + 3000


@pytest.mark.asyncio
async def test_a_bucket_the_account_does_not_own_is_not_counted() -> None:
    """The bucket list is the authority on scope; a bucket absent from it must contribute nothing
    even though the page query would happily sum it."""
    db = FakeDb({"b1": _objects(5, 100), "someone-elses": _objects(999, 999)}, account_buckets=["b1"])

    assert await _count(db) == 500


@pytest.mark.asyncio
async def test_a_page_that_sums_to_zero_still_advances_the_cursor() -> None:
    """Objects whose current version is a delete marker or soft-deleted contribute 0 bytes but DO
    occupy page rows. If a zero-byte page were read as "done", the walk would stop at the first one
    and under-count everything behind it — which is why rows_seen counts PAGE rows, not summed rows.
    """
    db = FakeDb({"b1": _objects(100, 0) + [(f"key-{i:08d}", 9) for i in range(100, 150)]})

    assert await _count(db) == 50 * 9
    assert len(db.page_calls) == 2


@pytest.mark.asyncio
async def test_a_cursor_that_never_advances_is_refused_rather_than_spun_on() -> None:
    """A malformed page reply must not become an infinite loop holding a pool connection."""

    class StuckDb(FakeDb):
        async def fetchrow(self, _query, bucket_id, cursor, page_size, timeout=None):  # type: ignore[override]
            return {"rows_seen": page_size, "last_key": "same", "bytes_used": 1}

    with pytest.raises(RuntimeError, match="may not be advancing"):
        await _count(StuckDb({"b1": _objects(1, 1)}))
