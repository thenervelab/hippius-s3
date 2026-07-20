"""RD-3: `build_chunk_plan` uses sizes already carried on the parts list, avoiding a size query.

The GET path builds the parts catalog once (now including `chunk_size_bytes`). When those sizes are
present the planner must NOT re-query `parts`; when they're absent (legacy/copy callers) it must fall
back to the batch query so the plan is still correct.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from hippius_s3.reader.planner import build_chunk_plan


@pytest.mark.asyncio
async def test_inline_sizes_skip_the_size_query() -> None:
    db = AsyncMock()  # .fetch must never be awaited
    parts = [
        {"part_number": 1, "size_bytes": 8192, "chunk_size_bytes": 4096},
        {"part_number": 2, "size_bytes": 4096, "chunk_size_bytes": 4096},
    ]

    plan = await build_chunk_plan(db, "obj-1", parts, None, object_version=1)

    # 8192/4096=2 chunks for part 1, 4096/4096=1 for part 2.
    assert [(i.part_number, i.chunk_index) for i in plan] == [(1, 0), (1, 1), (2, 0)]
    db.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_inline_sizes_fall_back_to_query() -> None:
    db = AsyncMock()
    parts = [{"part_number": 1, "cid": "x"}]  # no size fields

    with patch(
        "hippius_s3.reader.planner.read_parts_plain_and_chunk_sizes_batch",
        AsyncMock(return_value={1: (8192, 4096)}),
    ) as m:
        plan = await build_chunk_plan(db, "obj-1", parts, None, object_version=1)

    assert len(plan) == 2
    m.assert_awaited_once()


@pytest.mark.asyncio
async def test_partial_inline_sizes_fall_back_to_query() -> None:
    """If any part lacks the sizes, fall back rather than plan a truncated response."""
    db = AsyncMock()
    parts = [
        {"part_number": 1, "size_bytes": 8192, "chunk_size_bytes": 4096},
        {"part_number": 2, "cid": "x"},  # missing sizes
    ]

    with patch(
        "hippius_s3.reader.planner.read_parts_plain_and_chunk_sizes_batch",
        AsyncMock(return_value={1: (8192, 4096), 2: (4096, 4096)}),
    ) as m:
        plan = await build_chunk_plan(db, "obj-1", parts, None, object_version=1)

    assert len(plan) == 3
    m.assert_awaited_once()
