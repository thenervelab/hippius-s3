"""AP-2: set_object_version_address(only_if_null=True) uses the NULL-gated query so the append hot
path issues a no-op UPDATE instead of a redundant write."""

from __future__ import annotations

from typing import Any

import pytest

from hippius_s3.writer.db import set_object_version_address


class _RecordingDB:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def execute(self, query: str, *args: Any) -> None:
        self.queries.append(query)


@pytest.mark.asyncio
async def test_default_uses_unconditional_update() -> None:
    db = _RecordingDB()
    await set_object_version_address(db, object_id="o", object_version=1, address="a")
    assert "address IS NULL" not in db.queries[0]
    assert "SET address" in db.queries[0]


@pytest.mark.asyncio
async def test_only_if_null_uses_gated_update() -> None:
    db = _RecordingDB()
    await set_object_version_address(db, object_id="o", object_version=1, address="a", only_if_null=True)
    assert "address IS NULL" in db.queries[0]
