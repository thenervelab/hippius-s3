"""HIPPIUS_JANITOR_FS_GC_ENABLED skips pool walks and keeps DB GC.

Staging unmounts the shared cache PVC. If the janitor still created an FS store it
would mkdir the container overlay, walk nothing useful, and publish that disk as
fs_cache:pressure — which would 503 ingest. The flag must skip those phases and
still run hard-delete / version-reap / sentinel / A21.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from hippius_s3.config import Config
from workers import run_janitor_in_loop as janitor


def test_fs_gc_defaults_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HIPPIUS_JANITOR_FS_GC_ENABLED", raising=False)
    assert Config().janitor_fs_gc_enabled is True


def test_an_explicit_false_disables_fs_gc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIPPIUS_JANITOR_FS_GC_ENABLED", "false")
    assert Config().janitor_fs_gc_enabled is False


@pytest.mark.asyncio
async def test_fs_gc_off_skips_pool_walks_and_keeps_db_gc(monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    created_fs = {"called": False}

    async def _rec(name: str, ret: object = 0):
        order.append(name)
        return ret

    monkeypatch.setattr(janitor.config, "janitor_fs_gc_enabled", False)
    monkeypatch.setattr(janitor, "_update_disk_metrics", AsyncMock(side_effect=lambda *_a, **_k: order.append("disk")))
    monkeypatch.setattr(janitor, "_pressure_mode", lambda *_a, **_k: order.append("pressure") or 0)
    monkeypatch.setattr(janitor, "check_replication_sentinel", lambda *_a, **_k: _rec("sentinel"))
    monkeypatch.setattr(janitor, "get_all_dlq_object_ids", lambda *_a, **_k: _rec("dlq", set()))
    monkeypatch.setattr(janitor, "check_aged_pending_orphans", lambda *_a, **_k: _rec("aged_orphans"))
    monkeypatch.setattr(janitor, "evict_from_inventory", lambda *_a, **_k: _rec("sql_evict"))
    monkeypatch.setattr(janitor, "cleanup_parts_unified", lambda *_a, **_k: _rec("fs_unified", {}))
    monkeypatch.setattr(janitor, "gc_soft_deleted_objects", lambda *_a, **_k: _rec("hard_delete"))
    monkeypatch.setattr(janitor, "reap_deleted_object_versions", lambda *_a, **_k: _rec("version_reap"))
    monkeypatch.setattr(janitor, "_setup_janitor_metrics", lambda: None)

    def _create_fs(_config: object) -> MagicMock:
        created_fs["called"] = True
        return MagicMock()

    monkeypatch.setattr(janitor, "create_fs_store", _create_fs)
    monkeypatch.setattr(janitor.asyncpg, "create_pool", AsyncMock(return_value=AsyncMock()))
    monkeypatch.setattr(janitor.Redis, "from_url", lambda _url: AsyncMock())

    class _Stop(Exception):
        pass

    async def _sleep_then_stop(_seconds: float) -> None:
        raise _Stop

    monkeypatch.setattr(janitor.asyncio, "sleep", _sleep_then_stop)

    with pytest.raises(_Stop):
        await janitor.run_janitor_loop()

    assert created_fs["called"] is False
    assert "disk" not in order
    assert "pressure" not in order
    assert "sql_evict" not in order
    assert "fs_unified" not in order
    assert order == ["sentinel", "dlq", "aged_orphans", "hard_delete", "version_reap"]
