"""Unit tests for the shared fs_cache:pressure signal (publisher + consumer)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import hippius_s3.pressure_signal as ps
from hippius_s3.fs_pressure import should_reject_fs_cache_write
from hippius_s3.pressure_signal import PRESSURE_KEY
from hippius_s3.pressure_signal import PRESSURE_TTL_SECONDS
from hippius_s3.pressure_signal import PressurePublisher
from hippius_s3.pressure_signal import compute_mode
from hippius_s3.pressure_signal import get_published_pressure_mode


class FakeRedis:
    def __init__(self, value=None, fail=False):
        self.value = value
        self.fail = fail
        self.set_calls = []

    async def set(self, key, value, ex=None):
        if self.fail:
            raise ConnectionError("redis down")
        self.set_calls.append((key, value, ex))

    async def get(self, key):
        if self.fail:
            raise ConnectionError("redis down")
        return self.value


def _usage(used, total):
    class U:
        pass

    u = U()
    u.used = used
    u.total = total
    u.free = total - used
    return u


@pytest.fixture(autouse=True)
def _reset_consumer_memo():
    ps._published_cache = (None, 0.0)
    ps._last_good = (None, 0.0)
    yield
    ps._published_cache = (None, 0.0)
    ps._last_good = (None, 0.0)


# ------------------------------------------------------------------ compute_mode

def test_compute_mode_enter_and_exit_hysteresis():
    assert compute_mode(0.5, 0) == 0
    assert compute_mode(0.86, 0) == 1
    assert compute_mode(0.96, 0) == 2
    # Exit thresholds: mode is held until the ratio drops below the exit bound.
    assert compute_mode(0.94, 2) == 2
    assert compute_mode(0.92, 2) == 1
    assert compute_mode(0.84, 1) == 1
    assert compute_mode(0.82, 1) == 0


# ------------------------------------------------------------------ publisher

@pytest.mark.asyncio
async def test_publish_once_sets_key_with_ttl(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("hippius_s3.pressure_signal.shutil.disk_usage", lambda p: _usage(96, 100))
    redis = FakeRedis()
    pub = PressurePublisher(
        redis, tmp_path, mgr_metrics_url="", pools=[], probe_timeout_seconds=1.0
    )

    await pub.publish_once()

    assert len(redis.set_calls) == 1
    key, raw, ex = redis.set_calls[0]
    assert key == PRESSURE_KEY
    assert ex == PRESSURE_TTL_SECONDS
    payload = json.loads(raw)
    assert payload["mode"] == 2
    assert payload["source"] == "janitor"
    assert payload["ratio"] == pytest.approx(0.96)


@pytest.mark.asyncio
async def test_publish_skipped_when_statvfs_fails(monkeypatch, tmp_path: Path):
    def _boom(p):
        raise OSError("mount gone")

    monkeypatch.setattr("hippius_s3.pressure_signal.shutil.disk_usage", _boom)
    redis = FakeRedis()
    pub = PressurePublisher(
        redis, tmp_path, mgr_metrics_url="", pools=[], probe_timeout_seconds=1.0
    )

    await pub.publish_once()

    # Letting the TTL lapse (consumers fall back) beats publishing a guess.
    assert redis.set_calls == []


@pytest.mark.asyncio
async def test_publisher_hysteresis_across_ticks(monkeypatch, tmp_path: Path):
    ratios = iter([0.96, 0.94, 0.92])
    monkeypatch.setattr(
        "hippius_s3.pressure_signal.shutil.disk_usage", lambda p: _usage(int(next(ratios) * 100), 100)
    )
    redis = FakeRedis()
    pub = PressurePublisher(
        redis, tmp_path, mgr_metrics_url="", pools=[], probe_timeout_seconds=1.0
    )

    await pub.publish_once()  # 0.96 -> 2
    await pub.publish_once()  # 0.94 holds 2 (exit is 0.93)
    await pub.publish_once()  # 0.92 -> 1

    modes = [json.loads(raw)["mode"] for _, raw, _ in redis.set_calls]
    assert modes == [2, 2, 1]


# ------------------------------------------------------------------ consumer

@pytest.mark.asyncio
async def test_consumer_reads_published_mode():
    redis = FakeRedis(value=json.dumps({"mode": 2, "ratio": 0.96, "source": "janitor", "ts": 1.0}))
    assert await get_published_pressure_mode(redis) == 2


@pytest.mark.asyncio
async def test_consumer_memoizes_reads():
    redis = FakeRedis(value=json.dumps({"mode": 1, "ratio": 0.9, "source": "janitor", "ts": 1.0}))
    assert await get_published_pressure_mode(redis) == 1
    redis.fail = True  # a second read within the memo window must not hit Redis
    assert await get_published_pressure_mode(redis) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, b"not-json", json.dumps({"mode": 7})])
async def test_consumer_unavailable_signal_is_none(value):
    assert await get_published_pressure_mode(FakeRedis(value=value)) is None


@pytest.mark.asyncio
async def test_consumer_none_redis_is_none():
    assert await get_published_pressure_mode(None) is None


@pytest.mark.asyncio
async def test_consumer_holds_last_good_mode_on_read_error():
    """A Redis blip during genuine mode-2 must not open the PUT gate: read
    ERRORS hold the last-good mode (bounded by the publish TTL)."""
    redis = FakeRedis(value=json.dumps({"mode": 2, "ratio": 0.96, "source": "janitor", "ts": 1.0}))
    assert await get_published_pressure_mode(redis) == 2

    ps._published_cache = (2, -100.0)  # expire the memo, keep _last_good
    redis.fail = True
    assert await get_published_pressure_mode(redis) == 2


@pytest.mark.asyncio
async def test_consumer_key_absence_is_not_masked_by_last_good():
    """Key ABSENCE is the publisher's honest 'signal unavailable' — last-good
    must not override it (that's how a retired publisher would haunt the gate)."""
    redis = FakeRedis(value=json.dumps({"mode": 2, "ratio": 0.96, "source": "janitor", "ts": 1.0}))
    assert await get_published_pressure_mode(redis) == 2

    ps._published_cache = (2, -100.0)
    redis.value = None  # key expired/absent, read succeeds
    assert await get_published_pressure_mode(redis) is None


# ------------------------------------------------------------------ fs_pressure integration

def _config(tmp_path: Path):
    from types import SimpleNamespace

    return SimpleNamespace(
        object_cache_dir=str(tmp_path),
        fs_cache_min_free_bytes=0,
        fs_cache_min_free_ratio=0.0,
        fs_cache_retry_after_seconds=30,
    )


def test_reject_on_published_critical_even_with_local_headroom(tmp_path: Path):
    reject, retry_after, _pressure, reason = should_reject_fs_cache_write(
        config=_config(tmp_path), published_mode=2
    )
    assert reject is True
    assert reason == "pool"
    assert retry_after >= 1.0


@pytest.mark.parametrize("mode", [None, 0, 1])
def test_no_reject_below_critical_signal(tmp_path: Path, mode):
    reject, _r, _p, reason = should_reject_fs_cache_write(config=_config(tmp_path), published_mode=mode)
    assert reject is False
    assert reason == "ok"
