"""Unit tests for the janitor's queue depth/age sampler."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from hippius_s3.queue_metrics import QueueDepthSampler
from hippius_s3.queue_metrics import build_queue_key_sets


def _config(upload=None, download=None, delete=None):
    return SimpleNamespace(
        upload_backends=upload or ["arion", "ovh"],
        download_backends=download or ["arion", "ovh"],
        delete_backends=delete or ["arion", "ovh"],
    )


class FakeRedis:
    def __init__(self, lists=None, zsets=None, fail=False):
        self.lists = lists or {}
        self.zsets = zsets or {}
        self.fail = fail

    async def llen(self, key):
        if self.fail:
            raise ConnectionError("redis down")
        return len(self.lists.get(key, []))

    async def zcard(self, key):
        return len(self.zsets.get(key, []))

    async def lindex(self, key, index):
        items = self.lists.get(key)
        if not items:
            return None
        return items[index]


def test_key_sets_cover_all_backends_and_kinds():
    lists, zsets = build_queue_key_sets(_config())

    assert "upload_requests" in lists  # pinner queue
    for backend in ("arion", "ovh"):
        for kind in ("upload", "download", "unpin"):
            assert f"{backend}_{kind}_requests" in lists
            assert f"{backend}_{kind}_retries" in zsets
        assert f"{backend}_upload_requests:dlq" in lists
    assert "unpin_requests:dlq" in lists


@pytest.mark.asyncio
async def test_sample_reports_depths_and_age():
    payload_old = json.dumps({"object_id": "x", "first_enqueued_at": 900.0})
    payload_new = json.dumps({"object_id": "y", "first_enqueued_at": 990.0})
    redis = FakeRedis(
        lists={
            # LPUSH-head order: index -1 is the oldest (next BRPOP out).
            "ovh_download_requests": [payload_new, payload_old],
            "arion_upload_requests:dlq": [payload_old],
        },
        zsets={"ovh_download_retries": ["a", "b", "c"]},
    )
    sampler = QueueDepthSampler(redis, _config(), register_metrics=False)

    await sampler.sample_once(now=1000.0)

    assert sampler.depths["ovh_download_requests"] == 2
    assert sampler.depths["arion_upload_requests:dlq"] == 1
    assert sampler.depths["ovh_download_retries"] == 3
    assert sampler.depths["arion_download_requests"] == 0
    assert sampler.oldest_age["ovh_download_requests"] == pytest.approx(100.0)
    # DLQs report depth only — no age probe.
    assert "arion_upload_requests:dlq" not in sampler.oldest_age


@pytest.mark.asyncio
async def test_payload_without_timestamp_reports_no_age():
    redis = FakeRedis(lists={"ovh_download_requests": [json.dumps({"object_id": "x"})]})
    sampler = QueueDepthSampler(redis, _config(), register_metrics=False)

    await sampler.sample_once(now=1000.0)

    assert sampler.depths["ovh_download_requests"] == 1
    assert "ovh_download_requests" not in sampler.oldest_age


@pytest.mark.asyncio
async def test_malformed_payload_reports_no_age():
    redis = FakeRedis(lists={"ovh_download_requests": [b"not-json"]})
    sampler = QueueDepthSampler(redis, _config(), register_metrics=False)

    await sampler.sample_once(now=1000.0)

    assert sampler.depths["ovh_download_requests"] == 1
    assert "ovh_download_requests" not in sampler.oldest_age


@pytest.mark.asyncio
async def test_redis_failure_keeps_previous_values():
    redis = FakeRedis(lists={"ovh_download_requests": [json.dumps({"first_enqueued_at": 1.0})]})
    sampler = QueueDepthSampler(redis, _config(), register_metrics=False)
    await sampler.sample_once(now=10.0)
    assert sampler.depths["ovh_download_requests"] == 1

    redis.fail = True
    with pytest.raises(ConnectionError):
        await sampler.sample_once(now=20.0)
    # run() swallows the raise and keeps the last-good values; sample_once
    # surfaces it so the loop's except path owns the policy.
    assert sampler.depths["ovh_download_requests"] == 1
