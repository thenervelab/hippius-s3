"""Tests for the backfill-unpins --backends override and the counters around it.

The override exists to reach a backend OUTSIDE config.delete_backends (prod: rows live
on ovh while delete_backends is ['arion']). The generic enqueue path intersects the
request with that allowlist, so it can never widen routing — the script must enqueue
directly to each named queue. These tests pin that behavior against the exact prod
config the flag was written for, plus the three things that decide whether an operator
can trust a run: flag parsing, one retry identity per unpin across the fan-out queues,
and a dry-run whose counts equal what a real run would do.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import initialize_queue_client
from hippius_s3.scripts.backfill_soft_delete_unpins import _enqueue_unpin
from hippius_s3.scripts.backfill_soft_delete_unpins import _parse_csv_flag
from hippius_s3.scripts.backfill_soft_delete_unpins import _process_object
from hippius_s3.scripts.backfill_soft_delete_unpins import _RunPlan
from hippius_s3.scripts.backfill_soft_delete_unpins import _warn_unthrottled_backends


def _payload(backends: list[str] | None) -> UnpinChainRequest:
    return UnpinChainRequest(
        address="user1",
        object_id="obj-1",
        object_version=1,
        delete_backends=backends,
    )


def _config(delete: list[str]) -> AsyncMock:
    cfg = AsyncMock()
    cfg.delete_backends = delete
    return cfg


def _row(
    oid: str,
    *,
    needs_unpin: bool = True,
    has_deprecated: bool = False,
    main_account_id: str | None = "user1",
) -> dict[str, Any]:
    return {
        "object_id": oid,
        "object_version": 1,
        "main_account_id": main_account_id,
        "needs_unpin": needs_unpin,
        "has_deprecated": has_deprecated,
    }


# The fixture deliberately mixes every shape the loop can meet: a plain unpin, an unpin
# that also needs the deprecated-backend reconcile, a reconcile-only row, and a row the
# handler must skip because its bucket has no main_account_id.
_ROWS = [
    _row("obj-1"),
    _row("obj-2", has_deprecated=True),
    _row("obj-3", needs_unpin=False, has_deprecated=True),
    _row("obj-4", main_account_id=None),
]


class TestBackendsOverrideRouting:
    @pytest.mark.asyncio
    async def test_off_allowlist_override_reaches_its_queue(self) -> None:
        # Prod scenario: delete_backends=['arion'], operator passes --backends ovh.
        # The allowlist intersection would be empty; direct routing must still land it.
        redis = FakeRedis()
        initialize_queue_client(redis)
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            await _enqueue_unpin(_payload(["ovh"]), ["ovh"])
        assert await redis.llen("ovh_unpin_requests") == 1
        assert await redis.llen("arion_unpin_requests") == 0

    @pytest.mark.asyncio
    async def test_multi_backend_override_fans_to_every_named_queue(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            await _enqueue_unpin(_payload(["arion", "ovh"]), ["arion", "ovh"])
        assert await redis.llen("arion_unpin_requests") == 1
        assert await redis.llen("ovh_unpin_requests") == 1

    @pytest.mark.asyncio
    async def test_no_override_falls_back_to_config_fan_out(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            await _enqueue_unpin(_payload(None), [])
        assert await redis.llen("arion_unpin_requests") == 1
        assert await redis.llen("ovh_unpin_requests") == 0

    @pytest.mark.asyncio
    async def test_fan_out_queues_get_byte_identical_payloads(self) -> None:
        # One object's unpin must be ONE retry identity across every queue it lands on:
        # the DLQ and the retry accounting key off request_id, and first_enqueued_at is
        # the age that decides give-up. If the second queue got a fresh pair, the same
        # unpin would be two unrelated units of work with two different clocks.
        redis = FakeRedis()
        initialize_queue_client(redis)
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            await _enqueue_unpin(_payload(["arion", "ovh"]), ["arion", "ovh"])

        arion = json.loads((await redis.lrange("arion_unpin_requests", 0, -1))[0])
        ovh = json.loads((await redis.lrange("ovh_unpin_requests", 0, -1))[0])
        assert arion["request_id"] is not None
        assert arion["request_id"] == ovh["request_id"]
        assert arion["first_enqueued_at"] is not None
        assert arion["first_enqueued_at"] == ovh["first_enqueued_at"]
        assert arion == ovh


class TestParseCsvFlag:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (" ovh , ", ["ovh"]),
            ("ovh,,arion", ["ovh", "arion"]),
            ("", []),
            ("ovh,ovh", ["ovh"]),
            (",", []),
            ("arion, ovh ,arion", ["arion", "ovh"]),
        ],
    )
    def test_trims_drops_blanks_and_dedupes_in_order(self, raw: str, expected: list[str]) -> None:
        assert _parse_csv_flag(raw) == expected

    @pytest.mark.asyncio
    async def test_duplicate_backend_does_not_double_enqueue(self) -> None:
        # The reason dedupe lives in the parser: --backends ovh,ovh would otherwise push
        # the same payload twice to one queue and count the object twice against the cap.
        redis = FakeRedis()
        initialize_queue_client(redis)
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            await _enqueue_unpin(_payload(["ovh"]), _parse_csv_flag("ovh,ovh"))
        assert await redis.llen("ovh_unpin_requests") == 1


class TestDryRunParity:
    @pytest.mark.asyncio
    async def test_dry_run_counts_match_real_run_enqueues(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        db = AsyncMock()

        dry_enqueued = dry_reconciled = 0
        for row in _ROWS:
            e, d = await _process_object(row, _RunPlan(backends=["ovh"], deprecated=["ipfs"], dry_run=True), db)
            dry_enqueued += e
            dry_reconciled += d

        # Dry-run must be inert: no queue writes, no reconcile UPDATE.
        assert await redis.llen("ovh_unpin_requests") == 0
        db.execute.assert_not_called()

        real_enqueued = real_reconciled = 0
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            for row in _ROWS:
                e, d = await _process_object(row, _RunPlan(backends=["ovh"], deprecated=["ipfs"], dry_run=False), db)
                real_enqueued += e
                real_reconciled += d

        assert dry_enqueued == real_enqueued == await redis.llen("ovh_unpin_requests")
        assert dry_reconciled == real_reconciled == db.execute.await_count
        # The fixture must actually exercise both counters, or the equality is vacuous.
        assert (dry_enqueued, dry_reconciled) == (2, 2)

    @pytest.mark.asyncio
    async def test_enqueued_counts_objects_not_queue_pushes(self) -> None:
        # With two backends one object is two LPUSHes; the counter stays per-object, so
        # the summary line is not a queue-depth estimate.
        redis = FakeRedis()
        initialize_queue_client(redis)
        plan = _RunPlan(backends=["arion", "ovh"], deprecated=["ipfs"], dry_run=False)
        with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
            enqueued, _ = await _process_object(_row("obj-1"), plan, AsyncMock())
        assert enqueued == 1
        assert await redis.llen("arion_unpin_requests") == 1
        assert await redis.llen("ovh_unpin_requests") == 1

    @pytest.mark.asyncio
    async def test_row_without_main_account_is_skipped_in_both_modes(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        row = _row("obj-4", main_account_id=None)
        for dry in (True, False):
            plan = _RunPlan(backends=["ovh"], deprecated=["ipfs"], dry_run=dry)
            with patch("hippius_s3.queue.get_config", return_value=_config(["arion"])):
                enqueued, _ = await _process_object(row, plan, AsyncMock())
            assert enqueued == 0
        assert await redis.llen("ovh_unpin_requests") == 0


class TestThrottleMismatchWarning:
    def test_warns_when_a_routed_queue_is_unthrottled(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="backfill_unpins"):
            _warn_unthrottled_backends(["ovh"], ["arion_unpin_requests"])
        assert "ovh_unpin_requests" in caplog.text
        assert [r.levelno for r in caplog.records] == [logging.WARNING]

    def test_warns_naming_only_the_unthrottled_queue(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="backfill_unpins"):
            _warn_unthrottled_backends(["arion", "ovh"], ["arion_unpin_requests"])
        assert caplog.records[0].args == (["ovh_unpin_requests"], ["arion_unpin_requests"])

    def test_silent_when_every_routed_queue_is_throttled(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="backfill_unpins"):
            _warn_unthrottled_backends(["arion", "ovh"], ["arion_unpin_requests", "ovh_unpin_requests"])
        assert caplog.records == []

    def test_silent_without_a_backends_override(self, caplog: pytest.LogCaptureFixture) -> None:
        # No override means config.delete_backends routing, which this warning says
        # nothing about — an empty --throttle-queues there is a separate operator choice.
        with caplog.at_level(logging.WARNING, logger="backfill_unpins"):
            _warn_unthrottled_backends([], [])
        assert caplog.records == []

    def test_warns_when_throttling_is_disabled_entirely(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="backfill_unpins"):
            _warn_unthrottled_backends(["ovh"], [])
        assert "ovh_unpin_requests" in caplog.text
