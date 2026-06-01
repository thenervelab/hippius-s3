"""Arion uploader error-handling unit tests.

Covers the pure error helpers (classify_error / extract_http_status_code / compute_backoff_ms)
and the permanent-error path through the upload DLQ. Billing-specific behaviour lives in
test_uploader_billing.py.
"""

from types import SimpleNamespace
from typing import Any

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.dlq.upload_dlq import UploadDLQManager
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.workers.uploader import classify_error
from hippius_s3.workers.uploader import compute_backoff_ms
from hippius_s3.workers.uploader import extract_http_status_code


class _HTTPError(Exception):
    """Mimics httpx.HTTPStatusError carrying a response.status_code."""

    def __init__(self, status_code: int, message: str = "") -> None:
        super().__init__(message or f"HTTP {status_code}")
        self.response = SimpleNamespace(status_code=status_code)


class HippiusAPIError(Exception):
    """Name alone (type-based classification) should mark it permanent."""


def _req(object_id: str = "obj-err", upload_id: str = "up-err") -> UploadChainRequest:
    return UploadChainRequest(
        address="5Fake",
        bucket_name="bkt",
        object_key="k/o.bin",
        object_id=object_id,
        object_version=1,
        chunks=[Chunk(id=0)],
        upload_id=upload_id,
    )


# --------------------------- extract_http_status_code ---------------------------


def test_extract_status_from_http_error() -> None:
    assert extract_http_status_code(_HTTPError(507)) == "507"


def test_extract_status_empty_for_plain_exception() -> None:
    assert extract_http_status_code(Exception("boom")) == ""


def test_extract_status_empty_when_response_lacks_status() -> None:
    err = Exception("x")
    err.response = SimpleNamespace()  # type: ignore[attr-defined]
    assert extract_http_status_code(err) == ""


# --------------------------- classify_error ---------------------------


@pytest.mark.parametrize(
    "error, expected",
    [
        # status-code driven
        (_HTTPError(507), "permanent"),  # upstream disk full
        (_HTTPError(402), "billing"),  # out of credit
        # permanent keywords
        (Exception("Malformed request body"), "permanent"),
        (Exception("invalid chunk index"), "permanent"),
        (Exception("negative size reported"), "permanent"),
        (Exception("validation error: bad field"), "permanent"),
        (Exception("data integrity check failed"), "permanent"),
        (Exception("insufficient storage on device"), "permanent"),
        (Exception("pin operation failed"), "permanent"),
        (Exception("unpin was rejected"), "permanent"),
        (Exception("Hippius API rejected publish"), "permanent"),
        (HippiusAPIError("publish failed"), "permanent"),  # classified by type name
        # transient keywords
        (Exception("connection refused"), "transient"),
        (Exception("read timeout exceeded"), "transient"),
        (Exception("network unreachable"), "transient"),
        (Exception("503 Service Unavailable"), "transient"),
        (Exception("502 Bad Gateway"), "transient"),
        (Exception("504 Gateway Timeout"), "transient"),
        (Exception("429 Too Many Requests"), "transient"),
        (Exception("rate limit hit"), "transient"),
        (Exception("request was throttled"), "transient"),
        (Exception("part_meta_not_ready"), "transient"),
        (Exception("part_row_missing"), "transient"),
        # transient by exception type
        (ConnectionError("dropped"), "transient"),
        (TimeoutError("deadline"), "transient"),
        # nothing matches
        (Exception("something entirely unexpected"), "unknown"),
    ],
)
def test_classify_error(error: Exception, expected: str) -> None:
    assert classify_error(error) == expected


def test_disk_full_507_wins_over_billing_check() -> None:
    # 507 is classified before the billing check, so it stays permanent.
    assert classify_error(_HTTPError(507, "insufficient storage")) == "permanent"


# --------------------------- compute_backoff_ms ---------------------------


@pytest.mark.parametrize("attempt, exp_base", [(1, 1000), (2, 2000), (3, 4000), (4, 8000)])
def test_backoff_within_jitter_bounds(attempt: int, exp_base: int) -> None:
    # jitter is uniform in [0, 10%], so the result sits in [exp, exp*1.1]; max is huge here.
    for _ in range(50):
        v = compute_backoff_ms(attempt, base_ms=1000, max_ms=10**9)
        assert exp_base <= v <= exp_base * 1.1


def test_backoff_capped_at_max() -> None:
    for _ in range(50):
        v = compute_backoff_ms(20, base_ms=1000, max_ms=30000)
        assert v == 30000.0


def test_backoff_grows_per_attempt() -> None:
    # Even with jitter the windows don't overlap: attempt 1 caps at 1100 < attempt 2 floor of 2000.
    assert compute_backoff_ms(1, base_ms=1000, max_ms=10**9) <= 1100
    assert compute_backoff_ms(2, base_ms=1000, max_ms=10**9) >= 2000


# --------------------------- permanent-error DLQ routing ---------------------------


@pytest.mark.asyncio
async def test_permanent_error_dead_lettered_with_type() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    await mgr.push(_req(object_id="perm-1"), last_error="507 insufficient storage", error_type="permanent")

    entries = await mgr.peek()
    assert len(entries) == 1
    entry = entries[0]
    assert entry["error_type"] == "permanent"
    assert entry["object_id"] == "perm-1"
    assert entry["upload_id"] == "up-err"
    assert entry["last_error"] == "507 insufficient storage"
    assert (await mgr.stats())["error_types"]["permanent"] == 1


@pytest.mark.asyncio
async def test_requeue_refuses_permanent_without_force() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    calls: list[Any] = []
    mgr.enqueue_func = lambda p: calls.append(p)  # must not be invoked  # type: ignore[assignment]
    await mgr.push(_req(object_id="perm-2"), last_error="malformed body", error_type="permanent")

    assert await mgr.requeue("perm-2") is False
    assert calls == [], "permanent errors must not be auto-requeued"
    assert (await mgr.stats())["total_entries"] == 1, "entry must be put back on refusal"


@pytest.mark.asyncio
async def test_requeue_permanent_with_force_reenqueues() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    captured: list[UploadChainRequest] = []

    async def _capture(payload: UploadChainRequest) -> None:
        captured.append(payload)

    mgr.enqueue_func = _capture  # type: ignore[assignment]
    await mgr.push(_req(object_id="perm-3"), last_error="malformed body", error_type="permanent")

    assert await mgr.requeue("perm-3", force=True) is True
    assert [p.object_id for p in captured] == ["perm-3"]
    assert (await mgr.stats())["total_entries"] == 0
