"""Uploader-side billing bypass for service accounts.

The exemption requires BOTH identities, which differ:

  - the VERIFIED CALLER, persisted by the api as `object_versions.billing_bypass` and read back
    here (it cannot be recomputed — since drain-direct the Rust drain-agent builds the
    UploadChainRequest, so nothing on the payload carries the caller);
  - the BUCKET OWNER, `payload.address`, who is who Arion actually charges.

Requiring only the owner would exempt a third party writing into a service account's bucket.
Requiring only the caller would bill a stranger's storage to nobody. Both halves, or billed.
"""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.workers.uploader import Uploader


SERVICE_ACCOUNT = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
REGULAR_ACCOUNT = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
BYPASS_KEY = "secret-bypass-key"


def _pool(*, caller_was_exempt: bool) -> Any:
    """Uploader pool whose two fetchvals answer is_object_deleted then billing_bypass, in order."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.fetchval = AsyncMock(side_effect=[False, caller_was_exempt])
    pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn)))
    return pool


@pytest.fixture  # type: ignore[misc]
def mock_db_pool() -> Any:
    """The ordinary case: the api recorded that the writer was a service account."""
    return _pool(caller_was_exempt=True)


def _config(*, allowlist: frozenset[str], bypass_key: str = BYPASS_KEY) -> Any:
    config = MagicMock()
    config.uploader_multipart_max_concurrency = 5
    config.arion_upload_concurrency = 5
    config.cache_ttl_seconds = 1800
    config.object_cache_dir = "/tmp/test_cache"
    config.arion_billing_bypass_key = bypass_key
    config.service_account_ids = allowlist
    return config


def _payload(address: str, *, bypass_billing: bool = False) -> UploadChainRequest:
    return UploadChainRequest(
        address=address,
        bucket_name="test-bucket",
        object_key="test-key",
        object_id="obj-123",
        object_version=1,
        chunks=[Chunk(id=1)],
        upload_id="upload-123",
        bypass_billing=bypass_billing,
    )


async def _headers_for(config: Any, payload: UploadChainRequest, db_pool: Any) -> Any:
    """Run process_upload and return the extra_headers handed to _upload_chunks."""
    uploader = Uploader(
        db_pool, FakeRedis(), FakeRedis(), config, backend_name="arion", backend_client=MagicMock()
    )
    with patch.object(uploader, "_upload_chunks", new_callable=AsyncMock) as upload_chunks:
        upload_chunks.return_value = ["QmCID1"]
        await uploader.process_upload(payload)
        return upload_chunks.call_args.kwargs["extra_headers"]


# ---------------------------------------------------------------------------
# The feature
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_account_upload_carries_the_bypass_header(mock_db_pool: Any) -> None:
    """No bypass_billing flag on the payload — the drain never sets one. Caller (from the DB)
    and owner (from the payload) are both the service account, which is the ordinary case."""
    headers = await _headers_for(
        _config(allowlist=frozenset({SERVICE_ACCOUNT})),
        _payload(SERVICE_ACCOUNT),
        mock_db_pool,
    )

    assert headers == {"X-Billing-Bypass": BYPASS_KEY}


@pytest.mark.asyncio
async def test_guest_write_into_a_service_account_bucket_still_succeeds_unmetered() -> None:
    """Shared buckets. A guest holding a WRITE grant on a service-account bucket produces an
    upload whose address is on the allowlist but whose writer is not.

    It must still be exempted. Arion charges `account_ss58` = the bucket OWNER whoever wrote the
    bytes, so refusing the exemption here would not shift a cent onto the guest — it would 402
    against a service account that carries no credit, classify "billing" (permanent), and strand
    the upload in the DLQ. Owner-pays means the owner pays; the owner is us.
    """
    headers = await _headers_for(
        _config(allowlist=frozenset({SERVICE_ACCOUNT})),
        _payload(SERVICE_ACCOUNT),  # bucket owner IS the service account
        _pool(caller_was_exempt=False),  # written by someone else
    )

    assert headers == {"X-Billing-Bypass": BYPASS_KEY}


@pytest.mark.asyncio
async def test_guest_write_is_flagged_for_alerting(caplog: Any, monkeypatch: Any) -> None:
    """Legitimate but worth seeing: only a WRITE grant on one of our buckets makes it possible,
    so it must never be silent. Labelled on the metric and warned in the log."""
    from hippius_s3.workers import uploader as uploader_mod

    collector = MagicMock()
    monkeypatch.setattr(uploader_mod, "get_metrics_collector", lambda: collector)

    with caplog.at_level("WARNING"):
        await _headers_for(
            _config(allowlist=frozenset({SERVICE_ACCOUNT})),
            _payload(SERVICE_ACCOUNT),
            _pool(caller_was_exempt=False),
        )

    collector.record_billing_bypass.assert_called_once_with(surface="uploader", writer="guest")
    assert any("guest write into a service-account bucket" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_our_own_ingest_is_not_flagged_as_a_guest_write(caplog: Any, monkeypatch: Any) -> None:
    """The ordinary case must not trip the alert, or the signal is worthless."""
    from hippius_s3.workers import uploader as uploader_mod

    collector = MagicMock()
    monkeypatch.setattr(uploader_mod, "get_metrics_collector", lambda: collector)

    with caplog.at_level("WARNING"):
        await _headers_for(
            _config(allowlist=frozenset({SERVICE_ACCOUNT})),
            _payload(SERVICE_ACCOUNT),
            _pool(caller_was_exempt=True),
        )

    collector.record_billing_bypass.assert_called_once_with(surface="uploader", writer="owner")
    assert not any("guest write" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_service_account_writing_into_someone_elses_bucket_is_billed() -> None:
    """The mirror case, and the one owner-pays settles the other way: our service account writes
    into a regular user's bucket. The storage attributes to — and is charged to — that user, so
    it must stay billed. The exemption follows the owner, and the owner here is not us."""
    headers = await _headers_for(
        _config(allowlist=frozenset({SERVICE_ACCOUNT})),
        _payload(REGULAR_ACCOUNT),  # bucket owner is a regular user
        _pool(caller_was_exempt=True),  # writer was the service account
    )

    assert headers is None


@pytest.mark.asyncio
async def test_regular_account_upload_is_still_billed(mock_db_pool: Any) -> None:
    headers = await _headers_for(
        _config(allowlist=frozenset({SERVICE_ACCOUNT})),
        _payload(REGULAR_ACCOUNT),
        mock_db_pool,
    )

    assert headers is None


@pytest.mark.asyncio
async def test_empty_allowlist_bills_everyone(mock_db_pool: Any) -> None:
    headers = await _headers_for(
        _config(allowlist=frozenset()),
        _payload(SERVICE_ACCOUNT),
        mock_db_pool,
    )

    assert headers is None


@pytest.mark.asyncio
async def test_operator_bypass_flag_still_works_for_an_unlisted_account() -> None:
    """dlq_requeue --bypass-billing is the recovery path for 402-failed uploads on ordinary
    accounts, set by a human. It is a standalone escape and must survive the two-sided check —
    neither identity here is a service account."""
    headers = await _headers_for(
        _config(allowlist=frozenset({SERVICE_ACCOUNT})),
        _payload(REGULAR_ACCOUNT, bypass_billing=True),
        _pool(caller_was_exempt=False),
    )

    assert headers == {"X-Billing-Bypass": BYPASS_KEY}


@pytest.mark.asyncio
async def test_case_flipped_address_is_still_billed(mock_db_pool: Any) -> None:
    near_miss = SERVICE_ACCOUNT[0] + SERVICE_ACCOUNT[1].swapcase() + SERVICE_ACCOUNT[2:]
    assert near_miss != SERVICE_ACCOUNT

    headers = await _headers_for(
        _config(allowlist=frozenset({SERVICE_ACCOUNT})),
        _payload(near_miss),
        mock_db_pool,
    )

    assert headers is None


# ---------------------------------------------------------------------------
# The misconfiguration that would otherwise be silent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_bypass_key_sends_no_header_and_warns(mock_db_pool: Any, caplog: Any) -> None:
    """Allowlisted but ARION_BILLING_BYPASS_KEY unset — the half-configured deployment. The
    upload must not invent a header, and must leave a log line: otherwise it bills the account,
    402s, and lands in the DLQ classified as 'billing' with nothing pointing at the missing key.
    """
    with caplog.at_level("WARNING"):
        headers = await _headers_for(
            _config(allowlist=frozenset({SERVICE_ACCOUNT}), bypass_key=""),
            _payload(SERVICE_ACCOUNT),
            mock_db_pool,
        )

    assert headers is None
    assert any(
        "BILLING_BYPASS requested but ARION_BILLING_BYPASS_KEY is unset" in r.message for r in caplog.records
    ), "a silently-billed service account is the failure this warning exists to prevent"


@pytest.mark.asyncio
async def test_no_warning_when_nothing_was_requested(mock_db_pool: Any, caplog: Any) -> None:
    """The warning must be specific to a requested-but-impossible bypass, not fire on every
    ordinary upload in a deployment that has no bypass key."""
    with caplog.at_level("WARNING"):
        await _headers_for(
            _config(allowlist=frozenset(), bypass_key=""),
            _payload(REGULAR_ACCOUNT),
            mock_db_pool,
        )

    assert not any("BILLING_BYPASS" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bypass_is_recorded_and_logged(mock_db_pool: Any, caplog: Any, monkeypatch: Any) -> None:
    from hippius_s3.workers import uploader as uploader_mod

    collector = MagicMock()
    monkeypatch.setattr(uploader_mod, "get_metrics_collector", lambda: collector)

    with caplog.at_level("INFO"):
        await _headers_for(
            _config(allowlist=frozenset({SERVICE_ACCOUNT})),
            _payload(SERVICE_ACCOUNT),
            mock_db_pool,
        )

    collector.record_billing_bypass.assert_called_once_with(surface="uploader", writer="owner")
    assert any("BILLING_BYPASS surface=uploader" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_no_bypass_metric_for_a_billed_upload(mock_db_pool: Any, monkeypatch: Any) -> None:
    from hippius_s3.workers import uploader as uploader_mod

    collector = MagicMock()
    monkeypatch.setattr(uploader_mod, "get_metrics_collector", lambda: collector)

    await _headers_for(
        _config(allowlist=frozenset({SERVICE_ACCOUNT})),
        _payload(REGULAR_ACCOUNT),
        mock_db_pool,
    )

    collector.record_billing_bypass.assert_not_called()


@pytest.mark.asyncio
async def test_bypass_key_never_reaches_the_logs(mock_db_pool: Any, caplog: Any) -> None:
    """The header value is a shared secret with Arion. It is fine in a request header and must
    never be in a log line an operator or Loki can read."""
    with caplog.at_level("DEBUG"):
        await _headers_for(
            _config(allowlist=frozenset({SERVICE_ACCOUNT})),
            _payload(SERVICE_ACCOUNT),
            mock_db_pool,
        )

    assert not any(BYPASS_KEY in r.getMessage() for r in caplog.records)
