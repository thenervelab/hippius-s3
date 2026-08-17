"""Live-schema checks for the purge_jobs SQL (issue #421).

The unit tests exercise the purger against fakes; these pin the parts that only real
Postgres can validate:
  - insert_purge_job's `ON CONFLICT (account_id) WHERE state IN ('queued','running')`
    must infer the partial unique index uq_purge_jobs_account_active. If the predicate
    ever fails to match the index, the INSERT raises at runtime and every purge 500s.
  - claim_purge_job's queued/expired-lease selection + counter continuity.

Seed data is written via `pg_tx` (auto-rolled-back). Skips if no Postgres on DATABASE_URL.
"""

import uuid

import asyncpg
import pytest

from hippius_s3.utils import get_query


ACCOUNT = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"


async def _skip_if_claimable_rows_exist(conn: asyncpg.Connection, lease_seconds: float) -> None:
    """claim_purge_job is global (picks the oldest claimable job of ANY account), so these
    tests are only deterministic when no other claimable row exists. Skip rather than flake
    if the DB already holds one."""
    n = await conn.fetchval(
        "SELECT count(*) FROM purge_jobs "
        "WHERE state = 'queued' OR (state = 'running' AND heartbeat_at < now() - make_interval(secs => $1))",
        lease_seconds,
    )
    if n:
        pytest.skip(f"{n} pre-existing claimable purge_jobs row(s); claim test needs a clean table")


@pytest.mark.asyncio
async def test_second_active_purge_returns_no_row_via_partial_index(pg_tx: asyncpg.Connection) -> None:
    first = uuid.uuid4()
    second = uuid.uuid4()

    inserted = await pg_tx.fetchrow(get_query("insert_purge_job"), first, ACCOUNT)
    assert inserted is not None and inserted["job_id"] == first

    # Same account, existing queued job -> partial unique index conflict -> DO NOTHING.
    conflict = await pg_tx.fetchrow(get_query("insert_purge_job"), second, ACCOUNT)
    assert conflict is None

    active = await pg_tx.fetchrow(get_query("get_active_purge_job"), ACCOUNT)
    assert active["job_id"] == first


@pytest.mark.asyncio
async def test_new_purge_allowed_after_previous_done(pg_tx: asyncpg.Connection) -> None:
    done = uuid.uuid4()
    fresh = uuid.uuid4()

    await pg_tx.fetchrow(get_query("insert_purge_job"), done, ACCOUNT)
    # A 'done' row is outside the partial index predicate, so a new insert must NOT conflict.
    await pg_tx.execute("UPDATE purge_jobs SET state = 'done', finished_at = now() WHERE job_id = $1", done)

    inserted = await pg_tx.fetchrow(get_query("insert_purge_job"), fresh, ACCOUNT)
    assert inserted is not None and inserted["job_id"] == fresh


@pytest.mark.asyncio
async def test_claim_picks_queued_and_marks_running(pg_tx: asyncpg.Connection) -> None:
    await _skip_if_claimable_rows_exist(pg_tx, 600.0)
    job_id = uuid.uuid4()
    await pg_tx.fetchrow(get_query("insert_purge_job"), job_id, ACCOUNT)

    claimed = await pg_tx.fetchrow(get_query("claim_purge_job"), 600.0)
    assert claimed is not None
    assert claimed["account_id"] == ACCOUNT
    assert claimed["deleted_objects"] == 0

    row = await pg_tx.fetchrow("SELECT state, started_at, heartbeat_at FROM purge_jobs WHERE job_id = $1", job_id)
    assert row["state"] == "running"
    assert row["started_at"] is not None
    assert row["heartbeat_at"] is not None


@pytest.mark.asyncio
async def test_claim_reclaims_expired_running_lease(pg_tx: asyncpg.Connection) -> None:
    await _skip_if_claimable_rows_exist(pg_tx, 600.0)
    job_id = uuid.uuid4()
    await pg_tx.fetchrow(get_query("insert_purge_job"), job_id, ACCOUNT)
    # Simulate a crashed worker: running, but heartbeat is well past the lease.
    await pg_tx.execute(
        "UPDATE purge_jobs SET state = 'running', deleted_objects = 5, "
        "heartbeat_at = now() - interval '1 hour' WHERE job_id = $1",
        job_id,
    )

    claimed = await pg_tx.fetchrow(get_query("claim_purge_job"), 600.0)
    assert claimed is not None and claimed["job_id"] == job_id
    # Counters resume from the persisted value, not reset to zero.
    assert claimed["deleted_objects"] == 5


@pytest.mark.asyncio
async def test_claim_ignores_fresh_running_lease(pg_tx: asyncpg.Connection) -> None:
    await _skip_if_claimable_rows_exist(pg_tx, 600.0)
    job_id = uuid.uuid4()
    await pg_tx.fetchrow(get_query("insert_purge_job"), job_id, ACCOUNT)
    await pg_tx.execute(
        "UPDATE purge_jobs SET state = 'running', heartbeat_at = now() WHERE job_id = $1",
        job_id,
    )

    claimed = await pg_tx.fetchrow(get_query("claim_purge_job"), 600.0)
    assert claimed is None
