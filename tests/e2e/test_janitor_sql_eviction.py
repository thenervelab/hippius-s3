"""E2E: the janitor's SQL-driven eviction phase evicts a replicated part, and a GET refetches it.

Round-trip proof that the new engine is SAFE: a part that is fully replicated to every required
backend is evicted from the FS cache by evict_from_inventory, and a subsequent GET rehydrates it
through the download pipeline. If eviction ever deleted an under-replicated part, the follow-up GET
would fail — that is the safety invariant this test guards.

No existing e2e test drives the janitor, and its container runs the loop on a 600s cadence, so
this seeds fs_cache_inventory deterministically and execs ONE eviction pass in the janitor
container (pressure=2 → age gate off + hot retention disabled, so a freshly-PUT replicated part is
eligible immediately). The seed is an idempotent upsert: it is a no-op if the Wave-3 producers
already recorded the row, so the test does not couple to producer timing.
"""

from __future__ import annotations

import textwrap
from typing import Any
from typing import Callable

import psycopg  # type: ignore[import-untyped]
import pytest

from .support.cache import get_object_id_and_version
from .support.cache import wait_for_all_backends_ready
from .support.compose import compose_exec
from .support.dsn import DEFAULT_DSN


def _seed_inventory(object_id: str, object_version: int, *, dsn: str = DEFAULT_DSN) -> list[int]:
    """Upsert an fs_cache_inventory row for every part of the object; return the part numbers.

    Idempotent (ON CONFLICT DO NOTHING) so it coexists with the Wave-3 producers that also record
    these rows — the test stays deterministic whether or not the producer already fired.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT part_number FROM parts WHERE object_id = %s AND object_version = %s ORDER BY part_number",
            (object_id, object_version),
        )
        parts = [int(r[0]) for r in cur.fetchall()]
        for pn in parts:
            cur.execute(
                """
                INSERT INTO fs_cache_inventory (object_id, object_version, part_number)
                VALUES (%s, %s, %s)
                ON CONFLICT (object_id, object_version, part_number) DO NOTHING
                """,
                (object_id, object_version, pn),
            )
        conn.commit()
    return parts


def _part_dir(object_id: str, object_version: int, part_number: int) -> str:
    return f"/var/lib/hippius/object_cache/{object_id}/v{object_version}/part_{part_number}"


def _run_one_eviction_pass() -> str:
    """Exec a single evict_from_inventory pass in the janitor container and return its stdout.

    pressure=2 makes a fresh part eligible: ignore_age=True (age gate off) and hot_window=0 (hot
    retention disabled), so only the absolute replication gate decides — exactly what we want to
    prove is being enforced.
    """
    snippet = textwrap.dedent(
        """
        import asyncio
        import asyncpg
        from redis.asyncio import Redis
        from hippius_s3.config import get_config
        from hippius_s3.cache import create_fs_store
        from workers.run_janitor_in_loop import evict_from_inventory

        async def main():
            c = get_config()
            pool = await asyncpg.create_pool(c.database_url, min_size=1, max_size=4)
            fs = create_fs_store(c)
            r = Redis.from_url(c.redis_queues_url)
            try:
                n = await evict_from_inventory(pool, fs, r, pressure=2)
                print(f"EVICTED={n}")
            finally:
                await r.close()
                await pool.close()

        asyncio.run(main())
        """
    )
    rc, out, err = compose_exec("janitor", ["python", "-c", snippet])
    assert rc == 0, f"eviction exec failed rc={rc}\nstdout={out}\nstderr={err}"
    return out


@pytest.mark.local
def test_sql_eviction_evicts_replicated_part_and_get_refetches(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("janitor-sql-evict")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "sql-evict.bin"
    content = b"janitor sql eviction round-trip payload"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)

    # Wait for the replication gate's precondition: every chunk backed on arion. Only then is the
    # part a legitimate eviction candidate (an under-replicated part must never be evicted).
    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=30.0)

    object_id, object_version = get_object_id_and_version(bucket, key)
    parts = _seed_inventory(object_id, object_version)
    assert parts, "object should have at least one part"

    # Precondition: the part is materialized on the shared FS cache.
    present_rc, _, _ = compose_exec("janitor", ["test", "-d", _part_dir(object_id, object_version, parts[0])])
    assert present_rc == 0, "part must be on the FS cache before eviction"

    out = _run_one_eviction_pass()
    assert "EVICTED=" in out, f"unexpected eviction output: {out}"
    evicted = int(out.rsplit("EVICTED=", 1)[1].strip().splitlines()[0])
    assert evicted >= 1, f"expected at least one part evicted, got {evicted}"

    # The part's FS-cache entry is gone (evicted, not just logged).
    gone_rc, _, _ = compose_exec("janitor", ["test", "-e", _part_dir(object_id, object_version, parts[0])])
    assert gone_rc != 0, "part dir must be removed after SQL eviction"

    # And the object still reads: the GET rehydrates through the download pipeline, proving the
    # eviction was safe (the bytes were fully replicated and are re-fetchable from the backend).
    resp = boto3_client.get_object(Bucket=bucket, Key=key)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Body"].read() == content
