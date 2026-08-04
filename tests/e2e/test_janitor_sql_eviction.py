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


def _run_one_eviction_pass(pressure: int = 2) -> str:
    """Exec a single evict_from_inventory pass in the janitor container and return its stdout.

    pressure=2 makes a fresh part eligible: ignore_age=True (age gate off) and hot_window=0 (hot
    retention disabled), so only the absolute replication gate decides — exactly what we want to
    prove is being enforced. pressure=0 (NORMAL) keeps the age gate and hot retention ACTIVE, so the
    recency signals (write atime + last_access_at) actually decide — used by the read-recency tests.
    """
    snippet = textwrap.dedent(
        f"""
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
                n = await evict_from_inventory(pool, fs, r, pressure={pressure})
                print(f"EVICTED={{n}}")
            finally:
                await r.close()
                await pool.close()

        asyncio.run(main())
        """
    )
    rc, out, err = compose_exec("janitor", ["python", "-c", snippet])
    assert rc == 0, f"eviction exec failed rc={rc}\nstdout={out}\nstderr={err}"
    return out


def _backdate_uploaded_at(object_id: str, object_version: int, seconds_ago: int, *, dsn: str = DEFAULT_DSN) -> None:
    """Age a part past the janitor's max-age gate so, at NORMAL pressure (ignore_age=False), it is a
    legitimate candidate and the recency signals — not the age gate — decide whether it is evicted."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE parts SET uploaded_at = now() - make_interval(secs => %s) "
            "WHERE object_id = %s AND object_version = %s",
            (seconds_ago, object_id, object_version),
        )
        conn.commit()


def _set_last_access(
    object_id: str, object_version: int, part_number: int, seconds_ago: float, *, dsn: str = DEFAULT_DSN
) -> None:
    """Set fs_cache_inventory.last_access_at relative to now (the read-recency signal the candidate
    query now carries inline). seconds_ago small = fresh (protects); large = stale (does not pin)."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE fs_cache_inventory SET last_access_at = now() - make_interval(secs => %s) "
            "WHERE object_id = %s AND object_version = %s AND part_number = %s",
            (seconds_ago, object_id, object_version, part_number),
        )
        conn.commit()


def _backdate_part_atime(object_id: str, object_version: int, part_number: int, seconds_ago: int) -> None:
    """Make the part's FS write-recency (atime of its meta.json / dir, what stat_part reads) COLD so
    the write-recency short-circuit does not pin a freshly-PUT part — isolating last_access_at as the
    deciding recency signal at NORMAL pressure."""
    part_dir = _part_dir(object_id, object_version, part_number)
    snippet = textwrap.dedent(
        f"""
        import os, time
        old = time.time() - {seconds_ago}
        d = {part_dir!r}
        for name in ("meta.json", ""):
            p = os.path.join(d, name) if name else d
            if os.path.exists(p):
                os.utime(p, (old, old))
        print("BACKDATED")
        """
    )
    rc, out, err = compose_exec("janitor", ["python", "-c", snippet])
    assert rc == 0 and "BACKDATED" in out, f"atime backdate failed rc={rc}\nstdout={out}\nstderr={err}"


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


def _prepare_aged_candidate(boto3_client: Any, bucket: str, key: str, content: bytes) -> tuple[str, int, int]:
    """PUT + wait for replication + seed inventory + age it into a NORMAL-pressure candidate whose
    ONLY remaining recency signal is last_access_at. Returns (object_id, object_version, part_number).

    At pressure=0 both gates the pressure=2 test disables are ACTIVE, so a freshly-PUT part is held
    for two reasons unrelated to read-recency: the age gate (uploaded_at younger than max_age) and
    write-recency (its meta.json atime is ~now). We backdate BOTH past their windows so the part is a
    legitimate candidate whose eviction now turns solely on last_access_at.
    """
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)
    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=30.0)
    object_id, object_version = get_object_id_and_version(bucket, key)
    parts = _seed_inventory(object_id, object_version)
    assert parts, "object should have at least one part"
    part_number = parts[0]

    present_rc, _, _ = compose_exec("janitor", ["test", "-d", _part_dir(object_id, object_version, part_number)])
    assert present_rc == 0, "part must be on the FS cache before eviction"

    _backdate_uploaded_at(object_id, object_version, seconds_ago=10 * 86400)  # past the 1-day age gate
    _backdate_part_atime(object_id, object_version, part_number, seconds_ago=10 * 86400)  # cold write-recency
    return object_id, object_version, part_number


@pytest.mark.local
def test_fresh_last_access_keeps_part_at_normal_pressure(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    # NORMAL pressure (recency ACTIVE): a fully-replicated, aged, write-cold part with a FRESH
    # last_access_at must be KEPT — read-recency hot retention protects a recently-read part from
    # eviction even though every other gate would let it go.
    bucket = unique_bucket_name("janitor-recency-keep")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "recency-keep.bin"
    content = b"janitor read-recency keep payload"
    object_id, object_version, part_number = _prepare_aged_candidate(boto3_client, bucket, key, content)

    _set_last_access(object_id, object_version, part_number, seconds_ago=5)  # read just now → hot

    _run_one_eviction_pass(pressure=0)

    # KEPT: the FS-cache entry survives because last_access_at is inside the hot window.
    present_rc, _, _ = compose_exec("janitor", ["test", "-d", _part_dir(object_id, object_version, part_number)])
    assert present_rc == 0, "freshly-read part must NOT be evicted at normal pressure"

    resp = boto3_client.get_object(Bucket=bucket, Key=key)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Body"].read() == content


@pytest.mark.local
def test_stale_last_access_evicts_part_at_normal_pressure(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    # Companion to the keep test: the SAME aged, write-cold candidate but with a STALE last_access_at
    # (read long ago) IS evicted — a stale recency value must not pin a part forever. Proves recency
    # is a genuine gate, not an unconditional keep, and that the read-recency path lets cold parts go.
    bucket = unique_bucket_name("janitor-recency-evict")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "recency-evict.bin"
    content = b"janitor read-recency stale-evict payload"
    object_id, object_version, part_number = _prepare_aged_candidate(boto3_client, bucket, key, content)

    _set_last_access(object_id, object_version, part_number, seconds_ago=10 * 86400)  # read long ago → cold

    _run_one_eviction_pass(pressure=0)

    # EVICTED: stale read-recency does not pin it; the replication gate cleared it for deletion.
    gone_rc, _, _ = compose_exec("janitor", ["test", "-e", _part_dir(object_id, object_version, part_number)])
    assert gone_rc != 0, "stale-read part must be evicted at normal pressure"

    resp = boto3_client.get_object(Bucket=bucket, Key=key)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Body"].read() == content
