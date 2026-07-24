# Janitor SQL-Driven Eviction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the CephFS tree walk as the janitor's eviction-discovery engine with a SQL-driven, cursored candidate pipeline, demoting the walk to a low-cadence consistency sweep — without weakening any deletion-safety invariant.

**Architecture:** A new narrow `fs_cache_inventory` table (one row per part materialized on the cache FS, written by the upload/download pipelines, deleted on eviction) becomes the queryable cache index. A set-based candidate query — the `find_underreplicated_live_chunks` CTE with its coverage predicate flipped — joins inventory ⋈ parts ⋈ chunk_backend to stream fully-replicated, aged candidates via a keyset cursor persisted in a `janitor_state` table. The janitor's new discovery phase stats *only candidates* (atime hot-check + existence), re-runs the authoritative per-part replication gate, deletes, and clears the inventory row. The existing unified walk survives at reduced cadence as the reconciler: it backfills inventory rows for parts it finds on disk (which is also the initial backfill) and remains the only discoverer of no-DB-row orphans.

**Tech Stack:** Python 3.10+/asyncio, asyncpg, PostgreSQL, pytest. No new dependencies.

**Base branch:** `staging` — MUST `git fetch origin && git rebase origin/staging` (or branch from `origin/staging`) first. The local checkout predates the unified walk (PR #340) and pool gate (PR #338); this plan is written against the deployed code (`workers:237ed61`), where the FS phases are already collapsed into `cleanup_parts_unified` and pressure keys on max(statvfs, Ceph pool %USED).

---

## Why (measured on prod, 2026-07-24)

- Walk rate ~36 objects/s despite `walk_concurrency=8` — the flat cache root (~15.6M child dirs) serializes readdir; `_stream_shard_object_names` reads **every** name and filters by crc32 *after*, so even 64-shard cycles pay the full readdir.
- Budget truncation (480s) restarts from the readdir head next cycle (no cursor): non-deletable survivors are re-scanned every cycle, the directory tail is starved. Under pressure `shards=1` makes this permanent.
- Sustained eviction ≈ 19 parts/s (~11.4k deletes / ~600s cycle) while the pod idles at 48m CPU / 84Mi — it is MDS-latency bound, not CPU/DB bound (per-part replication query ≈ 0.8ms).

## Safety invariants (MUST NOT change — verify against each task)

1. **Replication is an absolute gate.** `is_replicated_on_all_backends` (per-version `upload_backends` ∪ `backup_backends`, `expected_chunks` guard) is re-run per part on the worker connection immediately before every delete. The SQL candidate query is a *prefilter only*; it never replaces the gate. (Team memory `mem_01KX0S2J2F239HVNKS54GDPJ3E`: the gate applies to ALL cleanup phases.)
2. **DLQ semantics:** stale-reap fails CLOSED when the DLQ set is unavailable; age-GC falls back to replication-gate-only (fail-open). The new discovery phase is an age-GC-class deleter → same fail-open rule, and it honours the DLQ set when available.
3. **Hot retention** (pressure-adjusted atime window) still protects candidates; atime is read from the FS at decision time.
4. **Census correctness:** gauges publish only from complete data. The walk census remains authoritative until Wave 5 adds the SQL census.
5. **Known populations that must remain non-evictable and must not break the pipeline:** CopyObject destinations (zero `chunk_backend` rows — never candidates, by construction of the coverage predicate; `mem_01KXQV6KH945F009SX78VBM5FW`) and DLQ-parked / non-replicated parts. They simply never match the candidate query.
6. **Failure direction:** every new failure mode must degrade to "part not evicted this cycle", never to "part deleted unsafely". Inventory is *advisory* — a missing row delays eviction (walk backfills later); a stale row costs one stat.

## Wave/PR structure

Each wave is an independently shippable PR that keeps all existing tests green, soaks on staging, and has a one-step rollback (revert). Waves 1–2 are risk-free groundwork; Wave 4 is the payoff; Wave 5 flips cadence only after a soak gate.

- **Wave 1** — mechanical perf fixes inside existing behavior (no schema, no semantics).
- **Wave 2** — schema: `fs_cache_inventory` + `janitor_state` (additive; nothing reads them yet).
- **Wave 3** — producers: writer/downloader record inventory; janitor clears it on delete; walk backfills.
- **Wave 4** — SQL discovery phase (new eviction engine, runs *alongside* the walk).
- **Wave 5** — cadence flip (walk → consistency sweep) + SQL census, gated on soak evidence.

---

## Wave 1 — Mechanical fixes (no behavior change)

### Task 1.1: Move `delete_part`'s on-loop FS calls into the worker thread

`fs_store.delete_part` does `part_dir.exists()` and the parent `exists()`/`rmdir()` prunes synchronously on the event loop — 3 CephFS metadata roundtrips per delete stalling all 32 workers.

**Files:**
- Modify: `hippius_s3/cache/fs_store.py` (`delete_part`, ~line 415)
- Test: `tests/unit/cache/test_fs_store_delete.py` (create)

**Step 1: Write the failing test**

```python
"""delete_part must not touch the filesystem from the event loop thread."""
import asyncio
import threading

import pytest

from hippius_s3.cache.fs_store import FileSystemPartsStore


@pytest.mark.asyncio
async def test_delete_part_does_all_fs_io_off_the_event_loop(tmp_path, monkeypatch):
    store = FileSystemPartsStore(root=str(tmp_path))
    part_dir = tmp_path / "obj-1" / "v1" / "part_1"
    part_dir.mkdir(parents=True)
    (part_dir / "chunk_0.bin").write_bytes(b"x")

    loop_thread = threading.get_ident()
    offending: list[str] = []
    real_stat = __import__("os").stat

    def spy_stat(path, *a, **kw):
        if threading.get_ident() == loop_thread:
            offending.append(str(path))
        return real_stat(path, *a, **kw)

    monkeypatch.setattr("os.stat", spy_stat)
    await store.delete_part("obj-1", 1, 1)

    assert not part_dir.exists()
    assert not [p for p in offending if "obj-1" in p], f"on-loop FS calls: {offending}"
```

**Step 2:** Run `pytest tests/unit/cache/test_fs_store_delete.py -xvs` → FAIL (the `exists()` stat happens on the loop thread).

**Step 3: Implementation** — fold the existence check, rmtree, and both parent prunes into ONE `asyncio.to_thread` blocking helper:

```python
    async def delete_part(self, object_id: str, object_version: int, part_number: int) -> None:
        part_dir = Path(self.part_path(object_id, object_version, part_number))

        def _delete_and_prune() -> bool:
            # One thread hop for the whole sequence: exists-check, rmtree, and the
            # empty-parent prunes are each a CephFS metadata roundtrip; doing them
            # on-loop serialized every janitor worker behind MDS latency.
            if not part_dir.exists():
                return False
            shutil.rmtree(part_dir, ignore_errors=False)
            for parent in (part_dir.parent, part_dir.parent.parent):
                with contextlib.suppress(OSError):
                    parent.rmdir()  # only succeeds if empty — race-safe by contract
            return True

        try:
            deleted = await asyncio.to_thread(_delete_and_prune)
        except Exception as e:
            logger.warning(
                f"FS: failed to delete part object_id={object_id} v={object_version} part={part_number}: {e}"
            )
            return
        if deleted:
            logger.debug(f"FS: deleted part object_id={object_id} v={object_version} part={part_number}")
        else:
            logger.debug(
                f"FS: delete_part no-op (not present) object_id={object_id} v={object_version} part={part_number}"
            )
```

Keep the method signature and idempotent/no-raise contract identical (callers in the janitor and `mpu_cleanup` rely on it). Note the per-delete log drops INFO→DEBUG here — that is Task 1.2's contract, done in the same touch.

**Step 4:** `pytest tests/unit/cache -xvs` → PASS (including existing fs_store tests).

**Step 5:** Commit: `perf(fs_store): single off-loop thread hop for delete_part`

### Task 1.2: Demote per-delete INFO logs in the janitor to sampled summaries

Two INFO lines per deleted part ≈ 23k lines/cycle under pressure — rotates the pod log buffer in ~13 min and costs Loki ingest. Counters (`fs_janitor_deleted_total{reason=…}`) already carry the signal.

**Files:**
- Modify: `workers/run_janitor_in_loop.py` — in `cleanup_parts_unified.handle` (and the standalone `cleanup_stale_parts` / `cleanup_old_parts_by_mtime` handlers): change the three per-part `logger.info(...)` success lines to `logger.debug(...)`. KEEP at INFO: the `abandoned` reclaim line (rare, safety-relevant), all warnings/errors, and the per-cycle summary lines.
- Test: `tests/unit/test_janitor_concurrency.py` — existing tests assert counts, not log lines; run to confirm green.

**Steps:** edit → `pytest tests/unit -k janitor -x -q` → PASS → commit `chore(janitor): per-delete success logs to DEBUG (counters carry the signal)`.

### Task 1.3: Keep shard rotation under elevated pressure + shrink the pressure sleep

Under `pressure=1` today: `shards=1` + 480s truncation ⇒ the same readdir prefix every cycle, tail starved, and 120s of every ~600s cycle is sleep while the disk sits at 85–95%.

**Files:**
- Modify: `hippius_s3/config.py` — add, next to the other janitor knobs:

```python
    # Under ELEVATED pressure keep rotating a small number of shards instead of collapsing to a
    # single head-restarting whole-tree walk (the 480s budget truncates a 15.6M-entry readdir long
    # before the tail; with shards=1 the tail is never reached). CRITICAL still forces shards=1 +
    # unbounded budget — freeing space beats coverage fairness there.
    janitor_elevated_walk_shards: int = env("HIPPIUS_JANITOR_ELEVATED_WALK_SHARDS:8", convert=int)
    # Sleep between cycles while under any disk pressure. 120s of a ~600s cycle was dead time
    # exactly when eviction throughput mattered most.
    janitor_pressure_sleep_seconds: int = env("HIPPIUS_JANITOR_PRESSURE_SLEEP_SECONDS:15", convert=int)
```

- Modify: `workers/run_janitor_in_loop.py` in `run_janitor_loop`:

```python
            if pressure >= 2:
                shards = 1
            elif pressure == 1:
                shards = max(1, config.janitor_elevated_walk_shards)
            else:
                shards = max(1, config.janitor_walk_shards)
```

  and `sleep_pressure = max(1, config.janitor_pressure_sleep_seconds)`.

- Mirror the two env defaults in `.env.defaults` and `k8s/base/configmap-defaults.yaml` (repo convention: config defaults ship in both).
- Test: `tests/unit/test_janitor_sharded_gc.py` — add a case asserting shard selection per pressure level (parametrize pressure 0/1/2 → expected shards 64/8/1). Existing sharded tests must stay green.

**Steps:** test first (FAIL on pressure=1 expecting 8) → implement → `pytest tests/unit -k janitor -q` → PASS → commit `fix(janitor): rotate shards under elevated pressure; shrink pressure sleep`.

---

## Wave 2 — Schema (additive, nothing reads it yet)

### Task 2.1: Migration — `fs_cache_inventory` + `janitor_state`

**Files:**
- Create: `hippius_s3/sql/migrations/20260725000000_fs_cache_inventory.sql`
- Test: `tests/integration/test_fs_cache_inventory_schema.py` (create)

**Migration content** (plain transactional DDL — new empty tables, no CONCURRENTLY needed):

```sql
-- Queryable index of which parts are materialized on the cache FS. Written by the
-- upload/download pipelines when a part's meta.json lands; deleted by the janitor after
-- eviction; reconciled by the walk sweep (backfill missing rows, drop rows whose dir is
-- gone). ADVISORY by design: a missing row only delays eviction until the sweep backfills
-- it, a stale row costs one stat — neither direction can cause an unsafe delete, because
-- the per-part replication gate runs at delete time regardless.
CREATE TABLE fs_cache_inventory (
    object_id      TEXT        NOT NULL,
    object_version BIGINT      NOT NULL,
    part_number    BIGINT      NOT NULL,
    cached_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (object_id, object_version, part_number)
);

-- Oldest-first candidate scans and the keyset cursor both walk this order.
CREATE INDEX fs_cache_inventory_cached_at
    ON fs_cache_inventory (cached_at, object_id, object_version, part_number);

-- Single-instance janitor's durable state (keyset cursor, shard counter). One row per key;
-- JSONB so cursor shape can evolve without migrations.
CREATE TABLE janitor_state (
    key        TEXT        PRIMARY KEY,
    value      JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Integration test** (runs against the docker-compose Postgres, mirroring existing migration tests): apply migrations, assert both tables + the index exist, insert/upsert/delete an inventory row, upsert `janitor_state` twice and read back the latest value.

**Steps:** write test → `pytest tests/integration/test_fs_cache_inventory_schema.py -xvs` (FAIL: relation does not exist) → add migration → `python -m hippius_s3.scripts.migrate` in the test env → PASS → commit `feat(janitor): fs_cache_inventory + janitor_state schema`.

### Task 2.2: Inventory repository helpers

**Files:**
- Create: `hippius_s3/repositories/fs_cache_inventory.py`
- Test: `tests/integration/test_fs_cache_inventory_repo.py`

```python
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def record_cached(conn: Any, object_id: str, object_version: int, part_number: int) -> None:
    """Best-effort: eviction discovery must never fail a PUT/GET pipeline. A lost row is
    backfilled by the walk sweep; that asymmetry (delayed eviction vs failed upload) is why
    this is one of the repo's few sanctioned swallows."""
    try:
        await conn.execute(
            """INSERT INTO fs_cache_inventory (object_id, object_version, part_number)
               VALUES ($1, $2, $3)
               ON CONFLICT (object_id, object_version, part_number) DO UPDATE SET cached_at = now()""",
            str(object_id),
            object_version,
            part_number,
        )
    except Exception as e:
        logger.warning(f"fs_cache_inventory record failed (walk sweep will backfill): {object_id} p{part_number}: {e}")


async def clear_cached(conn: Any, object_id: str, object_version: int, part_number: int) -> None:
    await conn.execute(
        "DELETE FROM fs_cache_inventory WHERE object_id = $1 AND object_version = $2 AND part_number = $3",
        str(object_id),
        object_version,
        part_number,
    )
```

Also `get_janitor_state(conn, key) -> dict | None` / `set_janitor_state(conn, key, value: dict)` (upsert). Tests: round-trip each helper; `record_cached` twice bumps `cached_at`; `record_cached` with a broken conn logs and does not raise; `clear_cached` on a missing row is a no-op.

Commit: `feat(janitor): fs_cache_inventory repository`.

---

## Wave 3 — Producers and clearers

Instrumentation choke points — every place a part's `meta.json` lands (verify the list with `rg -n "set_meta" hippius_s3/` before starting; it must cover):

| Pipeline | File | Where to hook |
|---|---|---|
| Simple PUT | `hippius_s3/writer/object_writer.py` | after the FS meta write + final `object_versions` update |
| MPU part | `hippius_s3/writer/write_through_writer.py` | after per-part meta write |
| Cache writer (copy/append paths) | `hippius_s3/writer/cache_writer.py` | after meta write |
| Downloader (cache fill) | `hippius_s3/workers/downloader.py` | after a part's last chunk lands (it writes meta eagerly, so hook on part completion, not meta write) |

### Task 3.1: Record inventory on materialization

**Files:** the four above + Test: `tests/unit/test_fs_cache_inventory_producers.py` (mock conn; assert `record_cached` called with the right tuple after each pipeline's meta write; assert pipeline still succeeds when `record_cached` raises → the swallow works).

Each hook is one line — `await fs_cache_inventory.record_cached(conn, object_id, object_version, part_number)` — using the connection/pool each pipeline already holds. NO new connections, NO transactional coupling with the data write (advisory table).

Commit per pipeline touched (4 small commits), e.g. `feat(writer): record fs_cache_inventory on simple-PUT materialization`.

### Task 3.2: Clear inventory on janitor deletes

**Files:**
- Modify: `workers/run_janitor_in_loop.py` — in `cleanup_parts_unified.handle`, after each successful `fs_store.delete_part(...)` (both the stale and the gc branches): `await fs_cache_inventory.clear_cached(conn, ...)`. The worker already holds `conn`.
- Modify: `hippius_s3/services/mpu_cleanup.py` — same after its `delete_part` calls (verify callsites with `rg -n "delete_part" hippius_s3/ workers/`).
- Test: extend `tests/unit/test_janitor_concurrency.py` fixtures — assert `clear_cached` called exactly once per successful delete, never on gate-refused parts.

Commit: `feat(janitor): clear fs_cache_inventory after eviction`.

### Task 3.3: Walk sweep backfills inventory (this is also the initial backfill)

**Files:**
- Modify: `workers/run_janitor_in_loop.py` — in `cleanup_parts_unified.candidates()`, every walked part that is NOT deleted this cycle gets `record_cached` batched: accumulate `(object_id, object_version, part_number)` tuples in the producer, flush every 500 via one `executemany`-style insert on a pooled connection (a dedicated small helper `record_cached_batch(conn, rows)` in the repository — same ON CONFLICT upsert, single statement with `unnest`).
- Test: `tests/unit/test_janitor_walk.py` — walked-but-kept parts appear in the batch; deleted parts do not; batch flushes at end of walk.

Why in the walk: the first full 64-shard sweep after deploy IS the backfill (~1 sweep ≈ 64 cycles at normal cadence). No separate backfill job, no separate code path to delete later.

Commit: `feat(janitor): walk sweep backfills fs_cache_inventory`.

---

## Wave 4 — SQL discovery phase (the new engine)

### Task 4.1: Candidate query

**Files:**
- Create: `hippius_s3/sql/queries/janitor_evictable_candidates.sql`
- Test: `tests/integration/test_janitor_evictable_candidates.py`

The query is `find_underreplicated_live_chunks.sql`'s CTE with the coverage predicate **flipped**, joined to inventory, with the `expected_chunks` guard from `count_chunk_backends.sql`, keyset-cursored:

```sql
-- Evictable-part prefilter for the janitor's SQL discovery phase. Returns parts that are
-- (a) present in fs_cache_inventory, (b) aged past $5 unless $6 disables the age gate
-- (pressure), (c) FULLY covered: every expected chunk has a live chunk_backend row for
-- every required backend (per-version upload_backends / migration=['ipfs'] / default $2,
-- UNION backup $1 — mirrors is_replicated_on_all_backends exactly).
-- PREFILTER ONLY: the worker re-runs the authoritative per-part gate before deleting.
-- Keyset cursor: strictly after ($7, $8, $9, $10) in (cached_at, object_id, version, part)
-- order; $3 caps the page.
WITH required_sets AS (
    SELECT
        ov.object_id,
        ov.object_version,
        ARRAY(
            SELECT DISTINCT unnest(
                (CASE
                    WHEN ov.version_type = 'migration' THEN ARRAY['ipfs']::text[]
                    WHEN ov.upload_backends IS NOT NULL AND cardinality(ov.upload_backends) > 0 THEN ov.upload_backends
                    ELSE $2::text[]
                END) || $1::text[]
            )
        ) AS required
    FROM object_versions ov
)
SELECT inv.object_id, inv.object_version, inv.part_number, inv.cached_at
FROM fs_cache_inventory inv
JOIN parts p
  ON p.object_id::text = inv.object_id
 AND p.object_version = inv.object_version
 AND p.part_number = inv.part_number
JOIN required_sets rs
  ON rs.object_id = p.object_id AND rs.object_version = p.object_version
WHERE ($6 OR p.uploaded_at < now() - make_interval(secs => $5))
  AND (inv.cached_at, inv.object_id, inv.object_version, inv.part_number)
      > ($7, $8, $9, $10)
  -- expected chunk population fully present…
  AND (SELECT COUNT(*) FROM part_chunks pc WHERE pc.part_id = p.part_id)
      >= CEIL(p.size_bytes::float / GREATEST(COALESCE(p.chunk_size_bytes, 4194304), 1))::int
  AND (SELECT COUNT(*) FROM part_chunks pc WHERE pc.part_id = p.part_id) > 0
  -- …and no chunk is missing any required backend (flipped sentinel predicate).
  AND NOT EXISTS (
    SELECT 1
    FROM part_chunks pc
    CROSS JOIN unnest(rs.required) AS req(backend)
    WHERE pc.part_id = p.part_id
      AND NOT EXISTS (
          SELECT 1 FROM chunk_backend cb
          WHERE cb.chunk_id = pc.id AND cb.backend = req.backend AND NOT cb.deleted
      )
  )
ORDER BY inv.cached_at, inv.object_id, inv.object_version, inv.part_number
LIMIT $3
```

($4 is reserved/unused — keep parameter numbering stable if you trim; adjust to actual count.)

**Integration test scenarios (seed real rows, assert membership):**
1. Fully replicated + aged + in inventory → returned.
2. Missing one backend row on one chunk → NOT returned (the CopyObject population: zero `chunk_backend` rows → never returned).
3. `part_chunks` count below `expected_chunks` (mid-materialization) → NOT returned.
4. Aged but not in inventory → NOT returned.
5. Young + age gate on → NOT returned; same row with `$6=true` (pressure) → returned.
6. Keyset: page 1 of LIMIT 2 then cursor from last row → page 2 disjoint, ordered, complete.
7. `chunk_backend.deleted = true` rows don't count as coverage.
8. Migration-version row requires `ipfs` only.

Run `EXPLAIN ANALYZE` in the test on the seeded set and assert no seq scan on `fs_cache_inventory` (uses `fs_cache_inventory_cached_at`).

Commit: `feat(janitor): evictable-candidate query (flipped-sentinel prefilter, keyset)`.

### Task 4.2: Discovery phase in the janitor

**Files:**
- Modify: `workers/run_janitor_in_loop.py` — new `evict_from_inventory(...)` + wiring; new phase name in `JANITOR_PHASES` (`"sql_evict"`); new counter reason `"sql_evict"` documented on `fs_janitor_deleted_total`.
- Modify: `hippius_s3/config.py` — knobs:

```python
    # SQL discovery phase: candidate rows fetched per keyset page, and the per-cycle delete
    # budget (0 disables the phase). The page feeds the same bounded worker pool as the walk.
    janitor_sql_page_size: int = env("HIPPIUS_JANITOR_SQL_PAGE_SIZE:1000", convert=int)
    janitor_sql_max_deletes_per_cycle: int = env("HIPPIUS_JANITOR_SQL_MAX_DELETES_PER_CYCLE:50000", convert=int)
```

- Test: `tests/unit/test_janitor_sql_evict.py` (create; mock pool/fs_store like `test_janitor_concurrency.py` does).

**Core implementation (producer/worker, reusing `_run_worker_pool`):**

```python
async def evict_from_inventory(
    pool: asyncpg.Pool,
    fs_store: FileSystemPartsStore,
    redis_client: Redis,
    *,
    pressure: int,
) -> int:
    """SQL-driven eviction: keyset-page fs_cache_inventory joined to full backend coverage,
    stat only the candidates (existence + atime hot-check), then apply the UNCHANGED absolute
    replication gate per part before deleting. O(evictable) instead of O(resident).

    Cursor lives in janitor_state['sql_evict_cursor'] and only advances past a page after the
    page is fully processed, so a crash mid-page re-processes (idempotent: delete_part is a
    no-op on missing dirs, clear_cached is a no-op on missing rows). The cursor resets to the
    beginning when a page comes back short (end of table) — the scan is a ring, not a one-shot.
    """
    hot_window = _effective_hot_retention(pressure)
    ignore_age = pressure > 0
    max_deletes = config.janitor_sql_max_deletes_per_cycle
    if max_deletes <= 0:
        return 0

    try:
        dlq_object_ids = await get_all_dlq_object_ids(redis_client)
    except DLQProtectionUnavailable as exc:
        # Same class of deleter as age-GC → same C1 fail-open: the replication gate below is
        # the hard safety net; a fully-replicated part is safe to evict regardless of DLQ.
        logger.error(f"DLQ protection unavailable — SQL eviction replication-gate-only: {exc}")
        dlq_object_ids = set()

    backup_backends = list(getattr(config, "backup_backends", []) or [])
    now = time.time()
    deleted_total = 0

    async with pool.acquire() as conn:
        state = await get_janitor_state(conn, "sql_evict_cursor") or {}
    cursor = (
        state.get("cached_at", "-infinity"),
        state.get("object_id", ""),
        int(state.get("object_version", 0)),
        int(state.get("part_number", 0)),
    )

    while deleted_total < max_deletes:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                get_query("janitor_evictable_candidates"),
                backup_backends,
                list(config.upload_backends),
                config.janitor_sql_page_size,
                None,
                config.fs_cache_gc_max_age_seconds,
                ignore_age,
                *cursor,
            )
        if not rows:
            async with pool.acquire() as conn:
                await set_janitor_state(conn, "sql_evict_cursor", {})  # wrap the ring
            break

        async def candidates() -> AsyncIterator[tuple[str, int, int]]:
            for r in rows:
                if r["object_id"] in dlq_object_ids:
                    continue
                yield (r["object_id"], r["object_version"], r["part_number"])

        async def handle(conn: asyncpg.Connection, item: tuple[str, int, int]) -> bool:
            object_id, object_version, part_number = item
            st = await asyncio.to_thread(fs_store.stat_part, object_id, object_version, part_number)
            if st is None:
                await clear_cached(conn, object_id, object_version, part_number)  # stale row: self-heal
                return False
            if hot_window > 0 and st.st_atime > (now - hot_window):
                return False
            # ABSOLUTE safety gate — identical call to the walk's, never bypassed.
            if not await is_replicated_on_all_backends(conn, object_id, object_version, part_number):
                return False
            await fs_store.delete_part(object_id, object_version, part_number)
            await clear_cached(conn, object_id, object_version, part_number)
            if _janitor_deleted_counter is not None:
                _janitor_deleted_counter.add(1, attributes={"reason": "sql_evict"})
            return True

        deleted_total += await _run_worker_pool(pool, candidates(), handle, config.janitor_concurrency)

        last = rows[-1]
        cursor = (last["cached_at"], last["object_id"], last["object_version"], last["part_number"])
        async with pool.acquire() as conn:
            await set_janitor_state(
                conn,
                "sql_evict_cursor",
                {
                    "cached_at": last["cached_at"].isoformat(),
                    "object_id": last["object_id"],
                    "object_version": last["object_version"],
                    "part_number": last["part_number"],
                },
            )
        if len(rows) < config.janitor_sql_page_size:
            break  # short page = end of ring this cycle

    logger.info(f"SQL eviction cycle: deleted={deleted_total} pressure={pressure} ignore_age={ignore_age}")
    return deleted_total
```

Supporting change: add `FileSystemPartsStore.stat_part(object_id, version, part) -> os.stat_result | None` — a blocking helper stat'ing `meta.json` (else the part dir), returning `None` if absent. (Unit-test alongside Task 1.1's file.)

**Wiring in `run_janitor_loop`:** insert as a new phase BEFORE the unified walk (`_janitor_phase` bookkeeping + try/except like the other phases). Both engines coexist in Wave 4; they are mutually idempotent (`delete_part` no-op on missing, `clear_cached` no-op on missing).

**Unit tests (mock pool/fs_store):**
1. Candidate absent on FS → row cleared, nothing deleted.
2. Hot candidate → skipped, row kept.
3. Gate returns False → NOT deleted, row kept (prefilter/gate divergence is survivable).
4. Gate True + cold → deleted + cleared + counter reason `sql_evict`.
5. DLQ-parked object skipped when set available; deleted (gate permitting) when set unavailable.
6. Cursor: advances only after a page completes; wraps to `{}` on empty page; crash-resume re-processes the last page without error.
7. `max_deletes` budget stops the loop.
8. Pressure>0 passes `ignore_age=True`.

**E2E test:** `tests/e2e/test_janitor_sql_eviction.py` — PUT an object through the stack (mock-arion records backend rows), wait for replication, run one janitor cycle, assert the part left the FS cache via reason `sql_evict` and a subsequent GET re-fetches through the pipeline successfully (proves eviction was safe).

Commits: `feat(fs_store): stat_part helper` → `feat(janitor): SQL-driven eviction phase with durable keyset cursor`.

---

## Wave 5 — Cadence flip + SQL census (gated on soak)

**Soak gate (staging, then prod, ≥1 week each in Wave-4 state) — flip only when ALL hold:**
1. `fs_janitor_deleted_total{reason="sql_evict"}` carries ≥90% of eviction volume.
2. Walk-phase deletes (`gc_age`/`stale_mtime`) trend toward only no-DB-row orphans.
3. No growth in `janitor_underreplicated_live_chunks` attributable to eviction (durability unchanged).
4. Inventory count vs walk census `parts_seen` agree within ~5% on a completed sweep (backfill converged).

### Task 5.1: Walk becomes a consistency sweep

- `hippius_s3/config.py`: `janitor_walk_sweep_interval_seconds: int = env("HIPPIUS_JANITOR_WALK_SWEEP_INTERVAL_SECONDS:21600", convert=int)` (6h between walk-phase *executions*; the SQL phase still runs every cycle). Persist `_walk_shard` into `janitor_state['walk_shard']` while here (survives restarts — today it resets to 0).
- `run_janitor_loop`: run `cleanup_parts_unified` only when `now - last_walk_at >= interval` **or** `pressure >= 2` (critical keeps the walk as belt-and-braces free-space path). Track `last_walk_at` in `janitor_state`.
- Walk sweep gains the reverse reconciliation: parts in inventory but not found by a COMPLETED (untruncated) sweep shard are already self-healed by Task 4.2's stat-miss path — no extra work needed; document this in the sweep docstring.
- Tests: cadence logic unit-tested (interval respected; critical pressure overrides; state round-trips).

> Known follow-up (from Task 1.3 review): the walk census accumulator does not detect a `shards` change mid-sweep on NORMAL↔ELEVATED transitions, so a "complete" publish can mix partition schemes (dashboards only; usually suppressed by the truncation guard). The SQL census below retires the walk census and closes this.

### Task 5.2: Census from SQL, walk census as cross-check

- New query `janitor_inventory_census.sql`: `SELECT count(*), min(cached_at), age-bucket counts FROM fs_cache_inventory` (bucket via `width_bucket`/CASE on `now()-cached_at` matching `AGE_BUCKET_BOUNDARIES`). Publishes the existing gauges every cycle; the walk census (now 6-hourly) logs a drift metric `fs_cache_inventory_drift_parts = |census - inventory|` — alert if it grows.
- Note the semantic shift in the gauge description: age is now time-since-materialization, not mtime (mtime was already polluted by `os.utime` on reads — the SQL census is *more* honest; hot parts are still counted from atime at candidate-stat time only under the walk. `fs_cache_hot_parts` stays walk-owned and 6-hourly — acceptable; keep its description updated).

Commits: `feat(janitor): walk demoted to consistency sweep` → `feat(janitor): census from fs_cache_inventory with walk drift cross-check`.

---

## Rollout / rollback

| Wave | Ship to | Rollback |
|---|---|---|
| 1 | staging → prod (release branch pattern `release/janitor-*-prod`) | revert commit; no state |
| 2 | with Wave 3 | tables are additive; revert code, leave tables (empty/idle is harmless) |
| 3 | staging soak 2–3 days (watch PUT/GET latency for the extra insert — expect noise-level) | revert; rows go stale, nothing reads them |
| 4 | staging soak ≥1 week → prod | set `HIPPIUS_JANITOR_SQL_MAX_DELETES_PER_CYCLE=0` (kills the phase without a deploy) or revert; walk is still full eviction engine |
| 5 | only after soak gate above | revert restores per-cycle walk; inventory keeps working |

**Ops notes:**
- `fs_cache_inventory` write rate = part-materialization rate (PUT parts + cache fills). Rows are ~100 bytes; 15.6M rows ≈ ~2 GB with index — fine on the NVMe Postgres. Autovacuum was just tuned for this DB (commit `2d70604`); the delete-heavy pattern is exactly what it now handles.
- The candidate query's coverage anti-join is the same shape the sentinel already runs every cycle against the whole table — the inventory join makes it strictly cheaper.
- Watch during Wave-4 soak: `fs_janitor_cycle_seconds` (should collapse), `fs_cache_disk_used_bytes` slope under load, DB p99 (candidate pages are indexed reads), and the two standing alarms that predate this work: `janitor_underreplicated_live_chunks` pegged at ≥500 and `aged_orphans≈426` — investigate separately; they are NOT caused by (and must not be masked by) this change.

## Explicit non-goals

- No directory-fanout relayout (obsolete for eviction once discovery is SQL; revisit only if the 6-hourly sweep is still too slow).
- No fix for the CopyObject never-replicated population (separate work: enqueue uploads for copies); this plan only guarantees those parts are cheap non-candidates instead of per-cycle re-stat victims.
- No change to `cephor_replication_status` / drain semantics (different replication concept: SSD→Ceph pool, drain-agent-owned).
- No multi-instance janitor (cursor + state assume single instance, as today).
