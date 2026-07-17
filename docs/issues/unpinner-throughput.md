# Arion unpinner throughput (~10x drain deficit)

## Summary

Live prod incident: the `arion_unpin_requests` queue is ~1.6M and growing. The unpinner drains
~446/min across 3 pods while inflow is ~4400/min — a ~10x deficit. As `redis-queues` fills it slows
down, which mirrors a prior incident where **1.29M** `arion_unpin_requests` made redis slow enough to
cause prod GET `IncompleteRead`s (see the team memory note "redis-queues unpin overrun incident";
cleared then with `UNLINK`). We need high parallelism to be both **safe** and **effective**.

## Root cause

Per-pod effective concurrency was pinned low by two things:

1. **Per-request ArionClient re-handshake.** `process_unpin_request` opened
   `async with backend_client_factory() as client:` for **every** request, i.e. a brand-new
   `ArionClient` (new `httpx.AsyncClient` + TLS handshake) per request. In prod nearly every unpin
   request is a single chunk, so the shared per-pod Arion-DELETE budget (`HIPPIUS_UNPINNER_PARALLELISM`)
   was mostly dead — effective concurrency was really just `HIPPIUS_UNPINNER_MAX_INFLIGHT` (was 4),
   each paying a full handshake. Raising inflight without fixing this would just multiply handshakes and
   storm ephemeral ports. Contrast `run_arion_uploader_in_loop.py`, which builds one `ArionClient()` for
   the whole loop and reuses it.

2. **Inflight ceiling coupled to the DB pool.** Raising `MAX_INFLIGHT` grew the pool floor
   (`delete_concurrency + max_inflight`) unboundedly. Per pod × replicas already runs close to Postgres
   `max_connections`, so cranking inflight risked exhausting Postgres.

## Fix (code — this PR)

- **Reuse one client across the loop.** `run_unpinner_loop` now creates the backend client once
  (`async with backend_client_factory() as client:` wrapping the dispatch loop, mirroring the uploader)
  and passes that live client into `process_unpin_request`. `process_unpin_request` gained a
  `client` param; when it is `None` (standalone callers / tests) it still builds one from
  `backend_client_factory` for that request. The DELETE / soft-delete logic is unchanged. This removes
  the per-DELETE TLS handshake and prevents a handshake / ephemeral-port storm at high concurrency.

- **Bounded, deadlock-safe DB pool.** New config `HIPPIUS_UNPINNER_DB_POOL_MAX` is now an explicit
  **cap** (not a floor) on the pool. The loop sizes the pool as:

  ```
  ideal_pool    = parallelism + max_inflight     # throughput-optimal peak conns
  deadlock_floor = parallelism + 1               # one request's max concurrent soft-deletes + 1 fetch
  pool_max = max(2, min(ideal_pool, HIPPIUS_UNPINNER_DB_POOL_MAX))
  # if the cap is below deadlock_floor, honor the floor and log a warning
  if min(ideal_pool, cap) < deadlock_floor: pool_max = deadlock_floor  (+ WARNING)
  ```

  So raising `MAX_INFLIGHT` can never balloon the pool past the cap, but the pool is never sized below
  the deadlock-safe floor. This is safe against deadlock because nothing acquires a second pool
  connection while holding one (the initial fetch conn is released before any DELETE; soft-delete conns
  are taken one-at-a-time under the shared `delete_sem`), and asyncpg `acquire()` has no timeout — an
  undersized pool merely throttles, it does not deadlock. The floor keeps liveness comfortable.

- **Modest default bump.** `HIPPIUS_UNPINNER_MAX_INFLIGHT` default 4 → 8 and
  `HIPPIUS_UNPINNER_DB_POOL_MAX` default 12 → 16 so non-prod benefits. Prod uses the env/secret values
  below — the defaults are **not** aggressive.

## Ops ramp (NOT code — ops levers)

Ramp in steps and watch Arion 429s + Postgres connection count between each step.

- **`HIPPIUS_UNPINNER_MAX_INFLIGHT` 4 → 32** via the `hippius-s3-secrets` secret (already wired as a
  `secretKeyRef` on the `arion-unpinner` deployment). This is the primary throughput lever now that the
  client is reused.
- **Replicas 3 → 6** in `k8s/base/workers-deployments.yaml` (the `arion-unpinner` Deployment, currently
  `replicas: 3`).
- **`HIPPIUS_UNPINNER_PARALLELISM` → 4.** In prod almost every request is one chunk, so this shared
  per-pod DELETE budget is mostly dead; keep it small. It only matters for fat multi-chunk requests.
- **`HIPPIUS_UNPINNER_DB_POOL_MAX`** — set per the connection budget below. If left at the default 16
  while `MAX_INFLIGHT=32`, the pool is capped at 16 (ideal would be 36); raise the cap to trade
  Postgres connections for throughput.

### Connection-budget math (why this can't exhaust Postgres)

Per pod, peak connections ≈ `min(parallelism + max_inflight, HIPPIUS_UNPINNER_DB_POOL_MAX)`.

| max_inflight | parallelism | ideal | db_pool_max cap | pool/pod | × 6 replicas |
|---|---|---|---|---|---|
| 32 | 4 | 36 | 16 (default) | 16 | 96 |
| 32 | 4 | 36 | 24 | 24 | 144 |
| 32 | 4 | 36 | 40 | 36 | 216 |

The cap makes the per-pod pool independent of how high inflight is cranked, so total unpinner
connections stay bounded and can be kept under Postgres `max_connections` alongside the API/other
workers. Deadlock floor at these settings is `parallelism + 1 = 5`, far below any of the caps.

## Testing

Unit tests in `tests/unit/test_unpinner_loop.py`:

- Client is constructed **once** for the loop and the **same** live client is passed to every request
  (not one per request).
- `process_unpin_request` uses a supplied `client` directly and never calls the factory; DELETE +
  soft-delete still fire once per chunk.
- Pool sizing: cap honored when below the ideal; ideal used when the cap is high; and when the cap is
  below the deadlock floor the floor wins and a warning is logged.

Existing dispatch/semaphore/shutdown/best-effort tests remain unchanged and green.

## Risks

- **Arion 429 at high concurrency.** Ramp `MAX_INFLIGHT` in steps; back off if Arion returns 429s.
- **Postgres connections.** Bounded by `HIPPIUS_UNPINNER_DB_POOL_MAX` (the cap) — raise it deliberately,
  watching `pg_stat_activity`, not implicitly via inflight.
