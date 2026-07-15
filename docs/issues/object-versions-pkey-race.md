# object_versions_pkey duplicate-key race on concurrent version reservation

## Summary
Concurrent writes to the same object can 500 the client with
`duplicate key value violates unique constraint "object_versions_pkey"`. PR #243
closed this for the simple/streaming PUT path only; three other version-reservation
callers (multipart initiate, copy fast-path, `upsert_with_cid`) had no protection.

## Impact
- Client receives S3 `InternalError` (HTTP 500) for a write that should succeed —
  the racing request is lost, not retried.
- Data path only: no corruption. `server_errors=0` in prod health snapshots (the 500 is
  logged as an app ERROR, not a gateway 5xx audit line), so it hides in the noise.
- Low but non-zero frequency; observed intermittently in prod api logs (1–2×/window).

## Root cause
`object_versions` PK is composite `(object_id, object_version)` with **no sequence** —
`object_version` is allocated in SQL as:

```
GREATEST(objects.current_object_version, MAX(object_versions.object_version)) + 1
```

inside `upsert_object_*.sql`'s `ON CONFLICT DO UPDATE`. The `objects.current_object_version`
term is row-locked and re-read post-lock (EvalPlanQual), which serializes PUT-vs-PUT. But
the `MAX(object_version)` term is a **snapshot-stale subquery under READ COMMITTED**.

`create_migration_version.sql` (the v4→v5 migrator) inserts a new version **without**
bumping `current_object_version` — it deliberately leaves the pointer behind and only
advances it later via `swap_current_version_cas.sql` once the migrated data is ready.
That leaves `current_object_version` behind `MAX(object_version)`, so a writer whose snapshot
predates the migrator's commit computes `GREATEST(1, stale 1) + 1 = 2`, collides with the
migration's version 2, and raises `object_versions_pkey`.

PR #243 (commit `375c043`) added a bounded retry-in-a-fresh-transaction, but **only** in
`put_simple_stream_full`. The identical allocation is used, unguarded, by:
- `hippius_s3/api/s3/multipart.py` — multipart initiate (`upsert_object_multipart`)
- `hippius_s3/services/copy_service_v5.py` — copy fast-path (`upsert_object_basic`)
- `hippius_s3/repositories/objects.py::upsert_with_cid` (`upsert_object_with_cid`)

## Why not "just bump the counter in the migrator"
Making `create_migration_version` advance `current_object_version` would make the collision
structurally impossible — but it would also expose a **half-migrated version as the current
(served) version** in the window before the migrated data lands, which is a correctness
regression. The migrator intentionally keeps create and swap separate. Rejected.

## Fix
Generalize PR #243's approach into one shared helper and apply it to all four reserve
callers (no duplicated retry loops):

`hippius_s3/writer/db.py::retry_on_object_version_conflict(reserve, *, attempts=3)`
- Runs `reserve()`; on `asyncpg.UniqueViolationError` **scoped to `object_versions_pkey`**,
  retries. Each `reserve()` runs in its own autocommit statement / fresh transaction, so the
  retry re-reads the committed `MAX` and converges (typically attempt 2).
- Bounded: a persistent collision re-raises (a real error, not masked).
- Scoped to the one constraint: a unique violation on any **other** constraint surfaces
  immediately rather than being retried and hidden.

`put_simple_stream_full` was refactored to call the helper too (the inline retry is gone),
so all four paths share one implementation.

## Testing
- `tests/unit/test_object_version_retry.py` (adversarial, no DB): first-try success does not
  retry; retries on `object_versions_pkey` and converges; **exhaustion re-raises and stays
  bounded** (no infinite loop); a **different constraint is not retried** (call count == 1);
  unrelated exceptions are not swallowed.
- `tests/integration/test_object_version_concurrency_sql.py` (live Postgres, runs in CI):
  the existing raw-collision test is retained; a new test drives a real migrator-vs-write race
  through the helper on the **multipart** reserve and asserts it resolves to v3 with no error —
  proving the wiring on a previously-unguarded path.

## Alternatives considered
- Advisory lock in the upsert statements — fragile: the writer locks the *candidate* object_id
  (a fresh UUID) on the conflict path, not the resolved existing object_id the migrator locks,
  so they wouldn't serialize.
- Split `current_object_version` into a separate next-version counter vs current pointer — the
  truly correct structural fix, but a larger schema change; out of scope here.
