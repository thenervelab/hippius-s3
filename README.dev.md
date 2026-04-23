# README.dev.md — Developer Onboarding

This is the **hands-on** companion to [CLAUDE.md](CLAUDE.md). Where CLAUDE.md answers *"what is this?"*, this file answers *"how do I actually work on it?"*.

Read in this order on your first day:

1. [README.md](README.md) — what the product does, user-facing quickstart.
2. [CLAUDE.md](CLAUDE.md) — 15-min architectural map. Topology, request lifecycle, subsystems.
3. **This file** — how to set up, debug, and ship changes.
4. [todo.md](todo.md) — backlog of pitfalls and good first issues.
5. Subsystem `CLAUDE.md` files when you're about to touch that area.

---

## Table of contents

1. [First-day setup](#1-first-day-setup)
2. [The dev loop](#2-the-dev-loop)
3. [Running the stack](#3-running-the-stack)
4. [Testing playbook](#4-testing-playbook)
5. [Debugging & observability](#5-debugging--observability)
6. [Recipes](#6-recipes)
7. [Code style & conventions](#7-code-style--conventions)
8. [Contribution workflow](#8-contribution-workflow)
9. [Common pitfalls](#9-common-pitfalls-new-devs-hit)
10. [Performance notes](#10-performance-notes)
11. [Glossary](#11-glossary)
12. [Where to ask for help](#12-where-to-ask-for-help)

---

## 1. First-day setup

### 1.1 Prerequisites

You need:

- **Python 3.10+** (3.11 preferred; that's what `.venv` is built against).
- **`uv`** for package management. `curl -LsSf https://astral.sh/uv/install.sh | sh` if you don't have it.
- **Docker Desktop** (macOS) or **Docker Engine** (Linux) with **Compose v2** (`docker compose ...`, not `docker-compose`). Check with `docker compose version`.
- **`git`** (any recent version).
- **~20 GB free disk** for the docker-compose stack + FS cache volume.
- **macOS Homebrew users**: use the system install at `/usr/local/Homebrew/bin/brew` if Apple Silicon is giving you architecture headaches.

### 1.2 Clone and venv

```bash
git clone git@github.com:thenervelab/hippius-s3.git
cd hippius-s3

python3 -m venv .venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

The `[dev]` extra includes pytest, ruff, mypy, pre-commit, and the monitoring/debugging bits. `uv pip` is strictly required — regular `pip` works but is 10-100× slower and will annoy everyone during code review.

### 1.3 Pre-commit hooks

```bash
pre-commit install
```

This hooks ruff + mypy into `git commit`. If a commit fails the hook, **fix the underlying issue** and re-commit — never bypass with `--no-verify`.

### 1.4 Environment config

Layered env file approach (loaded in order):

1. `.env.defaults` — base. Checked in. Contains feature flags, worker settings, etc.
2. `.env` — local overrides. Git-ignored. **You create this.** Copy from `.env.example`.

For host-level pytest runs, `.env.test-local` is used instead. For E2E tests in docker, `.env.test-docker` is used.

```bash
cp .env.example .env
# Edit .env. At minimum you need:
#   DATABASE_URL
#   REDIS_URL + other REDIS_*_URL
#   HIPPIUS_SERVICE_KEY (64-hex; can be random for local)
#   HIPPIUS_AUTH_ENCRYPTION_KEY (64-hex; can be random for local)
#   FRONTEND_HMAC_SECRET (any string)
#   ARION_SERVICE_KEY, ARION_BEARER_TOKEN (any string for local — mock-arion accepts anything)
#   HIPPIUS_KMS_MODE=disabled
```

For a fully local stack (no external deps), use `HIPPIUS_KMS_MODE=disabled` and let mock-arion handle storage — see the E2E compose in §3.3.

### 1.5 Bring up the stack

```bash
docker compose up -d
docker compose logs -f api
```

Services that come up:

| Service | Port | Purpose |
|---|---|---|
| `gateway` | 8080 | Public-facing. Hit this for S3 traffic. |
| `api` | 8000 | Internal. Gateway forwards here. |
| `postgres` | 5432 | Main DB + keystore. |
| `redis` | 6379 | General cache. |
| `redis-accounts` | 6380 | Account cache. |
| `redis-chain` | 6381 | Chain cache. |
| `redis-queues` | 6382 | Work queues + pub/sub. |
| `redis-rate-limiting` | 6383 | Rate limit counters. |
| `redis-acl` | 6384 | ACL cache. |
| Arion worker pods | — | Uploader, downloader, unpinner, janitor. |

Database migrations run automatically on API container start (see [hippius_s3/scripts/migrate.py](hippius_s3/scripts/migrate.py)).

### 1.6 Verify

```bash
curl http://localhost:8080/health         # gateway
curl http://localhost:8000/health         # api (should only be reachable inside the docker network normally; exposed in dev)

# List buckets with an anonymous call — expect an error because no auth
curl http://localhost:8080/
```

Run the unit tests to make sure your environment is sane:

```bash
pytest tests/unit -v
```

All should pass in under a minute. If anything fails here, your local env is out of sync — don't move on.

---

## 2. The dev loop

### 2.1 Hot reload

Python code changes inside `hippius_s3/`, `gateway/`, and `workers/` **auto-reload** the container — uvicorn runs with `reload=True` in dev mode. **Do NOT** `docker compose restart api` after every edit; that's 10+ seconds of wasted time. Just save the file.

### 2.2 When you DO need a restart

- You changed `docker-compose.yml` → `docker compose up -d` re-applies.
- You changed `pyproject.toml` (new dependency) → `docker compose build api gateway workers && docker compose up -d`.
- You changed `.env` → restart the affected service(s).

### 2.3 Iterative workflow

```bash
# 1. Edit code.
# 2. Save. Uvicorn picks it up.
# 3. Tail logs.
docker compose logs -f api gateway | grep -v '/health'

# 4. Hit the endpoint.
aws --endpoint-url http://localhost:8080 s3 ls

# 5. When it works, run the relevant tests.
pytest tests/unit/cache -xvs
```

### 2.4 When tests hang

Tests that rely on Redis pub/sub can hang if the `redis-queues` client is stale. First thing to try:

```bash
docker compose restart redis-queues
```

If that doesn't fix it, full reset:

```bash
docker compose down -v   # -v drops volumes; DB and FS cache are wiped
docker compose up -d
```

---

## 3. Running the stack

### 3.1 Default dev (the `docker compose up -d` you already ran)

Uses `docker-compose.yml` only. KMS is disabled, monitoring is off, no mock services. Arion URL points at the real Arion endpoint from `.env` — if your `HIPPIUS_ARION_BASE_URL` is `https://arion.hippius.com/` with a real service key, uploads go to real Arion.

### 3.2 With monitoring (LGTM stack)

```bash
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
```

Adds:
- Grafana at http://localhost:3000 (admin/admin). Pre-built dashboards for Hippius S3 Overview and S3 Workers.
- Prometheus at http://localhost:9090
- Loki at http://localhost:3100
- Tempo (traces) at http://localhost:3200
- OTel Collector at localhost:4317/4318

Most useful: **Tempo** for tracing a specific request end-to-end. Grab a `X-Ray-ID` from an API response header, paste into Tempo's search.

### 3.3 E2E stack with mocks

This is what you use when you want to run the full test suite OR when you don't have Arion/KMS creds.

```bash
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --wait
```

Adds:
- `mock-arion` — stands in for Arion storage.
- `mock-kms` — stands in for OVH KMS on port 8443.
- `mock-hippius-api` — stands in for the chain API. **Hardcodes all tokens as `token_type="master"`** — see [tests/e2e/CLAUDE.md](tests/e2e/CLAUDE.md).
- `toxiproxy` — fault injection. Use it to simulate Arion being slow, dropping connections, etc.

```bash
pytest tests/e2e -v

# Tear down (include -v so volumes reset)
COMPOSE_PROJECT_NAME=hippius-e2e docker compose -f docker-compose.yml -f docker-compose.e2e.yml down -v
```

### 3.4 Production / staging

```bash
# Production overrides (real rate limits, bigger Redis memory caps, etc.)
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Staging
docker compose -f docker-compose.yml -f docker-compose.staging.yml up -d
```

You rarely run these locally; they're there for parity investigation.

### 3.5 Base image rebuild

If you change Python deps, rebuild the base image:

```bash
COMPOSE_PROFILES=build-base docker compose build --no-cache base
docker compose build api gateway workers
docker compose up -d
```

This is slow (~3 minutes). Avoid unless the base image actually changed.

---

## 4. Testing playbook

### 4.1 Three tiers

| Tier | Speed | What's real | Location |
|---|---|---|---|
| Unit | <1s/test | Nothing — all mocks | [tests/unit/](tests/unit/) |
| Integration | 1-10s/test | DB + Redis; mocked externals | [tests/integration/](tests/integration/) |
| E2E | 5-60s/test | Full stack with mocked backends | [tests/e2e/](tests/e2e/) |

### 4.2 Running subsets

```bash
pytest tests/unit -v                                      # all unit
pytest tests/unit/cache -xvs                              # just cache tests, verbose
pytest tests/unit/test_download_coalescing.py -xvs        # single file
pytest tests/unit -k coalesc                              # match by keyword
pytest tests/unit -k "not slow" -v                        # exclude slow ones

pytest -m unit        # by marker
pytest -m integration
pytest -m e2e

# Single test
pytest tests/e2e/test_GetObject.py::test_get_object_downloads_and_matches_headers -xvs
```

Flags: `-x` stop on first failure, `-v` verbose, `-s` show stdout/logs, `-k <kw>` keyword filter.

### 4.3 Running against real AWS

```bash
RUN_REAL_AWS=1 pytest tests/e2e -v
```

E2E tests with `@pytest.mark.local` or `hippius_cache`/`hippius_headers` markers are skipped in this mode. You need real AWS creds in env.

### 4.4 What to test when you add code

- **Unit** — pure behavior, mocks everything. If your function is under 50 lines and has no I/O, this is enough.
- **Integration** — touches DB or Redis. Worker loops, queue round trips, ACL queries.
- **E2E** — touches the full request path. New S3 endpoints, behavior that spans gateway → api → writer → worker.

**Rule of thumb**: a PR adding a new feature should include at least one test at the highest tier that can exercise it end-to-end. Don't bury a Range-fetcher change in unit tests only — add an e2e Range test.

### 4.5 Coverage of critical paths

These paths have good coverage already — match their patterns:

- [tests/unit/test_download_coalescing.py](tests/unit/test_download_coalescing.py) — lock-key format, single-enqueuer invariant.
- [tests/unit/test_janitor_hot_retention.py](tests/unit/test_janitor_hot_retention.py) — no-deletion invariant under non-replication.
- [tests/e2e/test_GetObject_Range.py](tests/e2e/test_GetObject_Range.py) — range requests against FS cache.
- [tests/e2e/test_DLQ_Requeue.py](tests/e2e/test_DLQ_Requeue.py) — full fail-then-requeue cycle.

---

## 5. Debugging & observability

### 5.1 Logs

```bash
docker compose logs -f api              # just the API
docker compose logs -f gateway          # gateway (public-facing)
docker compose logs -f workers          # all worker loops at once
docker compose logs --since 5m api      # last 5 minutes
docker compose logs api | grep ERROR    # errors only
```

Log shape (structured): `timestamp level logger ray_id=... account=... <message>`. The `ray_id` is your best friend — it's generated in the gateway and propagates through every service, so you can follow a single request across the whole stack.

### 5.2 Tracing a single request

1. Make the request; note the `X-Ray-ID` response header.
2. Open Grafana → Tempo → paste the ray id. You get the full waterfall: gateway auth → forward → API middleware → writer → FS cache → worker enqueue → backend upload.
3. If monitoring isn't on, grep the logs for the ray id:

```bash
RAY_ID=xxx-yyy-zzz
docker compose logs --since 10m 2>&1 | grep "$RAY_ID"
```

### 5.3 Inspecting the FS cache

```bash
# List all cached objects
docker compose exec api ls /var/lib/hippius/object_cache/

# Drill into one
docker compose exec api ls -la /var/lib/hippius/object_cache/<object_id>/v1/part_1/

# Check meta.json
docker compose exec api cat /var/lib/hippius/object_cache/<object_id>/v1/part_1/meta.json
```

### 5.4 Inspecting Redis state

```bash
# General
docker compose exec redis redis-cli

# Queues
docker compose exec redis-queues redis-cli
# Inside:
LLEN arion_upload_requests
LRANGE arion_upload_requests 0 5
SUBSCRIBE 'notify:*'

# DLQ
docker compose exec redis-queues redis-cli LLEN arion_upload_requests:dlq
docker compose exec redis-queues redis-cli LRANGE arion_upload_requests:dlq 0 -1

# Download-coalescing locks (expect transient entries)
docker compose exec redis keys 'download_in_progress:*'
```

### 5.5 Inspecting Postgres state

```bash
docker compose exec postgres psql -U hippius -d hippius
```

Useful queries:

```sql
-- Recent object_versions
SELECT object_id, object_version, size_bytes, md5_hash, storage_version, kek_id IS NOT NULL AS has_kek
FROM object_versions ORDER BY created_at DESC LIMIT 10;

-- Broken v5 rows (the known-issue population)
SELECT COUNT(*) FROM object_versions
WHERE storage_version >= 5 AND (kek_id IS NULL OR wrapped_dek IS NULL);

-- chunk_backend status for one object
SELECT backend, backend_identifier, deleted, deleted_at
FROM chunk_backend
WHERE chunk_id IN (
  SELECT pc.chunk_id FROM part_chunks pc
  JOIN parts p USING (upload_id)
  WHERE p.object_id = '<uuid>'
);
```

### 5.6 Peeking a DLQ

```bash
python -m hippius_s3.scripts.dlq_requeue peek --queue arion_upload_requests:dlq --limit 5
python -m hippius_s3.scripts.dlq_requeue stats --queue arion_upload_requests:dlq
```

### 5.7 Enabling request profiling

Set `ENABLE_REQUEST_PROFILING=true` in `.env`, restart the API. [SpeedscopeProfilerMiddleware](hippius_s3/api/middlewares/profiler.py) emits per-request flame graphs to `/tmp/` inside the container. Copy them out and open in https://www.speedscope.app/.

Turn it off when you're done — there's measurable overhead.

---

## 6. Recipes

Concrete walkthroughs for common tasks. Each assumes you've done §1 setup.

### 6.1 Add a new S3 endpoint

Example: add `GET /{bucket}/{key}?acl` to return an object's ACL as XML.

1. **Handler**. Create [hippius_s3/api/s3/objects/get_object_acl_endpoint.py](hippius_s3/api/s3/objects/). Modeled after [get_object_endpoint.py](hippius_s3/api/s3/objects/get_object_endpoint.py):
   ```python
   async def handle_get_object_acl(bucket_name: str, object_key: str, request: Request, db: Any) -> Response:
       ...
   ```
2. **Router**. In [hippius_s3/api/s3/objects/router.py](hippius_s3/api/s3/objects/router.py), the GET route already exists for `/{bucket}/{key}`. Add a branch: if `"acl" in request.query_params`, dispatch to the new handler.
3. **Query**. If you need new SQL, add `hippius_s3/sql/queries/get_object_acl.sql` and load via `get_query("get_object_acl")`.
4. **Tests**. [tests/unit/api/](tests/unit/) for handler logic, [tests/e2e/](tests/e2e/) for end-to-end via boto3.
5. **Gateway permission mapping**. If the new endpoint requires a different S3 permission than the default mapping in [gateway/middlewares/acl.py:36-67](gateway/middlewares/acl.py), update it.

### 6.2 Add a database migration

1. Create `hippius_s3/sql/migrations/NNNN_description.sql` where `NNNN` is the next sequential number. Look at existing migrations for formatting.
2. **Always additive**. No `DROP COLUMN`, no destructive changes in-place — add new columns, do the rename in code, remove the old column in a later release.
3. Run locally: `python -m hippius_s3.scripts.migrate`.
4. Write a query if it's non-trivial: add to `hippius_s3/sql/queries/` and load via `get_query()`.
5. Test that the migration is idempotent (run twice, second should no-op).

### 6.3 Add a new worker

Say you're adding a `reconciler` worker that compares DB state with Arion state nightly.

1. **Core loop**. Add `hippius_s3/workers/reconciler.py` with the main async function.
2. **Entry script**. Add `workers/run_reconciler_in_loop.py`:
   ```python
   if __name__ == "__main__":
       asyncio.run(run_reconciler_loop())
   ```
3. **Docker**. Add a service to `docker-compose.yml` modeled on the other `workers/run_*` services.
4. **K8s**. Add a `Deployment` (or `CronJob` for scheduled work) under `k8s/base/workers-deployments.yaml`.
5. **Config**. Any new env vars go in `hippius_s3/config.py` with a sensible default.
6. **Tests**. Unit-test the inner logic with mocked DB/Redis; integration-test the loop with a real but empty DB.

### 6.4 Add a gateway middleware

1. Write the middleware in `gateway/middlewares/<name>.py` following the FastAPI `@app.middleware("http")` shape.
2. Register it in `gateway/main.py`. **Remember registration order is reversed** — last-registered runs first. Place it in the chain where semantically appropriate (before or after auth? before or after ACL?).
3. Test in [tests/unit/gateway/](tests/unit/gateway/). Mock upstream responses.
4. If it reads/writes Redis, wire a client in `gateway/main.py`'s startup handler.

### 6.5 Add a config/env var

1. Add a field to the `Config` dataclass in [hippius_s3/config.py](hippius_s3/config.py). Use the `env(...)` helper with a default:
   ```python
   my_new_flag: bool = env("HIPPIUS_MY_NEW_FLAG:false", convert=lambda x: x.lower() == "true")
   ```
2. Add the default to `.env.defaults`.
3. Add the `.env.example` line so people know it exists.
4. Add to the secrets table in the root `CLAUDE.md` (section 8) if it needs a real value in prod.
5. If prod/staging reads it from k8s secrets, update `.github/workflows/k8s-deploy.yaml`.

### 6.6 Trace down a 500 that reached prod

1. Get the ray id from client or from error tracking (Sentry, if wired).
2. Check structured logs in Loki: `{service="api"} |= "<ray_id>"`.
3. Open Tempo with the same ray id for the span timeline.
4. If it's a `DownloadNotReadyError`, the downloader is slow or stuck — check `arion_download_requests` queue depth.
5If it's a timeout, check if it's upstream (Arion, KMS, chain API).

### 6.7 Requeue a failed upload

```bash
# See what's in the DLQ
python -m hippius_s3.scripts.dlq_requeue stats --queue arion_upload_requests:dlq
python -m hippius_s3.scripts.dlq_requeue peek --queue arion_upload_requests:dlq --limit 5

# Requeue a specific one
python -m hippius_s3.scripts.dlq_requeue requeue --queue arion_upload_requests:dlq --identifier <object_id>

# Requeue everything (use --force to include permanent errors)
python -m hippius_s3.scripts.dlq_requeue requeue-all --queue arion_upload_requests:dlq
```

### 6.8 Run a one-shot migration as a k8s Job

Example: the Arion identifier migration:

```bash
# Locally
python -m hippius_s3.scripts.migrate_arion_identifiers --dry-run

# In k8s (staging)
kubectl -n hippius-s3-staging apply -f k8s/migrate-arion-identifiers-job.yaml
kubectl -n hippius-s3-staging logs -f job/migrate-arion-identifiers
```

---

## 7. Code style & conventions

### 7.1 Style

- Line length 120.
- Ruff-format on save. `ruff check . --fix` before committing.
- Single-line imports. Isort is configured with `force-single-line = true`.
- Type hints everywhere. mypy is strict.
- No blanket `# type: ignore`; narrow it to a code (`# ty: ignore[unresolved-import]`).

### 7.2 Error handling

**Avoid `try/except` unless absolutely necessary.** Let errors bubble up. The default-deny here comes from hard experience: swallowed errors make debugging 10× harder, and FastAPI's exception handler + our global handler in [hippius_s3/main.py:301-315](hippius_s3/main.py) turns common exceptions into meaningful S3 errors.

Legitimate `try/except` cases:
- Resource cleanup in `finally`.
- Best-effort operations that genuinely shouldn't kill the request (e.g. Redis metric writes).
- Catching a specific, expected exception type and translating to a domain exception.

If you're about to write `try/except Exception`, stop and rethink.

### 7.3 Comments

- Default to **no comments**. Code that needs explanation usually needs refactoring.
- Write a comment only when *why* is non-obvious: a workaround, a hidden constraint, a race window, a past incident.
- Never write docstrings at the top of modules (waste of space). Per-function docstrings are fine when behavior is non-trivial.
- No "this function does X" — that's in the name. "This function does X because of Y constraint" is worth writing.

### 7.4 Patterns to follow

- **Never buffer large payloads**. Use `AsyncIterator[bytes]` and stream. See `put_simple_stream_full` and `ForwardService`.
- **Atomic writes to shared state**. FS writes always go through tmp + rename. DB writes use transactions (or atomic CTEs like `upsert_object_basic`). Redis mutations should be `SET NX EX` or pipelined.
- **Pub/sub for readiness, not for transport**. The FS is the source of truth; pub/sub tells you "go check again".
- **Ray-id propagation**. Any cross-service call must carry the ray_id. Every log line should have it.
- **Use the query loader**: `get_query("name")` loads from `hippius_s3/sql/queries/name.sql`. Don't inline SQL in Python.

### 7.5 Patterns to avoid

- **Blocking calls in async paths**. Use `asyncio.to_thread(blocking_fn)` for CPU/disk work. Grep for `asyncio.to_thread` to see existing examples.
- **`print`**. Use the logger.
- **`time.sleep`**. Use `asyncio.sleep`.
- **Creating a new Redis/DB client per request**. Use `request.app.state.redis_client` / `request.app.state.postgres_pool`.
- **Swallowing asyncio.CancelledError**. Let it propagate so tasks can be cancelled cleanly.

---

## 8. Contribution workflow

### 8.1 Branches

- `main` — production-ish, stable.
- `staging` — auto-deploys to `hippius-s3-staging` on push.
- `k8s-production` — auto-deploys to `hippius-s3-prod` on push. Guarded by review.
- Feature branches: `feat/<short-description>`, `fix/<short-description>`, `refactor/<short-description>`, `chore/<short-description>`, `docs/<short-description>`.

### 8.2 Commits

- Conventional-style messages preferred: `feat(cache): add fs touch_part on uploader success`.
- Don't include "Generated by Claude Code" footers — we don't mark those.
- **Never commit directly to `main`, `staging`, or `k8s-production`** — always via PR.
- `--no-verify` is reserved for Radu and only with a reason.

### 8.3 PRs

1. Branch from `main`. Push early, open a PR in draft if you want feedback mid-flight.
2. CI runs lint + unit + integration. PRs don't merge red.
3. E2E is run on PR for anything touching the data plane.
4. Ping Radu (`radu.mutilica`) for review on non-trivial changes.
5. Title: short (<70 chars). Body: a summary + bullet list of changes, biggest to smallest, ending with a short conclusion.
6. Don't squash large refactors — a clean sequence of commits is easier to bisect.

### 8.4 Review expectations

We move fast. Reviewers look for:

- Glaring correctness issues (races, missing auth, unchecked input).
- Security red flags (SQL injection, leaked secrets in logs, unbounded inputs).
- Hot-path performance regressions.
- Missing tests at the appropriate tier.

We don't nitpick style (ruff handles that). We don't block on taste.

### 8.5 Releases

- Staging: push to `staging`. Auto-deploys. Smoke tests run.
- Production: merge `staging` → `main`, then push `main` → `k8s-production`. Don't skip staging.
- Rollbacks: revert the merge on `k8s-production`, CI re-deploys.

---

## 9. Common pitfalls new devs hit

### 9.1 "My changes don't show up"

- Did you save the file?
- Are you hitting the gateway (`:8080`) or the api (`:8000`)? The gateway forwards, so sometimes your change to an endpoint works on `:8000` direct but your client is talking to `:8080`.
- Is uvicorn's reloader stuck? `docker compose restart api`.

### 9.2 "The test passes locally but fails in CI"

- CI runs with `HIPPIUS_BYPASS_CREDIT_CHECK=true` and `ENABLE_BANHAMMER=false`. Check your local env isn't setting them differently.
- Timing-sensitive tests against Redis pub/sub are often flaky. Use generous timeouts.
- If your test requires a specific FS cache state, make sure you clean it in setup.

### 9.3 "Upload succeeds but GET returns 500"

Almost always a broken-v5 row — the version got reserved but the envelope wasn't written. The write-side fix is in [object_writer.py:244-261](hippius_s3/writer/object_writer.py); if you're seeing fresh broken rows, check your code path writes `kek_id` and `wrapped_dek` before the object_versions row becomes serveable.

### 9.4 "Streamer hangs forever on GET"

The download-coalescing lock has a typo somewhere. The lock key format MUST be exactly:

```
download_in_progress:{object_id}:v:{object_version}:part:{part_number}
```

— no leading/trailing spaces, no zero-padding, int-cast the version and part number. If the streamer sets lock key X and the downloader deletes lock key Y, everyone waits forever. See [tests/unit/test_download_coalescing.py](tests/unit/test_download_coalescing.py).

### 9.5 "My chunks don't decrypt"

AEAD AAD binds `(bucket_id, object_id, part_number, chunk_index, upload_id)`. If any of these differ between write and read, decryption silently fails with `InvalidTag`. Common causes:

- Copy operations that don't rewrap the DEK under the destination's AAD.
- Chunk index reshuffling (don't do this — always encrypt with the **global** chunk index).
- Wrong `upload_id` — simple PUT uses `""`, MPU uses the real id.

### 9.6 "Tests create files in `/var/lib/hippius/...` and don't clean up"

Unit tests must use `tmp_path` fixture, not the real cache dir. If you see `/var/lib/hippius` in a test, it's wrong.

### 9.7 "Janitor deleted something I needed"

It didn't. The janitor has hard invariants:
- Replicated to all required backends → eligible.
- NOT replicated → never eligible, ever.

If something "was deleted", check whether it was actually replicated. If not, it couldn't have been the janitor. Look at the upload worker instead.

### 9.8 "Arion auth failed"

Three possibilities:
- `ARION_SERVICE_KEY` / `ARION_BEARER_TOKEN` missing or wrong.
- Arion is rate-limiting; check your `X-Hippius-Bypass-Rate-Limiting` header logic.
- The `backend_identifier` you're looking up is stale — see the [migrate_arion_identifiers.py](hippius_s3/scripts/migrate_arion_identifiers.py) script.

---

## 10. Performance notes

### 10.1 Hot paths

- **`put_simple_stream_full`** — every PUT goes through this. Be extra careful about adding per-chunk awaits here.
- **`stream_plan`** — every GET. Prefetch depth is 0 by default for correctness; enabling >0 is a perf win but needs exception-propagation care.
- **Gateway `forward_service.py`** — every request. Don't add buffering steps.
- **`fs_store.set_chunk`** — every chunk write. The atomic rename is already optimal; don't add fsync.
- **`fs_store.get_chunk`** — every chunk read. The `os.utime` is cheap; keep it.

### 10.2 Things that look expensive but aren't

- `os.utime` on chunk read. Microseconds.
- JSON marshalling for meta.json. The file is tiny.
- Redis pub/sub. Fan-out is bounded by the number of concurrent streamers on the same chunk.

### 10.3 Things that are expensive

- **Backend (Arion) roundtrips**. 500ms-1.5s each. Batch, coalesce, retry intelligently.
- **Synchronous FS ops on large dirs** (e.g. `os.listdir` on a hot part dir). Use `os.scandir`.
- **Postgres queries** that don't use indexes. Check `EXPLAIN ANALYZE` when adding queries.
- **Cross-pod CephFS coordination** — atomic rename is fast, but reading a file that was just renamed from another pod may have a brief consistency lag.

### 10.4 Measuring before optimizing

- Grafana → Hippius S3 Overview → p50/p95/p99 per endpoint.
- `X-Hippius-Gateway-Time-Ms` response header tells you how long the gateway spent.
- Speedscope profiler (§5.7) shows per-function cost.
- `py-spy` works against the running API container if you shell in: `py-spy top --pid 1`.

Don't optimize based on intuition. Measure, change, re-measure.

---

## 11. Glossary

- **AAD** — Additional Authenticated Data. Part of AEAD. Binds a ciphertext to a context so rewrapping requires re-encryption.
- **AEAD** — Authenticated Encryption with Associated Data. AES-256-GCM is an AEAD suite.
- **Arion** — Our backend storage service. Every object chunk lives on Arion. API client: [hippius_s3/services/arion_service.py](hippius_s3/services/arion_service.py).
- **CID** — Content IDentifier. A content-addressed identifier from the backend. Stored in `chunk_backend.backend_identifier`.
- **DCR** — DownloadChainRequest. A message on `arion_download_requests` describing parts + chunks to fetch.
- **DEK** — Data Encryption Key. Per-object-version AES key. Wrapped by the bucket KEK.
- **DLQ** — Dead-Letter Queue. Redis list for permanently-failed operations. See [hippius_s3/dlq/CLAUDE.md](hippius_s3/dlq/CLAUDE.md).
- **FS cache** — the filesystem-backed chunk cache at `/var/lib/hippius/object_cache`. See [hippius_s3/cache/CLAUDE.md](hippius_s3/cache/CLAUDE.md).
- **Janitor** — FS cache GC worker. [workers/run_janitor_in_loop.py](workers/run_janitor_in_loop.py).
- **KEK** — Key Encryption Key. Per-bucket AES key that wraps object DEKs. Stored (wrapped by KMS master) in the keystore DB.
- **KMS** — Key Management Service. OVH KMS in prod, mocked locally.
- **MPU** — MultiPart Upload. Standard S3 thing. `parts` table holds these.
- **Object version** — one row per PUT/overwrite/append on an object. The `current_object_version` pointer on `objects` points at the live one.
- **Part** — in MPU, one of the component parts. In simple PUT, there's still a part (part_number=1).
- **Part chunk** — a chunk within a part. Chunk size defaults to 4 MiB but is stored per-part in the DB.
- **Pub/sub chunk notification** — Redis pub/sub on `notify:{chunk_key}`. Used by streamers to wake up when a downloader lands a chunk.
- **Ray id** — `X-Ray-ID` correlation id, generated at the gateway, propagated everywhere.
- **S4** — Hippius's S3 extension with atomic append. Spec at [docs/s4.md](docs/s4.md).
- **SigV4** — AWS Signature Version 4. The signing scheme we accept for S3 requests.
- **Storage version** — version of the crypto + layout scheme for an object_version. v5 is current; v≤4 is decrypt-only.
- **Subaccount / seed phrase** — alternative to access keys. Derives SS58 address from a 12-word mnemonic.

---

## 12. Where to ask for help

- **Slack/Discord**: the team channel. Please search first; most questions have been answered.
- **Radu directly** (`radu.mutilica`) for:
  - Anything touching KMS / encryption.
  - Anything touching the janitor's replication invariants.
  - Re-enabling disabled features (`deploy-cache-production`, rate-limit, banhammer).
  - Destructive scripts in [hippius_s3/scripts/CLAUDE.md](hippius_s3/scripts/CLAUDE.md).
- **Open a GitHub Issue** for bugs, regressions, or design discussion. Link the ray_id or a specific commit.
- **[todo.md](todo.md)** lists known-but-unsolved issues with context — check there before filing a new issue.

---

## TL;DR first day

1. Clone, venv, `uv pip install -e ".[dev]"`, `pre-commit install`.
2. `cp .env.example .env`, fill required fields, `HIPPIUS_KMS_MODE=disabled`.
3. `docker compose up -d && docker compose logs -f api`.
4. `pytest tests/unit -v` — all green.
5. Read [CLAUDE.md](CLAUDE.md) end-to-end.
6. Skim [todo.md](todo.md); pick a P2.
7. Branch, code, test, PR.

Welcome aboard.
