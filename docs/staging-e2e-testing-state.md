# Staging E2E Testing — Current State & Path Forward

**Status:** discussion doc for the team · **Author:** Camden (with Claude) · **Date:** 2026-06-22

## TL;DR

"End-to-end testing for staging" largely **already exists** — it's the `tests/smoke/`
suite, which is endpoint-driven and staging-aware, and there's already a
`staging-smoke-tests.yml` workflow that points it at `s3-staging.hippius.com`.

The real gaps are **automation** and **coverage**, not building from scratch:

1. The staging smoke workflow is **manual (`workflow_dispatch`) only** — nothing
   exercises the staging data path automatically after a deploy. The deploy's only
   post-step is `kubectl get pods/services/ingress`.
2. The big `tests/e2e/` suite (60 tests) is **hard-wired to the local mock stack**
   and is *not* portable to a live remote endpoint without significant rework — and
   much of it is inherently local (fault injection, direct-DB assertions).

Recommended 80/20: wire the existing smoke suite to run automatically after each
staging deploy, confirm/provision the CI secrets, then optionally port high-value
e2e scenarios into the remote-capable smoke style over time.

---

## The two test suites

### `tests/e2e/` — local-only, NOT staging-portable

60 test files covering the full S3 surface (PutObject, Range GET, MPU, Copy,
Versioning, ACLs, EncryptionAtRest, DLQ, Backend fanout/resilience, Append/S4, …).
These run against the **local docker-compose stack with mocked backends**
(`mock-arion`, `mock-kms`, `mock-hippius-api`, `toxiproxy`) and are invoked in the
PR workflow, not on staging.

Why it can't simply be "pointed at" staging (`tests/e2e/conftest.py`,
`tests/e2e/support/`):

- **Hardcoded local endpoint** — `endpoint_url="http://localhost:8080"` in every
  boto3 client fixture (4+ places) and the scope client.
- **Toxiproxy dependency** — session fixtures assert `wait_for_toxiproxy()` and
  toggle the `mock-arion` / `mock-kms` proxies for fault-injection tests
  (`test_Backend_Resilience.py`, `test_EnvelopeRace.py`, etc.). You can't
  fault-inject a real KMS/Arion this way.
- **Direct Postgres access** — `support/chunks.py` connects straight to
  `postgresql://postgres:postgres@localhost:5432/hippius` to inspect chunk rows.
  CI has no route to the prod/staging DB.
- **Local-only assertions** — e.g. asserting the `x-hippius-source: cache|pipeline`
  header, which is Hippius-internal behavior not meaningful as a black-box check.
- The existing `RUN_REAL_AWS=1` mode points boto3 at **real AWS** (for S3-parity
  checks) and *skips* all Hippius-specific tests — it is not a "run against staging"
  switch.

**Conclusion:** remotifying `tests/e2e/` is a large rework with low marginal value;
a meaningful fraction of the suite is fundamentally local. Not recommended as the
path to staging coverage.

### `tests/smoke/` — already remote & staging-aware

This is the suite designed to hit a live, deployed endpoint
(`tests/smoke/conftest.py`):

- **Endpoint-driven** — `HIPPIUS_ENDPOINT` env var; real SigV4 auth via
  `AWS_ACCESS_KEY` / `AWS_SECRET_KEY`.
- **Explicitly staging-aware** — `target_environment = "staging" if "s3-staging."
  in endpoint`; `AccessDenied` on the shared bucket is handled with a "running
  against staging with a different account" skip; the master account SS58 is
  discovered at runtime via `list_buckets()["Owner"]["ID"]` so swapping
  staging/prod credentials "just works."
- **Real black-box coverage** (`test_smoke_production.py`): cleanup → simple PUT →
  multipart upload → download current-session → download historical → session
  manifest write/verify → presigned-URL roundtrip → CORS preflight.
- **Sub-token scope** (`test_smoke_subtoken_scope.py`): gated on
  `FRONTEND_HMAC_SECRET` + `HIPPIUS_USER_TOKEN`; self-skips cleanly if unset.
- Self-cleaning: sweeps stale smoke objects, orphan sub-token buckets, and orphan
  sub-tokens older than the retention window.

**This suite *is* the staging E2E.** It just isn't automated.

---

## Existing automation

`.github/workflows/staging-smoke-tests.yml`:

- Trigger: **`workflow_dispatch` only** (manual).
- Runs `test_smoke_production.py` + `test_smoke_subtoken_scope.py` against
  `HIPPIUS_ENDPOINT=https://s3-staging.hippius.com`.
- Secrets used: `HIPPIUS_PROD_ACCESS_KEY`, `HIPPIUS_PROD_SECRET_KEY`,
  `FRONTEND_HMAC_SECRET`, `HIPPIUS_USER_TOKEN`.
  - **Note:** it currently reuses *prod* credentials against staging.
- Uploads a JSON report artifact (90-day retention).

`.github/workflows/staging-deploy.yaml`:

- Triggers on push to `staging`.
- Ends with a "Verify deployment" step that only runs
  `kubectl get pods/services/ingress` — **no functional test gate.**

---

## Gaps

1. **No automatic post-deploy verification.** A staging deploy can go green at the
   k8s level while the data path is broken; nothing catches it until someone
   manually triggers smoke. (Caveat: smoke wouldn't catch every issue — e.g. the
   `upload-promoter` crash-loop fixed on 2026-06-22 was a gated-off worker outside
   the PUT/GET path, so smoke would have stayed green. Pod-health monitoring is the
   complement here.)
2. **Credential strategy.** Smoke uses prod creds against staging. Decide whether
   to provision a dedicated staging smoke account instead (cleaner blast-radius and
   ownership semantics; avoids `AccessDenied` skips on the shared bucket).
3. **Coverage depth.** Smoke is ~8 flows + sub-token; the e2e suite has 60. Range
   GET, MPU edge cases, Copy variants, Versioning, and ACL paths are not covered
   against staging today.

---

## Recommended path

| # | Step | Effort | Value |
|---|------|--------|-------|
| 1 | Auto-run existing smoke after each staging deploy | Small (1 workflow edit) | High — makes staging an actually-gated env |
| 2 | Confirm / provision staging smoke CI secrets | Small | Required for #1 |
| 3 | Port high-value e2e scenarios into remote smoke style | Medium | Deeper coverage; do incrementally |
| — | ~~Remotify `tests/e2e/`~~ | Large | Not recommended — much of it is inherently local |

### Step 1 — proposed implementation

Add a `smoke-staging` job to `staging-deploy.yaml` gated on the deploy job, so the
check lives in the same workflow run and is visible alongside the deploy:

```yaml
  smoke-staging:
    needs: [deploy-staging]
    runs-on: ubuntu-latest          # GitHub-hosted: s3-staging endpoint is public
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install -r tests/smoke/requirements.txt
      - name: Smoke against s3-staging.hippius.com
        env:
          AWS_ACCESS_KEY: ${{ secrets.HIPPIUS_STAGING_ACCESS_KEY }}   # see decision below
          AWS_SECRET_KEY: ${{ secrets.HIPPIUS_STAGING_SECRET_KEY }}
          HIPPIUS_ENDPOINT: https://s3-staging.hippius.com
          FRONTEND_HMAC_SECRET: ${{ secrets.FRONTEND_HMAC_SECRET }}
          HIPPIUS_USER_TOKEN: ${{ secrets.HIPPIUS_USER_TOKEN }}
        run: pytest tests/smoke/test_smoke_production.py tests/smoke/test_smoke_subtoken_scope.py -v --tb=short
```

Alternative: keep smoke as its own workflow and add a `workflow_run` trigger that
fires it after `staging-deploy` completes. (Downside: `workflow_run` results don't
surface as a check on the triggering run and always use the default-branch workflow
file — the inline job is more visible.)

Add a deploy delay / readiness wait before smoke runs so pods are actually serving
(the deploy job already does `kubectl rollout status`, so chaining on `needs`
mostly covers this).

---

## Open decisions for the team

1. **Credentials:** reuse prod creds against staging (status quo) **vs.** provision a
   dedicated staging smoke account? (Recommend dedicated — cleaner ownership, no
   `AccessDenied` skips, no prod-creds-in-staging-CI smell.)
2. **Failure policy:** should a red smoke run **block/alert** (hard gate, page or
   Mattermost) or be **informational** only at first?
3. **Coverage scope:** which e2e scenarios are worth porting to smoke first?
   (Candidates: Range GET, MPU complete/abort, CopyObject cross-bucket, Versioning,
   ACL/public-read.)
4. **Cadence:** post-deploy only, or also on a schedule (e.g. hourly) as a
   continuous staging health signal?

---

## Appendix — key files

- `tests/e2e/conftest.py`, `tests/e2e/support/compose.py`, `tests/e2e/support/chunks.py`
  — local/toxiproxy/DB coupling.
- `tests/smoke/conftest.py`, `tests/smoke/test_smoke_production.py`,
  `tests/smoke/test_smoke_subtoken_scope.py` — the remote suite.
- `tests/smoke/requirements.txt` — boto3, httpx, pytest, pytest-json-report, pytest-xdist.
- `.github/workflows/staging-smoke-tests.yml` — existing manual workflow.
- `.github/workflows/staging-deploy.yaml` — deploy workflow (hook point for step 1).
- `scripts/print_smoke_test_secrets.sh` — referenced for generating smoke secrets.
