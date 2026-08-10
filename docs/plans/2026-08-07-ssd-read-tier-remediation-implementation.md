# SSD Read-Tier Remediation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task.

**Goal:** Fix the 15 valid findings from the PR #398/#400 adversarial review plus one gap found
while checking them, closing an unauthenticated public data path and three silent-failure modes in
the SSD read tier.

**Architecture:** Ten independent PRs. The trust boundary (WI-1) is closed with a shared secret the
gateway already cannot forge, plus a fail-closed serve flag. The two control-loop bugs (WI-2, WI-3)
are fixed in Rust, with WI-3 moving the promote floor from a mirrored constant to a number the
agent publishes. The peer read path (WI-5, WI-6, WI-10) gains input validation, an exact size
check, and AEAD-failure invalidation. Everything else is a metric, a comment, or a config knob.

**Tech Stack:** Python 3.13 (FastAPI, httpx, asyncpg, pytest, ruff, ty) and Rust (tokio, sqlx,
redis, thiserror). Redis for the agent↔api contract. Kustomize for k8s.

**Design rationale for every decision here lives in
[2026-08-07-ssd-read-tier-review-remediation.md](2026-08-07-ssd-read-tier-review-remediation.md).**
That document is the spec; this one is the build order. When they disagree, the spec wins — but
they should not disagree, so fix both.

---

## Ground rules

**Read before you start.** [CLAUDE.md](../../CLAUDE.md) §9.3 (repo conventions) and
[hippius_s3/cache/CLAUDE.md](../../hippius_s3/cache/CLAUDE.md). Two conventions bite hardest here:

- **Avoid `try/except` unless necessary.** Let errors bubble. The exceptions in this plan are all
  in best-effort paths (metrics, promotion, registration) and each is called out where it appears.
- **Comments explain WHY, never WHAT.** Several tasks here are *entirely* comment changes because
  the comment is the safety argument. Write them as reasoning, not description.

**Test naming is behavioural.** Existing examples set the bar:
`test_a_single_readers_prefetch_window_never_sheds_against_an_idle_peer`,
`test_a_pool_only_chunk_is_a_404_not_a_pool_read`. Name the property, not the function.

**Every task follows RED → GREEN → COMMIT.** Write the test, run it, *watch it fail for the right
reason*, implement, run, commit. A test that passes before the fix is a broken test — go back and
make it exercise the real path. This matters more than usual here: several findings are "the alert
never fires", and a test that cannot distinguish silence from success is worthless.

**Baselines to beat, per PR:**

```bash
source .venv/bin/activate
pytest tests/unit -q                                        # 2477 passed, 37 skipped
cargo test -p hippius-drain-core --lib                      # 225 passed
ruff check . --fix && ruff format . && ty check hippius_s3 gateway
cargo clippy --all-targets --all-features -- -D warnings && cargo fmt
```

**Branch per work item**, off `staging`, named `fix/<wi>-<slug>`. Never commit to `staging`
directly. Never push unless asked.

---

## Task 0: Pre-flight reserved-name audit — BLOCKS WI-1

**This is not optional and it is not code.** WI-1 rejects every request whose first path segment is
`internal`. If a customer already owns a bucket by that name, WI-1 silently kills every read on it.
The audit script is read-only; run it before writing any code.

**Step 1: Run the audit against staging**

```bash
source .venv/bin/activate
DATABASE_URL="<staging-url>" python -m hippius_s3.scripts.report_reserved_name_buckets
```

**Step 2: Run it against prod**

```bash
DATABASE_URL="<prod-url>" python -m hippius_s3.scripts.report_reserved_name_buckets
```

**Step 3: Grep for a bucket literally named `internal`**

The script reports on the *current* `RESERVED_BUCKET_SEGMENTS`, which does not yet contain
`internal`. So also run directly:

```sql
SELECT bucket_name, main_account, created_at
FROM buckets WHERE bucket_name = 'internal';
```

**Expected:** zero rows on both environments.

**If it returns rows: STOP.** Do not proceed to WI-1. This is an ops conversation — someone owns
that name and WI-1 will break them. Options are a rename with the customer, or changing the peer
route prefix to something unclaimable (e.g. `/-internal/`, which is not a legal bucket name). Take
it to the team before writing code.

**Step 4: Record the result**

Paste both outputs into the WI-1 PR description. The next reviewer needs to see this ran.

---

## Task 1 (WI-1): Close the public peer-serve path — P0

**Finding A-1 + A-8.1.** `GET /internal/parts/{object_id}/{version}/{part}/chunks/{index}` is
reachable unauthenticated from the public internet: the gateway catch-all forwards it, anonymous
auth returns valid for a GET, the ACL middleware passes through because no bucket named `internal`
exists, and `internal_parts_router` is registered before the S3 catch-all so it wins the route
match. `ip_whitelist` is not the boundary it is believed to be, because the gateway is inside it.

**Files:**
- Create: `hippius_s3/peer_auth.py`
- Modify: `hippius_s3/api/internal_parts.py`
- Modify: `hippius_s3/main.py:151-179` (peer wiring), `hippius_s3/main.py:428` (router mount)
- Modify: `hippius_s3/config.py` (after `peer_serve_max_inflight`, ~line 410)
- Modify: `hippius_s3/cache/peers.py` (`PeerChunkFetcher.__call__`, ~line 264)
- Modify: `gateway/middlewares/input_validation.py:86`
- Modify: `k8s/staging/api-local-deployments-staging.yaml`, `k8s/base/` secret
- Test: `tests/unit/test_internal_parts_endpoint.py` (extend),
  `tests/unit/test_peer_handshake.py` (create),
  `tests/unit/test_internal_route_precedence.py` (create),
  `tests/unit/gateway/test_input_validation_internal.py` (create)

### Step 1: Create the shared header constant

Both sides must derive the header name from one place, or a casing mismatch takes the tier dark
and every one-sided test still passes.

Create `hippius_s3/peer_auth.py`:

```python
from __future__ import annotations

import hmac


# One definition, imported by both the fetcher and the endpoint. A header name duplicated as two
# string literals is how a fail-closed handshake silently stops matching: each side's own tests
# keep passing because each side agrees with itself.
#
# The X-Hippius- prefix is load-bearing, not cosmetic: the gateway strips every inbound
# x-hippius-* header before forwarding (gateway/services/forward_service.py:117-119), so a client
# cannot forge this one. That strip is what makes a shared secret sufficient here.
PEER_AUTH_HEADER = "X-Hippius-Peer-Auth"


def peer_auth_matches(presented: str | None, expected: str) -> bool:
    """Constant-time comparison of a presented peer secret against the configured one.

    Returns False when either side is empty, so an unset secret can never authenticate — the
    serve path must fail closed rather than degrade to "no auth required".
    """
    if not presented or not expected:
        return False
    return hmac.compare_digest(presented, expected)
```

### Step 2: Write the failing endpoint-auth tests

Append to `tests/unit/test_internal_parts_endpoint.py`. Note the existing `_app` helper does not
set a secret; add a parameter rather than changing its default, so the existing tests keep
describing the unauthenticated-store behaviour they were written for.

```python
from hippius_s3.peer_auth import PEER_AUTH_HEADER


SECRET = "a" * 64


def _app_with_secret(fs_store: object | None, secret: str = SECRET) -> FastAPI:
    app = _app(fs_store)
    app.state.peer_auth_secret = secret
    return app


@pytest.mark.asyncio
async def test_a_chunk_request_with_no_peer_auth_header_is_refused(tmp_path) -> None:
    """The endpoint must not be an existence oracle for anyone who can reach the api.

    404 rather than 403 deliberately: 403 would confirm the route exists and that the caller
    guessed a real (object, version, part), which is most of what the oracle was worth.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    async with await _client(_app_with_secret(store)) as client:
        response = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0")

    assert response.status_code == 404
    assert response.content != b"local-bytes"


@pytest.mark.asyncio
async def test_a_chunk_request_with_a_wrong_secret_is_refused(tmp_path) -> None:
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    async with await _client(_app_with_secret(store)) as client:
        response = await client.get(
            f"/internal/parts/{OBJ}/1/1/chunks/0", headers={PEER_AUTH_HEADER: "b" * 64}
        )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_a_chunk_request_with_the_right_secret_is_served(tmp_path) -> None:
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    async with await _client(_app_with_secret(store)) as client:
        response = await client.get(
            f"/internal/parts/{OBJ}/1/1/chunks/0", headers={PEER_AUTH_HEADER: SECRET}
        )

    assert response.status_code == 200
    assert response.content == b"local-bytes"


@pytest.mark.asyncio
async def test_an_unset_secret_refuses_every_request_rather_than_disabling_the_check(tmp_path) -> None:
    """Fail closed. An empty configured secret must not read as "no auth configured, allow all" —
    that is the exact shape of the bug this task exists to fix.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    async with await _client(_app_with_secret(store, secret="")) as client:
        response = await client.get(
            f"/internal/parts/{OBJ}/1/1/chunks/0", headers={PEER_AUTH_HEADER: ""}
        )

    assert response.status_code == 404
```

### Step 3: Run them and watch them fail

```bash
pytest tests/unit/test_internal_parts_endpoint.py -v
```

Expected: the three refusal tests **FAIL** (they get 200, because there is no auth check yet); the
"right secret" test **PASSES** trivially. That asymmetry is the finding — note it in the PR.

### Step 4: Add the auth check to the endpoint

In `hippius_s3/api/internal_parts.py`, add the import and insert the check as the **first** thing
in `get_local_chunk`, before the `fs_store` lookup:

```python
from hippius_s3.peer_auth import PEER_AUTH_HEADER
from hippius_s3.peer_auth import peer_auth_matches
```

```python
    # Authenticate BEFORE touching the store, so an unauthenticated caller cannot distinguish
    # "wrong secret" from "no such chunk" by timing or by status. The gateway strips inbound
    # x-hippius-* headers, so this header can only have been set by a peer inside the cluster.
    #
    # This check is the trust boundary. The api's ip_whitelist is NOT: the gateway is a pod on
    # the same network and forwards arbitrary paths from the public internet, which is how this
    # endpoint was reachable unauthenticated before (review finding A-1).
    if not peer_auth_matches(
        request.headers.get(PEER_AUTH_HEADER), getattr(request.app.state, "peer_auth_secret", "")
    ):
        return Response(status_code=404)
```

Also update the module docstring: the claim *"It sits behind the api's `ip_whitelist` middleware
(10.x/172.x pod network only)"* is the wrong safety argument and must be replaced with the shared
secret + gateway-strip reasoning.

### Step 5: Run the tests

```bash
pytest tests/unit/test_internal_parts_endpoint.py -v
```

Expected: **all PASS**, including the pre-existing ones (which use `_app`, so
`peer_auth_secret` is absent → `getattr` default `""` → refused). **They will now fail.**

That is correct and expected: those tests asserted the old unauthenticated contract. Update each to
use `_app_with_secret` and pass the header — they are testing local-vs-pool tier behaviour, not
auth, so the secret is incidental to what they mean.

### Step 6: Add the config knob

In `hippius_s3/config.py`, after `peer_serve_max_inflight`:

```python
    # Serving peers is now gated separately from fetching from them. A node that serves but does
    # not fetch still needs its in-flight cap, and the endpoint must not be mounted at all on a
    # deployment that does neither — it was previously mounted unconditionally while the fetch
    # flag was off, which is how it shipped reachable AND uncapped (review findings A-1, A-8).
    peer_serve_enabled: bool = env("HIPPIUS_PEER_SERVE_ENABLED:false", convert=bool)
    # Shared secret for /internal/*. Empty means the peer-serve route is not mounted: there is no
    # "authentication disabled" mode, because that is indistinguishable from the bug.
    internal_peer_secret: str = env("HIPPIUS_INTERNAL_PEER_SECRET:", convert=str)
```

### Step 7: Write the route-precedence test

The precedence is load-bearing and currently untested in either direction. Create
`tests/unit/test_internal_route_precedence.py`:

```python
"""The internal peer route must win against the S3 catch-all, and only for its own shape.

`/{bucket}/{key:path}` matches everything, so the only reason `/internal/parts/...` reaches the
peer handler is that its router is registered first (hippius_s3/main.py:428-429) and Starlette
matches in registration order. Nothing else enforces that, and reordering the includes would
silently route peer fetches into GetObject — which returns 403/404 to peers and, worse, makes any
bucket named `internal` unreachable. Both directions are asserted here.
"""

from __future__ import annotations

from hippius_s3.main import factory


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"


def _match(app, path: str) -> str:
    scope = {"type": "http", "method": "GET", "path": path, "headers": []}
    for route in app.routes:
        match, _ = route.matches(scope)
        if match.name == "FULL":
            return getattr(route, "name", "") or getattr(route.endpoint, "__name__", "")
    return ""


def test_the_internal_peer_route_wins_over_the_s3_catch_all(monkeypatch) -> None:
    monkeypatch.setenv("HIPPIUS_PEER_SERVE_ENABLED", "true")
    monkeypatch.setenv("HIPPIUS_INTERNAL_PEER_SECRET", "a" * 64)
    app = factory()
    assert _match(app, f"/internal/parts/{OBJ}/1/1/chunks/0") == "get_local_chunk"


def test_an_ordinary_bucket_key_still_reaches_the_s3_router(monkeypatch) -> None:
    monkeypatch.setenv("HIPPIUS_PEER_SERVE_ENABLED", "true")
    monkeypatch.setenv("HIPPIUS_INTERNAL_PEER_SECRET", "a" * 64)
    app = factory()
    assert _match(app, "/my-bucket/some/key.txt") != "get_local_chunk"


def test_the_route_is_absent_when_peer_serve_is_off(monkeypatch) -> None:
    """No flag, no route. A mounted-but-disabled endpoint is what shipped reachable and uncapped."""
    monkeypatch.setenv("HIPPIUS_PEER_SERVE_ENABLED", "false")
    app = factory()
    assert _match(app, f"/internal/parts/{OBJ}/1/1/chunks/0") != "get_local_chunk"
```

> **Note:** `factory()` may need env stubs to construct (KMS mode, DB URL). Check
> `tests/unit/conftest.py` for the existing fixture that other `factory()` tests use and reuse it
> rather than inventing new stubs. If no such fixture exists, build the app by calling
> `app.include_router(...)` in the same order as `main.py:427-429` and assert on that — the
> property under test is the ordering, not the factory.

### Step 8: Gate the router mount and build the limiter unconditionally

In `hippius_s3/main.py`, replace the unconditional `app.include_router(internal_parts_router, ...)`
at line 428 with a conditional mount, and decouple the limiter from `peer_fetch_enabled`:

```python
    # Mounted only when this node actually serves peers AND has a secret to authenticate them
    # with. Previously unconditional, which meant that with HIPPIUS_PEER_FETCH_ENABLED=false —
    # the prod setting — the endpoint was both reachable and uncapped, because the limiter was
    # built inside the fetch-flag branch and a missing limiter reads as "no cap".
    if config.peer_serve_enabled and config.internal_peer_secret:
        app.include_router(internal_parts_router, prefix="")
```

In the lifespan (~line 151-179), set `app.state.peer_auth_secret = config.internal_peer_secret` and
move `app.state.peer_serve_limiter = asyncio.Semaphore(config.peer_serve_max_inflight)` **out** of
the `if config.peer_fetch_enabled and node_name and pod_ip:` block, into its own
`if config.peer_serve_enabled:` block. Update the comment: it currently says *"Set whenever peer
fetch is on, since a node that fetches from peers is also one peers fetch from"* — that reasoning
is what coupled them, and it is wrong in the other direction.

### Step 9: Make the fetcher send the header

In `hippius_s3/cache/peers.py`, `PeerChunkFetcher.__init__` takes a new `auth_secret: str`, and
`__call__` sends it:

```python
                response = await self._client.get(url, headers={PEER_AUTH_HEADER: self._auth_secret})
```

Wire it from `main.py` with `config.internal_peer_secret`.

### Step 10: Write the handshake integration test — the one the rollout fears

Every test so far exercises one side. Create `tests/unit/test_peer_handshake.py`:

```python
"""Client and server must agree on the peer-auth handshake, end to end.

This is the test the fail-closed rollout actually needs. A header NAME or CASING mismatch between
PeerChunkFetcher and get_local_chunk passes every single-sided test in this repo — the endpoint
tests hand-build the header, the fetcher tests assert on a mock — and takes the whole peer tier
dark in production, visible only as chunk_reads_by_tier_total{tier=peer} going flat. So drive the
real fetcher against the really-mounted router with no hand-built headers anywhere.
"""

from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI

from hippius_s3.api.internal_parts import router
from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.peers import PeerChunkFetcher


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
SECRET = "c" * 64


@pytest.mark.asyncio
async def test_the_real_fetcher_authenticates_against_the_real_endpoint(tmp_path) -> None:
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await store.set_chunk(OBJ, 1, 1, 0, b"peer-bytes")
    await store.set_meta(OBJ, 1, 1, chunk_size=10, num_chunks=1, size_bytes=10)

    app = FastAPI()
    app.include_router(router)
    app.state.fs_store = store
    app.state.peer_auth_secret = SECRET

    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://peer")
    fetcher = PeerChunkFetcher(
        _pool_returning_node("peer-node"), _registry_returning("http://peer"),
        "this-node", client, auth_secret=SECRET,
    )

    assert await fetcher(OBJ, 1, 1, 0) == b"peer-bytes"


@pytest.mark.asyncio
async def test_a_secret_mismatch_between_the_two_sides_is_caught_here(tmp_path) -> None:
    """Guards the deploy-order hazard: new client, old secret (or vice versa) must be a test
    failure, not a silent fleet-wide fallback to pool reads."""
    # ... same wiring, fetcher built with "wrong-secret"
    # assert await fetcher(OBJ, 1, 1, 0) is None
```

Build `_pool_returning_node` / `_registry_returning` from the `FakePool` / `FakeRedis` helpers
already in `tests/unit/test_peer_fetch.py` — import them or lift them into a shared conftest
rather than writing a third copy.

### Step 11: Add the gateway denylist

In `gateway/middlewares/input_validation.py`, alongside the existing reserved-name rejection at
line 86, reject **any method** whose first path segment is `internal` (the existing check only
covers CreateBucket):

```python
    # Defence in depth behind the api's peer-auth secret. The api route is the real boundary; this
    # keeps a public request from reaching it at all, and holds even if the secret leaks.
    #
    # This is a blocklist and will rot — the durable fix is serving /internal/* from a port the
    # gateway cannot reach. Recorded as a follow-up; see the remediation plan WI-1.
    if path_parts and path_parts[0] == "internal":
        return s3_error_response(
            code="InvalidBucketName",
            message="Bucket name 'internal' is reserved",
            status_code=400,
        )
```

Test in `tests/unit/gateway/test_input_validation_internal.py`: a `GET /internal/parts/...` is
rejected 400 and `call_next` is never awaited.

### Step 12: Full suite + lint

```bash
pytest tests/unit -q && ruff check . --fix && ruff format . && ty check hippius_s3 gateway
```

### Step 13: k8s manifests

Add to `k8s/base/` secret: `HIPPIUS_INTERNAL_PEER_SECRET` (generate with `openssl rand -hex 32`).
Add `HIPPIUS_PEER_SERVE_ENABLED: "true"` and the secret ref to
`k8s/staging/api-local-deployments-staging.yaml`.

**Deploy the secret before the image.** This is fail-closed: an image that expects the secret
against a cluster that lacks it serves zero peer reads. Verify with
`kubectl -n hippius-s3-staging get secret <name> -o jsonpath='{.data.HIPPIUS_INTERNAL_PEER_SECRET}'`
before rolling out.

### Step 14: Commit

```bash
git add -A && git commit -m "fix: require a shared secret on the peer-serve endpoint

The endpoint was reachable unauthenticated from the public internet: the
gateway forwards arbitrary paths, anonymous auth succeeds for a GET, the ACL
middleware passes through on an unknown bucket, and the internal router is
matched before the S3 catch-all. ip_whitelist is not the boundary because the
gateway is inside it.

Serving is now gated on its own flag plus a secret, so the route is absent
rather than mounted-and-uncapped when the feature is off."
```

---

## Task 2 (WI-2): An unlink failure must not halt all eviction — P1

**Finding A-3.** `remover.unlink_part(...)?` at
[ssd_evict.rs:319](../../crates/hippius-drain-core/src/ssd_evict.rs) returns out of the page loop
before `mark_evicted`, which is the only thing that advances the cursor. The worklist is ordered
`COALESCE(last_read_at, resident_at)` — stable — so a persistently un-unlinkable part pins the head
and every pass dies on it having freed nothing. Neither `starved` nor `skipped_unreplicated` is set,
because the report is never produced.

**Files:**
- Modify: `crates/hippius-drain-core/src/ssd_evict.rs` (report struct ~line 60, page loop ~317-319)
- Modify: `crates/hippius-drain-agent/src/runtime.rs:355-395` (`evict_once`)
- Modify: `crates/hippius-drain-core/src/snapshot.rs` (~line 375, next to
  `record_evict_blocked_unreplicated`)
- Test: `crates/hippius-drain-core/src/ssd_evict.rs` `mod tests`

### Step 1: Write the failing tests

In the existing `mod tests`, add a remover that fails on a nominated part. Model it on the existing
fake remover; check what is already there before writing a new one.

```rust
#[tokio::test]
async fn a_candidate_whose_unlink_fails_does_not_stop_the_rest_of_the_page() {
    // The durability story depends on eviction actually running. A part that cannot be unlinked
    // (EIO, EACCES, EROFS, or ENOTEMPTY from a promotion renaming into the dir being removed)
    // used to abort the whole pass before mark_evicted, so it pinned the head of a stable
    // oldest-first cursor and every later pass died on the same row having freed nothing.
    let log = FakeLog::with_parts(5);
    let remover = FailingRemover::on_index(2);

    let report = evict_to_target(&log, &remover, &probe, &TestClock::new(), target, pass(5))
        .await
        .unwrap();

    assert_eq!(report.evicted, 4, "the other four parts must still be evicted");
    assert_eq!(report.remove_failed, 1);
    assert_eq!(log.marked_evicted().len(), 4, "and must be marked, or the cursor never advances");
}

#[tokio::test]
async fn a_persistently_failing_head_does_not_starve_later_passes() {
    // The regression that matters operationally: pass N+1 must not repeat pass N's zero.
    let log = FakeLog::with_parts(5);
    let remover = FailingRemover::on_index(0);

    let first = evict_to_target(...).await.unwrap();
    let second = evict_to_target(...).await.unwrap();

    assert!(first.evicted > 0 && second.evicted > 0);
}

#[tokio::test]
async fn a_failed_unlink_does_not_credit_its_bytes_to_the_early_stop() {
    // `projected` is the within-page early-stop. Crediting a part that was never unlinked stops
    // the page believing it freed more than it did — a phantom credit. The pass self-corrects on
    // the next page (free space is re-probed) but wastes an iteration under exactly the disk
    // pressure that makes iterations expensive.
    let log = FakeLog::with_parts_of_size(4, 10 * GIB);
    let remover = FailingRemover::on_index(0);
    // target needs 20 GiB freed; if the failed 10 GiB were credited only one more part evicts.

    let report = evict_to_target(...).await.unwrap();

    assert_eq!(report.evicted, 2, "two real unlinks are needed to cover the deficit");
}

#[tokio::test]
async fn remove_failed_is_zero_when_every_unlink_succeeds() {
    // Guards against a counter that always fires, which is the same as no counter.
}
```

### Step 2: Run and confirm the failure mode

```bash
cargo test -p hippius-drain-core --lib ssd_evict
```

Expected: the first three **FAIL**. The first two fail to compile (no `remove_failed` field) — add
the field first, then re-run so they fail on the *assertion*, which is what proves the test is
exercising the bug. Do not skip this.

### Step 3: Add the report field

```rust
    /// Parts whose unlink failed and were skipped. Not itself alertable — the existing
    /// `starved` covers "a whole page failed" — but it is the DISCRIMINATOR between
    /// starved-because-unlink-is-failing (a disk or permissions fault, or a promotion racing the
    /// removal) and starved-because-the-cursor-is-exhausted (genuine backlog). Those two have
    /// completely different operator responses and were previously indistinguishable, because
    /// the pass aborted before producing a report at all.
    pub remove_failed: u64,
```

### Step 4: Fix the page loop

Replace lines 317-319. **Order matters** — `projected` must advance only after a successful unlink:

```rust
            if let Err(err) = remover.unlink_part(&candidate.part).await {
                // Count and continue rather than propagating. The `?` here used to return before
                // `mark_evicted`, and mark_evicted -> drop_residency is the only thing that
                // advances the cursor, so one un-unlinkable part halted ALL eviction silently.
                report.remove_failed = report.remove_failed.saturating_add(1);
                tracing::warn!(part = %candidate.part, error = %err, "evict: unlink failed, skipping candidate");
                continue;
            }
            // Credited only now: a part that was not removed must not count toward the
            // within-page early stop.
            projected = projected.saturating_add(candidate.bytes);
            report.evicted += 1;
            freed_this_page = freed_this_page.saturating_add(candidate.bytes);
            evicted.push(candidate.part);
```

Delete the now-unused `EvictionError::Remove` variant **only if nothing else constructs it** —
`ssd_reclaim.rs` has its own `ReclaimError::Remove`, so check before removing.

### Step 5: Surface it in the agent

`snapshot.rs`: add `record_evict_remove_failed(&self, n: u64)` next to
`record_evict_blocked_unreplicated` (line 375), following that method's exact shape.

`runtime.rs` `evict_once`: call it, and **fold `remove_failed` into the existing `starved` ERROR
line** rather than adding a new alert:

```rust
            if report.starved {
                tracing::error!(
                    free_bytes = usage.free_bytes,
                    deficit = target.deficit(),
                    evicted = report.evicted,
                    remove_failed = report.remove_failed,
                    "eviction ran out of resident parts before restoring the free-space floor"
                );
            }
```

Add a separate `warn!` when `remove_failed > 0` but the pass was not starved.

> **Do not add a new hippius-otel alert rule.** The existing
> `if evicted.is_empty() { report.starved = true; break; }` already covers "a whole page failed to
> unlink" — it simply never ran, because the `?` short-circuited first. This restores an
> already-wired alert.

### Step 6: Green + lint + commit

```bash
cargo test -p hippius-drain-core --lib && cargo clippy --all-targets --all-features -- -D warnings && cargo fmt
git commit -m "fix: a failed part unlink must not abort the eviction pass"
```

---

## Task 3 (WI-3): Promote floor tracks the allocator's published reserve — P1

**Finding A-4.** `validate_promotion_band` checks
`evict_reserve < promote_floor < evict_reserve + headroom` once at startup against hardcoded
`EVICT_RESERVE_RATIO = 0.150` ([fs_pressure.py:98](../../hippius_s3/fs_pressure.py)). The deployed
evictor uses `allocated_reserve_permille.unwrap_or(policy.reserve_permille)`
([runtime.rs:309](../../crates/hippius-drain-agent/src/runtime.rs)) and the allocator interpolates
150→400 permille. At any allocated reserve ≥ 175 the ordering inverts and promotion writes while
the evictor is armed — precisely when the drain is in trouble.

**This is the largest task. Do it third, not first — WI-1 and WI-2 are smaller and higher-severity.**

**Files:**
- Modify: `crates/hippius-drain-agent/src/runtime.rs` (`eviction_target`, `evict_once`)
- Modify: `crates/hippius-drain-core/src/alloc.rs` (`reserve_permille` doc)
- Create: publisher — extend `runtime.rs` or add to `crates/hippius-drain-agent/src/metrics.rs`
- Modify: `hippius_s3/fs_pressure.py` (`FreeSpaceGate`)
- Modify: `hippius_s3/cache/__init__.py:38-41`
- Test: `crates/hippius-drain-agent/src/runtime.rs` tests,
  `tests/unit/test_promotion_pressure_guard.py`

### The contract

Follow the shape of `fs_cache:pressure`
([pressure_signal.py:11-19](../../hippius_s3/pressure_signal.py)) — it is the precedent and its
docstring documents the convention:

```
key   cephor:promote_floor:{node_name}          (main Redis, config.redis_url)
value {"floor_permille": int, "source": "drain-agent", "ts": unix}
SET every eviction poll (30s) with EX=120
```

**Publish the RESOLVED floor, not the inputs.** Publishing `reserve` + `headroom` and computing
`(reserve + headroom/2)/1000` in Python would still mirror — it mirrors the *formula* instead of
the constant, so the next time the Rust side changes what headroom means the Python expression is
silently wrong again. That is precisely finding A-4. Python divides by 1000 and does no policy
arithmetic.

### Step 1: Rust — write the failing property test

In `runtime.rs` tests:

```rust
#[test]
fn the_promote_floor_sits_strictly_inside_the_evictors_band_at_every_allocated_reserve() {
    // This is the assertion validate_promotion_band should always have been making, now in the
    // language that owns the constants. The Python side previously checked it against a hardcoded
    // 150 permille while the allocator was free to publish anything up to 400.
    for reserve in 150u16..=400 {
        let target = eviction_target(usage, policy, Some(reserve));
        let floor = promote_floor_permille(reserve, policy.headroom_permille);
        assert!(u64::from(reserve) < u64::from(floor), "floor must be above the reserve");
        assert!(
            u64::from(floor) < u64::from(reserve) + u64::from(policy.headroom_permille),
            "floor must be below the evictor's target, or it can never be restored"
        );
    }
}

#[test]
fn the_published_floor_tracks_the_allocated_reserve_not_the_static_one() {
    assert_eq!(promote_floor_permille(150, 50), 175);
    assert_eq!(promote_floor_permille(400, 50), 425);
}
```

### Step 2: Implement `promote_floor_permille` and publish it

```rust
/// The free-space floor below which read-through promotion must stop, in permille of disk.
///
/// The midpoint of the evictor's hysteresis band. It is the only point that neither chatters
/// (a floor equal to the target is live only in the instant a pass completes) nor deadlocks
/// (a floor above the target can never be restored, because the evictor never frees past it).
///
/// # Why this is published rather than mirrored
///
/// The api gates promotion on this number and CANNOT derive it: the reserve it must sit above is
/// whatever the allocator published for this node, which the api never sees. Mirroring the
/// constant in Python is what shipped broken (review finding A-4) — the check passed at startup
/// against 150 permille while the allocator was free to raise the live reserve to 400, inverting
/// the ordering exactly when a stalled drain made promotion most harmful.
///
/// Mirroring the FORMULA is the same bug one level up. Publish the resolved number.
fn promote_floor_permille(reserve_permille: u16, headroom_permille: u16) -> u16 {
    reserve_permille.saturating_add(headroom_permille / 2)
}
```

Publish from `evict_once` after resolving `target`, on the existing agent Redis connection
(`redis` is already a dependency — see `crates/hippius-drain-agent/src/enqueue.rs`). Best-effort:
a failed publish must not fail the eviction pass, and the api falls back to its static floor.

Add a cross-reference comment at `alloc.rs`'s `reserve_permille`: nothing on the Rust side
currently hints that a Python threshold depends on that number.

### Step 3: Python — write the failing tests

In `tests/unit/test_promotion_pressure_guard.py`:

```python
def test_a_published_floor_overrides_the_static_one() -> None:
    """The evictor's reserve is set at runtime by the allocator, so a startup-validated constant
    describes a system that may not exist. At a published floor of 425 permille, promotion must
    stop at 30% free — a level the static 0.175 floor would happily allow."""
    gate = FreeSpaceGate("/cache", 0.175, probe=lambda _p: 0.30, published_floor=lambda: 425)
    assert gate.allows() is False


def test_no_published_floor_falls_back_to_the_configured_one() -> None:
    """Fail open to today's behaviour. A missing signal must not disable the read tier."""
    gate = FreeSpaceGate("/cache", 0.175, probe=lambda _p: 0.30, published_floor=lambda: None)
    assert gate.allows() is True


def test_a_malformed_published_floor_falls_back_rather_than_raising() -> None:
    gate = FreeSpaceGate("/cache", 0.175, probe=lambda _p: 0.30, published_floor=lambda: "garbage")
    assert gate.allows() is True
```

### Step 4: Implement, run, commit

`FreeSpaceGate.__init__` takes `published_floor: Callable[[], int | None] | None = None`; `allows`
prefers it, memoised under the same 5s TTL as the statvfs probe. Log at `warn` + counter when the
published and static floors disagree — the divergence should be visible, not merely handled.

Keep `validate_promotion_band` as the startup check on the *static fallback*; update its docstring
to say it validates the fallback, not the live band, and point at the Rust property test for the
live one.

```bash
cargo test -p hippius-drain-agent && pytest tests/unit/test_promotion_pressure_guard.py -v
git commit -m "fix: derive the promote floor from the agent's published eviction band"
```

---

## Task 4 (WI-4): Reserved `internal` + real RFC1918 + doc fix — P2

**Findings A-2 and B-3.** Two small independent fixes, one PR, because both close A-1's
neighbourhood.

**Files:**
- Modify: `hippius_s3/reserved_bucket_names.py:32-41`
- Modify: `hippius_s3/api/middlewares/ip_whitelist.py:36-38`
- Modify: `hippius_s3/config.py`
- Modify: `hippius_s3/api/CLAUDE.md:27`
- Test: `tests/unit/test_report_reserved_name_buckets.py`,
  `tests/unit/test_api_routes_are_reserved_names.py` (create),
  `tests/unit/test_ip_whitelist_cidrs.py` (create)

### Step 1: Reserved name — failing test first

`tests/unit/test_api_routes_are_reserved_names.py`:

```python
def test_every_api_side_non_s3_route_segment_is_a_reserved_bucket_name() -> None:
    """The third source of danger, added by the peer tier and not covered by any existing guard.

    reserved_bucket_names.py documents two sources: a gateway route, and an auth exemption. The
    peer tier introduced a third — an API route that shadows the S3 catch-all — and
    test_every_auth_exempt_segment_is_a_reserved_bucket_name cannot catch it, because that test
    walks the GATEWAY's exempt segments and this is an api-side route.

    A bucket created under such a segment is unreachable by its owner yet permanently holds the
    globally-unique name (prod incident 2026-08-03, buckets "docs" and "docs2").
    """
    for segment in _api_static_first_segments():
        assert segment in RESERVED_BUCKET_SEGMENTS, (
            f"api route /{segment}/... shadows the S3 catch-all but is not a reserved bucket name"
        )
```

Run it — it **FAILS** on `internal`. Then add `"internal"` to the frozenset and extend the module
comment with the third source. Re-run: **PASSES**.

### Step 2: RFC1918 — boundary table test

`tests/unit/test_ip_whitelist_cidrs.py`:

```python
@pytest.mark.parametrize(
    "ip,allowed",
    [
        ("172.15.255.255", False),  # public, admitted by the old startswith("172.")
        ("172.16.0.0", True),
        ("172.31.255.255", True),
        ("172.32.0.0", False),      # public, admitted by the old startswith("172.")
        ("10.0.0.1", True),
        ("9.255.255.255", False),
        ("127.0.0.1", True),
        ("::1", True),
        ("not-an-ip", False),
        ("", False),
    ],
)
```

`172.15.255.255` and `172.32.0.0` **FAIL** before the fix. Then replace the `startswith` chain with
`ipaddress.ip_address` parsed against a CIDR list from config, defaulting to
`10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 127.0.0.1/32, ::1/128`. Deny on parse failure.

### Step 3: Delete the fictional env var from the docs

[hippius_s3/api/CLAUDE.md:27](../../hippius_s3/api/CLAUDE.md) says *"if `API_IP_WHITELIST` is
configured, only allows those IPs"*. That name exists nowhere in the codebase — I grepped; the
middleware hardcodes prefixes. Replace it with the real `API_IP_WHITELIST_CIDRS` in the same
commit, or the repo has one thing under two names, one imaginary.

### Step 4: Commit

```bash
git commit -m "fix: reserve the 'internal' bucket segment and match real RFC1918"
```

---

## Task 5 (WI-7): Residency-claim failure — metric + claim before write — P2

**Finding A-5.** A failed claim is `logger.debug` and nothing else
([residency.py:70-88](../../hippius_s3/cache/residency.py)), while a replicated part on disk with
no residency row has **no owner in either process** — `ssd_reclaim`'s `Replicated` arm only
increments `skipped_replicated`, and the evictor is scoped to the residency table. It leaks
permanently, on the disk that 503s PUTs when it fills.

**Do this before WI-6.** WI-6's exact-size check reads `part_chunks.cipher_size_bytes` alongside
this claim; landing WI-6 first means writing that query twice.

**Files:**
- Modify: `hippius_s3/cache/residency.py`, `hippius_s3/cache/dual_fs_store.py:207-215`,
  `hippius_s3/monitoring.py:31` (`PromotionSkipReason`)
- Test: `tests/unit/test_part_memo_and_promotion_cost.py` or a new
  `tests/unit/cache/test_promotion_residency.py`

### Step 1: Failing tests

```python
@pytest.mark.asyncio
async def test_a_failed_residency_claim_does_not_write_an_unclaimable_copy(tmp_path) -> None:
    """Fail closed on an optimisation.

    A residency-DB outage must disable promotion, not leak one unreclaimable copy per promoted
    chunk for its duration. The bytes are already served and the pool copy is authoritative, so
    refusing to warm the cache costs latency and nothing else — whereas an unclaimed copy is
    invisible to BOTH reclaimers and sits on the ingest disk forever.
    """
    store = _store_with_failing_recorder(tmp_path)
    await store.get_chunk(OBJ, 1, 1, 0)
    assert await FileSystemPartsStore.get_chunk(store, OBJ, 1, 1, 0) is None


@pytest.mark.asyncio
async def test_a_failed_residency_claim_is_counted(tmp_path) -> None:
    """The inverted-observability finding: this failure was debug-logged while the strictly less
    consequential last_read_at stamp had a first-class metric."""
```

### Step 2: Implement

- `PromotionSkipReason = Literal["disk_pressure", "residency_failed"]`.
- `ResidencyRecorder.__call__` returns `bool`; raise the log from `debug` to `warning`; the
  `PromotionRecorder` type alias becomes `Callable[[str, int, int, int], Awaitable[bool]]`.
- In `_promote_chunk`, move the `self._on_promote(...)` call **before** `set_chunk` and return
  early when it returns `False`, recording `residency_failed`.

Update the `residency.py` module docstring: it currently explains why the claim follows the write.
That reasoning is now inverted, and the *why* must change with it.

### Step 3: Commit

```bash
git commit -m "fix: claim residency before promoting, and count claim failures"
```

---

## Task 6 (WI-5 + WI-6): Peer URL allow-list, exact size check, real deadline — P2

**Findings B-1 and B-4.** One PR — same file.

**Files:**
- Modify: `hippius_s3/cache/peers.py` (`PeerRegistry.resolve`, `PeerChunkFetcher.__call__`)
- Modify: `hippius_s3/cache/dual_fs_store.py` (`_promote_chunk`)
- Modify: `hippius_s3/monitoring.py:29` (`PeerShedReason`)
- Modify: `hippius_s3/config.py` (new overall deadline)
- Test: `tests/unit/test_peer_fetch.py`

### Step 1: URL validation — failing test

```python
@pytest.mark.parametrize(
    "url", ["http://169.254.169.254/", "file:///etc/passwd", "https://evil.example",
            "http://8.8.8.8:8000", "http://10.1.2.3:9999"],
)
@pytest.mark.asyncio
async def test_a_peer_url_outside_the_pod_network_is_never_fetched(url) -> None:
    """The registry value is untrusted input crossing a process boundary.

    Severity is P2, not P1: the peer keys live on the same Redis that backs the gateway's auth
    cache, so anyone who can SET them can already forge an access-key auth result and impersonate
    any account — SSRF is strictly weaker than the precondition. Validate anyway; it is ~15 lines
    and the correct posture for a URL you did not construct.
    """
    transport = _CountingTransport()
    ...
    assert await fetcher(OBJ, 1, 1, 0) is None
    assert transport.calls == 0, "must not issue the request at all"
```

Validate in `resolve` — one choke point, so no caller can bypass it. Require `http`, a **literal**
private IP (no DNS name, so there is no resolution step to poison), the api port, and an empty
path/query/fragment. Count `bad_peer_url`.

### Step 2: Size check — the test the upper bound fails

```python
@pytest.mark.asyncio
async def test_a_short_body_that_is_legal_for_the_part_but_wrong_for_this_chunk_is_rejected() -> None:
    """An upper bound cannot close this arm, which is why the exact size is worth a query.

    A peer on a rolled-back or half-deployed image typically returns a SHORT body, and the bound
    has to be an upper bound because the last chunk of a part is legitimately short. So
    `len <= chunk_size + overhead` accepts exactly the failure it was added to catch.

    The exact size lives in part_chunks.cipher_size_bytes, fetched alongside the residency claim
    WI-7 already makes — one round trip, not two.
    """


@pytest.mark.asyncio
async def test_a_genuinely_short_final_chunk_is_still_accepted() -> None:
    """Regression guard against overcorrecting into an equality check."""
```

### Step 3: The overall deadline

```python
@pytest.mark.asyncio
async def test_a_drip_feeding_peer_is_cut_off_at_the_overall_deadline() -> None:
    """httpx has NO total-response timeout.

    Verified against the installed client (0.28.1): Timeout carries only connect/read/write/pool,
    and `read` bounds the wait BETWEEN body chunks. So peer_fetch_timeout_seconds=0.5 does not
    bound a fetch: a peer sending one byte every 0.4s holds the connection and the buffer
    indefinitely, today, on the plain client.get() path.

    CODE_REVIEW.md §4 states this arm is "capped at whatever the pod network delivers in 0.5s".
    It is not capped at all. The deadline below is closing a pre-existing hole, not compensating
    for the switch to streaming.
    """
```

Add `HIPPIUS_PEER_FETCH_DEADLINE_SECONDS` — a **new** value, not the 0.5s inter-chunk timeout
reused; they mean different things and reusing it would break legitimate large-chunk fetches. Wrap
the fetch in `asyncio.timeout(...)` and switch to `client.stream()` with an abort past
`chunk_size + AEAD overhead`.

### Step 4: Commit

```bash
git commit -m "fix: validate peer URLs and bound peer response bodies"
```

---

## Task 7 (WI-10): Invalidate a cached chunk that fails AEAD — P2

**Not from either review.** Surfaced while checking whether WI-6's length bound closes B-4's
serious arm; it does not, and this is why.

Nothing invalidates a locally-cached chunk when decrypt fails. `decrypter.py` has no `InvalidTag`
handling; `errors.py:157` maps it to an S3 error and the bytes stay on disk. Because the local tier
is checked first, one bad chunk is a **permanent, retry-immune** read failure for that object on
that node until eviction — which under retention may be never on an uncontended node.

**Files:**
- Modify: `hippius_s3/reader/decrypter.py`, `hippius_s3/reader/streamer.py`,
  `hippius_s3/cache/fs_store.py` (new narrow per-chunk unlink — only `trim_chunks_from` and
  `delete_part` exist, neither has the right granularity), `hippius_s3/monitoring.py`
- Test: `tests/unit/cache/test_aead_invalidation.py` (create)

### Constraints — get these right or the fix is worse than the bug

- **Local copy only.** Never unlink the pool copy: it is authoritative, and the fault may be in the
  DEK rather than the bytes. A pool chunk failing AEAD is a genuine 500 and must stay one.
- **One invalidate-and-retry per chunk per request.** Never a loop. A DEK-level fault would
  otherwise turn every read into a fleet-wide cache-wipe storm.
- **Attribute the tier** that served the failing bytes, so a systemic poisoner is distinguishable
  from isolated corruption.

### Tests

```python
async def test_a_poisoned_local_chunk_is_invalidated_and_the_read_succeeds_from_the_pool()
async def test_a_poisoned_pool_chunk_is_an_error_and_nothing_is_unlinked()
async def test_a_wrong_dek_retries_at_most_once_rather_than_wiping_the_cache()
```

```bash
git commit -m "fix: invalidate a local cached chunk that fails AEAD"
```

---

## Task 8 (WI-8): Shared-disk startup guard — P2, agent only

**Finding B-5.** Hardening plan §0-M2 required a startup check that logs loudly when accounted
cache bytes are far below `total − free`, so a process that does not own its filesystem cannot
silently drive free-space gates. It never shipped. Staging still runs on shared `/dev/md3` — so
**every free-space gate in this work is currently unvalidatable on the environment validating it.**

**Agent only.** Both reviewers proposed "agent + api", but the api has no accounted-bytes number to
compare against and would have to walk a multi-TB directory at lifespan start — exactly the
readdir-bound cost the evictor exists to avoid. The agent already has `Store::node_cache_bytes()`,
an O(1) SQL sum.

**Files:** `crates/hippius-drain-agent/src/runtime.rs` (startup), `snapshot.rs` (gauge).

At startup, compare `node_cache_bytes()` against `total − free` from `statvfs`. If accounted < 50%
of used, log `ERROR` with both numbers and set `drain_ssd_shared_filesystem = 1`. **Do not refuse to
start** — prod may legitimately have small co-tenants. Make the signal alertable instead.

Tests: 10% accounted trips the flag; 90% does not; a probe error leaves it unset rather than
defaulting either way.

**This one does need a hippius-otel alert rule.** File it alongside.

---

## Task 9 (WI-9): Comment and observability cleanup — P3

One PR. No behaviour change except the last two items. These are comment fixes in code whose whole
review value is that the reasoning is written down — which makes a *wrong* comment worse than none.

| Item | File | Change |
|---|---|---|
| A-6 | `crates/hippius-drain-core/src/store.rs:1260-1278` | Mirror the accumulate-vs-overwrite comment onto `record_resident`, naming `hippius_s3/cache/residency.py` as the other writer and stating the invariant that keeps them disjoint — and that nothing in the schema enforces it stays true |
| A-7a | `crates/hippius-drain-core/src/ssd_evict.rs:29-37` | Module doc still says FIFO by `resident_at` and that recency "does not exist yet". It exists (migration 0017) and the query was changed to `ORDER BY COALESCE(r.last_read_at, r.resident_at)` |
| A-7b | `crates/hippius-drain-core/src/partdrain.rs:554` | Claims `mark_replicated` stamps `resident_at` in the same statement. It does not — `mark_resident` is separate and issued first. The design is safe; the comment asserts the wrong mechanism for a real correctness property |
| B-6 | `hippius_s3/cache/dual_fs_store.py:242-247` | Justification describes pre-retention drain behaviour this work removed. The conclusion is still right; only the cause is dead. On the GetObject hot path |
| B-7 | `hippius_s3/cache/peers.py:209-218` | Add `ORDER BY r.resident_at` — `LIMIT 1` with no `ORDER BY` is non-deterministic across replans, not merely arbitrary |
| B-8 | `hippius_s3/monitoring.py:29`, `peers.py:284-286` | Extend `PeerShedReason` with `peer_miss` / `peer_error`; a wholesale peer-tier failure is currently visible only as a metric *sagging* |

```bash
git commit -m "docs: correct comments superseded by the SSD read tier, and count peer misses"
```

---

## Verification before prod promotion

Per-PR gates are in the Ground rules. Before promoting, on staging with the soak running:

1. `HIPPIUS_PEER_SERVE_ENABLED` on and the secret deployed → `chunk_reads_by_tier_total{tier=peer}`
   is **non-zero**. This is the check that WI-1 did not silently kill the tier; the handshake test
   should have caught a mismatch, but confirm in the real cluster.
2. Unauthenticated `GET https://<staging-gateway>/internal/parts/...` returns an S3 error, not
   ciphertext.
3. `drain_ssd_shared_filesystem` reports **1** on staging. That is the guard working, and it is the
   reason free-space numbers from this soak need reading with care.
4. `evict_blocked_unreplicated` still **zero**; `remove_failed` observable and explicable.

## Runs in parallel, not behind this queue

**B-2** — `UploadPart` re-upload after `replicated` never re-drives the pool. Real P1 integrity bug,
pre-existing under drain-direct, not caused by anything here and not fixed by reverting any of it.
It is the only item in either review that returns **wrong plaintext with no error**, and the
promotion path now caches the stale copy fleet-wide, so this work makes it longer-lived rather than
neutral. Open a drain issue now; do not let it inherit this plan's sequencing. Of the three fix
options in CODE_REVIEW.md §3, option 3 (content-hash the SSD set at drain time and re-drive on
divergence) is the honest one.

## Recorded, not scheduled

- **Peer-serve port split.** The durable answer to A-1 — a separate port is unreachable by
  construction, whereas WI-1's path denylist holds only until someone adds another non-S3 route,
  which is exactly how A-1 happened.
- **`HIPPIUS_PEER_SERVE_MAX_INFLIGHT = 16` is an unmeasured guess** (review open question 3).
- **Prod carries no `CEPHOR_EVICT_*`**, so the first prod binary runs the evictor on code defaults.
