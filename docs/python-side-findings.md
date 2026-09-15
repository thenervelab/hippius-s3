# Python-side (hippius-s3) findings — surfaced during the Rust-rewrite research

**Date:** 2026-09-15 · **Scope:** issues/gaps found in the **live Python product** while researching the Rust rewrite. These are worth triaging on the Python side **independent of** the Rust plan (they are not rewrite tasks). Each cites the deep-research section (`docs/rust-rewrite/NN`) and code where it was found.

> Ordering is by severity, not by area. "sn85" tags the items relevant to the evidence-store customer conversation.

## P0 — correctness / data integrity

1. **S4 `append` bypasses Object Lock (live WORM hole).** `extensions/append.py` / `writer.append_stream` mutate a version in place and never check `is_version_locked`, despite `object-lock.md` §5.2 mandating a 403 when the current version is protected (the design's "G9" gap). The only lock taken is a DB row lock, not an Object-Lock check. **Impact:** an append-based flow is not WORM-safe; a customer relying on Object Lock for immutability (e.g. **sn85**) does not actually get it on appended objects. Source: `docs/rust-rewrite/06`. **sn85.**

2. **~203k `object_versions` rows with a NULL envelope 500 on read — 141k of them LIVE (2026-09-15 prod count).** On the prod `hippius` DB: **203,277** v5 committed content rows have NULL `kek_id`/`wrapped_dek` (undecryptable → 500 on GET), and **141,367 are the current version of a live, non-deleted object** — i.e. ~141k objects that error on GET *right now*. (Together with finding #14's 25.9k v4-stuck objects, ~167k live objects are unreadable in prod.) **Fix going forward:** add the envelope-required CHECK so new NULL-envelope rows can't be created; reconcile/repair or purge-and-report the existing rows. **Migration impact:** the re-encrypt migrator cannot decrypt these (no key) → they land in skip-and-report/quarantine, so ~167k live objects will *not* carry over — the Python team needs to repair or consciously abandon them before cutover. Source: prod count this session; `docs/rust-rewrite/01`, `docs/rust-rewrite/02` (todo.md P0).

## P1 — customer-facing gaps & billing

3. **No per-prefix / finer-grained bucket policy — the real sn85 gap.** Bucket policy is a **public/not-public toggle at the bucket grain** (`bucket_policy_endpoint.py:74` `set_bucket_policy` + `_validate_public_policy` only accepts a single canned `Allow * s3:GetObject .../*`). You cannot seal a *subset* of an otherwise-public bucket. Source: `docs/rust-rewrite/05`, `docs/rust-rewrite/06`. **sn85.**

4. **Deploy-lag on two already-merged conformance fixes.** `If-None-Match: *` create-only (PR **#523**) and Content-MD5 verification (PR **#522**) both merged to `main` **2026-09-13**, but sn85's report described the exact pre-fix behavior — they likely tested an environment behind `main`. **Action:** verify the deployed version of the env sn85 hit and have them re-test on current `main`. Source: assessment §1.5. **sn85.**

5. **Billing enforcement is loose.** Enforcement lags a ~120s cache refresh with **nothing accruing between refreshes**; overwrites are charged **additively** (not as a delta); the plan `active` flag is deliberately ignored in `_is_enforceable_plan_row`; the credit gate **fails open** on a cache miss. The `plans` cache has no TTL and swaps atomically with a 0.5 shrink guard — a partial roll would 402 the whole fleet. Source: `docs/rust-rewrite/05`, `docs/rust-rewrite/07`.

6. **Pay-as-you-go `can_upload` keys quota on the caller, not the bucket owner.** The main PUT path now resolves the bucket **owner** (`account.py:_billed_account`), so the old "quota keyed on caller-not-owner" P1 is *partly* stale — but the pay-as-you-go `can_upload` path still keys on the caller. Source: `docs/rust-rewrite/05`.

## P2 — S3 conformance gaps

7. **GetObjectAttributes has no handler** — `GET ...?attributes` silently falls through to a normal GetObject and returns the object **body**, not the attributes XML. Source: `docs/rust-rewrite/06`.

8. **PostObject (browser form upload) absent** — `POST /{bucket}` only handles `?delete`; anything else is 501. Source: `docs/rust-rewrite/06`.

9. **CopyObject rejects every `If-None-Match` (including `*`) with 501** — create-only isn't implemented on any copy path; the v5 fast-path copy is disabled (always streaming/alias). Source: `docs/rust-rewrite/06`.

## Doc hygiene — stale committed docs that mislead (verify-against-code)

10. Several committed `CLAUDE.md`/module docstrings are stale and were contradicted by the code during research:
    - `object_lock_enforcement.py` docstring says delete enforcement is "NOT WIRED YET" — but `delete_object_endpoint.py:107` / `delete_objects_endpoint.py:205` **do** enforce it (403 on locked-version delete).
    - `writer/CLAUDE.md` says the chunk index is "global" — it is **per-part**.
    - `writer/CLAUDE.md` describes a 5-tuple chunk AAD — the live V2 adapter uses a 4-field AAD (omits upload_id and object_version).
    - `gateway/CLAUDE.md` and the top-level CLAUDE.md say **5 auth methods** and describe seed-phrase auth — there are **4** (seed-phrase removed).
    - `services/CLAUDE.md` calls the v5 copy fast-path a "latent risk" — worth confirming whether it's actually reachable (see item below).

## Needs a runtime probe (not clearly a bug yet)

11. **v5 copy fast-path — CONFIRMED disabled (2026-09-15 probe).** `should_use_v5_fast_path()` (`api/s3/copy_helpers.py:223`) unconditionally returns `False, None, "v5_fast_path_disabled_object_id_binding"`, so `execute_v5_fast_path_copy` (imported at `copy_object_endpoint.py:25`) is never reached — every copy streams/re-encrypts. The static "would `InvalidTag`" reading was right; that's *why* it's gated off. Not a live bug. Rewrite impact: the migration's undecryptable-copy class is empty for current-code objects (register E2). Source: `docs/rust-rewrite/01`.

12. **Download-queue path — CONFIRMED vestigial (2026-09-15 probe).** No `run_download_*` entrypoint exists; `queue_metrics.py` documents `ovh_download_requests` as "consumer scaled to 0". The live fleet is upload/unpin only; the download queue + `classify_download_error` are dead/parked, not a live path. Source: `docs/rust-rewrite/04`.

13. **`HIPPIUS_VALIDATOR_REGION` — CONFIRMED dead (2026-09-15 probe).** Only occurrence is the `env()` read at `config.py:216`; no consumer anywhere in the repo. Dead config. Source: `docs/rust-rewrite/07`, `docs/rust-rewrite/09`.

14. **~25.9k live objects stuck at `storage_version=4` are unreadable in prod (2026-09-15 prod count).** On the prod `hippius` DB, `object_versions` holds **212,498 v4 rows** (and ~176.94M v5; no v1/v2/v3). The read path hard-rejects `<5` — `storage_version.py` `MIN_SUPPORTED_STORAGE_VERSION=5`, `require_supported_storage_version` raises `UnsupportedStorageVersionError` (called from `reader/decrypter.py:34`). Of the v4 rows, **25,886 are the current version of a live, non-deleted object** (so a client GET hits them and errors — 501/`NotImplemented` via the read-path error map, or 404 if the download query skips non-`uploaded` status first), and **102 have `status='uploaded'`** (the clearest "should work, returns error" cases). These were presumably never upgraded by the old in-place v4→v5 migrator. **Impact:** a bounded set of objects clients cannot retrieve. **Options:** re-encrypt/upgrade them to v5 (needs a v4 decrypt path), or confirm they're abandoned and hard-delete + report. Same class as the ~200k NULL-envelope rows (#2). Independent of the rewrite (the Rust migration will skip-and-report v4 per register E1). Source: prod count this session.

---

*These were found by reading current source (per the project's "verify against code, not config" rule). Where an item contradicts a committed doc, the code is authoritative and the doc is the thing to fix.*
