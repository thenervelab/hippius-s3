# 07 — Substrate/Blockchain Publishing, Credit Accounting & IPFS/CID Identity

> **⚠️ Rewrite delta: the rewrite gains NO direct chain-write path.** HCFS owns usage→chain reporting
> and the per-tenant 402 credit gate; the S3 service **inherits** them (stores under real ss58) and
> submits **no extrinsics**. So §5–§6's subxt/write-path guidance and the "S3 product's equivalent
> parallel reporter" are **not** the rewrite's design — they describe how HCFS's own reporter works
> (informational). §1–§4 (HTTP pin surface, read-only credit scrape, CID = blake3(ciphertext)) are an
> accurate current-system trace. Authority: `decisions-register.md` (G3), IMPLEMENTATION-PLAN §7/§8.

Code-anchored description of the **current Python** on-chain publishing, credit/plan accounting, and
IPFS/CID content identity (reference for the rewrite).

> Scope note. Two code bases are cross-referenced throughout:
> - **`hippius-s3`** (Python, at `/Users/camden/Source/hippius-s3`) — the product being
>   reimplemented. File refs below are relative to that root.
> - **`hcfs-chain-reporter`** (Rust, at `/Users/camden/Source/hcfs/hcfs-chain-reporter`) —
>   a *sibling* service that already does direct Substrate reporting with `subxt` +
>   `secrecy::SecretString`. It is the proven Rust pattern to reuse for the parts of this
>   subsystem that must actually sign and submit extrinsics. File refs to it are prefixed
>   `hcfs-chain-reporter/`.

---

## 0. Executive summary — the single most important architectural fact

**`hippius-s3` does not submit blockchain extrinsics itself for object storage.** There is no
`pallet.*.sign_and_submit` anywhere in the storage write path. What the CLAUDE-style mental
model calls "blockchain publishing" is, in the current Python product, split into three very
different mechanisms, and the Rust rewrite must preserve that split rather than collapse it
into "the S3 server talks to a chain node":

| Concern | Mechanism in Python `hippius-s3` | Direction | Transport |
|---|---|---|---|
| Store object / pin CID on chain | Delegated to Arion (`POST /upload`) and/or `api.hippius.com` storage-control; the validator/Arion side does any actual extrinsic | write (indirect) | HTTP + ServiceToken |
| Unpin / cancel storage | `api.hippius.com` / Arion `unpin` HTTP calls | write (indirect) | HTTP |
| Read account **credit** | `cacher/run_cacher.py` queries Substrate storage **read-only** (`SubstrateInterface.query_map`) and caches to Redis | read | Direct WS RPC (read-only) |
| Read **plan** roll | Scraped over HTTP from `api.hippius.com/api/s3/plans/accounts/` into Redis | read | HTTP |
| Compute content identity (CID) | Arion content-addresses ciphertext; BLAKE3 of ciphertext == the S3/Arion hash | local | in-process |

The only *direct* Substrate connection in `hippius-s3` today is **read-only credit scraping**.
Direct extrinsic submission (the `subxt` pattern) lives in the sibling `hcfs-chain-reporter`
and is the model for any future "hippius-s3 reports usage on-chain itself" work — see §5–§6.

Confirmation that the write-side pin call is dead code in the current product:

```
$ grep -rn "\.pin_file(" hippius_s3 workers cacher --include="*.py"
   (no matches)
```

`HippiusApiClient.pin_file` (`hippius_s3/services/hippius_api_service.py:320`) is defined but
has **no callers**. The live upload path uses `ArionService.upload_file_and_get_cid`
(`hippius_s3/workers/uploader.py:540`), which POSTs ciphertext to Arion and reads back a CID;
chain pinning is Arion's responsibility, not the S3 server's.

---

## 1. What is published on-chain, and when

### 1.1 In the current Python product: nothing, directly

The S3 write path (`hippius_s3/workers/uploader.py`) uploads encrypted chunks to a
`backend_client` (Arion) and records CIDs in Postgres. It never signs an extrinsic. The
retry doc is explicit about this:

> "The system interacts with the Hippius blockchain via the Hippius HTTP API
> (`hippius_api_service.py`), not via direct Substrate node connections."
> — `docs/sequence-substrate-retry.md:1-6`

So for the Rust rewrite, the equivalent of "publish on store" is an **HTTP call to Arion**,
and the retry/backoff contract is the HTTP retry decorator, not extrinsic submission (§1.3).

### 1.2 The HTTP "pin"/"unpin" surface (available, mostly unused today)

`HippiusApiClient` (`hippius_s3/services/hippius_api_service.py`) wraps `api.hippius.com`:

- `pin_file(cid, size_bytes, account_ss58, filename)` → `POST /storage-control/requests/`
  with body `{"cid", "original_name", "size_bytes", "account_ss58", "request_type": "Pin"}`
  (lines 320–361). **Unused** in the current tree.
- `unpin_file(cid, account_ss58)` → same endpoint with `"request_type": "Unpin"`
  (lines 363–400).
- `upload_file_and_get_cid(...)` → `POST /storage-control/upload/` (lines 432–484).

The *live* unpin path is `ArionService.unpin_file` / `unpin_files_batch`
(`hippius_s3/workers/unpinner.py:204,413`; `hippius_s3/services/arion_service.py:375,413`),
capped at **1000 file_ids per batch call** (`arion_service.py:413`).

**Granularity: per chunk, not per object.** Each cipher chunk is uploaded and CID'd
independently (`uploader.py:540`, inside `upload_one_chunk`), so identity is per-chunk. An
object's chunk CIDs are recorded against `part_chunks` / `chunk_backend` (§4).

### 1.3 Rate limits, retry, backoff (HTTP side)

`hippius-s3` deliberately has **no artificial rate caps** on the chain-facing HTTP client. The
only backoff is the retry decorator:

```python
# hippius_s3/services/hippius_api_service.py:202
def retry_on_error(retries: int = 3, backoff: float = 5.0):
    # retries 4xx/5xx with fixed 5s sleep; DOES NOT retry 401/403 (auth) or 404
```

Key behaviours to replicate exactly (lines 218–260):
- **3 retries, fixed 5.0 s backoff** (not exponential).
- **401/403 → immediate `HippiusAuthenticationError`, no retry.**
- **404 → re-raise, no retry** (resource genuinely absent).
- Any non-HTTP exception → re-raise immediately.
- Client timeout: `httpx.Timeout(60.0, connect=10.0)` (line 287).

The uploader worker has its own separate exponential backoff for the storage write
(`HIPPIUS_UPLOADER_BACKOFF_BASE_MS:500`, `..._MAX_MS:60000`, `..._MAX_ATTEMPTS:7` —
`config.py:447-449`), and the unpinner mirrors it (`config.py:495-497`).

### 1.4 The signer identity today

There is **no signing seed** in `hippius-s3` for chain writes. Authentication to
`api.hippius.com`/Arion is a **service token**, not a keypair:

```python
# hippius_s3/services/hippius_api_service.py:306
"Authorization": f"ServiceToken {self._config.hippius_service_key}",
```

`hippius_service_key` = env `HIPPIUS_SERVICE_KEY` (`config.py:211`);
`arion_service_key` = env `ARION_SERVICE_KEY` (`config.py:212`). The read-only credit scraper
connects with **no key at all** — it only reads public storage (§3.1).

> **Do not invent a signing seed for the S3 write path in the rewrite** unless the product is
> deliberately being changed to submit its own extrinsics. If it is, adopt the
> `hcfs-chain-reporter` seed handling verbatim (§6.3).

---

## 2. The Substrate client (read-only, credit scraping)

### 2.1 Endpoint & connection

`cacher/run_cacher.py` is the only direct Substrate consumer:

```python
# cacher/run_cacher.py:37
self.substrate = SubstrateInterface(
    url=self.substrate_url,            # HIPPIUS_SUBSTRATE_URL
    ss58_format=42,
    type_registry_preset="substrate-node-template",
)
```

- `substrate_url` = env `HIPPIUS_SUBSTRATE_URL` (required; `config.py:210`).
- **SS58 network prefix = 42** — pinned in three places and load-bearing:
  `HIPPIUS_SS58_FORMAT = 42` (`config.py:43`), the cacher above, and the plans-cacher's
  address validation (`workers/run_plans_cacher_in_loop.py:138`). Every account key the
  system stores/looks up is prefix-42.
- Library: `py-substrate-interface` (`substrateinterface`), a synchronous JSON-RPC/WS client.

### 2.2 What is read

`fetch_free_credits()` (`cacher/run_cacher.py:53`) does `query_map(module="Credits",
storage_function="FreeCredits")` → `{account_id: credits}`. Two optional maps follow and are
skipped if the pallet is absent (`run_cache_update` catches `RuntimeError`, lines 260–271):
- `SubAccount.SubAccountRole` → `{subaccount: role}` (role ∈ `Upload`, `UploadDelete`).
- `SubAccount.SubAccount` → `{subaccount: main_account}`.

### 2.3 Signing, nonce, batching

**None.** This client only reads storage maps; there is no signing, nonce management, or
extrinsic batching in `hippius-s3`. (Those concerns exist only in the sibling reporter — §2.4.)

### 2.4 Reference: how the sibling `hcfs-chain-reporter` does the *write* client

This is the pattern to reuse if/when the rewrite needs to submit extrinsics.

- **Transport:** subxt `OnlineClient<SubstrateConfig>` over the **reconnecting** RPC client
  (`ReconnectingRpcClient` + `ExponentialBackoff::from_millis(10).max_delay(30s)`), *not* the
  plain jsonrpsee client — the plain client permanently wedges after one disconnect
  (`hcfs-chain-reporter/src/chain_subxt.rs:143-172, 300-330`). RPC URL from
  `HCFS_CHAIN_RPC_URL`, validated `wss://`/`https://`-only via `validate_url_is_secure`
  (`chain_subxt.rs:273`, `main.rs:166-172`).
- **Signing:** `subxt_signer::sr25519::Keypair` (sr25519), signed via
  `client.tx().sign_and_submit_then_watch(&tx, &signer, params)` then
  `.wait_for_finalized_success()` (`chain_subxt.rs:513-524`).
- **Nonce management:** pool-aware. It deliberately does **not** use subxt's default
  finalized-only `account_nonce()`; it calls
  `LegacyRpcMethods::system_account_next_index(&signer_account_id)` which accounts for
  in-pool txs, avoiding `InvalidTransaction::Stale` when a prior tick's tx is not yet
  finalized (`chain_subxt.rs:555-595`). Mortality is explicit: `.mortal(64)` (~6.4 min at
  6 s blocks) so a pool-dropped tx era-expires instead of zombie-holding the nonce
  (`chain_subxt.rs:496-499`).
- **Batching:** one signed extrinsic per tick carrying a `Vec` of per-account updates, bounded
  by the runtime constant `MaxUserFileUsageUpdatesPerCall` (mirrored as
  `MAX_UPDATES_PER_CALL = 250`, `loop_::76`), probed against the chain at startup
  (`chain_subxt.rs:363-389`).
- **Runtime-version tracking:** a background task runs
  `client.updater().perform_runtime_updates()` so a mid-lifetime runtime upgrade doesn't
  freeze the cached `spec_version` and cause `BadProof` rejections — a real 2026-05-07
  production incident (`chain_subxt.rs:837-921`).

---

## 3. Credit / plan accounting

Two independent caches feed two independent gates. Both live on **`redis-accounts`**
(`REDIS_ACCOUNTS_URL`, default `redis://127.0.0.1:6380/0` — `config.py:237`).

### 3.1 Substrate credits → Redis (the account cacher)

- Worker `workers/run_account_cacher_in_loop.py` runs `SubstrateCacher.run_cache_update()`
  **every 300 s** (`main()`, line 63), recording success/duration metrics per cycle.
- Writes two Redis key families **with a 600 s TTL** (`cacher/run_cacher.py:212,238`):
  - `hippius_main_account_credits:{ss58}` → `{"main_account_id", "free_credits", "has_credits"}`
  - `hippius_subaccount_cache:{subaccount}` → `{subaccount_id, main_account_id, role, free_credits, has_credits}`
- `has_credits = free_credits > 0` (`run_cacher.py:207,236`).

> **TTL semantics differ from the plan cache — reproduce exactly.** The credit cache has a
> 600 s TTL (2× the 300 s refresh), so an extended Substrate outage lets credit keys expire.

### 3.2 How the credit gate consumes it

`fetch_account_by_main_address` (`hippius_s3/gateway/services/account_service.py:18`):

```python
main_account_data = await redis_accounts_client.get(f"hippius_main_account_credits:{account_address}")
has_credits, free_credits = True, 0
if main_account_data:
    has_credits = main_data.get("has_credits", True)
    ...
else:
    # No cached credits → defaults has_credits=True (FAIL-OPEN)
```

**Fail-open on cache miss** (line 52): an absent credit key defaults `has_credits=True`. This is
a deliberate availability-over-strictness choice and must be preserved. There is also a global
override `HIPPIUS_BYPASS_CREDIT_CHECK` (`enable_bypass_credit_check`, `config.py:196`).

### 3.3 Plan roll → Redis (the plans cacher)

Source: `workers/run_plans_cacher_in_loop.py`, service layer
`hippius_s3/services/plans_cache.py`, wire types in
`hippius_s3/services/hippius_api_service.py:144-188`.

- Polls `GET /api/s3/plans/accounts/?page=1&page_size=500` every
  `HIPPIUS_PLANS_LOOP_SLEEP` (default **120 s**, `config.py:295`), following `next` pagination
  up to `MAX_PAGES = 500` (`run_plans_cacher_in_loop.py:74`). **One endpoint carries both**
  the plan catalog (`plans`) and the per-account roll (`results`).
- Three Redis structures on `redis-accounts` (`plans_cache.py:46-48`):
  - `hippius_s3_plan_accounts` (hash) field = ss58 → `{plan, storage_limit_bytes, used_bytes}` — **the only one on the serving path**
  - `hippius_s3_plans` (hash) field = plan name → `{h256, storage_bytes}` — operator-facing only
  - `hippius_s3_plans:meta` (JSON) → `{fetched_at, accounts, plans}`
- **No TTL, ever** (`plans_cache.py:24-26`). Publication is an **atomic whole-hash swap**:
  build into `<key>:building`, `RENAME` over the live key (`_publish_hash`, lines 87–106). This
  guarantees (a) a partial scrape is never published, (b) accounts that left a plan disappear on
  swap, (c) readers never see a half-built map.
- **Shrink guard:** refuses to publish an empty roll over a non-empty live hash, or one that
  lost > `MAX_ACCOUNT_MAP_SHRINK_RATIO` = 0.5 of entries (`plans_cache.py:56, 128-138`). This
  also blocks intentional shrinks/rollbacks — the rollback runbook is `DEL
  hippius_s3_plan_accounts` first (documented at lines 121–126).
- **Usage is computed by us**, not upstream: `_attach_usage` does one indexed `SUM` over
  `bucket_storage_usage` per plan account (`run_plans_cacher_in_loop.py:154-183`), concurrency
  bounded by the DB pool (`plans_usage_concurrency`, default 4). Reads from
  `database_readonly_url` (`config.py`, DSN chosen at line 293) to avoid stressing the primary.
- **All-or-nothing:** any page fetch or any usage read failing aborts the whole cycle before
  publish; the previous roll keeps serving; failed cycles sleep 60 s instead of 120 s
  (`run_cycle`, `refresh_plan_roll_once`, loop lines 265–315).
- **Staleness** is surfaced only via the meta timestamp + `record_plans_cache_age` metric and a
  log at `HIPPIUS_PLANS_STALE_AFTER_SECONDS` (default 600, `config.py:319`) — never by data
  vanishing (`_report_cache_age`, lines 247–262).

### 3.4 Enforceable-plan classification (subtle, must copy exactly)

`_is_enforceable_plan_row` (`run_plans_cacher_in_loop.py:81-120`):

```python
return bool(row.billing == "plan" and row.plan)
```

- `active` is **deliberately NOT consulted** — upstream returns `active=false` on every row
  (3069 of 3069 observed), including the one genuine subscriber with a future `next_charge`.
- `S3PlanAccountRow.active` is typed `bool | None` on purpose: one `"active": null` row must
  not fail `model_validate` and abort the whole page (`hippius_api_service.py:162-169`).
- Non-network-42 ss58 rows are dropped with a `PLANS_BAD_ADDRESS` log (they'd otherwise match
  no bucket, count 0, and read as unlimited headroom — lines 138–145).
- Account's own `results[].storage_bytes` wins over the catalog list price when present
  (`_parse_page`, line 147).

### 3.5 The plan gate (request path)

`hippius_s3/gateway/services/plan_gate.py` + `plans_cache.get_plan_for_account`:

- Request path is **one Redis `HGET` + a comparison**, no DB
  (`plans_cache.py:170-194`, `plan_gate.py:6-9`).
- An account on a plan is **not credit-metered**: it skips both the Substrate `has_credits`
  check and Arion `can_upload`, and is gated on total stored bytes instead
  (`plan_gate.py:1-9`).
- `PlanQuota.enforceable`: a missing/zero/negative allowance means "unknown", never "zero
  allowed" (`plans_cache.py:71-76`).
- Failure posture (`plan_gate.py:19-27`):
  - Lookup itself fails → `PlanLookupUnavailable` → caller falls back to pay-as-you-go.
  - Lookup succeeds but quota unknown (`catalog_miss`) → **ALLOW, loudly** (never block a
    positively-identified paying customer on a cold cache).
- `evaluate_quota(..., enforcing=)` — one function for both enforce and shadow modes; shadow
  returns `would_deny` (`plan_gate.py:69-89`). Shadow vs enforce is
  `HIPPIUS_ENABLE_BILLING_PLANS` (with `_STAGING`/`_PROD` env-specific overrides preferred over
  the bare flag — `config.py:117-139`). Plan **caching is unconditional**; the flag only
  decides what the gate *does* with the answer (`run_plans_cacher_in_loop.py:26-28`).
- The 402 body is user-visible verbatim and rendered in **binary units** (TiB/GiB/MiB) to match
  what customers were sold; it must not contain any substring in
  `_TRANSIENT_BILLING_ERROR_MARKERS` (`plan_gate.py:92-121`).

### 3.6 "thebrain" and the Hippius API base

- **`HIPPIUS_API_BASE_URL`** (default `https://api.hippius.com/`, `config.py:217`) is the HTTP
  host for: token auth (`/objectstore/tokens/auth/`), storage-control (files/upload/requests),
  and the plan roll (`s3/plans/accounts/`). Note the base already ends in `/api`, so the plans
  call is written `s3/plans/accounts/` (no leading `/api`) — spelling `/api` again yields
  `/api/api/...` and 404s (`hippius_api_service.py:561-564`).
- The plans code **re-homes** upstream's absolute `next` URL onto the configured host
  (keeps only path+query), both so e2e's mock host is never escaped and so an upstream field
  can't redirect a service-token-authenticated client at an unchosen host
  (`hippius_api_service.py:566-591`).
- **"thebrain"** is the Hippius Substrate chain itself in the sibling reporter's vocabulary —
  the reporter's compiled metadata file is `metadata/thebrain.scale`
  (`hcfs-chain-reporter/src/chain_subxt.rs:74`) and its RPC target is thebrain. In `hippius-s3`
  the term surfaces only indirectly (usage stats ultimately land on thebrain via Arion/the
  reporter, not via S3 directly). Do not conflate `api.hippius.com` (HTTP billing/storage API)
  with thebrain (the chain) — see also `arion_vs_hcfs` memory: Arion is a separate service.

### 3.7 Other config knobs (§ Environment)

| Env | Field | Default | Notes |
|---|---|---|---|
| `HIPPIUS_SUBSTRATE_URL` | `substrate_url` | — (required) | read-only credit scrape |
| `HIPPIUS_API_BASE_URL` | `hippius_api_base_url` | `https://api.hippius.com/` | billing + storage-control |
| `HIPPIUS_ARION_BASE_URL` | `arion_base_url` | `https://arion.hippius.com/` | storage backend |
| `HIPPIUS_SERVICE_KEY` | `hippius_service_key` | — | `ServiceToken` for api.hippius.com |
| `ARION_SERVICE_KEY` | `arion_service_key` | — | Arion auth |
| `HIPPIUS_VALIDATOR_REGION` | `validator_region` | — (required) | see Open Questions §7 |
| `REDIS_ACCOUNTS_URL` | `redis_accounts_url` | `redis://127.0.0.1:6380/0` | both caches |
| `HIPPIUS_PLANS_LOOP_SLEEP` | `plans_loop_sleep` | 120 | plan scrape interval |
| `HIPPIUS_PLANS_STALE_AFTER_SECONDS` | `plans_stale_after_seconds` | 600 | staleness alarm |
| `HIPPIUS_PLANS_USAGE_CONCURRENCY` | `plans_usage_concurrency` | 4 | usage SUM pool size |
| `HIPPIUS_ENABLE_BILLING_PLANS[_STAGING/_PROD]` | via `_resolve_enable_billing_plans` | off | enforce vs shadow |
| `HIPPIUS_BYPASS_CREDIT_CHECK` | `enable_bypass_credit_check` | false | global credit bypass |
| `HIPPIUS_SERVICE_ACCOUNT_IDS` | `service_account_ids` | ∅ | gate-bypass allowlist (prefix-42 validated) |

`HIPPIUS_VALIDATOR_REGION` is required (`config.py:216`) but has **no Python reader** in this
repo — see Open Questions.

---

## 4. IPFS / CID identity

### 4.1 How CIDs are produced

The CID is **not computed by `hippius-s3`** — Arion content-addresses the uploaded ciphertext
and returns the CID in the upload response. `ArionService.upload_file_and_get_cid` POSTs the
cipher chunk to Arion's `/upload` and reads back `UploadResponse.cid`
(`hippius_s3/services/arion_service.py:472-520`; response model
`hippius_api_service.py:85-96`).

The link between the returned CID and content addressing is BLAKE3 of the **ciphertext**:

```python
# hippius_s3/workers/uploader.py:252  (_arion_hash_of)
# HCFS returns the real Arion hash as ``arion_hash``; older servers return the same value as
# ``upload_id`` (``cid``), which falls back to the S3 hash — the BLAKE3 of the same ciphertext,
# so identical to what Arion content-addresses it by.
```

Separately, `hippius_s3/blake3_hash.py` computes BLAKE3 of the **plaintext** (the digest the
console shows as "Arion hash"), computed in-flight on the write pipeline
(`blake3_hash.py:1-23`, `hex_of` / `new_hasher`, `max_threads=1`). **These are two different
digests** — plaintext-BLAKE3 for display vs ciphertext content-address for storage identity —
and must not be interchanged.

Critical rule from `persist_version_hash` (`blake3_hash.py:26-44`): the plaintext BLAKE3 gets
its **own column** (`update_object_version_body_blake3`), NOT `ipfs_cid`/`cid_id`, because the
purge/unpin scripts read those columns back as real CIDs
(`COALESCE(c.cid, ov.ipfs_cid)`); a 64-hex digest parked there would enter the unpin worklist
as if it were a pin. The rewrite must keep display-hash and CID columns strictly separate.

### 4.2 Where CIDs are stored

Schema (from `hippius_s3/sql/migrations/`):

- **`cids`** table (`20250901000000_create_cid_table_and_fix_constraints.sql`): `id UUID PK`,
  `cid TEXT NOT NULL UNIQUE`, `created_at`. Indexed on `cid`. Referenced by `objects.cid_id`,
  `parts.cid_id`, and `files.cid_id` (all `UUID REFERENCES cids(id)`). `objects.ipfs_cid`
  became nullable.
- **`part_chunks.cid`** (`20251003000000_create_part_chunks.sql`; made nullable by
  `20260131000000_parts_nullable_ipfs_cid.sql`) — the per-chunk CID.
- **`chunk_backend`** (`20260130000000_add_chunk_backend.sql`): the multi-backend
  storage-location table. `PRIMARY KEY (chunk_id, backend)`, columns `backend TEXT`,
  `backend_identifier TEXT`, `deleted BOOLEAN`. **The CID lives in `backend_identifier` when
  `backend='ipfs'`** — the migration backfills
  `INSERT ... SELECT id, 'ipfs', cid, false, created_at FROM part_chunks WHERE cid IS NOT NULL`
  (lines 24–27). A live (`deleted=false`) `chunk_backend` row is the backend's claim to hold
  the acknowledged bytes; the uploader writes it only after the whole part's digest is verified
  against the drain's (`uploader.py:525-528` commentary).
- **`object_versions.ipfs_cid`** — per-version CID, read by purge/unpin
  (`COALESCE(c.cid, ov.ipfs_cid)`), guarded against `''`/`'pending'`/`NULL`.

### 4.3 Is the CID a storage-identity contract that must match?

**Yes.** The CID (== BLAKE3 content-address of the ciphertext) is the identifier under which
Arion stores and later serves the bytes, and the identifier used to unpin/purge. Consequences
for the rewrite:

- The rewrite must persist exactly the CID Arion returns; it must not recompute a CID by a
  different scheme and store that, or download/purge will address the wrong content.
- Chunk digest fencing (`uploader.py`, "the bytes this upload sent must be the bytes the drain
  hashed at hand-off", ~line 578) must be preserved so a rewritten SSD part cannot get a stale
  CID recorded.
- Unpin batches address content by CID/backend_identifier, capped at 1000 per call
  (`arion_service.py:413`).

---

## 5. The migration reality — `hcfs-chain-reporter` already does this in Rust

The sibling `hcfs-chain-reporter` is a **standalone worker** that reads aggregated per-account
usage from Postgres and pushes it to thebrain via signed extrinsics. It is the concrete Rust
template for the "report usage on-chain" capability. (Note: it is HCFS's own reporter; the S3
product's equivalent would be a parallel worker, analogous to the Python
`hcfs-chain-reporter` mentioned in the HCFS root CLAUDE.md.)

### 5.1 Exactly which pallet/call it uses

Per tick it submits **two** extrinsics, sequentially, inside one outer timeout
(`chain_subxt.rs:453-524, 695-786`; trait contract `chain.rs:74-177`):

1. **`pallet_marketplace::update_users_file_usage(updates: Vec<UserBackendFileUsageUpdate>)`**
   — call_index 25 (plural), spec 9184+. `Pays::No`. Per-backend split preserved.
   - Field mapping (`chain_subxt.rs:462-481`, `chain.rs:82-87`):
     `ss58 → account_id`, `hcfs_bytes → drive_file_size`, `hcfs_count → drive_file_count`,
     `s3_bytes → s3_file_size`, `s3_count → s3_file_count`.
   - **Auth:** signer must equal `Marketplace::SubscriptionCanceller` storage; else
     `SubscriptionCancellationNotAuthorized` → `ChainError::Unauthorized`. Operator remediation:
     sudo `Marketplace::set_subscription_canceller(reporter_ss58)`
     (`chain_subxt.rs:16-27`, `chain.rs:12-20`).
2. **`pallet_arion::update_multiple_user_file_sizes(updates: Vec<UserStorageUsageUpdate>)`**
   — call_index 36 (plural), spec 9188+. `Pays::No`.
   - Field mapping (`chain_subxt.rs:702-723`, `chain.rs:124-127`):
     `ss58 → account_id`, `file_size = hcfs_bytes + s3_bytes` (saturating u128),
     `file_count = hcfs_count + s3_count` (saturating u128) — the combined per-user total.
   - **Auth (different principal):** signer must be a
     `pallet_registration::NodeType::Validator` (or proxy); else `InvalidNodeType` /
     `NodeNotRegistered` → `ChainError::Dispatch` (`chain.rs:141-156`).

**Pallet history** (`chain_subxt.rs:5-27`): pre-9183 the singular call lived in `pallet_arion`;
spec 9183 moved it to `pallet_marketplace::update_user_file_usage` (call_index 24, singular);
spec 9184 added the plural batch variant (call_index 25).

### 5.2 Env / configuration (`HCFS_CHAIN_*`)

From `hcfs-chain-reporter/src/main.rs`:

| Env | Meaning | Default |
|---|---|---|
| `HCFS_CHAIN_BACKEND` | `log_only` (dry run; advances watermark, no chain write) or `subxt` | `log_only` |
| `HCFS_CHAIN_RPC_URL` | thebrain WS/HTTPS RPC; `wss://`/`https://` only | — (required for subxt) |
| `HCFS_CHAIN_SIGNER_SEED` | sr25519 mnemonic **or** `0x`-prefixed 32-byte hex seed | — (required for subxt) |
| `HCFS_CHAIN_REPORT_INTERVAL_SECS` | tick interval | 6 (block time) |
| `HCFS_EXEMPT_ACCOUNTS` | CSV of ss58s never reported | ∅ |
| `DATABASE_URL` | Postgres source of `user_summaries` | `postgres://localhost/hcfs` |

`log_only` is the safe v1 default before a `SubscriptionCanceller` is set — per-account state
and watermark still advance (`chain.rs:179-227`).

### 5.3 The tick / dedup / watermark model (reusable)

- A singleton `chain_report_cursor` row is locked `FOR UPDATE`; dirty `user_summaries` rows
  since the watermark are pulled, capped at `MAX_ROWS_PER_TICK = 50_000`
  (`loop_.rs:24, 163-191`).
- Per-account **payload hash gate** so unchanged aggregates don't resubmit — the hash covers
  the usage numbers but **excludes `ss58` and `observed_at`** (`types.rs:50-55`,
  `AccountUsageReport::payload_hash`).
- `user_id` classification into `SyncEngine` / `FileShares` / `Bare` origins with S3 derived as
  `bare − Σ(folder) − share` (`identify.rs:1-45`).
- Cursor contract: returning `Ok` means durably finalized; the worker only then advances state
  + watermark (`chain.rs:88-94`). Batches over `MAX_UPDATES_PER_CALL` submit the first chunk and
  hold the watermark for the next tick (`loop_.rs:504-512`).

### 5.4 How much transfers

| Component | Transfer to Rust rewrite |
|---|---|
| subxt online client + reconnecting transport | **Verbatim** — solves real prod wedges |
| sr25519 signer via `subxt_signer` + `SecretString` | **Verbatim** (§6.3) |
| Pool-aware nonce (`system_account_next_index`) + `.mortal(64)` | **Verbatim** |
| Runtime-version updater task | **Verbatim** |
| Metadata pinning (`subxt::subxt!(runtime_metadata_path=...)`) | **Verbatim** pattern, own `.scale` |
| Watermark/hash-gate/tick loop | **Reusable design**, S3-specific SQL |
| Exact pallet **field names/order** | **Re-derive** — S3's usage rows differ from HCFS's drive/share model |
| Error classification (`1010` substring, module errors) | **Verbatim** |
| Pin/unpin of objects (write path) | **Does not apply** — that stays Arion HTTP in S3 today |

---

## 6. Rust implementation notes

### 6.1 subxt vs raw JSON-RPC

- For **read-only credit scraping** (§2, the only thing `hippius-s3` does with Substrate
  today): subxt's dynamic storage API or plain `state_getKeysPaged` + SCALE decoding both work.
  This path signs nothing, so the heavy subxt static-codegen machinery is optional — but reusing
  the sibling's `OnlineClient` (with the reconnecting transport) is the least-surprise choice
  and gets you the runtime-version safety for free.
- For any **write path** (only if the product changes to submit its own extrinsics): use subxt
  static codegen (`subxt::subxt!`), not raw JSON-RPC — encoding a call by hand and matching the
  runtime's SCALE layout is exactly the trap §6.4 warns about.

### 6.2 Metadata pinning

`#[subxt::subxt(runtime_metadata_path = "metadata/thebrain.scale")]`
(`chain_subxt.rs:74`) reads the metadata at **compile time** and generates typed call builders.
A runtime upgrade that changes a call signature then surfaces as a **build failure**, not a
production panic (`chain_subxt.rs:46-50`). Refresh the `.scale` per runtime upgrade. At runtime,
the background runtime-version updater keeps `spec_version`/`transaction_version` current so
signatures stay valid mid-pod-lifetime (§2.4). Also probe runtime constants
(`MaxUserFileUsageUpdatesPerCall`) at startup and warn on mismatch rather than failing
(`chain_subxt.rs:363-389`).

### 6.3 Key management (`SecretString`)

```rust
// hcfs-chain-reporter/src/main.rs:179  — wrap the env seed at the boundary
let seed = std::env::var("HCFS_CHAIN_SIGNER_SEED").map(SecretString::from) ...;
let signer = load_signer(seed)?;    // chain_subxt.rs:1216
```

`load_signer` (`chain_subxt.rs:1216-1234`):
- Empty/whitespace → `SignerLoadError::Missing`.
- `0x`-prefixed → `hex::decode` → `[u8;32]` → `Keypair::from_secret_key`.
- Else treat as BIP-39 → `subxt_signer::bip39::Mnemonic::from_str` → `Keypair::from_phrase`.
- Error types carry **no seed-derived bytes**; the seed buffer is zeroized on drop
  (`secrecy`). Never log the seed, and never put it in an error `Display`.

**No seed/secret values appear in this document, and none must appear in logs, error messages,
config committed to the repo, or the rewrite's source.** For the S3 write-side HTTP path, the
analogous secret is `HIPPIUS_SERVICE_KEY`/`ARION_SERVICE_KEY` — treat identically.

### 6.4 The exact-encoding trap (matching what the other side sends)

Because on-chain records must stay consistent across the fleet, the rewrite must reproduce the
*exact* extrinsic argument shapes — this is where silent divergence hides:

1. **Field order and names in the SCALE struct.** `UserBackendFileUsageUpdate` has five fields
   in a fixed order (`account_id, drive_file_size, drive_file_count, s3_file_size,
   s3_file_count`, `chain_subxt.rs:465-471`). SCALE is positional; a field swap encodes silently
   and corrupts on-chain state. Use the codegen types, never hand-rolled structs.
2. **Integer widths.** Sizes are `u128` on chain; saturating-add at u128 boundaries, don't cast
   to u64 (`chain_subxt.rs:706-707`).
3. **Account identifier duality.** `user_id` appears as either SS58 base58 **or** 64-char hex
   pubkey; `parse_account_id` tries SS58 first, then 64-hex → `AccountId32`
   (`chain_subxt.rs:952-979`). SS58 must be **prefix 42** to match everything else in the
   ecosystem. A malformed id must be dropped-and-metric'd per-row, never aborting the whole
   batch (`chain.rs:95-101`).
4. **`Pays::No` must hold end-to-end.** The pre-9184 shape wrapped singular calls in
   `utility.batch_all`, which carried `Pays::Yes` on the outer dispatch and charged the signer
   → `InvalidTransaction("Inability to pay some fees")`. Use the native plural pallet call whose
   `Pays::No` is inherited end-to-end (`chain_subxt.rs:630-657`).
5. **Nonce/mortality/finality.** Reuse pool-aware nonce + `.mortal(64)` +
   `wait_for_finalized_success`, or reproduce the 2026-05-07 stale-nonce and BadProof incidents
   (§2.4).
6. **HTTP side (the part S3 actually does today).** Match the exact JSON body keys Arion/
   api.hippius.com expect: pin body `{cid, original_name, size_bytes, account_ss58,
   request_type}` (`hippius_api_service.py:346-352`), and the plans-call relative-path
   `s3/plans/accounts/` (no `/api` prefix). And match the retry semantics exactly (3× / fixed
   5 s / no-retry on 401/403/404 — §1.3).
7. **Do not `RENAME` a never-written key / do not publish partial rolls.** The atomic-swap and
   shrink-guard invariants (§3.3) are correctness requirements: violating them fleet-wide-402s
   paying customers.

---

## 7. Open questions

1. **`HIPPIUS_VALIDATOR_REGION`** is a required env (`config.py:216`) with **no reader** in this
   Python repo. Where is it consumed — a header the rewrite must send to Arion/api.hippius.com,
   a chain-side region tag, or dead config? Must be resolved before the rewrite drops or
   reimplements it. (Verify against the actual consumer per the `verify_against_code_not_config`
   memory.)
2. **Does the rewrite gain a direct chain-write path?** Today `hippius-s3` never signs
   extrinsics for storage; pinning is Arion's. If the rewrite is meant to submit
   `pallet_arion`/`pallet_marketplace` usage itself (mirroring `hcfs-chain-reporter`), that is a
   product change, not a port — confirm intent before adding a signer seed to the S3 service.
3. **Is `HippiusApiClient.pin_file` truly dead, or is it a planned/again-needed path?** It has no
   callers now but is fully maintained. Decide whether to port it or drop it.
4. **Credit-cache TTL (600 s) vs plan-cache no-TTL divergence.** Intentional (they degrade
   differently), but the rewrite should document/decide whether the Substrate credit read should
   also move to a no-TTL last-known-good model given the same "outage must not demote customers"
   argument that drove the plan cache.
5. **`SubAccount` pallet presence.** The account cacher treats the subaccount pallet as
   optional (`RuntimeError` → skip). Confirm whether the target chain has it, so the rewrite can
   decide between graceful-skip and hard-require.
6. **Which chain does `HIPPIUS_SUBSTRATE_URL` point at vs thebrain?** The read-only credit
   scraper's `Credits.FreeCredits` and the reporter's thebrain `pallet_marketplace` may or may
   not be the same runtime; the reporter pins `metadata/thebrain.scale`. Confirm they're the
   same chain (and pin one metadata file) before unifying clients.
7. **k8s config drift** (per memory `hcfs_k8s_config_drift`): the live deployment may set chain/
   billing env vars absent from committed manifests. Reconcile the real env against `config.py`
   defaults before finalizing the rewrite's config surface.

---

### Appendix — primary source file map

**hippius-s3 (Python, subject):**
- `hippius_s3/services/hippius_api_service.py` — HTTP client (pin/unpin/upload/auth/plan roll), retry decorator, plan wire types
- `hippius_s3/services/plans_cache.py` — Redis plan cache: atomic swap, shrink guard, `PlanQuota`, `get_plan_for_account`
- `hippius_s3/services/arion_service.py` — live Arion upload/unpin
- `hippius_s3/gateway/services/plan_gate.py` — request-path quota gate
- `hippius_s3/gateway/services/account_service.py` — credit gate (fail-open)
- `hippius_s3/models/account.py` — `HippiusAccount`
- `hippius_s3/blake3_hash.py` — plaintext BLAKE3 (display hash), column-separation rule
- `cacher/run_cacher.py` — read-only Substrate credit scraper (`SubstrateInterface`)
- `workers/run_account_cacher_in_loop.py` — 300 s credit-cache loop
- `workers/run_plans_cacher_in_loop.py` — plan scrape/publish loop, enforceable-row logic
- `hippius_s3/config.py` — all env (`HIPPIUS_SUBSTRATE_URL`, `HIPPIUS_API_BASE_URL`, keys, region, SS58=42, flags)
- `hippius_s3/sql/migrations/20250901000000_*`, `20260130000000_add_chunk_backend.sql`, `20251003000000_create_part_chunks.sql` — CID schema
- `docs/sequence-substrate-retry.md` — "HTTP API, not direct Substrate" statement

**hcfs-chain-reporter (Rust, migration template):**
- `src/chain_subxt.rs` — subxt client, signing, nonce, mortality, runtime updater, error classification, `load_signer`
- `src/chain.rs` — `ChainReporter` trait, `ChainError`, field-mapping contracts, `LogOnlyChainReporter`
- `src/main.rs` — `HCFS_CHAIN_*` env, backend selection, tick loop
- `src/loop_.rs` — watermark, `MAX_ROWS_PER_TICK`, `MAX_UPDATES_PER_CALL=250`, tick flow
- `src/types.rs` — `AccountUsageReport`, `payload_hash`
- `src/identify.rs` — user_id origin classification
- `src/exempt.rs` — `HCFS_EXEMPT_ACCOUNTS`
- `Cargo.toml` — deps: `subxt`, `subxt-signer`, `secrecy`, `blake3`, `sqlx`
