# Substrate Data Cacher

Caches Hippius blockchain (Substrate) account data in Redis to avoid per-request RPC calls from the gateway and API.

## What It Caches

The cacher fetches three data sets from the Hippius Substrate chain and stores them in Redis:

| Data | Source (Substrate Storage) | Redis Key | TTL |
|------|---------------------------|-----------|-----|
| Free credits | `Credits.FreeCredits` | `hippius_main_account_credits:{account_id}` | 10 min |
| Subaccount roles | `SubAccount.SubAccountRole` | `hippius_subaccount_cache:{subaccount_id}` | 10 min |
| Subaccount mappings | `SubAccount.SubAccount` | (included in subaccount cache) | 10 min |

### Redis Value Format

**Main account credits**:
```json
{"main_account_id": "...", "free_credits": 12345, "has_credits": true}
```

**Subaccount cache**:
```json
{"subaccount_id": "...", "main_account_id": "...", "role": "admin", "free_credits": 12345, "has_credits": true}
```

## Configuration

| Variable | Description |
|----------|-------------|
| `HIPPIUS_SUBSTRATE_URL` | Substrate RPC endpoint (default: `wss://rpc.hippius.network`) |
| `REDIS_ACCOUNTS_URL` | Redis connection for account cache (port 6380) |

## Running

**Docker (recommended)** - runs as the `account-cacher` service in Docker Compose:

```bash
docker compose up -d account-cacher
```

**Standalone**:

```bash
source .venv/bin/activate
REDIS_ACCOUNTS_URL=redis://127.0.0.1:6380/0 python cacher/run_cacher.py
```

The cacher runs a single pass and exits. In Docker Compose it is wrapped in a loop by the worker entry point (`run_account_cacher_in_loop.py`), executing approximately every 5 minutes.
