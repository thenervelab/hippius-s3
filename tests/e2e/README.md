# E2E Tests for Hippius S3

This directory contains end-to-end tests that verify the full Hippius S3 pipeline from S3 client to IPFS storage and blockchain publishing.

## Current Setup (with bypass flags)

The tests currently use environment variable bypasses to skip credit checks and blockchain publishing:

- `HIPPIUS_BYPASS_CREDIT_CHECK=true` - Skips credit verification for write operations
- `HIPPIUS_PUBLISH_MODE=ipfs_only` - Uses IPFS-only mode instead of full blockchain publishing for single file uploads

These bypasses allow tests to run without:

- A funded blockchain account
- Chain connectivity
- Credit verification

**Note**: Multipart uploads are not currently bypassed and still require full blockchain publishing. The current tests focus on single-file operations only.

## Running the Tests

```bash
# Run e2e tests
pytest tests/e2e/ -v
```

### Run against real AWS (optional)

You can run the same tests against AWS S3 to validate test expectations.

Requirements:

- AWS credentials configured (env vars, default profile, or other supported methods)
- A region with S3 enabled (default: us-east-1)

Command:

```bash
RUN_REAL_AWS=1 AWS_REGION=us-east-1 pytest tests/e2e -v -m 'not local_only'
```

Notes:

- Some tests may be marked `local_only` if they exercise Hippius-specific endpoints/behaviors.
- Buckets are created with globally unique names and cleaned up after tests, but charges may apply.

## Making Tests Truly E2E (Removing Bypasses)

To make the tests truly end-to-end, you need to:

### 1. Remove Environment Bypasses

Remove these from `docker-compose.e2e.yml` (they are set under the `api`, `pinner`, and `unpinner` services):

```yaml
# In docker-compose.e2e.yml
services:
  api:
    environment:
      # delete these lines
      - HIPPIUS_BYPASS_CREDIT_CHECK=true
      - HIPPIUS_PUBLISH_MODE=ipfs_only
  pinner:
    environment:
      # delete this line
      - HIPPIUS_PUBLISH_MODE=ipfs_only
  unpinner:
    environment:
      # delete this line
      - HIPPIUS_PUBLISH_MODE=ipfs_only
```

### 2. Remove Bypass Code from Source

Remove bypass logic from:

- `hippius_s3/api/middlewares/credit_check.py` - Remove the `HIPPIUS_BYPASS_CREDIT_CHECK` check
- `hippius_s3/api/s3/endpoints.py` - Remove the `HIPPIUS_PUBLISH_MODE` check
- `hippius_s3/ipfs_service.py` - Remove the `HIPPIUS_PUBLISH_MODE` check

### 3. Set Up Blockchain Infrastructure

Add to `docker-compose.yml`:

```yaml
services:
  # Add a local Hippius/Substrate devnet
  substrate:
    image: hippius/substrate:latest # Or appropriate image
    ports:
      - "9944:9944" # RPC port
    environment:
      - CHAIN=dev
    # Pre-fund test accounts in genesis

  # Optional: Add a faucet service
  faucet:
    image: hippius/faucet:latest
    depends_on:
      - substrate
    environment:
      - SUBSTRATE_RPC_URL=http://substrate:9944
```

### 4. Configure Chain Connection

Add environment variables:

```bash
# In docker-compose.yml or test setup
HIPPIUS_SUBSTRATE_RPC_URL=http://substrate:9944
```

Update `hippius_s3/dependencies.py` to use configurable RPC URL:

```python
# In check_account_has_credit()
substrate_client = SubstrateClient(
    rpc_url=os.getenv("HIPPIUS_SUBSTRATE_RPC_URL"),
    password=None,
    account_name=None
)
```

### 5. Fund Test Account

Either:

- Pre-fund the test seed in the devnet genesis
- Use a faucet to fund the account before running tests
- Generate a new seed per test run and fund it

### 6. Update Test Configuration

Modify `conftest.py`:

```python
@pytest.fixture(scope="session")
def test_seed_phrase():
    """Generate a funded test seed phrase."""
    # Option 1: Use pre-funded seed
    return "funded twelve word seed phrase"

    # Option 2: Generate and fund via faucet
    # seed = generate_new_seed()
    # fund_account_via_faucet(seed)
    # return seed
```

### 7. Add Verification Steps

After upload operations, verify on-chain:

```python
# In test_single_file_upload.py
def verify_on_chain(tx_hash):
    """Verify transaction exists on blockchain."""
    # Use Hippius SDK to query transaction
    substrate_client = SubstrateClient(rpc_url="http://substrate:9944")
    tx_info = substrate_client.get_transaction(tx_hash)
    assert tx_info is not None
    assert tx_info["status"] == "finalized"
```

### 8. Handle Timing

Blockchain operations introduce latency. Add polling:

```python
def wait_for_publish(bucket, key, timeout=30):
    """Wait for object to be fully published."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        obj = boto3_client.get_object(Bucket=bucket, Key=key)
        metadata = obj.get("Metadata", {})
        if metadata.get("hippius", {}).get("tx_hash"):
            return obj
        time.sleep(2)
    raise TimeoutError("Object not published within timeout")
```

## Test Flow (True E2E)

1. **Account Setup**: Verify/fund test seed has credit
2. **Bucket Creation**: PUT /{bucket}
3. **File Upload**: PUT /{bucket}/{key} → IPFS upload + encrypt + pin + blockchain publish
4. **Verification**:
   - GET /{bucket}/{key} returns decrypted content
   - On-chain verification of transaction
5. **Cleanup**: DELETE /{bucket}/{key}, DELETE /{bucket}

## Migration Steps

1. ✅ Add bypass flags (current state)
2. 🔄 Set up local devnet infrastructure
3. 🔄 Fund test accounts
4. 🔄 Remove bypass flags from code
5. 🔄 Add on-chain verification
6. 🔄 Update CI/CD to use real chain

## Security Notes

- Never commit funded seed phrases to version control
- Use CI secrets for funded accounts
- Consider rotating test seeds periodically
- Monitor chain costs for test accounts
