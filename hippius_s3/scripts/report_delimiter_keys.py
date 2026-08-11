"""Report objects whose key contains `?` or `#`, which the gateway now refuses.

Run this BEFORE deploying the delimiter refusal, and again after, to know whether any client is
affected.

Such keys are legal in S3 and AWS accepts them, so the refusal is a deliberate compatibility
divergence. It is not, however, a regression: `ForwardService` interpolates the decoded path into
a URL string that httpx re-parses, and both characters are delimiters there — so a request naming
one of these keys was already being TRUNCATED at it. A GET returned the wrong object, and a PUT
landed on the wrong one. The refusal turns that silent wrongness into a 400.

Rows this finds therefore fall in two groups, and the distinction is what matters:

  * A key CONTAINING a delimiter — reachable only through a path that skips gateway validation
    (the internal api, CopyObject's x-amz-copy-source header, the batch DeleteObjects body, or a
    migration script). Its owner cannot address it over the S3 path today either; the object is
    reachable by CopyObject or batch delete, which take the key from a header or body rather than
    the path, and those still work.
  * A key that is a TRUNCATION of one — i.e. the damage already done. These are indistinguishable
    from ordinary keys here, so this script cannot find them; it reports the first group only.

Read-only. Prints a report and exits non-zero if it finds anything, so it can gate a deploy step.
"""

from __future__ import annotations

import asyncio
import os
import sys

import asyncpg


QUERY = """
SELECT
    b.bucket_name,
    o.object_key,
    o.created_at,
    b.main_account_id
FROM objects o
JOIN buckets b ON b.bucket_id = o.bucket_id
WHERE o.deleted_at IS NULL
  AND b.deleted_at IS NULL
  AND o.object_key ~ '[?#]'
ORDER BY b.main_account_id, b.bucket_name, o.object_key
"""


async def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 2

    conn = await asyncpg.connect(dsn=dsn)
    try:
        rows = await conn.fetch(QUERY)
    finally:
        await conn.close()

    if not rows:
        print("No live object keys contain '?' or '#'. The delimiter refusal affects nothing.")
        return 0

    print(f"{len(rows)} live object key(s) contain '?' or '#':\n")
    for r in rows:
        print(f"  account={r['main_account_id']}  bucket={r['bucket_name']}  key={r['object_key']!r}")

    accounts = sorted({r["main_account_id"] for r in rows})
    print(
        f"\nAcross {len(accounts)} account(s). These keys are already unaddressable over the S3 "
        "path (the request truncates at the delimiter before reaching the api), so the refusal "
        "changes the error rather than the access. CopyObject and batch DeleteObjects still reach "
        "them, since those take the key from a header or body."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
