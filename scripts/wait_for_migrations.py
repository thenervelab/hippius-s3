"""Block until the DB's applied dbmate migrations include the newest migration baked into this
image, then exit 0. Used as a worker initContainer (D2): workers `exec python <script>` with no
migrate step of their own, unlike the API (start-api.sh runs migrate before uvicorn), so without a
gate a freshly-rolled worker can start against a schema older than the code it runs and error on a
column/table a new migration added. This only WAITS — the API startup and the db-migrations Job
own APPLYING migrations. Version-agnostic: the target is the newest local migration's version
prefix, so no manifest needs updating when a migration is added. The workers image lacks dbmate
(that lives only in the API image), so this checks schema_migrations directly via asyncpg.
"""

from __future__ import annotations

import asyncio
import glob
import os
import sys

import asyncpg

import hippius_s3


def _latest_local_version() -> str | None:
    mig_dir = os.path.join(os.path.dirname(hippius_s3.__file__), "sql", "migrations")
    # dbmate records `version` as the numeric filename prefix (e.g. 20260706130000).
    versions = sorted(os.path.basename(f).split("_", 1)[0] for f in glob.glob(os.path.join(mig_dir, "*.sql")))
    return versions[-1] if versions else None


async def _wait(want: str, dsn: str) -> None:
    while True:
        try:
            conn = await asyncpg.connect(dsn)
            try:
                row = await conn.fetchrow("SELECT 1 FROM schema_migrations WHERE version = $1", want)
            finally:
                await conn.close()
            if row is not None:
                print(f"migrations current (>= {want}); starting worker", flush=True)
                return
            print(f"waiting for migration {want} to be applied", flush=True)
        except Exception as exc:
            # Table absent / DB not up yet / transient connection error — keep polling.
            print(f"waiting for migrations db: {exc}", flush=True)
        await asyncio.sleep(3)


def main() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set; cannot gate on migrations", file=sys.stderr)
        sys.exit(1)
    want = _latest_local_version()
    if not want:
        print("no local migrations found; nothing to wait for", flush=True)
        return
    asyncio.run(_wait(want, dsn))


if __name__ == "__main__":
    main()
