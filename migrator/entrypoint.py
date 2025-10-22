from __future__ import annotations

import asyncio
import os
import sys


async def main() -> int:
    bucket = os.environ.get("MIGRATE_BUCKET", "")
    key = os.environ.get("MIGRATE_KEY", "")
    dry = os.environ.get("MIGRATE_DRY_RUN", "false").lower() == "true"

    args = [sys.executable, "-m", "hippius_s3.scripts.migrate_objects"]
    if bucket:
        args += ["--bucket", bucket]
    if key:
        args += ["--key", key]
    if dry:
        args += ["--dry-run"]

    proc = await asyncio.create_subprocess_exec(*args)
    return await proc.wait()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
