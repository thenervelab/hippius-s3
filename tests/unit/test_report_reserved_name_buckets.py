from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any

import pytest

from hippius_s3.scripts.report_reserved_name_buckets import ACL_BYPASSED_SEGMENTS
from hippius_s3.scripts.report_reserved_name_buckets import RESERVED_SEGMENTS
from hippius_s3.scripts.report_reserved_name_buckets import _severity


def _row(name: str, deleted: bool = False) -> dict[str, Any]:
    return {
        "bucket_name": name,
        "deleted_at": datetime.now(timezone.utc) if deleted else None,
    }


@pytest.mark.parametrize("name", ["user", "health"])
def test_acl_bypassed_names_are_the_worst_severity(name: str) -> None:
    """acl_middleware returns before the permission check for `/health` and `/user/*`, so a bucket
    on those names has no auth AND no ACL in front of its objects — not merely stranded."""
    assert _severity(_row(name)) == "acl-bypass"


@pytest.mark.parametrize("name", ["docs", "metrics", "redoc", "robots.txt", "openapi.json"])
def test_other_reserved_names_are_stranded(name: str) -> None:
    assert _severity(_row(name)) == "stranded"


def test_soft_deleted_rows_are_resolved() -> None:
    """Uniqueness is enforced by a partial index over live rows, so a soft-deleted row no longer
    holds the name — it's history, not something to remediate."""
    assert _severity(_row("docs", deleted=True)) == "resolved"


def test_acl_bypassed_segments_are_a_subset_of_reserved() -> None:
    assert ACL_BYPASSED_SEGMENTS <= set(RESERVED_SEGMENTS)
