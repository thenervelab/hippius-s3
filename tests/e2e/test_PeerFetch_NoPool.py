"""E2e job re-runs the no-pool peer-fetch pins once the stack is up.

Single-node compose cannot PUT on node A and GET on node B. The real two-node
path lives in tests/unit/test_peer_fetch_no_pool_path.py and is imported here so
`pytest tests/e2e` still fails if that contract is deleted.
"""

from __future__ import annotations

from tests.unit.test_peer_fetch_no_pool_path import (  # noqa: F401
    test_a_five_part_mpu_is_read_entirely_from_the_ingest_peer_with_no_pool,
)
from tests.unit.test_peer_fetch_no_pool_path import (  # noqa: F401
    test_dropping_peer_fetch_at_the_factory_makes_the_cross_node_read_miss,
)
from tests.unit.test_peer_fetch_no_pool_path import (  # noqa: F401
    test_reader_node_with_no_pool_serves_an_uploading_part_from_the_ingest_peer,
)
