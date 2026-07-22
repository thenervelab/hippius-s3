"""Cross-language wire-contract test for the drain -> uploader UploadChainRequest.

The Rust drain is the sole producer of backend upload requests and the Python uploader
the sole consumer, coupled only by a hand-kept `KEEP IN SYNC` comment. This pins the
contract with a golden fixture: the Rust side asserts it serializes to
`tests/fixtures/upload_chain_request.golden.json`
(crates/hippius-drain-agent/src/enqueue.rs), and this test asserts the Python model
accepts the same bytes. Drift on either side fails one of the two tests.
"""

from __future__ import annotations

import json
from pathlib import Path

from hippius_s3.queue import UploadChainRequest


GOLDEN = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "upload_chain_request.golden.json"


def test_rust_golden_upload_chain_request_validates_in_python() -> None:
    payload = json.loads(GOLDEN.read_text())
    req = UploadChainRequest.model_validate(payload)

    assert req.address == "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
    assert req.bucket_name == "my-bucket"
    assert req.object_key == "path/to/key.bin"
    assert req.object_id == "466916c0-d61b-4518-b81b-9576b574270a"
    assert req.object_version == 5
    assert [c.id for c in req.chunks] == [1]
    assert req.upload_id == "11111111-1111-4111-8111-111111111111"
    assert req.upload_backends == ["arion"]
    assert req.attempts == 0
    assert req.bypass_billing is False
    # Fields the Rust producer omits must take the model's defaults, not raise.
    assert req.request_id is None
    assert req.ray_id is None
    assert req.last_error is None
    # The upload_id presence drives the uploader's request name (simple:: vs multipart::).
    assert req.name == (
        "multipart::466916c0-d61b-4518-b81b-9576b574270a::11111111-1111-4111-8111-111111111111::5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
    )
