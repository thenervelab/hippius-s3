from __future__ import annotations

import os
from typing import Any

import requests


def test_trailing_slash_normalizer_for_public_path(docker_services: Any) -> None:
    base_url = os.environ.get("HIPPIUS_E2E_BASE_URL", "http://localhost:8000")

    # Without trailing slash
    r1 = requests.get(f"{base_url}/robots.txt", timeout=5)
    assert r1.status_code == 200
    assert r1.text.startswith("User-agent: *")

    # With trailing slash - middleware should normalize and still serve the file
    r2 = requests.get(f"{base_url}/robots.txt/", timeout=5)
    assert r2.status_code == 200
    assert r2.text.startswith("User-agent: *")
