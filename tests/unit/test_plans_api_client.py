"""URL construction for the S3 plans endpoint.

Two mistakes here are invisible until they hit an environment:

  * HIPPIUS_API_BASE_URL already ends in /api, so spelling "/api/s3/plans/accounts/" produces
    /api/api/s3/plans/accounts/ and 404s forever — in prod only, because the e2e mock's base has no
    prefix.
  * upstream's `next` is an ABSOLUTE url on api.hippius.com. Following it verbatim would walk the
    e2e cacher straight out of the mock and into production, and would let an upstream response
    field point our service-token-authenticated client at a host we did not choose.
"""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from hippius_s3.services.hippius_api_service import HippiusApiClient


PAYLOAD = {"generated_at": "2026-09-08T16:05:12Z", "count": 0, "next": None, "plans": {}, "results": []}


async def captured_url(base_url: str, **kwargs: object) -> str:
    with patch("hippius_s3.services.hippius_api_service.get_config") as get_config:
        get_config.return_value = MagicMock(
            hippius_api_base_url=base_url,
            hippius_service_key="k",
            plans_api_timeout_seconds=30.0,
        )
        client = HippiusApiClient()

    response = MagicMock()
    response.json.return_value = PAYLOAD
    response.raise_for_status = MagicMock()

    with patch.object(client._client, "get", new_callable=AsyncMock, return_value=response) as get:
        await client.get_s3_plan_accounts(**kwargs)  # type: ignore[arg-type]

    call = get.call_args
    return str(client._client.build_request("GET", call.args[0], params=call.kwargs.get("params")).url)


@pytest.mark.asyncio
async def test_the_first_page_does_not_double_the_api_prefix() -> None:
    url = await captured_url("https://api.hippius.com/api")
    assert url == "https://api.hippius.com/api/s3/plans/accounts/?page=1&page_size=500"
    assert "/api/api/" not in url


@pytest.mark.asyncio
async def test_the_first_page_resolves_against_a_prefixless_mock_base() -> None:
    url = await captured_url("http://mock-hippius-api:8001")
    assert url == "http://mock-hippius-api:8001/s3/plans/accounts/?page=1&page_size=500"


@pytest.mark.asyncio
async def test_a_custom_page_size_is_honoured() -> None:
    url = await captured_url("https://api.hippius.com/api", page_size=100)
    assert "page_size=100" in url


@pytest.mark.asyncio
async def test_the_next_url_is_re_homed_onto_our_own_host() -> None:
    """Path and query are kept; scheme and host are ours."""
    url = await captured_url(
        "http://mock-hippius-api:8001",
        next_url="https://api.hippius.com/api/s3/plans/accounts/?page=2&page_size=500",
    )

    parsed = httpx.URL(url)
    assert parsed.host == "mock-hippius-api"
    assert parsed.port == 8001
    assert parsed.path == "/api/s3/plans/accounts/"
    assert parsed.params.get("page") == "2"
    assert "api.hippius.com" not in url


@pytest.mark.asyncio
async def test_a_next_url_pointing_at_an_unrelated_host_cannot_redirect_us() -> None:
    """An upstream field must never be able to send our authenticated client somewhere else."""
    url = await captured_url(
        "https://api.hippius.com/api",
        next_url="https://evil.example.com/api/s3/plans/accounts/?page=2",
    )

    assert httpx.URL(url).host == "api.hippius.com"
    assert "evil.example.com" not in url
