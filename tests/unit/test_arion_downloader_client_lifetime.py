"""The Arion downloader must reuse ONE ArionClient for the worker's lifetime.

A client per `fetch_fn` call means a fresh `httpx.AsyncClient` — and a fresh TCP+TLS
handshake — for every 4 MiB chunk (~1280 of them for a 5 GiB part). The uploader
(`run_arion_uploader_in_loop.py`) and unpinner (`workers/unpinner.py`) already hold one
client per pod; these tests pin the downloader to the same contract.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


_PROJECT_ROOT = Path(__file__).parents[2]


def _load_entrypoint() -> ModuleType:
    """Import the worker entrypoint by path — `workers/` is not an importable package."""
    sys.path.insert(0, str(_PROJECT_ROOT))
    path = _PROJECT_ROOT / "workers" / "run_arion_downloader_in_loop.py"
    spec = importlib.util.spec_from_file_location("run_arion_downloader_in_loop", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeArionClient:
    """Records construction/close and counts downloads, so tests can assert reuse."""

    instances: list["FakeArionClient"] = []

    def __init__(self) -> None:
        self.downloads: list[str] = []
        self.closed = False
        FakeArionClient.instances.append(self)

    async def __aenter__(self) -> "FakeArionClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        self.closed = True

    async def download_file(self, file_id: str, account_ss58: str, chunk_size: int = 65536) -> AsyncIterator[bytes]:
        self.downloads.append(file_id)
        yield b"cipher-"
        yield file_id.encode()


@pytest.fixture
def entrypoint(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    FakeArionClient.instances = []
    module = _load_entrypoint()
    monkeypatch.setattr(module, "ArionClient", FakeArionClient)
    return module


async def _run_main_with_fetches(module: ModuleType, monkeypatch: pytest.MonkeyPatch, num_chunks: int) -> list[bytes]:
    """Drive `main()` with a stub loop that calls the supplied fetch_fn `num_chunks` times."""
    results: list[bytes] = []

    async def fake_loop(*, backend_name: str, queue_name: str, fetch_fn: Any) -> None:
        assert backend_name == "arion"
        assert queue_name == "arion_download_requests"
        for i in range(num_chunks):
            results.append(await fetch_fn(f"chunk-id-{i}", "5TestAddr"))

    monkeypatch.setattr(module, "run_downloader_loop", fake_loop)
    await module.main()
    return results


@pytest.mark.asyncio
async def test_single_client_across_many_chunk_fetches(entrypoint: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    """One ArionClient serves every chunk — not one per chunk."""
    await _run_main_with_fetches(entrypoint, monkeypatch, num_chunks=10)

    assert len(FakeArionClient.instances) == 1, (
        f"downloader built {len(FakeArionClient.instances)} ArionClients for 10 chunks; "
        "each one is a fresh connection pool and a fresh TLS handshake"
    )
    assert FakeArionClient.instances[0].downloads == [f"chunk-id-{i}" for i in range(10)]


@pytest.mark.asyncio
async def test_client_stays_open_across_loop_and_closes_on_exit(
    entrypoint: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The client must outlive individual fetches, and close only when the loop exits."""
    closed_during_loop: list[bool] = []

    async def fake_loop(*, backend_name: str, queue_name: str, fetch_fn: Any) -> None:
        for i in range(3):
            await fetch_fn(f"chunk-id-{i}", "5TestAddr")
            # A client closed while the loop is still running means the next chunk cold-starts.
            closed_during_loop.append(any(c.closed for c in FakeArionClient.instances))

    monkeypatch.setattr(entrypoint, "run_downloader_loop", fake_loop)
    await entrypoint.main()

    assert closed_during_loop == [False, False, False]
    assert FakeArionClient.instances[0].closed is True


@pytest.mark.asyncio
async def test_fetch_concatenates_streamed_chunks(entrypoint: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    """fetch_fn still returns the whole ciphertext body, joined in stream order."""
    results = await _run_main_with_fetches(entrypoint, monkeypatch, num_chunks=2)

    assert results == [b"cipher-chunk-id-0", b"cipher-chunk-id-1"]


@pytest.mark.asyncio
async def test_client_survives_a_failing_fetch(entrypoint: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    """A chunk-level failure must not tear down the shared pool for later chunks.

    `_fetch_chunk` catches fetch errors and retries, so a raising `download_file` has to
    leave the client usable — otherwise one bad chunk would cold-start every chunk after it.
    """
    calls: list[str] = []

    async def flaky_download(self: FakeArionClient, file_id: str, account_ss58: str, chunk_size: int = 65536):
        calls.append(file_id)
        if file_id == "chunk-id-1":
            raise RuntimeError("arion 500")
        yield b"ok"

    monkeypatch.setattr(FakeArionClient, "download_file", flaky_download)

    async def fake_loop(*, backend_name: str, queue_name: str, fetch_fn: Any) -> None:
        for i in range(3):
            try:
                await fetch_fn(f"chunk-id-{i}", "5TestAddr")
            except RuntimeError:
                pass

    monkeypatch.setattr(entrypoint, "run_downloader_loop", fake_loop)
    await entrypoint.main()

    assert len(FakeArionClient.instances) == 1
    assert calls == ["chunk-id-0", "chunk-id-1", "chunk-id-2"]
    assert FakeArionClient.instances[0].closed is True
