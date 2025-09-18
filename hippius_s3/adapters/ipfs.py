from __future__ import annotations

from typing import Protocol
from typing import cast
from typing import runtime_checkable

from hippius_s3.ipfs_service import IPFSService


@runtime_checkable
class IpfsAdapter(Protocol):
    async def publish(
        self,
        *,
        content: bytes,
        encrypt: bool,
        seed_phrase: str,
        subaccount_id: str,
        bucket_name: str,
        file_name: str,
        store_node: str,
        pin_node: str,
        substrate_url: str,
        publish_on_chain: bool,
    ) -> str: ...  # returns CID

    async def get(self, cid: str) -> bytes: ...

    async def pin(self, cid: str) -> None: ...


class DefaultIpfsAdapter(IpfsAdapter):
    def __init__(self, ipfs_service: IPFSService) -> None:
        self._svc = ipfs_service

    async def publish(
        self,
        *,
        content: bytes,
        encrypt: bool,
        seed_phrase: str,
        subaccount_id: str,
        bucket_name: str,
        file_name: str,
        store_node: str,
        pin_node: str,
        substrate_url: str,
        publish_on_chain: bool,
    ) -> str:
        s3_result = await self._svc.client.s3_publish(
            content=content,
            encrypt=encrypt,
            seed_phrase=seed_phrase,
            subaccount_id=subaccount_id,
            bucket_name=bucket_name,
            file_name=file_name,
            store_node=store_node,
            pin_node=pin_node,
            substrate_url=substrate_url,
            publish=publish_on_chain,
        )
        return cast(str, s3_result.cid)

    async def get(self, cid: str) -> bytes:
        return cast(bytes, await self._svc.client.get(cid))

    async def pin(self, cid: str) -> None:
        await self._svc.client.pin(cid)
