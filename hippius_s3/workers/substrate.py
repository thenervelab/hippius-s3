import asyncio
import contextlib
import json
import logging
import pprint
import random
import time
import uuid
from typing import Any
from typing import Dict
from typing import List

import httpx
from substrateinterface import Keypair
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException

from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import SubstratePinningRequest


logger = logging.getLogger(__name__)


def compute_substrate_backoff_ms(attempt: int, base_ms: int = 500, max_ms: int = 5000) -> float:
    """Compute exponential backoff with jitter for substrate retries."""
    exp_backoff = base_ms * (2 ** (attempt - 1))
    jitter = random.uniform(0, exp_backoff * 0.1)
    return float(min(exp_backoff + jitter, max_ms))


class SubstrateWorker:
    def __init__(self, db: Any, config: Any) -> None:
        self.db = db
        self.config = config
        self.substrate: Any = None
        self.keypair: Any = None

    def connect(self) -> None:
        if self.substrate:
            with contextlib.suppress(Exception):
                self.substrate.close()
        self.substrate = SubstrateInterface(url=self.config.substrate_url, use_remote_preset=True)
        self.keypair = Keypair.create_from_mnemonic(self.config.resubmission_seed_phrase)
        logger.info(f"Connected to substrate {self.config.substrate_url} using account {self.keypair.ss58_address}")

    async def _upload_file_list_to_ipfs(self, file_list: List[Dict[str, str]]) -> str:
        files_json = json.dumps(
            file_list,
            indent=2,
        )
        logger.debug(f"Uploading file list with {len(file_list)} entries to IPFS")

        url = f"{self.config.ipfs_store_url.rstrip('/')}/api/v0/add"
        params = {"wrap-with-directory": "false", "cid-version": "1"}

        async with httpx.AsyncClient(timeout=30.0) as client:
            files = {"file": ("file_list.json", files_json.encode())}
            resp = await client.post(
                url,
                params=params,
                files=files,
            )
            resp.raise_for_status()
            result = resp.json()
            cid: str = result["Hash"]
            logger.debug(f"File list uploaded and pinned to IPFS cid={cid}")
            return cid

    async def _submit_extrinsic_with_retry(
        self, extrinsic: Any, max_retries: int = 3, timeout_seconds: float = 20.0
    ) -> Any:
        """Submit extrinsic with retry logic and fresh connections."""
        last_exc: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                self.connect()

                return await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: self.substrate.submit_extrinsic(
                            extrinsic,
                            wait_for_finalization=True,
                        ),
                    ),
                    timeout=timeout_seconds,
                )
            except (asyncio.TimeoutError, Exception) as e:
                last_exc = e
                if attempt >= max_retries:
                    logger.error(f"Substrate submit failed after {attempt} attempts: {e}")
                    raise
                backoff_ms = compute_substrate_backoff_ms(
                    attempt, self.config.substrate_retry_base_ms, self.config.substrate_retry_max_ms
                )
                logger.warning(f"Substrate submit failed (attempt {attempt}), retrying in {backoff_ms:.0f}ms: {e}")
                await asyncio.sleep(backoff_ms / 1000.0)
            finally:
                if self.substrate:
                    with contextlib.suppress(Exception):
                        self.substrate.close()
                    self.substrate = None

        if last_exc:
            raise last_exc
        raise SubstrateRequestException("Unknown substrate submission failure")

    async def process_batch(self, requests: List[SubstratePinningRequest]) -> bool:
        start_time = time.time()
        logger.info(f"Processing substrate batch requests={len(requests)}")

        self.connect()

        user_cid_map: Dict[str, List[str]] = {}
        user_request_map: Dict[str, List[SubstratePinningRequest]] = {}
        for req in requests:
            if req.address not in user_cid_map:
                user_cid_map[req.address] = []
                user_request_map[req.address] = []
            user_cid_map[req.address].extend(req.cids)
            user_request_map[req.address].append(req)

        file_list_start = time.time()
        storage_request_substrate_calls = []
        for user_addr, cids in user_cid_map.items():
            file_list = [
                {
                    "cid": cid,
                    "filename": f"s3-{cid}",
                }
                for cid in cids
            ]

            storage_request_cid = await self._upload_file_list_to_ipfs(
                file_list,
            )

            call_params = {
                "owner": user_addr,
                "file_inputs": [
                    {
                        "file_hash": storage_request_cid,
                        "file_name": f"files_list_{uuid.uuid4()}",
                    }
                ],
                "miner_ids": [],
            }
            call = self.substrate.compose_call(
                call_module="IpfsPallet",
                call_function="submit_storage_request_for_user",
                call_params=call_params,
            )
            storage_request_substrate_calls.append(call)

        file_list_duration = time.time() - file_list_start

        batch_call = self.substrate.compose_call(
            call_module="Utility",
            call_function="batch",
            call_params={
                "calls": storage_request_substrate_calls,
            },
        )

        logger.info(f"{pprint.pformat(storage_request_substrate_calls)}")

        extrinsic = self.substrate.create_signed_extrinsic(
            call=batch_call,
            keypair=self.keypair,
        )

        submit_start = time.time()
        logger.info(
            f"Submitting batched transaction users={len(user_cid_map)} total_calls={len(storage_request_substrate_calls)}"
        )

        receipt = await self._submit_extrinsic_with_retry(
            extrinsic,
            max_retries=self.config.substrate_max_retries,
            timeout_seconds=self.config.substrate_call_timeout_seconds,
        )
        submit_duration = time.time() - submit_start

        error_msg = None

        try:
            is_success = receipt.is_success
            tx_hash = receipt.extrinsic_hash
            if not is_success:
                error_msg = receipt.error_message
        except Exception as e:
            logger.error(f"Failed to check receipt status: {e}")
            raise SubstrateRequestException(f"Failed to verify transaction receipt: {e}") from None

        if is_success:
            total_duration = time.time() - start_time
            logger.info(
                f"Batched transaction successful tx_hash={tx_hash} "
                f"file_list={file_list_duration:.2f}s submit={submit_duration:.2f}s total={total_duration:.2f}s"
            )

            for req in requests:
                await self.db.execute(
                    "UPDATE object_versions SET status = 'uploaded' WHERE object_id = $1 AND object_version = $2",
                    req.object_id,
                    int(getattr(req, "object_version", 1) or 1),
                )
                logger.info(f"Ensured object status is 'uploaded' object_id={req.object_id}")

            for user_addr, user_requests in user_request_map.items():
                total_cids = sum(len(r.cids) for r in user_requests)
                get_metrics_collector().record_substrate_operation(
                    main_account=user_addr,
                    success=True,
                    num_cids=total_cids,
                    duration=total_duration,
                )

            return True

        logger.error(f"Batched transaction failed: {error_msg}")

        for user_addr, user_requests in user_request_map.items():
            total_cids = sum(len(r.cids) for r in user_requests)
            get_metrics_collector().record_substrate_operation(
                main_account=user_addr,
                success=False,
                num_cids=total_cids,
                duration=time.time() - start_time,
            )

        raise SubstrateRequestException(f"Transaction failed: {error_msg}")
