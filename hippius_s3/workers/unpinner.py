import asyncio
import contextlib
import logging
import pprint
import random
import time
from typing import Any
from typing import Dict
from typing import List

from substrateinterface import Keypair
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException

from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import UnpinChainRequest


logger = logging.getLogger(__name__)


def compute_substrate_backoff_ms(attempt: int, base_ms: int = 500, max_ms: int = 5000) -> float:
    exp_backoff = base_ms * (2 ** (attempt - 1))
    jitter = random.uniform(0, exp_backoff * 0.1)
    return float(min(exp_backoff + jitter, max_ms))


class UnpinnerWorker:
    def __init__(self, config: Any) -> None:
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

    async def _submit_extrinsic_with_retry(
        self, extrinsic: Any, max_retries: int = 3, timeout_seconds: float = 20.0
    ) -> Any:
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
                    attempt,
                    self.config.substrate_retry_base_ms,
                    self.config.substrate_retry_max_ms,
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

    async def process_batch(self, requests: List[UnpinChainRequest], manifest_cids: Dict[str, str]) -> bool:
        start_time = time.time()
        logger.info(f"Processing unpinner batch requests={len(requests)} manifest_cids={len(manifest_cids)}")

        self.connect()

        user_request_map: Dict[str, List[UnpinChainRequest]] = {}
        for req in requests:
            if req.address not in user_request_map:
                user_request_map[req.address] = []
            user_request_map[req.address].append(req)

        unpin_substrate_calls = []
        for user_addr in manifest_cids:
            manifest_cid = manifest_cids[user_addr]
            call = self.substrate.compose_call(
                call_module="Marketplace",
                call_function="storage_unpin_request",
                call_params={
                    "file_hash": manifest_cid,
                },
            )
            unpin_substrate_calls.append(call)

        batch_call = self.substrate.compose_call(
            call_module="Utility",
            call_function="batch",
            call_params={
                "calls": unpin_substrate_calls,
            },
        )

        logger.info(f"{pprint.pformat(unpin_substrate_calls)}")

        extrinsic = self.substrate.create_signed_extrinsic(
            call=batch_call,
            keypair=self.keypair,
        )

        submit_start = time.time()
        logger.info(
            f"Submitting batched unpin transaction users={len(manifest_cids)} total_calls={len(unpin_substrate_calls)}"
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
                f"Batched unpin transaction successful tx_hash={tx_hash} "
                f"submit={submit_duration:.2f}s total={total_duration:.2f}s"
            )

            for user_addr, user_requests in user_request_map.items():
                total_cids = sum(1 for r in user_requests if r.cid)
                get_metrics_collector().record_unpinner_operation(
                    main_account=user_addr,
                    success=True,
                    num_files=total_cids,
                    duration=total_duration,
                )

            return True

        logger.error(f"Batched unpin transaction failed: {error_msg}")

        for user_addr, user_requests in user_request_map.items():
            total_cids = sum(1 for r in user_requests if r.cid)
            get_metrics_collector().record_unpinner_operation(
                main_account=user_addr,
                success=False,
                num_files=total_cids,
                duration=time.time() - start_time,
            )

        raise SubstrateRequestException(f"Transaction failed: {error_msg}")
