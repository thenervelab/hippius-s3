import asyncio
import logging
import random
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Optional

from hippius_sdk.client import HippiusClient

from hippius_s3.config import Config


logger = logging.getLogger(__name__)


class ResolvedPublish:
    def __init__(self, cid: str, path: str, tx_hash: Optional[str] = None):
        self.cid = cid
        self.path = path  # 'sdk' | 'fallback'
        self.tx_hash = tx_hash


class ResilientPublishAdapter:
    """
    Wraps HippiusClient.s3_publish with bounded retries and fallback to upload+pin.
    """

    def __init__(self, config: Config, client: HippiusClient):
        self.config = config
        self.client = client

    def _backoff_ms(self, attempt: int) -> float:
        base = getattr(self.config, "publish_retry_base_ms", 500)
        max_ms = getattr(self.config, "publish_retry_max_ms", 5000)
        exp = base * (2 ** max(0, attempt - 1))
        jitter = random.uniform(0, exp * 0.1)
        return float(min(exp + jitter, max_ms))

    async def publish_manifest(
        self,
        *,
        file_data: bytes,
        file_name: str,
        should_encrypt: bool,
        seed_phrase: Optional[str],
        subaccount_id: Optional[str],
        bucket_name: Optional[str],
    ) -> ResolvedPublish:
        # If account context missing or publish disabled, go straight to fallback
        if not getattr(self.config, "publish_to_chain", True) or not (seed_phrase and subaccount_id and bucket_name):
            logger.info("Publish disabled or missing account context; using upload+pin fallback")
            return await self._fallback_upload_pin(
                file_data=file_data,
                file_name=file_name,
                seed_phrase=seed_phrase,
                should_encrypt=should_encrypt,
            )

        # Try SDK s3_publish with retries
        last_exc: Optional[Exception] = None
        max_retries = int(getattr(self.config, "publish_max_retries", 3))
        for attempt in range(1, max_retries + 1):
            tmp_path: Optional[str] = None
            try:
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    tmp_path = temp_file.name
                    temp_file.write(file_data)

                logger.info(
                    f"SDK s3_publish attempt {attempt}/{max_retries} file={file_name} size={len(file_data)} subaccount={subaccount_id} bucket={bucket_name}"
                )
                pub = await self.client.s3_publish(
                    tmp_path,
                    seed_phrase=seed_phrase,
                    encrypt=should_encrypt,
                    subaccount_id=subaccount_id,
                    bucket_name=bucket_name,
                    store_node=self.config.ipfs_store_url,
                )
                tx_hash = getattr(pub, "tx_hash", None)
                logger.info(f"SDK publish complete cid={pub.cid} tx={tx_hash}")
                return ResolvedPublish(cid=pub.cid, path="sdk", tx_hash=tx_hash)
            except Exception as e:
                last_exc = e
                if attempt >= max_retries:
                    logger.exception(f"SDK publish failed after {attempt} attempts: {e}")
                    break
                backoff = self._backoff_ms(attempt)
                logger.warning(f"SDK publish failed (attempt {attempt}), retrying in {backoff:.0f}ms: {e}")
                await asyncio.sleep(backoff / 1000.0)
            finally:
                if tmp_path and Path(tmp_path).exists():
                    with suppress(Exception):
                        Path(tmp_path).unlink()

        # Fallback if enabled
        if getattr(self.config, "publish_enable_fallback", True):
            logger.warning(f"Falling back to upload+pin due to SDK publish failure: {last_exc}")
            return await self._fallback_upload_pin(
                file_data=file_data,
                file_name=file_name,
                seed_phrase=seed_phrase,
                should_encrypt=should_encrypt,
            )

        # If fallback disabled, rethrow last error
        if last_exc:
            raise last_exc
        raise RuntimeError("SDK publish failed with unknown error")

    async def _fallback_upload_pin(
        self, *, file_data: bytes, file_name: str, seed_phrase: Optional[str], should_encrypt: bool = False
    ) -> ResolvedPublish:
        # Upload
        tmp_path: Optional[str] = None
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                tmp_path = temp_file.name
                temp_file.write(file_data)

            result = await self.client.upload_file(
                tmp_path,
                encrypt=should_encrypt,
                seed_phrase=seed_phrase,
            )
            cid = str(result["cid"])
            # Pin
            await self.client.pin(cid, seed_phrase=seed_phrase)
            return ResolvedPublish(cid=cid, path="fallback", tx_hash=None)
        finally:
            if tmp_path and Path(tmp_path).exists():
                with suppress(Exception):
                    Path(tmp_path).unlink()
