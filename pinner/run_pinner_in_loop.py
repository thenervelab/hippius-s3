#!/usr/bin/env python3
import asyncio
import logging
import pathlib
import sys
from io import BytesIO
from pathlib import Path

import asyncpg
import redis.asyncio as async_redis

from hippius_sdk.client import HippiusClient
from hippius_sdk.errors import HippiusSubstrateError
from hippius_sdk.substrate import FileInput
from hippius_sdk.substrate import SubstrateClient


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.queue import dequeue_s3_publish_request
from hippius_s3.queue import dequeue_upload_request


config = get_config()

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def process_upload_request(upload_requests: list[dict]) -> bool:
    """Process a single upload request by creating a storage request using the Hippius SDK."""
    seed_phrase = None
    files = []
    for req in upload_requests:
        cid = req["cid"]
        subaccount = req["subaccount"]
        seed_phrase = req["seed_phrase"]
        file_name = pathlib.Path(req["file_path"]).name

        logger.info(f"Processing upload request for CID={cid}, subaccount={subaccount}")

        files.append(
            FileInput(
                file_hash=cid,
                file_name=file_name,
            )
        )
        logger.info(f"Submitting storage request to substrate for file: {file_name}, CID: {cid}")

    substrate_client = SubstrateClient(
        seed_phrase=seed_phrase,
        url=config.substrate_url,
    )

    tx_hash = await substrate_client.storage_request(
        files=files,
        miner_ids=[],
        seed_phrase=seed_phrase,
    )

    logger.debug(f"Substrate call result: {tx_hash}")

    # Check if we got a valid transaction hash
    if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
        logger.error(f"Invalid transaction hash received: {tx_hash}")
        raise HippiusSubstrateError(
            f"Invalid transaction hash received: {tx_hash}. This might indicate insufficient credits or transaction failure."
        )

    logger.info(f"Successfully published to substrate with transaction: {tx_hash}")
    return True


async def process_s3_publish_request(
    s3_publish_req: dict,
    redis_client: async_redis.Redis,
    db_pool: asyncpg.Pool,
) -> bool:
    """Process s3_publish request by reassembling chunks and calling s3_publish."""
    try:
        object_id = s3_publish_req["object_id"]
        chunks = s3_publish_req["chunks"]
        should_encrypt = s3_publish_req["should_encrypt"]

        logger.info(f"Processing s3_publish for object_id={object_id} with {len(chunks)} chunks")

        # Reassemble file data from chunks using BytesIO for efficient memory handling
        file_buffer = BytesIO()
        for chunk in sorted(chunks, key=lambda x: x["chunk_index"]):
            chunk_key = chunk["chunk_key"]
            chunk_data = await redis_client.get(chunk_key)
            logger.info(f"Got {chunk_key=}")
            if chunk_data is None:
                logger.error(f"Missing chunk data for key={chunk_key}")
                return False
            file_buffer.write(chunk_data)
            logger.info(f"Wrote {chunk_key=}")

        logger.info(f"Finished reassembling bytestream for object_id={object_id}")
        file_buffer.seek(0)

        client = HippiusClient(
            ipfs_gateway=config.ipfs_get_url,
            ipfs_api_url=config.ipfs_store_url,
            substrate_url=config.substrate_url,
            encrypt_by_default=False,
        )

        # Perform s3_publish
        logger.info(
            f"Starting s3_publish call for object_id={object_id}, file_name={s3_publish_req['file_name']}, encrypt={should_encrypt}"
        )
        s3_result = await client.s3_publish(
            content=file_buffer,
            encrypt=should_encrypt,
            seed_phrase=s3_publish_req["seed_phrase"],
            subaccount_id=s3_publish_req["subaccount_id"],
            bucket_name=s3_publish_req["bucket_name"],
            file_name=s3_publish_req["file_name"],
            store_node=s3_publish_req["store_node"],
            pin_node=s3_publish_req["pin_node"],
            substrate_url=s3_publish_req["substrate_url"],
            publish=False,
        )

        logger.info(f"Completed s3_publish call for object_id={object_id}, result_cid={s3_result.cid}")

        # Update object status to 'uploaded'
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE objects SET status = 'uploaded', ipfs_cid = $2 WHERE object_id = $1",
                object_id,
                s3_result.cid,
            )

            logger.info(f"Successfully published object_id={object_id}, cid={s3_result.cid}")

            # Clean up chunk data from Redis
            for chunk in chunks:
                await redis_client.delete(chunk["chunk_key"])

            return True

    except Exception as e:
        logger.error(f"Error processing s3_publish for object_id={object_id}: {e}")
        # Delete failed object from database
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM objects WHERE object_id = $1",
                    object_id,
                )
            logger.info(f"Deleted failed object_id={object_id} from database")
        except Exception as cleanup_error:
            logger.error(f"Failed to delete object_id={object_id} from database: {cleanup_error}")

        # Clean up chunk data from Redis
        try:
            for chunk in chunks:
                await redis_client.delete(chunk["chunk_key"])
            logger.info(f"Cleaned up chunks for failed object_id={object_id}")
        except Exception as cleanup_error:
            logger.error(f"Failed to cleanup chunks for object_id={object_id}: {cleanup_error}")

        return False


async def run_pinner_loop():
    """Main loop that monitors Redis queues and processes upload/s3_publish requests."""
    redis_client = None
    db_pool = None

    # Retry logic for Redis connection
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        await asyncio.sleep(retry_delay)
        redis_client = async_redis.from_url(config.redis_url)
        db_pool = await asyncpg.create_pool(config.database_url)

        try:
            await redis_client.ping()
            logger.info("Successfully connected to Redis and database")
            break
        except Exception as e:
            logger.info(f"Redis connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if redis_client:
                await redis_client.aclose()
            if db_pool:
                await db_pool.close()
            redis_client = None
            db_pool = None

            if attempt == max_retries - 1:
                logger.error("Failed to connect to Redis after max retries")
                raise

    logger.info("Starting pinner service...")
    logger.info(f"Redis URL: {config.redis_url}")
    logger.info(f"Database URL: {config.database_url}")
    user_upload_requests = {}

    try:
        while True:
            # Check for s3_publish requests first (higher priority)
            s3_publish_request = await dequeue_s3_publish_request(redis_client)
            if s3_publish_request:
                success = await process_s3_publish_request(s3_publish_request, redis_client, db_pool)
                if success:
                    logger.info(f"Successfully processed s3_publish for {s3_publish_request['object_id']}")
                else:
                    logger.error(f"Failed to process s3_publish for {s3_publish_request['object_id']}")
                continue

            # Check for traditional upload requests
            upload_request = await dequeue_upload_request(redis_client)

            if upload_request:
                try:
                    user_upload_requests[upload_request["owner"]].append(upload_request)
                except KeyError:
                    user_upload_requests[upload_request["owner"]] = [upload_request]

            else:
                # No items in either queue, process batched upload requests
                for user in user_upload_requests:
                    success = await process_upload_request(user_upload_requests[user])
                    if success:
                        logger.info(
                            f"SUCCESSFULLY processed user's {user} with {len(user_upload_requests[user])} files"
                        )
                    else:
                        logger.info(
                            f"Failed to batch and serve {len(user_upload_requests[user])} pin requests for user {user}"
                        )

                user_upload_requests = {}
                await asyncio.sleep(5)  # Reduced sleep time for faster s3_publish processing

    except KeyboardInterrupt:
        logger.info("Pinner service stopping...")
    except Exception as e:
        logger.error(f"Error in pinner loop: {e}")
        raise
    finally:
        await redis_client.aclose()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(run_pinner_loop())
