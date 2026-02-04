"""Full-featured async Hippius S3 client using MinIO SDK with ThreadPoolExecutor."""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from io import BytesIO

from minio import Minio


class HippiusClient:
    def __init__(self):
        self.executor = ThreadPoolExecutor()
        self.client = Minio(
            "s3.hippius.com",
            access_key=os.environ["AWS_ACCESS_KEY_ID"],
            secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            secure=True,
            region="decentralized",
        )

    async def ensure_bucket(self, bucket: str) -> None:
        loop = asyncio.get_event_loop()
        exists = await loop.run_in_executor(self.executor, self.client.bucket_exists, bucket)
        if not exists:
            await loop.run_in_executor(self.executor, self.client.make_bucket, bucket)
            print(f"Created bucket: {bucket}")

    async def upload(self, bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self.executor,
            lambda: self.client.put_object(bucket, key, data=BytesIO(data), length=len(data), content_type=content_type),
        )
        print(f"Uploaded: {key} ({len(data)} bytes)")

    async def upload_file(self, bucket: str, key: str, file_path: str) -> dict:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(self.executor, self.client.fput_object, bucket, key, file_path)
        print(f"Uploaded file: {key}")
        return {"etag": result.etag, "version_id": result.version_id}

    async def presigned_url(self, bucket: str, key: str, expires_seconds: int = 3600) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            lambda: self.client.presigned_get_object(bucket, key, expires=timedelta(seconds=expires_seconds)),
        )

    async def download(self, bucket: str, key: str) -> bytes:
        loop = asyncio.get_event_loop()

        def _download():
            response = self.client.get_object(bucket, key)
            data = response.read()
            response.close()
            response.release_conn()
            return data

        return await loop.run_in_executor(self.executor, _download)

    async def delete(self, bucket: str, key: str) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, self.client.remove_object, bucket, key)
        print(f"Deleted: {key}")

    async def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        loop = asyncio.get_event_loop()

        def _list():
            return [obj.object_name for obj in self.client.list_objects(bucket, prefix=prefix)]

        return await loop.run_in_executor(self.executor, _list)


async def main():
    client = HippiusClient()
    bucket = os.environ["S3_BUCKET_NAME"]

    await client.ensure_bucket(bucket)

    content = b"Hello from the async Hippius client!"
    await client.upload(bucket, "async-demo.txt", content, content_type="text/plain")

    url = await client.presigned_url(bucket, "async-demo.txt", expires_seconds=7200)
    print(f"Presigned URL (2h expiry): {url}")

    downloaded = await client.download(bucket, "async-demo.txt")
    assert downloaded == content, "Download content mismatch!"
    print(f"Downloaded and verified: {len(downloaded)} bytes")

    objects = await client.list_objects(bucket)
    print(f"Objects in {bucket}: {objects}")

    await client.delete(bucket, "async-demo.txt")
    print("Cleanup complete")


if __name__ == "__main__":
    asyncio.run(main())
