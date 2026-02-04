"""Upload a file to Hippius S3 and generate a presigned download URL using MinIO SDK."""

import os
from datetime import timedelta

from minio import Minio

client = Minio(
    "s3.hippius.com",
    access_key=os.environ["AWS_ACCESS_KEY_ID"],
    secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    secure=True,
    region="decentralized",
)

bucket = os.environ["S3_BUCKET_NAME"]
client.make_bucket(bucket)
print(f"Created bucket: {bucket}")

object_name = "hello.txt"
client.put_object(bucket, object_name, data=open(__file__, "rb"), length=os.path.getsize(__file__))
print(f"Uploaded: {object_name}")

url = client.presigned_get_object(bucket, object_name, expires=timedelta(hours=1))
print(f"Presigned URL (1h expiry): {url}")
