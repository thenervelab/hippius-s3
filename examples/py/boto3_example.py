"""Upload a file to Hippius S3 and generate a presigned download URL using boto3."""

import os

import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="https://s3.hippius.com",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="decentralized",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

bucket = os.environ["S3_BUCKET_NAME"]
s3.create_bucket(Bucket=bucket)
print(f"Created bucket: {bucket}")

object_name = "hello.txt"
s3.upload_file(__file__, bucket, object_name)
print(f"Uploaded: {object_name}")

url = s3.generate_presigned_url(
    "get_object",
    Params={"Bucket": bucket, "Key": object_name},
    ExpiresIn=3600,
)
print(f"Presigned URL (1h expiry): {url}")
