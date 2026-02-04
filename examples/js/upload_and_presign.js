/**
 * Upload a file to Hippius S3 and generate a presigned download URL.
 * Uses @aws-sdk/client-s3 and @aws-sdk/s3-request-presigner.
 */

const { S3Client, CreateBucketCommand, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const fs = require("fs");

const client = new S3Client({
  endpoint: "https://s3.hippius.com",
  region: "decentralized",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
  forcePathStyle: true,
});

const bucket = process.env.S3_BUCKET_NAME;
const objectKey = "hello.txt";

async function main() {
  await client.send(new CreateBucketCommand({ Bucket: bucket }));
  console.log(`Created bucket: ${bucket}`);

  const body = fs.readFileSync(__filename);
  await client.send(new PutObjectCommand({ Bucket: bucket, Key: objectKey, Body: body }));
  console.log(`Uploaded: ${objectKey}`);

  const url = await getSignedUrl(
    client,
    new GetObjectCommand({ Bucket: bucket, Key: objectKey }),
    { expiresIn: 3600 }
  );
  console.log(`Presigned URL (1h expiry): ${url}`);
}

main();
