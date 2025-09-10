# Workers

This folder contains the background workers that handle S3 operations asynchronously.

## Workers Overview

- **`uploader.py`** - Handles file uploads to IPFS and creates substrate transactions for storing metadata. **Run only one instance** to avoid exceeding substrate rate limits.

- **`deleter.py`** - Processes file deletions by removing data from IPFS and creating substrate transactions to update metadata. **Run only one instance** to avoid exceeding substrate rate limits.

- **`downloader.py`** - Downloads files from IPFS and caches them in Redis for S3 API responses. **Can be scaled horizontally** - run multiple instances to handle high download volumes.

## Scaling Notes

The uploader and deleter workers create individual substrate transactions and must run as single instances to prevent rate limit violations when handling spam uploads/deletes. The downloader worker only reads from IPFS and Redis, so it can be safely replicated for better performance.
