# Substrate Data Cacher

Caches substrate blockchain data in Redis for improved FastAPI performance.

## Setup

Add to crontab to run every 3 minutes:

```bash
*/3 * * * * source /home/ubuntu/hippius-s3/.venv/bin/activate && REDIS_URL=redis://127.0.0.1:6379/0 python /home/ubuntu/hippius-s3/cacher/run_cacher.py >> /home/ubuntu/cacher.log 2>&1
```

Requires Redis container to be running.
