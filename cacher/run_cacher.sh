#!/bin/bash

# Navigate to the project directory
cd /path/to/hippius-s3

# Source the virtual environment
source .venv/bin/activate

# Set environment variables
export REDIS_URL=redis://127.0.0.1:6379/0

# Run the cacher
python cacher/run_cacher.py

# Log completion
echo "$(date): Cache update completed" >> /var/log/hippius-cacher.log
