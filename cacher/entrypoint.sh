#!/bin/bash
set -e

echo "Starting account cacher service (runs every 5 minutes)"

while true; do
    echo "$(date): Running account cacher..."
    python run_cacher.py || echo "$(date): Cacher run failed, will retry in 5 minutes"
    echo "$(date): Sleeping for 5 minutes..."
    sleep 300
done
