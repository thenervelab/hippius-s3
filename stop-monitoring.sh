#!/bin/bash

# Hippius S3 Monitoring Stack Stop Script

set -e

echo "🛑 Stopping Hippius S3 Monitoring Stack..."

# Stop monitoring services
docker compose -f docker-compose.monitoring.yml down

echo "✅ Monitoring stack stopped successfully!"
echo ""
echo "To stop the main application as well:"
echo "  docker compose -f docker-compose.yml down"
echo ""
echo "To remove all containers and data:"
echo "  docker compose -f docker-compose.yml -f docker-compose.monitoring.yml down -v"
