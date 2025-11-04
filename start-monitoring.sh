#!/bin/bash

# Hippius S3 Monitoring Stack Startup Script

set -e

echo "🚀 Starting Hippius S3 Monitoring Stack..."

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Set default credentials if not provided
export GRAFANA_ADMIN_USER=${GRAFANA_ADMIN_USER:-admin}
if [ -z "$GRAFANA_ADMIN_PASSWORD" ]; then
    export GRAFANA_ADMIN_PASSWORD=$(openssl rand -base64 12)
    echo "🔐 Generated Grafana password: $GRAFANA_ADMIN_PASSWORD"
else
    echo "🔐 Using provided Grafana password"
fi

# Install monitoring dependencies if in virtual environment
if [ -n "$VIRTUAL_ENV" ]; then
    echo "📦 Installing monitoring dependencies..."
    pip install -r monitoring-requirements.txt
else
    echo "⚠️  Not in virtual environment - monitoring dependencies may need manual installation"
fi

# Create monitoring directories if they don't exist
mkdir -p monitoring/{prometheus,grafana/{provisioning/{datasources,dashboards},dashboards},otel}

# Handle alerting configuration based on environment
ALERTING_DIR="monitoring/grafana/provisioning/alerting"
ALERTING_DISABLED_DIR="monitoring/grafana/provisioning/alerting.disabled"

if [ "${ENVIRONMENT}" = "production" ]; then
    echo "🔔 Enabling alerting for production environment"
    if [ -d "$ALERTING_DISABLED_DIR" ]; then
        mv "$ALERTING_DISABLED_DIR" "$ALERTING_DIR"
    fi
else
    echo "🔕 Disabling alerting for non-production environment (ENVIRONMENT=${ENVIRONMENT:-not set})"
    if [ -d "$ALERTING_DIR" ]; then
        mv "$ALERTING_DIR" "$ALERTING_DISABLED_DIR"
    fi
fi

# Start the monitoring stack
echo "🐳 Starting Docker containers..."
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Health checks
echo "🏥 Checking service health..."

# Check Prometheus
if curl -s http://localhost:9090/-/healthy >/dev/null; then
    echo "✅ Prometheus is healthy"
else
    echo "❌ Prometheus health check failed"
fi

# Check Grafana
if curl -s http://localhost:3000/api/health >/dev/null; then
    echo "✅ Grafana is healthy"
else
    echo "❌ Grafana health check failed"
fi

echo ""
echo "🎉 Monitoring stack started successfully!"
echo ""
echo "Access URLs:"
echo "  📊 Grafana:    http://localhost:3000"
echo "     Username:   $GRAFANA_ADMIN_USER"
echo "     Password:   $GRAFANA_ADMIN_PASSWORD"
echo ""
echo "  📈 Prometheus: http://localhost:9090"
echo "  🔧 API Health: http://localhost:8000/health"
echo ""
echo "📖 See MONITORING.md for detailed configuration and usage instructions."
