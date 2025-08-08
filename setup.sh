#!/usr/bin/env bash
set -euo pipefail

# Start services in detached mode
if ! docker compose up -d; then
  echo "docker compose up failed" >&2
  exit 1
fi

# Wait for MinIO to be ready
until curl -s "http://localhost:9000/minio/health/ready" >/dev/null; do
  echo "Waiting for MinIO..."
  sleep 2
done

# Wait for Hive Metastore to be ready
until nc -z localhost 9083; do
  echo "Waiting for Hive metastore..."
  sleep 2
done

# Create warehouse bucket in MinIO
if ! docker run --rm --network container:minio \
    -e MC_HOST_minio=http://minioadmin:minioadmin@localhost:9000 \
    minio/mc mb --ignore-existing minio/warehouse; then
  echo "Failed to create MinIO bucket" >&2
  exit 1
fi

# Launch Kafka connectors
if ! docker compose exec -d kafka-standalone \
    /opt/kafka/bin/connect-standalone.sh \
    /opt/kafka/config-cdc/connect-standalone.properties \
    /opt/kafka/config-cdc/connect-postgres-source.json \
    /opt/kafka/config-cdc/connect-iceberg-sink.json; then
  echo "Failed to start Kafka connectors" >&2
  exit 1
fi

echo "Services started and connectors launched."
