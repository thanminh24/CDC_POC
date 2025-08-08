#!/usr/bin/env bash
set -euo pipefail

DEBEZIUM_VERSION="${DEBEZIUM_VERSION:-3.1.2.Final}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.6.1}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGINS_DIR="${ROOT_DIR}/kafka/plugins"
DBZ_PLUGIN_DIR="${PLUGINS_DIR}/debezium-connector-postgres"
IC_PLUGIN_DIR="${PLUGINS_DIR}/iceberg-kafka-connect"

STAGE_ROOT="${ROOT_DIR}/target/dependencies"
STAGE_DBZ="${STAGE_ROOT}/debezium-subfolder"
STAGE_IC="${STAGE_ROOT}/iceberg-subfolder"

# Maven plugin coords
MDEP="org.apache.maven.plugins:maven-dependency-plugin:3.6.1"

MINIO_BUCKET="${MINIO_BUCKET:-iceberg}"
MINIO_HOST="${MINIO_HOST:-http://localhost:9000}"
SVC_KAFKA="${SVC_KAFKA:-kafka-standalone}"
SVC_MINIO="${SVC_MINIO:-minio}"

log() { printf "\n=== %s ===\n" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

mkdir -p "${DBZ_PLUGIN_DIR}" "${IC_PLUGIN_DIR}" "${STAGE_DBZ}" "${STAGE_IC}"
# Clean our staging dirs and the default maven-dependency dir to avoid confusion
rm -rf "${STAGE_DBZ:?}/"* "${STAGE_IC:?}/"* "${ROOT_DIR}/target/dependency"

# ---------------------------------------------
# A) Debezium: official plugin ZIP path
# ---------------------------------------------
log "A1) Downloading Debezium Postgres connector ZIP -> ${STAGE_DBZ}"
mvn -q -DskipTests \
  "${MDEP}:copy" \
  -Dartifact="io.debezium:debezium-connector-postgres:${DEBEZIUM_VERSION}:zip:plugin" \
  -DoutputDirectory="${STAGE_DBZ}"

DBZ_ZIP="$(ls -1 "${STAGE_DBZ}"/debezium-connector-postgres-*plugin.zip | head -n1 || true)"
[[ -n "${DBZ_ZIP}" ]] || die "Debezium plugin ZIP not found in ${STAGE_DBZ}"

log "A2) Extracting Debezium plugin ZIP"
if command -v unzip >/dev/null 2>&1; then
  unzip -q -o -d "${STAGE_DBZ}" "${DBZ_ZIP}"
else
  ( cd "${STAGE_DBZ}" && jar xf "${DBZ_ZIP}" )
fi

log "A3) Installing Debezium jars -> ${DBZ_PLUGIN_DIR}"
mkdir -p "${DBZ_PLUGIN_DIR}"
find "${STAGE_DBZ}" -type f -name "*.jar" -print0 | xargs -0 -I{} cp -f "{}" "${DBZ_PLUGIN_DIR}/"

# ---------------------------------------------
# B) Iceberg: copy ALL runtime deps from POM
#    (only Iceberg KC is declared there)
# ---------------------------------------------
log "B1) Resolving Iceberg Kafka Connect runtime deps -> ${STAGE_IC}"
mvn -q -DskipTests \
  "${MDEP}:copy-dependencies" \
  -DincludeScope=runtime \
  -DexcludeTransitive=false \
  -DoutputDirectory="${STAGE_IC}"

# If Maven still wrote to the default 'target/dependency', migrate it (paranoia fallback)
if [ ! -e "${STAGE_IC}/iceberg-kafka-connect-${ICEBERG_VERSION}.jar" ] && [ -d "${ROOT_DIR}/target/dependency" ]; then
  echo "Note: Detected artifacts in target/dependency; moving them to ${STAGE_IC}"
  shopt -s nullglob
  mv "${ROOT_DIR}/target/dependency/"*.jar "${STAGE_IC}/" || true
fi

# Sanity check
if ! compgen -G "${STAGE_IC}/iceberg-kafka-connect-*.jar" > /dev/null; then
  die "Iceberg Kafka Connect jar not found in ${STAGE_IC}. Check pom.xml and ICEBERG_VERSION."
fi

log "B2) Installing Iceberg jars -> ${IC_PLUGIN_DIR}"
mkdir -p "${IC_PLUGIN_DIR}"
if command -v rsync >/dev/null 2>&1; then
  rsync -av --delete --include="*/" --include="*.jar" --exclude="*" "${STAGE_IC}/" "${IC_PLUGIN_DIR}/"
else
  find "${IC_PLUGIN_DIR}" -type f -name "*.jar" -delete || true
  find "${STAGE_IC}" -type f -name "*.jar" -print0 | xargs -0 -I{} cp -f "{}" "${IC_PLUGIN_DIR}/"
fi

log "Installed plugin dirs:"
echo "  - Debezium -> ${DBZ_PLUGIN_DIR}"
echo "  - Iceberg  -> ${IC_PLUGIN_DIR}"

# ---------------------------------------------
# C) Bring up infrastructure
# ---------------------------------------------
log "C1) docker compose up -d (all services)"
docker compose up -d

# Wait for MinIO readiness (best-effort)
log "C2) Waiting for MinIO readiness (${MINIO_HOST})"
for i in {1..30}; do
  if curl -sf "${MINIO_HOST}/minio/health/ready" >/dev/null; then
    echo "MinIO is ready."
    break
  fi
  sleep 2
done || true

# ---------------------------------------------
# D) Create MinIO bucket (best-effort)
# ---------------------------------------------
log "D1) Creating MinIO bucket '${MINIO_BUCKET}'"
if docker compose exec -T "${SVC_MINIO}" sh -lc 'command -v mc >/dev/null 2>&1' ; then
  docker compose exec -T "${SVC_MINIO}" sh -lc '
    set -e
    : "${MINIO_ROOT_USER:?}"; : "${MINIO_ROOT_PASSWORD:?}"
    mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 || true
    mc mb -p "local/'"${MINIO_BUCKET}"'" >/dev/null 2>&1 || true
    mc ls local >/dev/null 2>&1 || true
  ' && echo "MinIO bucket ensured: ${MINIO_BUCKET}"
else
  echo "⚠️  Skipping bucket creation: 'mc' not found in ${SVC_MINIO} container."
fi

# ---------------------------------------------
# E) Launch Kafka Connect (standalone)
# ---------------------------------------------
log "E1) Starting Kafka Connect (standalone) with your configs"
if ! docker compose exec -d "${SVC_KAFKA}" \
    /opt/kafka/bin/connect-standalone.sh \
    /opt/kafka/config-cdc/connect-standalone.properties \
    /opt/kafka/config-cdc/connect-postgres-source.json \
    /opt/kafka/config-cdc/connect-iceberg-sink.json; then
  die "Failed to start Kafka connectors"
fi

log "All services started and connectors launched."
echo
echo "Set/confirm in worker config:"
echo "  plugin.path=${DBZ_PLUGIN_DIR},${IC_PLUGIN_DIR}"
echo
