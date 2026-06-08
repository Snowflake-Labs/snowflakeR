#!/usr/bin/env bash
# Local docker build + optional push to Snowflake image repository.
#
# Usage:
#   source config.env
#   ./build_local.sh
#   PUSH=1 ./build_local.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${SNOW_CONNECTION:?Set SNOW_CONNECTION}"
: "${IMAGE_NAME:=rstudio-rsnowflake}"
: "${IMAGE_TAG:=dev}"
: "${PLATFORM:=linux/amd64}"

REGISTRY_URL="${REGISTRY_URL:-}"
if [[ -z "${REGISTRY_URL}" ]]; then
  : "${SNOW_DATABASE:?Set SNOW_DATABASE or REGISTRY_URL}"
  : "${SNOW_SCHEMA:?Set SNOW_SCHEMA or REGISTRY_URL}"
  REGISTRY_URL="$("${SNOW_CLI:-snow}" spcs image-repository url MY_IMAGES \
    --connection "${SNOW_CONNECTION}" \
    --database "${SNOW_DATABASE}" \
    --schema "${SNOW_SCHEMA}" 2>/dev/null | tail -1 | tr -d '[:space:]')" || true
fi
if [[ -z "${REGISTRY_URL}" ]]; then
  echo "ERROR: Set REGISTRY_URL in config.env"
  exit 1
fi

LOCAL_TAG="${IMAGE_NAME}:${IMAGE_TAG}"
REMOTE_TAG="${REGISTRY_URL}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "==> Building ${LOCAL_TAG} (${PLATFORM})"
bash "${SCRIPT_DIR}/prepare_build_ctx.sh"

docker build --platform "${PLATFORM}" \
  -f "${SCRIPT_DIR}/build-ctx/Dockerfile" \
  -t "${LOCAL_TAG}" \
  "${SCRIPT_DIR}/build-ctx"

docker tag "${LOCAL_TAG}" "${REMOTE_TAG}"
echo "==> Tagged ${REMOTE_TAG}"

if [[ "${PUSH:-}" != "1" ]]; then
  echo "==> Build complete. Set PUSH=1 to push."
  echo "    snow spcs image-registry login --connection ${SNOW_CONNECTION}"
  echo "    docker push ${REMOTE_TAG}"
  exit 0
fi

echo "==> Logging in to Snowflake registry"
"${SNOW_CLI:-snow}" spcs image-registry login --connection "${SNOW_CONNECTION}"

echo "==> Pushing ${REMOTE_TAG}"
docker push "${REMOTE_TAG}"
echo "==> Push complete"
