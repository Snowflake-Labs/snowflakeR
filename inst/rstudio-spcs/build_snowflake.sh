#!/usr/bin/env bash
# In-account image build via SPCS Image Builder (Private Preview).
# Requires Snowflake CLI >= 3.16 and enable_spcs_build_image feature flag.
#
# Usage:
#   source config.env
#   ./build_snowflake.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export SNOWFLAKE_CLI_FEATURES_ENABLE_SPCS_BUILD_IMAGE="${SNOWFLAKE_CLI_FEATURES_ENABLE_SPCS_BUILD_IMAGE:-true}"

: "${SNOW_CONNECTION:?Set SNOW_CONNECTION}"
: "${SNOW_DATABASE:?Set SNOW_DATABASE}"
: "${SNOW_SCHEMA:?Set SNOW_SCHEMA}"
: "${SFNB_IMAGE_REPO:?Set SFNB_IMAGE_REPO}"
: "${SFNB_BUILD_POOL:?Set SFNB_BUILD_POOL}"
: "${SFNB_BUILD_EAI:?Set SFNB_BUILD_EAI}"

SNOW="${SNOW_CLI:-snow}"
ROLE="${SNOW_ROLE:?Set SNOW_ROLE}"
IMAGE_NAME="${IMAGE_NAME:-rstudio-rsnowflake}"
IMAGE_TAG="${IMAGE_TAG:-dev}"
BUILD_CTX="${SCRIPT_DIR}/build-ctx"

if ! "${SNOW}" spcs service build-image --help >/dev/null 2>&1; then
  echo "ERROR: snow spcs service build-image not available."
  echo "  Requires Snowflake CLI >= 3.16 and account Image Builder preview."
  echo "  Fallback: ./build_local.sh"
  exit 1
fi

bash "${SCRIPT_DIR}/prepare_build_ctx.sh"

echo "==> Resuming build pool ${SFNB_BUILD_POOL}"
"${SNOW}" sql -c "${SNOW_CONNECTION}" -q \
  "ALTER COMPUTE POOL ${SFNB_BUILD_POOL} RESUME" 2>/dev/null || true

echo "==> Server-side build (pool=${SFNB_BUILD_POOL}, repo=${SFNB_IMAGE_REPO})"
"${SNOW}" spcs service build-image \
  --connection "${SNOW_CONNECTION}" \
  --role "${ROLE}" \
  --database "${SNOW_DATABASE}" \
  --schema "${SNOW_SCHEMA}" \
  --compute-pool "${SFNB_BUILD_POOL}" \
  --image-repository "${SFNB_IMAGE_REPO}" \
  --image-name "${IMAGE_NAME}" \
  --image-tag "${IMAGE_TAG}" \
  --build-context-dir "${BUILD_CTX}" \
  --eai-name "${SFNB_BUILD_EAI}" \
  --verbose

REPO_PATH="$(echo "${SFNB_IMAGE_REPO}" | tr '[:upper:]' '[:lower:]' | tr '.' '/')"
echo ""
echo "==> Image: /${REPO_PATH}/${IMAGE_NAME}:${IMAGE_TAG}"
echo "==> Run: ./deploy_service.sh"
