#!/usr/bin/env bash
# Create or replace RStudio SPCS service from service-spec.template.yaml.
#
# Prereqs: image in registry; stage @<db>.<schema>.VOLUMES exists
# Usage:
#   source config.env
#   RSTUDIO_PASSWORD='...' ./create_password_secret.sh   # first time or rotation
#   ./deploy_service.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNOW="${SNOW_CLI:-snow}"

: "${SNOW_CONNECTION:?Set SNOW_CONNECTION in config.env}"
: "${SNOW_ROLE:?Set SNOW_ROLE}"
: "${RSTUDIO_SERVICE_NAME:?Set RSTUDIO_SERVICE_NAME}"
: "${RSTUDIO_SERVICE_POOL:?Set RSTUDIO_SERVICE_POOL}"
: "${REGISTRY_URL:?Set REGISTRY_URL}"
: "${IMAGE_NAME:=rstudio-rsnowflake}"
: "${IMAGE_TAG:=dev}"
: "${RSTUDIO_PASSWORD_SECRET:?Set RSTUDIO_PASSWORD_SECRET (Snowflake secret name)}"
: "${SNOWFLAKE_ACCOUNT:?Set SNOWFLAKE_ACCOUNT}"
: "${SNOWFLAKE_WAREHOUSE:?Set SNOWFLAKE_WAREHOUSE}"
: "${SNOWFLAKE_DATABASE:=${SNOW_DATABASE}}"
: "${SNOWFLAKE_SCHEMA:=${SNOW_SCHEMA}}"
: "${SNOWFLAKE_ROLE:?Set SNOWFLAKE_ROLE}"
: "${SNOW_DATABASE:?Set SNOW_DATABASE}"
: "${SNOW_SCHEMA:?Set SNOW_SCHEMA}"

SNOW_SQL=( "${SNOW}" sql -c "${SNOW_CONNECTION}" --role "${SNOW_ROLE}" )
IMAGE_URI="${REGISTRY_URL}/${IMAGE_NAME}:${IMAGE_TAG}"
VOLUME_STAGE="${SNOW_DATABASE}.${SNOW_SCHEMA}.VOLUMES"
RSTUDIO_PASSWORD_SECRET_FQN="${SNOW_DATABASE}.${SNOW_SCHEMA}.${RSTUDIO_PASSWORD_SECRET}"
SPEC_FILE="${SCRIPT_DIR}/.service-spec.rendered.yaml"
TEMPLATE="${SCRIPT_DIR}/service-spec.template.yaml"

sed -e "s|<IMAGE_URI>|${IMAGE_URI}|g" \
    -e "s|<RSTUDIO_PASSWORD_SECRET_FQN>|${RSTUDIO_PASSWORD_SECRET_FQN}|g" \
    -e "s|<SNOWFLAKE_ACCOUNT>|${SNOWFLAKE_ACCOUNT}|g" \
    -e "s|<SNOWFLAKE_WAREHOUSE>|${SNOWFLAKE_WAREHOUSE}|g" \
    -e "s|<SNOWFLAKE_DATABASE>|${SNOWFLAKE_DATABASE}|g" \
    -e "s|<SNOWFLAKE_SCHEMA>|${SNOWFLAKE_SCHEMA}|g" \
    -e "s|<SNOWFLAKE_ROLE>|${SNOWFLAKE_ROLE}|g" \
    -e "s|<VOLUME_STAGE>|${VOLUME_STAGE}|g" \
    "${TEMPLATE}" > "${SPEC_FILE}"

echo "==> Creating service ${RSTUDIO_SERVICE_NAME}"
echo "    Image: ${IMAGE_URI}"
echo "    Pool:  ${RSTUDIO_SERVICE_POOL}"

"${SNOW_SQL[@]}" -q "ALTER COMPUTE POOL ${RSTUDIO_SERVICE_POOL} RESUME;" 2>/dev/null || true
"${SNOW_SQL[@]}" -q "DROP SERVICE IF EXISTS ${RSTUDIO_SERVICE_NAME};" 2>/dev/null || true
"${SNOW_SQL[@]}" -q "
CREATE SERVICE ${RSTUDIO_SERVICE_NAME}
  IN COMPUTE POOL ${RSTUDIO_SERVICE_POOL}
  FROM SPECIFICATION \$\$
$(cat "${SPEC_FILE}")
\$\$
  MIN_INSTANCES = 1
  MAX_INSTANCES = 1;
"

echo "==> Waiting for service..."
sleep 10
"${SNOW_SQL[@]}" -q "CALL SYSTEM\$GET_SERVICE_STATUS('${RSTUDIO_SERVICE_NAME}', 0);"
"${SNOW_SQL[@]}" -q "SHOW ENDPOINTS IN SERVICE ${RSTUDIO_SERVICE_NAME};"

rm -f "${SPEC_FILE}"
