#!/usr/bin/env bash
# Render provision SQL from config.env and run via snow sql.
#
# Usage:
#   cp config.example.env config.env   # edit once
#   ./provision.sh                     # stages + resume pool (deploy role)
#   ./provision.sh --eai               # Image Builder EAI (admin role, once)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNOW="${SNOW_CLI:-snow}"
MODE="provision"
RENDERED=""

if [[ "${1:-}" == "--eai" ]]; then
  MODE="eai"
elif [[ -n "${1:-}" ]]; then
  echo "Usage: $0 [--eai]"
  exit 1
fi

if [[ -z "${SNOW_CONNECTION:-}" ]] && [[ -f "${SCRIPT_DIR}/config.env" ]]; then
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/config.env"
fi

: "${SNOW_CONNECTION:?Set SNOW_CONNECTION in config.env}"
: "${SNOW_ROLE:?Set SNOW_ROLE}"
: "${SNOW_DATABASE:?Set SNOW_DATABASE}"
: "${SNOW_SCHEMA:?Set SNOW_SCHEMA}"
: "${SNOWFLAKE_WAREHOUSE:?Set SNOWFLAKE_WAREHOUSE}"

COMPUTE_POOL="${RSTUDIO_SERVICE_POOL:-${SFNB_BUILD_POOL:-}}"
: "${COMPUTE_POOL:?Set RSTUDIO_SERVICE_POOL or SFNB_BUILD_POOL}"

VOLUME_STAGE="${VOLUME_STAGE:-${SNOW_DATABASE}.${SNOW_SCHEMA}.VOLUMES}"

render_template() {
  local template="$1"
  local output="$2"
  sed -e "s|<SNOW_ROLE>|${SNOW_ROLE}|g" \
      -e "s|<SNOW_EAI_ROLE>|${SNOW_EAI_ROLE:-ACCOUNTADMIN}|g" \
      -e "s|<SNOWFLAKE_WAREHOUSE>|${SNOWFLAKE_WAREHOUSE}|g" \
      -e "s|<SNOW_DATABASE>|${SNOW_DATABASE}|g" \
      -e "s|<SNOW_SCHEMA>|${SNOW_SCHEMA}|g" \
      -e "s|<COMPUTE_POOL>|${COMPUTE_POOL}|g" \
      -e "s|<VOLUME_STAGE>|${VOLUME_STAGE}|g" \
      -e "s|<IMAGE_BUILDER_NETWORK_RULE>|${IMAGE_BUILDER_NETWORK_RULE:-IMAGE_BUILDER_EGRESS}|g" \
      -e "s|<SFNB_BUILD_EAI>|${SFNB_BUILD_EAI:?Set SFNB_BUILD_EAI}|g" \
      "${template}" > "${output}"
}

if [[ "${MODE}" == "eai" ]]; then
  TEMPLATE="${SCRIPT_DIR}/provision_image_builder_eai.sql.template"
  RENDERED="${SCRIPT_DIR}/.provision_eai.rendered.sql"
  SQL_ROLE="${SNOW_EAI_ROLE:-ACCOUNTADMIN}"
  echo "==> Provisioning Image Builder EAI (role=${SQL_ROLE})"
else
  TEMPLATE="${SCRIPT_DIR}/provision.sql.template"
  RENDERED="${SCRIPT_DIR}/.provision.rendered.sql"
  SQL_ROLE="${SNOW_ROLE}"
  echo "==> Provisioning RStudio SPCS prerequisites (role=${SQL_ROLE})"
fi

render_template "${TEMPLATE}" "${RENDERED}"
echo "    Rendered: ${RENDERED}"

"${SNOW}" sql -c "${SNOW_CONNECTION}" --role "${SQL_ROLE}" -f "${RENDERED}"
rm -f "${RENDERED}"
echo "==> Done"
