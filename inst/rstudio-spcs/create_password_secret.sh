#!/usr/bin/env bash
# Create or rotate the Snowflake secret used for RStudio Server PASSWORD.
# The password is read from the environment only — never from committed files.
#
# Usage:
#   source config.env
#   RSTUDIO_PASSWORD='new-strong-password' ./create_password_secret.sh
#   # or (prompts securely):
#   ./create_password_secret.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNOW="${SNOW_CLI:-snow}"

if [[ -z "${SNOW_CONNECTION:-}" ]] && [[ -f "${SCRIPT_DIR}/config.env" ]]; then
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/config.env"
fi

: "${SNOW_CONNECTION:?Set SNOW_CONNECTION in config.env}"
: "${SNOW_ROLE:?Set SNOW_ROLE}"
: "${SNOW_DATABASE:?Set SNOW_DATABASE}"
: "${SNOW_SCHEMA:?Set SNOW_SCHEMA}"

RSTUDIO_PASSWORD_SECRET="${RSTUDIO_PASSWORD_SECRET:-RSTUDIO_SERVER_PASSWORD}"

if [[ -z "${RSTUDIO_PASSWORD:-}" ]]; then
  read -rsp "RStudio PASSWORD (not echoed): " RSTUDIO_PASSWORD
  echo
fi
if [[ -z "${RSTUDIO_PASSWORD}" ]]; then
  echo "ERROR: Set RSTUDIO_PASSWORD in the environment or enter at prompt." >&2
  exit 1
fi

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}
SECRET_STRING_ESCAPED="$(sql_escape "${RSTUDIO_PASSWORD}")"
SECRET_FQN="${SNOW_DATABASE}.${SNOW_SCHEMA}.${RSTUDIO_PASSWORD_SECRET}"

echo "==> Creating secret ${SECRET_FQN} (GENERIC_STRING → env PASSWORD at runtime)"

"${SNOW}" sql -c "${SNOW_CONNECTION}" --role "${SNOW_ROLE}" -q "
USE ROLE ${SNOW_ROLE};
USE DATABASE ${SNOW_DATABASE};
USE SCHEMA ${SNOW_SCHEMA};

CREATE OR REPLACE SECRET ${SECRET_FQN}
  TYPE = GENERIC_STRING
  SECRET_STRING = '${SECRET_STRING_ESCAPED}';

GRANT USAGE ON SECRET ${SECRET_FQN} TO ROLE ${SNOW_ROLE};
"

unset RSTUDIO_PASSWORD SECRET_STRING_ESCAPED
echo "==> Secret ready. Redeploy with: ./deploy_service.sh"
