#!/usr/bin/env bash
# Flatten build context for SPCS Image Builder (single directory, no subfolders).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNOWFLAKER_PKG_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MONOREPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
CTX="${SCRIPT_DIR}/build-ctx"
PKG_CACHE="${SCRIPT_DIR}/pkg"

mkdir -p "${CTX}" "${PKG_CACHE}"
rm -rf "${CTX:?}"/*

ensure_tarball() {
  local pkg_dir="$1"
  local pkg_name="$2"
  local search_root="$3"

  if ls "${search_root}/${pkg_name}"_*.tar.gz 1>/dev/null 2>&1; then
    return 0
  fi
  if [[ ! -d "${pkg_dir}" ]]; then
    return 1
  fi
  echo "==> Building ${pkg_name} tarball from ${pkg_dir}"
  (cd "$(dirname "${pkg_dir}")" && R CMD build --no-build-vignettes --no-manual "$(basename "${pkg_dir}")")
}

copy_tarball() {
  local pkg_name="$1"
  local dest="$2"
  local hit=""

  for root in "${MONOREPO_ROOT}" "${PKG_CACHE}" "$(dirname "${SNOWFLAKER_PKG_ROOT}")"; do
    hit="$(ls "${root}/${pkg_name}"_*.tar.gz 2>/dev/null | head -1 || true)"
    if [[ -n "${hit}" ]]; then
      cp "${hit}" "${dest}/"
      echo "==> Using tarball: ${hit}"
      return 0
    fi
  done
  return 1
}

ensure_tarball "${SNOWFLAKER_PKG_ROOT}" "snowflakeR" "$(dirname "${SNOWFLAKER_PKG_ROOT}")"
ensure_tarball "${MONOREPO_ROOT}/RSnowflake" "RSnowflake" "${MONOREPO_ROOT}" || true

if ! copy_tarball "snowflakeR" "${CTX}"; then
  echo "ERROR: snowflakeR tarball required for Image Builder (COPY *.tar.gz)."
  echo "  Run from snowflakeR source tree or place snowflakeR_*.tar.gz in pkg/"
  exit 1
fi
copy_tarball "RSnowflake" "${CTX}" || echo "WARN: no RSnowflake tarball — install_packages.R will use GitHub"

cp "${SCRIPT_DIR}/Dockerfile.imagebuilder" "${CTX}/Dockerfile"
cp "${SCRIPT_DIR}/install_packages.R" "${CTX}/install_packages.R"
cp "${SCRIPT_DIR}/.Rprofile" "${CTX}/rstudio.Rprofile"
cp "${SCRIPT_DIR}/smoke_test.R" "${SCRIPT_DIR}/smoke_test.py" \
   "${SCRIPT_DIR}/spcs_helpers.R" "${SCRIPT_DIR}/WELCOME.md" "${CTX}/"

echo "==> build-ctx ready (flat): ${CTX}"
ls -la "${CTX}"
