#!/usr/bin/env bash
# Copy pre-built package vignette HTML into the Quarto book _site/ for GitHub Pages.
# snowflakeR: inst/doc/*.html (monorepo or public repo root)
# RSnowflake: monorepo RSnowflake/inst/doc, or vignettes-source/rsnowflake/ on public clone
#
# Called via project.post-render in the book _quarto.yml.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOK_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SITE_DIR="${BOOK_DIR}/_site"

if [[ ! -d "$SITE_DIR" ]]; then
  echo "copy_vignette_html_to_site: no _site at ${SITE_DIR} (skip — run quarto render first)" >&2
  exit 0
fi

REPO_ROOT=""
if git -C "$BOOK_DIR" rev-parse --show-toplevel &>/dev/null; then
  REPO_ROOT="$(git -C "$BOOK_DIR" rev-parse --show-toplevel)"
fi

_copy_package_vignettes() {
  local src_dir="$1"
  local dest_subdir="$2"
  local exclude_ref="${3:-}"

  if [[ ! -d "$src_dir" ]]; then
    return 0
  fi

  local dest="${SITE_DIR}/vignettes/${dest_subdir}"
  mkdir -p "$dest"
  local count=0
  for html in "$src_dir"/*.html; do
    [[ -f "$html" ]] || continue
    local base
    base="$(basename "$html")"
    if [[ -n "$exclude_ref" ]] && [[ "$base" == "$exclude_ref" ]]; then
      continue
    fi
    cp "$html" "${dest}/${base}"
    count=$((count + 1))
  done
  if [[ "$count" -gt 0 ]]; then
    echo "copy_vignette_html_to_site: copied ${count} file(s) to vignettes/${dest_subdir}/"
  fi
}

# --- snowflakeR ---
SF_DOC=""
for candidate in \
  "${REPO_ROOT}/snowflakeR/inst/doc" \
  "${REPO_ROOT}/inst/doc"; do
  if [[ -d "$candidate" ]] && [[ -f "$candidate/getting-started.html" ]]; then
    SF_DOC="$candidate"
    break
  fi
done

if [[ -n "$SF_DOC" ]]; then
  _copy_package_vignettes "$SF_DOC" "snowflakeR" "snowflakeR-reference.html"
else
  echo "copy_vignette_html_to_site: snowflakeR inst/doc not found (skip)" >&2
fi

# --- RSnowflake (rendered on same Pages host — do not use github.com/raw URLs) ---
RS_DOC=""
for candidate in \
  "${REPO_ROOT}/RSnowflake/inst/doc" \
  "${BOOK_DIR}/vignettes-source/rsnowflake"; do
  if [[ -d "$candidate" ]] && [[ -f "$candidate/getting-started.html" ]]; then
    RS_DOC="$candidate"
    break
  fi
done

if [[ -n "$RS_DOC" ]]; then
  _copy_package_vignettes "$RS_DOC" "RSnowflake" ""
else
  echo "copy_vignette_html_to_site: RSnowflake HTML not found (skip)" >&2
fi
