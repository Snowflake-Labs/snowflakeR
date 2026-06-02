#!/usr/bin/env bash
# Copy pre-built package vignette HTML into the Quarto book _site/ for GitHub Pages.
# Source of truth: snowflakeR/inst/doc/*.html (monorepo) or inst/doc/*.html (public repo).
#
# Called via project.post-render in the book _quarto.yml.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOK_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SITE_DIR="${BOOK_DIR}/_site"
DEST="${SITE_DIR}/vignettes/snowflakeR"

if [[ ! -d "$SITE_DIR" ]]; then
  echo "copy_vignette_html_to_site: no _site at ${SITE_DIR} (skip — run quarto render first)" >&2
  exit 0
fi

REPO_ROOT=""
if git -C "$BOOK_DIR" rev-parse --show-toplevel &>/dev/null; then
  REPO_ROOT="$(git -C "$BOOK_DIR" rev-parse --show-toplevel)"
fi

SF_DOC=""
for candidate in \
  "${REPO_ROOT}/snowflakeR/inst/doc" \
  "${REPO_ROOT}/inst/doc"; do
  if [[ -d "$candidate" ]] && [[ -f "$candidate/getting-started.html" ]]; then
    SF_DOC="$candidate"
    break
  fi
done

if [[ -z "$SF_DOC" ]]; then
  echo "copy_vignette_html_to_site: snowflakeR inst/doc not found (skip)" >&2
  exit 0
fi

mkdir -p "$DEST"
count=0
for html in "$SF_DOC"/*.html; do
  [[ -f "$html" ]] || continue
  base="$(basename "$html")"
  [[ "$base" == "snowflakeR-reference.html" ]] && continue
  cp "$html" "${DEST}/${base}"
  count=$((count + 1))
done

echo "copy_vignette_html_to_site: copied ${count} snowflakeR vignette(s) to ${DEST}"
