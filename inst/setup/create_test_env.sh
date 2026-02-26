#!/bin/bash
# =============================================================================
# Create the r-snowflakeR test environment
# =============================================================================
# Two-step approach for speed:
#   Step 1: conda creates env with Python 3.11, R, rpy2, and core R packages
#   Step 2: pip installs snowflake-ml-python (faster than conda solve)
#
# Usage:
#   bash inst/setup/create_test_env.sh
# =============================================================================

set -euo pipefail

ENV_NAME="r-snowflakeR"
PYTHON_VERSION="3.11"

echo "=== Creating $ENV_NAME environment ==="
echo ""

# Step 1: Conda base (R + rpy2 + core R packages)
echo "--- Step 1: Conda base (R + rpy2 + R packages) ---"
conda create -n "$ENV_NAME" \
  -c conda-forge \
  python="$PYTHON_VERSION" \
  r-base">=4.1" \
  rpy2">=3.5" \
  pandas \
  pyarrow \
  cryptography \
  r-reticulate \
  r-cli \
  r-rlang \
  r-jsonlite \
  r-testthat \
  r-devtools \
  r-roxygen2 \
  r-withr \
  r-dplyr \
  r-dbplyr \
  r-dbi \
  r-knitr \
  r-rmarkdown \
  --yes

echo ""
echo "--- Step 2: Pip install Snowflake packages ---"
conda run -n "$ENV_NAME" pip install \
  "snowflake-ml-python>=1.5.0" \
  "snowflake-snowpark-python"

echo ""
echo "=== Environment $ENV_NAME ready ==="
echo ""
echo "Activate with:  conda activate $ENV_NAME"
echo "Use in R with:  RETICULATE_PYTHON=\$(conda run -n $ENV_NAME which python) Rscript ..."
