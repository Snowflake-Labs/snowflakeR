# Test helpers for snowflakeR
# =============================================================================
# Loaded automatically by testthat before every test file.

# Skip if no Snowflake connection is available (integration tests)
skip_if_no_snowflake <- function() {
  testthat::skip_if_not(
    sfr_has_connection(),
    "No Snowflake connection available"
  )
}

# Skip if Python/reticulate is not configured
skip_if_no_python <- function() {
  testthat::skip_if_not(
    reticulate::py_available(),
    "Python not available via reticulate"
  )
}

# Skip if snowflake-ml-python is not installed
skip_if_no_snowflake_ml <- function() {
  skip_if_no_python()
  testthat::skip_if_not(
    reticulate::py_module_available("snowflake.ml"),
    "snowflake-ml-python not available"
  )
}
