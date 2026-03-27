# Version Compatibility Infrastructure
# =============================================================================
# Runtime version checking for snowflake-ml-python and Snowflake server.
# Gates new features on minimum SDK versions with clear error messages.

#' Get installed snowflake-ml-python version
#'
#' @returns A [numeric_version] object, or `NULL` if the package is not
#'   installed or Python is not available.
#'
#' @examples
#' \dontrun{
#' sfr_ml_version()
#' #> [1] '1.27.0'
#' }
#'
#' @export
sfr_ml_version <- function() {
  tryCatch({
    md <- reticulate::import("importlib.metadata")
    numeric_version(md$version("snowflake-ml-python"))
  }, error = function(e) {
    NULL
  })
}


#' Check minimum snowflake-ml-python version
#'
#' Aborts with an informative error if the installed version of
#' snowflake-ml-python is missing or older than the required minimum.
#'
#' @param min Character. Minimum version required (e.g., `"1.24.0"`).
#' @param feature Character. Feature name for the error message.
#'
#' @returns Invisibly returns `TRUE` if the version check passes.
#'
#' @noRd
sfr_requires_ml <- function(min, feature) {
  current <- sfr_ml_version()
  if (is.null(current)) {
    cli::cli_abort(c(
      "{feature} requires {.pkg snowflake-ml-python} >= {min}.",
      "x" = "{.pkg snowflake-ml-python} is not installed.",
      "i" = "Install it with: {.code pip install 'snowflake-ml-python>={min}'}"
    ))
  }
  if (current < numeric_version(min)) {
    cli::cli_abort(c(
      "{feature} requires {.pkg snowflake-ml-python} >= {min}.",
      "x" = "You have version {.val {as.character(current)}}.",
      "i" = "Upgrade with: {.code pip install 'snowflake-ml-python>={min}'}"
    ))
  }
  invisible(TRUE)
}


#' Check which snowflakeR features are available
#'
#' Reports which optional features are available based on the installed
#' version of `snowflake-ml-python`.
#'
#' @param conn An `sfr_connection` object (optional). If provided, also
#'   reports the Snowflake server version.
#'
#' @returns A data.frame with columns `feature`, `min_version`, `available`,
#'   and `installed`.
#'
#' @examples
#' \dontrun{
#' sfr_check_features()
#' #>                                  feature min_version available installed
#' #> 1 Core Feature Store + Model Registry       1.5.0      TRUE   1.27.0
#' #> 2 ML Observability (model monitoring)       1.7.1      TRUE   1.27.0
#' #> ...
#' }
#'
#' @export
sfr_check_features <- function(conn = NULL) {
  current <- sfr_ml_version()

  features <- data.frame(
    feature = c(
      "Core Feature Store + Model Registry",
      "ML Observability (model monitoring)",
      "Online feature serving",
      "Experiment Tracking",
      "Tile-based aggregation",
      "Autocapture inference logs",
      "Min instances (auto-scale to zero)",
      "ParamSpec inference parameters",
      "auto_prefix / with_name",
      "Iceberg-backed Feature Views"
    ),
    min_version = c(
      "1.5.0", "1.7.1", "1.18.0", "1.19.0", "1.24.0",
      "1.25.0", "1.25.0", "1.26.0", "1.26.0", "1.26.0"
    ),
    stringsAsFactors = FALSE
  )

  features$available <- if (is.null(current)) {
    FALSE
  } else {
    current >= numeric_version(features$min_version)
  }

  features$installed <- as.character(current %||% "not installed")

  if (!is.null(conn)) {
    sv <- tryCatch(sfr_snowflake_version(conn), error = function(e) NA_character_)
    attr(features, "snowflake_version") <- sv
  }

  features
}


#' Get Snowflake server version
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#'
#' @returns A character string with the Snowflake server version.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' sfr_snowflake_version(conn)
#' #> [1] "9.26.0"
#' }
#'
#' @export
sfr_snowflake_version <- function(conn) {
  validate_connection(conn)
  result <- sfr_query(conn, "SELECT CURRENT_VERSION() AS VERSION")
  result$VERSION
}
