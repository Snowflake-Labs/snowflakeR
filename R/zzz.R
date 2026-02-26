# Package load/attach hooks
# Following CRAN rules: no side effects on load, no Python init here

# Internal package environment for caching bridge modules, sessions, etc.
.pkg_env <- new.env(parent = emptyenv())

.onLoad <- function(libname, pkgname) {
  # Set default options (user can override)
  op <- options()
  op_sfr <- list(
    snowflakeR.python_env = "r-snowflakeR",
    snowflakeR.verbose = FALSE,
    snowflakeR.print_width = 200L
  )
  toset <- !(names(op_sfr) %in% names(op))
  if (any(toset)) options(op_sfr[toset])

  # Register S3 methods for generics in Suggests packages.
  # Note: tbl.sfr_connection is NOT registered -- since sfr_connection
  # includes "DBIConnection" in its class vector, dbplyr's own
  # tbl.DBIConnection dispatches correctly.
  if (requireNamespace("dbplyr", quietly = TRUE)) {
    registerS3method("dbplyr_edition", "sfr_connection",
                     dbplyr_edition.sfr_connection,
                     envir = asNamespace("dbplyr"))
    registerS3method("db_query_fields", "sfr_connection",
                     db_query_fields.sfr_connection,
                     envir = asNamespace("dbplyr"))
  }

  invisible()
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage(
    "snowflakeR ", utils::packageVersion("snowflakeR"),
    " - R interface to the Snowflake ML platform"
  )
}
