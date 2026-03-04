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

  invisible()
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage(
    "snowflakeR ", utils::packageVersion("snowflakeR"),
    " - R interface to the Snowflake ML platform"
  )
}
