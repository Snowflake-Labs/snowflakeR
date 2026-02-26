# Python Dependency Management
# =============================================================================
# Opt-in installation only (CRAN requirement).

#' Install Python dependencies for snowflakeR
#'
#' Creates a conda environment with all required Python packages for
#' `snowflakeR` to function. This environment is shared between `reticulate`
#' and `rpy2`, avoiding duplicate installations.
#'
#' @param method Character. Installation method: `"conda"` (recommended),
#'   `"pip"`, or `"auto"`. Default: `"conda"`.
#' @param envname Character. Name of the conda/virtual environment.
#'   Default: `"r-snowflakeR"`.
#' @param python_version Character. Python version to use. Default: `"3.11"`.
#'
#' @details
#' The recommended setup is a single conda environment that both `reticulate`
#' and `rpy2` share. This avoids installing packages twice. In Workspace
#' Notebooks, the micromamba environment already serves this purpose.
#'
#' @returns Invisibly returns `TRUE` on success.
#'
#' @examples
#' \dontrun{
#' sfr_install_python_deps()
#' }
#'
#' @export
sfr_install_python_deps <- function(method = "conda",
                                    envname = "r-snowflakeR",
                                    python_version = "3.11") {
  packages <- c(
    "snowflake-ml-python>=1.5.0",
    "snowflake-snowpark-python",
    "rpy2>=3.5",
    "pandas"
  )

  if (method == "conda") {
    cli::cli_inform("Creating conda environment {.val {envname}}...")
    reticulate::conda_create(envname, python_version = python_version)
    reticulate::conda_install(
      envname = envname,
      packages = packages,
      channel = c("conda-forge", "defaults")
    )
  } else if (method == "pip") {
    cli::cli_inform("Installing into virtualenv {.val {envname}}...")
    reticulate::virtualenv_create(envname, python = python_version)
    reticulate::virtualenv_install(envname, packages = packages)
  } else {
    cli::cli_abort("Unknown method {.val {method}}. Use {.val conda} or {.val pip}.")
  }

  cli::cli_inform(c(
    "v" = "Python dependencies installed in {.val {envname}}.",
    "i" = "Activate with: {.code reticulate::use_condaenv(\"{envname}\")}"
  ))
  invisible(TRUE)
}
