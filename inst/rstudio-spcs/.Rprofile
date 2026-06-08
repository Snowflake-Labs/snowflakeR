# RStudio Server on SPCS — session defaults
options(
  repos = c(CRAN = "https://packagemanager.posit.co/cran/__linux__/noble/latest"),
  menu.graphics = FALSE
)

Sys.setenv(
  RETICULATE_PYTHON = "/opt/conda/envs/snowflake_ml/bin/python",
  LD_LIBRARY_PATH = paste(
    "/opt/conda/envs/snowflake_ml/lib",
    Sys.getenv("LD_LIBRARY_PATH", ""),
    sep = ":"
  )
)

in_spcs <- nzchar(Sys.getenv("SNOWFLAKE_HOST", "")) ||
  file.exists("/snowflake/session/token")

if (in_spcs) {
  message(
    "[rstudio-spcs] SPCS detected — ",
    "run source(\"~/smoke_test.R\") or see ~/WELCOME.md"
  )
}

if (requireNamespace("reticulate", quietly = TRUE)) {
  py <- Sys.getenv("RETICULATE_PYTHON", "/opt/conda/envs/snowflake_ml/bin/python")
  if (file.exists(py)) {
    tryCatch(reticulate::use_python(py, required = FALSE), error = function(e) NULL)
  }
}
