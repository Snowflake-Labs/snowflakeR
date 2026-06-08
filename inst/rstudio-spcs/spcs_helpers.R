# Helpers for RStudio on custom SPCS services (not Workspace Notebooks).
# Source from smoke tests or: source("~/spcs_helpers.R")

#' Detect Snowflake Container Services runtime
in_spcs_runtime <- function() {
  nzchar(Sys.getenv("SNOWFLAKE_HOST", "")) ||
    file.exists("/snowflake/session/token")
}

#' Read SPCS OAuth token (never print to logs)
.spcs_token <- function() {
  path <- "/snowflake/session/token"
  if (!file.exists(path)) {
    stop("SPCS token file not found at ", path, call. = FALSE)
  }
  trimws(paste(readLines(path, warn = FALSE), collapse = ""))
}

#' Connect snowflakeR via Snowpark + SPCS OAuth (custom SPCS services)
#'
#' Workspace Notebooks should use [snowflakeR::sfr_connect()] directly.
#' RStudio on a custom SPCS service needs host + oauth token explicitly.
sfr_connect_spcs <- function(account = Sys.getenv("SNOWFLAKE_ACCOUNT", ""),
                             warehouse = Sys.getenv("SNOWFLAKE_WAREHOUSE", ""),
                             database = Sys.getenv("SNOWFLAKE_DATABASE", ""),
                             schema = Sys.getenv("SNOWFLAKE_SCHEMA", ""),
                             role = Sys.getenv("SNOWFLAKE_ROLE", "")) {
  if (!nzchar(account)) {
    stop("Set SNOWFLAKE_ACCOUNT in the service spec or container env.", call. = FALSE)
  }
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("Package 'reticulate' is required.", call. = FALSE)
  }
  if (!requireNamespace("snowflakeR", quietly = TRUE)) {
    stop("Package 'snowflakeR' is required.", call. = FALSE)
  }

  py <- Sys.getenv("RETICULATE_PYTHON", "/opt/conda/envs/snowflake_ml/bin/python")
  if (!file.exists(py)) {
    stop("Python not found at ", py, " — rebuild image with Miniconda.", call. = FALSE)
  }
  conda_lib <- "/opt/conda/envs/snowflake_ml/lib"
  if (dir.exists(conda_lib)) {
    Sys.setenv(LD_LIBRARY_PATH = paste(conda_lib, Sys.getenv("LD_LIBRARY_PATH", ""), sep = ":"))
  }
  reticulate::use_python(py, required = TRUE)

  bridge <- reticulate::import_from_path(
    "sfr_connect_bridge",
    path = system.file("python", package = "snowflakeR")
  )

  session <- bridge$create_session(
    account = account,
    warehouse = warehouse,
    database = database,
    schema = schema,
    role = role,
    authenticator = "oauth",
    host = Sys.getenv("SNOWFLAKE_HOST"),
    token = .spcs_token()
  )

  conn <- structure(
    list(
      session = session,
      dbi_con = NULL,
      .connect_name = NULL,
      account = account,
      user = NULL,
      database = database,
      schema = schema,
      warehouse = warehouse,
      role = role,
      auth_method = "oauth",
      environment = "spcs",
      created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  conn <- snowflakeR:::refresh_conn_from_session(conn)
  message("Connected via SPCS OAuth Snowpark session (", account, ").")
  conn
}
