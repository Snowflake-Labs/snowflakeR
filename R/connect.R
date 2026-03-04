# Connection & Session Management
# =============================================================================
# Foundation module: all other modules depend on a connection object.
# Integrates with snowflakeauth (optional) for connections.toml support.
# Falls back to reading connections.toml directly via RcppTOML or Python toml.

# -----------------------------------------------------------------------------
# Internal: Python bridge lazy loader
# -----------------------------------------------------------------------------

#' Get the Python bridge module for a given submodule
#'
#' Lazy-loads the Python bridge from `inst/python/` on first call.
#'
#' @param module_name Base name of the Python module (without `.py`)
#' @returns A Python module object (via reticulate)
#' @noRd
get_bridge_module <- function(module_name) {
  cache_key <- paste0("bridge_", module_name)
  if (!is.null(.pkg_env[[cache_key]])) {
    return(.pkg_env[[cache_key]])
  }

  python_dir <- system.file("python", package = "snowflakeR")
  if (!nzchar(python_dir)) {
    cli::cli_abort(c(
      "Cannot find {.path inst/python/} directory in {.pkg snowflakeR}.",
      "i" = "This suggests the package is not installed correctly."
    ))
  }

  mod <- reticulate::import_from_path(module_name, path = python_dir)
  .pkg_env[[cache_key]] <- mod
  mod
}


# -----------------------------------------------------------------------------
# Internal: Read connections.toml directly (fallback when snowflakeauth absent)
# -----------------------------------------------------------------------------

#' Read a connection profile from connections.toml
#'
#' Searches standard locations for Snowflake's `connections.toml` and returns
#' the named profile (or the default / only profile).
#'
#' @param name Character or NULL. Profile name. When `NULL`, looks for
#'   `[default]`, then falls back to the first profile.
#' @returns A named list of connection parameters, or `NULL` if not found.
#' @noRd
.read_connections_toml <- function(name = NULL) {
  # Standard search paths
  toml_path <- Sys.getenv("SNOWFLAKE_HOME", file.path(Sys.getenv("HOME"), ".snowflake"))
  toml_file <- file.path(toml_path, "connections.toml")
  if (!file.exists(toml_file)) return(NULL)

  toml <- tryCatch(
    {
      if (requireNamespace("RcppTOML", quietly = TRUE)) {
        RcppTOML::parseTOML(toml_file)
      } else {
        # Fallback: Python toml / tomllib
        py_toml <- tryCatch(
          reticulate::import("tomllib", convert = TRUE),
          error = function(e) reticulate::import("toml", convert = TRUE)
        )
        py_builtins <- reticulate::import_builtins()
        fh <- py_builtins$open(toml_file, "rb")
        on.exit(fh$close(), add = TRUE)
        py_toml$load(fh)
      }
    },
    error = function(e) NULL
  )

  if (is.null(toml) || length(toml) == 0) return(NULL)

  # Select the right profile
  if (!is.null(name) && name %in% names(toml)) {
    conn <- toml[[name]]
  } else if ("default" %in% names(toml)) {
    conn <- toml[["default"]]
  } else if (length(toml) == 1) {
    # Single profile -- use it
    conn <- toml[[1]]
  } else {
    # Multiple profiles, none selected -- use first and warn
    first_name <- names(toml)[1]
    cli::cli_inform(c(
      "i" = "No connection name specified; using profile {.val {first_name}} from {.file connections.toml}.",
      "i" = "Pass {.arg name} to {.fn sfr_connect} to choose a specific profile."
    ))
    conn <- toml[[1]]
  }

  conn
}


# -----------------------------------------------------------------------------
# Exported: Connection
# -----------------------------------------------------------------------------

#' Connect to Snowflake
#'
#' Creates a connection to Snowflake, returning an `sfr_connection` object.
#' Supports multiple authentication methods:
#'
#' - **Auto-detect:** In Workspace Notebooks, wraps the active Snowpark session.
#'   Locally, reads `connections.toml` / `config.toml` (via `snowflakeauth` if
#'   installed, or directly via RcppTOML / Python toml as fallback).
#' - **Named connection:** Pass `name` to select a connection from
#'   `connections.toml`.
#' - **Explicit parameters:** Pass `account`, `user`, `authenticator`, etc.
#'
#' @param name Character. Named connection from `connections.toml`. If `NULL`,
#'   uses the `[default]` profile, or the only profile if there is exactly one.
#' @param account Character. Snowflake account identifier.
#' @param user Character. Snowflake username.
#' @param warehouse Character. Default warehouse.
#' @param database Character. Default database.
#' @param schema Character. Default schema.
#' @param role Character. Role to use.
#' @param authenticator Character. Authentication method (e.g.,
#'   `"externalbrowser"`, `"snowflake"`, `"SNOWFLAKE_JWT"`, `"oauth"`).
#' @param private_key_file Character. Path to PEM-encoded private key for
#'   key-pair authentication. Also read from `connections.toml` field
#'   `private_key_path`.
#' @param ... Additional connection parameters passed to Snowpark session
#'   builder or `snowflakeauth::snowflake_connection()`.
#' @param .use_snowflakeauth Logical. Whether to use `snowflakeauth` for
#'   credential resolution when available. Default: `TRUE`.
#'
#' @returns An `sfr_connection` object (S3 class).
#'
#' @examples
#' \dontrun{
#' # Default connection from connections.toml
#' conn <- sfr_connect()
#'
#' # Named connection
#' conn <- sfr_connect(name = "my_profile")
#'
#' # Explicit parameters with key-pair auth
#' conn <- sfr_connect(
#'   account = "xy12345.us-east-1",
#'   user = "MYUSER",
#'   private_key_file = "~/.snowflake/keys/rsa_key.p8"
#' )
#'
#' # Explicit parameters with browser SSO
#' conn <- sfr_connect(
#'   account = "xy12345.us-east-1",
#'   user = "MYUSER",
#'   authenticator = "externalbrowser"
#' )
#' }
#'
#' @export
sfr_connect <- function(name = NULL,
                        account = NULL,
                        user = NULL,
                        warehouse = NULL,
                        database = NULL,
                        schema = NULL,
                        role = NULL,
                        authenticator = NULL,
                        private_key_file = NULL,
                        ...,
                        .use_snowflakeauth = TRUE) {
  # Attempt Workspace Notebook auto-detect first
  session <- tryCatch(
    {
      bridge <- get_bridge_module("sfr_connect_bridge")
      bridge$get_active_session()
    },
    error = function(e) NULL
  )

  if (!is.null(session)) {
    # Workspace Notebook environment
    env_type <- "workspace"
    auth_method <- "session_token"
    cli::cli_inform("Connected via active Workspace Notebook session.")
  } else {
    # Local environment - build session from parameters
    env_type <- "local"

    # Strategy 1: snowflakeauth (if installed)
    sf_conn <- NULL
    if (.use_snowflakeauth &&
        requireNamespace("snowflakeauth", quietly = TRUE)) {
      sf_conn <- tryCatch(
        snowflakeauth::snowflake_connection(
          name = name,
          account = account,
          user = user,
          warehouse = warehouse,
          database = database,
          schema = schema,
          role = role,
          authenticator = authenticator,
          private_key_file = private_key_file,
          ...
        ),
        error = function(e) NULL
      )
    }

    if (!is.null(sf_conn)) {
      # Extract params from snowflakeauth connection
      account         <- account %||% sf_conn$account
      user            <- user %||% sf_conn$user
      warehouse       <- warehouse %||% sf_conn$warehouse
      database        <- database %||% sf_conn$database
      schema          <- schema %||% sf_conn$schema
      role            <- role %||% sf_conn$role
      private_key_file <- private_key_file %||%
        sf_conn$private_key_path %||%
        sf_conn$private_key_file
      auth_method     <- sf_conn$authenticator %||% "snowflake"
    } else if (is.null(account)) {
      # Strategy 2: read connections.toml directly
      toml_conn <- .read_connections_toml(name)
      if (!is.null(toml_conn)) {
        account         <- account %||% toml_conn$account
        user            <- user %||% toml_conn$user
        warehouse       <- warehouse %||% toml_conn$warehouse
        database        <- database %||% toml_conn$database
        schema          <- schema %||% toml_conn$schema
        role            <- role %||% toml_conn$role
        private_key_file <- private_key_file %||% toml_conn$private_key_path
        authenticator    <- authenticator %||% toml_conn$authenticator
      }
      auth_method <- authenticator %||% "snowflake"
    } else {
      auth_method <- authenticator %||% "snowflake"
    }

    # Validate minimum required params
    if (is.null(account)) {
      cli::cli_abort(c(
        "A Snowflake {.arg account} is required.",
        "i" = "Provide it directly, via {.file connections.toml}, or set",
        " " = "{.envvar SNOWFLAKE_ACCOUNT}."
      ))
    }

    # Create Snowpark session via Python bridge
    bridge <- get_bridge_module("sfr_connect_bridge")
    session <- bridge$create_session(
      account = account,
      user = user,
      warehouse = warehouse,
      database = database,
      schema = schema,
      role = role,
      authenticator = auth_method,
      private_key_file = private_key_file
    )

    cli::cli_inform("Connected to Snowflake account {.val {account}}.")
  }

  conn <- structure(
    list(
      session = session,
      dbi_con = NULL,
      .connect_name = name,
      account = account,
      user = user,
      database = database,
      schema = schema,
      warehouse = warehouse,
      role = role,
      auth_method = auth_method,
      environment = env_type,
      created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  # Refresh from live session to fill any NULLs
  conn <- refresh_conn_from_session(conn)

  # Warn about unset context values
  missing <- character(0)
  if (is.null(conn$warehouse)) missing <- c(missing, "warehouse")
  if (is.null(conn$database))  missing <- c(missing, "database")
  if (is.null(conn$schema))    missing <- c(missing, "schema")
  if (length(missing) > 0) {
    cli::cli_warn(c(
      "!" = "The following are not set on this session: {.val {missing}}.",
      "i" = "Use {.fn sfr_use} to set them: {.code conn <- sfr_use(conn, {missing[1]} = \"...\")}"
    ))
  }

  conn
}


#' Print an sfr_connection object
#'
#' @param x An `sfr_connection` object.
#' @param ... Ignored.
#' @returns Invisibly returns `x`.
#' @export
print.sfr_connection <- function(x, ...) {
  env_label <- x$environment %||% "unknown"
  dbi_label <- if (!is.null(x$dbi_con)) "attached" else "not attached"
  cli::cli_text("<{.cls sfr_connection}> [{env_label}]")
  fields <- list(
    account = x$account,
    user = x$user,
    database = x$database,
    schema = x$schema,
    warehouse = x$warehouse,
    role = x$role,
    auth_method = x$auth_method,
    environment = x$environment,
    dbi_connection = dbi_label,
    created_at = format(x$created_at, "%Y-%m-%d %H:%M:%S")
  )
  fields <- Filter(Negate(is.null), fields)
  labels <- lapply(names(fields), function(n) cli::format_inline("{.field {n}}"))
  items <- lapply(fields, function(v) cli::format_inline("{.val {v}}"))
  cli::cli_dl(items, labels = labels)
  invisible(x)
}


#' Check if an object is an sfr_connection
#'
#' @param x Object to test.
#' @returns Logical.
#' @noRd
is_sfr_connection <- function(x) {
  inherits(x, "sfr_connection")
}


#' Validate that conn is an sfr_connection
#'
#' @param conn Object to validate.
#' @noRd
validate_connection <- function(conn) {
  if (!is_sfr_connection(conn)) {
    cli::cli_abort(
      "{.arg conn} must be an {.cls sfr_connection} object from {.fn sfr_connect}."
    )
  }
}


#' Check connection status
#'
#' @param conn An `sfr_connection` object.
#' @returns Invisibly returns `TRUE` if the connection is active.
#'
#' @export
sfr_status <- function(conn) {
  validate_connection(conn)
  # TODO: implement actual session health check

  cli::cli_inform(c(
    "v" = "Connection active ({.val {conn$environment}} environment)",
    "i" = "Account: {.val {conn$account}}",
    "i" = "Database: {.val {conn$database %||% '<not set>'}}",
    "i" = "Warehouse: {.val {conn$warehouse %||% '<not set>'}}"
  ))
  invisible(TRUE)
}


#' Switch warehouse, database, or schema
#'
#' Runs `USE WAREHOUSE/DATABASE/SCHEMA` on the Snowpark session and updates
#' the connection object. **Important:** R objects are pass-by-value, so you
#' must reassign the result: `conn <- sfr_use(conn, schema = "NEW")`.
#'
#' @param conn An `sfr_connection` object.
#' @param warehouse Character. New warehouse name.
#' @param database Character. New database name.
#' @param schema Character. New schema name.
#' @returns The updated `sfr_connection` object (invisibly). You **must**
#'   reassign: `conn <- sfr_use(conn, ...)`.
#'
#' @export
sfr_use <- function(conn, warehouse = NULL, database = NULL, schema = NULL) {
  validate_connection(conn)
  session <- conn$session

  if (!is.null(warehouse)) {
    session$sql(paste0("USE WAREHOUSE ", warehouse))$collect()
  }
  if (!is.null(database)) {
    session$sql(paste0("USE DATABASE ", database))$collect()
  }
  if (!is.null(schema)) {
    session$sql(paste0("USE SCHEMA ", schema))$collect()
  }

  # Refresh R-side fields from the actual session state
  conn <- refresh_conn_from_session(conn)

  invisible(conn)
}


#' Refresh connection object fields from the live Snowpark session
#'
#' Queries the session for current warehouse, database, schema, and role.
#' This ensures the R-side `conn` object matches the actual session state.
#'
#' @param conn An `sfr_connection` object.
#' @returns The updated `sfr_connection` object.
#' @noRd
refresh_conn_from_session <- function(conn) {
  session <- conn$session

  # Query the session for current context values
  # These methods return quoted identifiers; strip quotes
  strip_quotes <- function(x) {
    if (is.null(x) || length(x) == 0) return(NULL)
    val <- tryCatch(as.character(x), error = function(e) NULL)
    if (is.null(val) || val == "" || val == "None") return(NULL)
    gsub('^"|"$', '', val)
  }

  conn$warehouse <- strip_quotes(tryCatch(session$get_current_warehouse(), error = function(e) NULL))
  conn$database  <- strip_quotes(tryCatch(session$get_current_database(), error = function(e) NULL))
  conn$schema    <- strip_quotes(tryCatch(session$get_current_schema(), error = function(e) NULL))
  conn$role      <- strip_quotes(tryCatch(session$get_current_role(), error = function(e) NULL))

  conn
}


#' Check if a Snowflake connection can be established
#'
#' Useful for `@examplesIf` and test guards.
#'
#' @param ... Arguments passed to [sfr_connect()].
#' @returns Logical.
#' @export
sfr_has_connection <- function(...) {
  tryCatch(
    {
      sfr_connect(...)
      TRUE
    },
    error = function(e) FALSE
  )
}


#' Get an RSnowflake DBI connection from an sfr_connection
#'
#' Returns an `RSnowflake::SnowflakeConnection` that can be used with
#' standard DBI methods (`dbGetQuery`, `dbWriteTable`, etc.) and dbplyr.
#' The connection is created lazily on first call and cached on the
#' `sfr_connection` object.
#'
#' Requires the `RSnowflake` package to be installed.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @returns An `RSnowflake::SnowflakeConnection` object.
#'
#' @examples
#' \dontrun{
#' sfr_conn <- sfr_connect(name = "my_profile")
#' dbi_con  <- sfr_dbi_connection(sfr_conn)
#' DBI::dbGetQuery(dbi_con, "SELECT 1")
#' }
#'
#' @export
sfr_dbi_connection <- function(conn) {
  validate_connection(conn)

  if (!is.null(conn$dbi_con)) {
    return(conn$dbi_con)
  }

  rlang::check_installed("RSnowflake",
    reason = "for DBI database connectivity")
  rlang::check_installed("DBI",
    reason = "for DBI database connectivity")

  dbi_con <- DBI::dbConnect(
    RSnowflake::Snowflake(),
    name      = conn$.connect_name,
    account   = conn$account,
    user      = conn$user,
    database  = conn$database,
    schema    = conn$schema,
    warehouse = conn$warehouse,
    role      = conn$role
  )

  conn$dbi_con <- dbi_con
  dbi_con
}
