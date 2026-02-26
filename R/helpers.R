# Output Formatting & Diagnostics
# =============================================================================
# Pure R implementations of workspace helpers. These work both in
# Workspace Notebooks and local R environments.


# Internal: convert the dict returned by Python _pandas_to_r_dict / query_to_dict
# back into an R data.frame.  Handles NA sentinels and optional numeric coercion.
#
# The Python side uses Series.tolist() to convert numpy arrays to native
# Python types, avoiding the NumPy 1.x/2.x ABI mismatch with reticulate.
# For large datasets, an Apache Arrow IPC transfer path would be faster --
# see TODO.md for the roadmap item.
#
# @param result  A list with `$columns`, `$data`, `$nrows` (from the Python bridge).
# @param lowercase Logical. Lowercase column names? Default TRUE.
# @returns A data.frame.
# @noRd
.bridge_dict_to_df <- function(result, lowercase = TRUE) {
  cols <- result$columns

  if (result$nrows == 0L) {
    df <- data.frame(matrix(ncol = length(cols), nrow = 0))
    names(df) <- if (lowercase) tolower(cols) else cols
    return(df)
  }

  df <- as.data.frame(result$data, stringsAsFactors = FALSE)

  # Replace NA sentinels with proper R NA

  na_sentinel <- "NA_SENTINEL_"
  for (col in names(df)) {
    if (is.character(df[[col]])) {
      df[[col]][df[[col]] == na_sentinel] <- NA_character_
    }
  }

  # Try to coerce character columns to numeric where appropriate
  for (col in names(df)) {
    if (is.character(df[[col]])) {
      non_na <- df[[col]][!is.na(df[[col]])]
      if (length(non_na) > 0) {
        num_vals <- suppressWarnings(as.numeric(non_na))
        if (!any(is.na(num_vals))) {
          df[[col]] <- as.numeric(df[[col]])
        }
      }
    }
  }

  if (lowercase) {
    names(df) <- tolower(names(df))
  }
  df
}

#' Print a data.frame cleanly
#'
#' Bypasses extra formatting that can cause issues in Workspace Notebooks.
#' Sets a wide print width to prevent line wrapping in notebook cells.
#'
#' @param x A data.frame or other printable object.
#' @param width Integer. Print width in characters. Default: 200.
#' @param ... Additional arguments passed to `print()`.
#'
#' @returns Invisibly returns `x`.
#'
#' @export
rprint <- function(x, width = 200L, ...) {
  old_width <- getOption("width")
  on.exit(options(width = old_width), add = TRUE)
  options(width = width)

  if (is.data.frame(x)) {
    print.data.frame(x, right = FALSE, ...)
  } else {
    print(x, ...)
  }
  invisible(x)
}


#' View the head of a data.frame
#'
#' @param x A data.frame.
#' @param n Integer. Number of rows to show. Default: 10.
#' @param width Integer. Print width in characters. Default: 200.
#'
#' @returns Invisibly returns the first `n` rows.
#'
#' @export
rview <- function(x, n = 10L, width = 200L) {
  stopifnot(is.data.frame(x))
  head_df <- utils::head(x, n = n)
  rprint(head_df, width = width)
  cli::cli_text(cli::col_grey(
    "[Showing {nrow(head_df)} of {nrow(x)} rows x {ncol(x)} cols]"
  ))
  invisible(head_df)
}


#' Glimpse a data.frame
#'
#' Shows structure information similar to `dplyr::glimpse()` but without
#' requiring dplyr.
#'
#' @param x A data.frame.
#'
#' @returns Invisibly returns `x`.
#'
#' @export
rglimpse <- function(x) {
  stopifnot(is.data.frame(x))

  if (requireNamespace("dplyr", quietly = TRUE)) {
    dplyr::glimpse(x)
  } else {
    cli::cli_text("Rows: {.val {nrow(x)}}")
    cli::cli_text("Columns: {.val {ncol(x)}}")
    for (col_name in names(x)) {
      col <- x[[col_name]]
      type <- class(col)[1]
      preview <- paste(utils::head(col, 5), collapse = ", ")
      cli::cli_text("$ {.field {col_name}} <{type}> {preview}")
    }
  }
  invisible(x)
}


#' Print text cleanly
#'
#' A `cat()`-like function that avoids Workspace Notebook formatting issues.
#'
#' @param ... Objects to concatenate and print.
#'
#' @returns Invisibly returns `NULL`.
#'
#' @export
rcat <- function(...) {
  msg <- paste0(...)
  cat(msg, "\n")
  invisible(NULL)
}


#' Load notebook configuration and set execution context
#'
#' Reads a `notebook_config.yaml` file and applies the execution context
#' (warehouse, database, schema) to the connection. This ensures consistent
#' context across all notebook cells.
#'
#' Copy `notebook_config.yaml.template` to `notebook_config.yaml` and edit
#' with your values before running.
#'
#' @param conn An `sfr_connection` object.
#' @param config_path Character. Path to the YAML config file. Default:
#'   `"notebook_config.yaml"` in the current directory.
#'
#' @returns The updated `sfr_connection` object (invisibly). You **must**
#'   reassign: `conn <- sfr_load_notebook_config(conn)`.
#'
#' @export
sfr_load_notebook_config <- function(conn, config_path = "notebook_config.yaml") {
  validate_connection(conn)

  if (!file.exists(config_path)) {
    # Check for template
    template <- paste0(config_path, ".template")
    if (file.exists(template)) {
      cli::cli_abort(c(
        "Config file {.file {config_path}} not found.",
        "i" = "Copy the template: {.code file.copy(\"{template}\", \"{config_path}\")}",
        "i" = "Then edit {.file {config_path}} with your values."
      ))
    }
    cli::cli_abort("Config file {.file {config_path}} not found.")
  }

  # Read YAML (use a simple parser to avoid hard dependency on yaml package)
  config <- tryCatch(
    {
      if (requireNamespace("yaml", quietly = TRUE)) {
        yaml::read_yaml(config_path)
      } else {
        # Fallback: read via reticulate/Python yaml
        py_yaml <- reticulate::import("yaml", convert = TRUE)
        py_builtins <- reticulate::import_builtins()
        fh <- py_builtins$open(config_path, "r")
        on.exit(fh$close(), add = TRUE)
        py_yaml$safe_load(fh)
      }
    },
    error = function(e) {
      cli::cli_abort(c(
        "Failed to parse {.file {config_path}}.",
        "x" = conditionMessage(e)
      ))
    }
  )

  # Apply execution context
  ctx <- config$context
  if (is.null(ctx)) {
    cli::cli_warn("No {.field context} section found in {.file {config_path}}.")
    return(invisible(conn))
  }

  wh <- ctx$warehouse
  db <- ctx$database
  sc <- ctx$schema
  rl <- ctx$role

  # Set context on the session
  session <- conn$session
  if (!is.null(wh) && nchar(wh) > 0 && wh != "<YOUR_WAREHOUSE>") {
    session$sql(paste0("USE WAREHOUSE ", wh))$collect()
  }
  if (!is.null(db) && nchar(db) > 0 && db != "<YOUR_DATABASE>") {
    session$sql(paste0("USE DATABASE ", db))$collect()
  }
  if (!is.null(sc) && nchar(sc) > 0 && sc != "<YOUR_SCHEMA>") {
    session$sql(paste0("USE SCHEMA ", sc))$collect()
  }
  if (!is.null(rl) && nchar(rl) > 0 && rl != "<YOUR_ROLE>") {
    session$sql(paste0("USE ROLE ", rl))$collect()
  }

  # Refresh R-side fields
  conn <- refresh_conn_from_session(conn)

  # Store config on conn for use in notebooks (e.g., table prefix)
  conn$notebook_config <- config

  cli::cli_inform(c(
    "v" = "Notebook config loaded from {.file {config_path}}.",
    "i" = "Warehouse: {.val {conn$warehouse %||% '<not set>'}}",
    "i" = "Database:  {.val {conn$database %||% '<not set>'}}",
    "i" = "Schema:    {.val {conn$schema %||% '<not set>'}}"
  ))

  invisible(conn)
}


#' Build a fully qualified table name
#'
#' Constructs `DATABASE.SCHEMA.TABLE_NAME` from the connection context.
#' Ensures all notebook table references are fully qualified as recommended
#' by Snowflake for Workspace Notebooks.
#'
#' By default, uses the connection's current database and schema. Pass
#' `database` and/or `schema` to override -- useful when referencing tables
#' in a different schema or database without changing the session context.
#'
#' @param conn An `sfr_connection` object.
#' @param table_name Character. Unqualified table name.
#' @param database Character. Database name override. Default: connection's
#'   current database.
#' @param schema Character. Schema name override. Default: connection's
#'   current schema.
#'
#' @returns Character. Fully qualified table name.
#'
#' @examples
#' \dontrun{
#' # Uses connection's current database/schema
#' sfr_fqn(conn, "MY_TABLE")
#' # => "MY_DB.MY_SCHEMA.MY_TABLE"
#'
#' # Override schema (same database)
#' sfr_fqn(conn, "MY_TABLE", schema = "OTHER_SCHEMA")
#' # => "MY_DB.OTHER_SCHEMA.MY_TABLE"
#'
#' # Override both
#' sfr_fqn(conn, "MY_TABLE", database = "OTHER_DB", schema = "RAW")
#' # => "OTHER_DB.RAW.MY_TABLE"
#' }
#'
#' @export
sfr_fqn <- function(conn, table_name, database = NULL, schema = NULL) {
  validate_connection(conn)
  stopifnot(is.character(table_name), length(table_name) == 1)

  db <- database %||% conn$database
  sc <- schema %||% conn$schema

  if (is.null(db) || is.null(sc)) {
    cli::cli_warn(c(
      "Database or schema not set -- returning unqualified name.",
      "i" = "Set context with: {.code conn <- sfr_use(conn, database = \"...\", schema = \"...\")}"
    ))
    return(table_name)
  }

  paste(db, sc, table_name, sep = ".")
}


#' Check the snowflakeR environment
#'
#' Runs diagnostics on R, Python, and Snowflake ML dependencies.
#'
#' @returns Invisibly returns a list of check results.
#'
#' @export
sfr_check_environment <- function() {
  checks <- list()

  # R version
  checks$r_version <- paste0(R.version$major, ".", R.version$minor)
  cli::cli_inform("R version: {.val {checks$r_version}}")

  # reticulate
  checks$reticulate <- requireNamespace("reticulate", quietly = TRUE)
  if (checks$reticulate) {
    cli::cli_inform(c("v" = "{.pkg reticulate} available"))
    checks$python <- tryCatch(
      reticulate::py_config()$python,
      error = function(e) NA_character_
    )
    if (!is.na(checks$python)) {
      cli::cli_inform("Python: {.path {checks$python}}")
    } else {
      cli::cli_warn("Python not configured")
    }
  } else {
    cli::cli_warn("{.pkg reticulate} not installed")
  }

  # snowflake-ml-python
  checks$snowflake_ml <- tryCatch(
    {
      reticulate::py_module_available("snowflake.ml")
    },
    error = function(e) FALSE
  )
  if (checks$snowflake_ml) {
    cli::cli_inform(c("v" = "{.pkg snowflake-ml-python} available"))
  } else {
    cli::cli_warn(c(
      "x" = "{.pkg snowflake-ml-python} not available",
      "i" = "Install with: {.code sfr_install_python_deps()}"
    ))
  }

  # snowflakeauth
  checks$snowflakeauth <- requireNamespace("snowflakeauth", quietly = TRUE)
  if (checks$snowflakeauth) {
    cli::cli_inform(c("v" = "{.pkg snowflakeauth} available"))
  } else {
    cli::cli_inform(c(
      "i" = "{.pkg snowflakeauth} not installed (optional, for connections.toml)"
    ))
  }

  invisible(checks)
}
