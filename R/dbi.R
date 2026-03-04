# S3 Database Helper Functions
# =============================================================================
# Lightweight wrappers around the Python/Snowpark bridge for table operations.
# These are used internally by ML modules (registry, features, datasets, admin)
# that need to run SQL through the Snowpark session.
#
# For standard DBI-compliant database access (dbGetQuery, dbWriteTable, dbplyr,
# Connections Pane, etc.), use the RSnowflake package directly:
#
#   con <- DBI::dbConnect(RSnowflake::Snowflake(), name = "my_profile")
#
# Or obtain an RSnowflake connection from an existing sfr_connection:
#
#   dbi_con <- sfr_dbi_connection(sfr_conn)

#' Disconnect a Snowflake connection
#'
#' Closes the underlying Snowpark session. If an RSnowflake DBI connection
#' is also attached, it is disconnected too.
#'
#' @param conn An `sfr_connection` object.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_disconnect <- function(conn) {
  validate_connection(conn)

  if (!is.null(conn$dbi_con)) {
    tryCatch(
      DBI::dbDisconnect(conn$dbi_con),
      error = function(e) NULL
    )
  }

  tryCatch(
    conn$session$close(),
    error = function(e) {
      cli::cli_warn("Session close failed: {conditionMessage(e)}")
    }
  )
  cli::cli_inform("Disconnected.")
  invisible(TRUE)
}


#' List tables in the current database/schema
#'
#' @param conn An `sfr_connection` object.
#' @param database Character. Database to query. Default: connection's current.
#' @param schema Character. Schema to query. Default: connection's current.
#'
#' @returns A character vector of table names.
#'
#' @export
sfr_list_tables <- function(conn, database = NULL, schema = NULL) {
  validate_connection(conn)
  db <- database %||% conn$database
  sc <- schema %||% conn$schema

  if (is.null(db)) {
    db <- tryCatch({
      val <- as.character(conn$session$get_current_database())
      gsub('^"|"$', '', val)
    }, error = function(e) NULL)
  }
  if (is.null(sc)) {
    sc <- tryCatch({
      val <- as.character(conn$session$get_current_schema())
      gsub('^"|"$', '', val)
    }, error = function(e) NULL)
  }

  if (is.null(db) || is.null(sc)) {
    cli::cli_abort(c(
      "Both {.arg database} and {.arg schema} are required.",
      "i" = "Set them with: {.code conn <- sfr_use(conn, database = \"...\", schema = \"...\")}"
    ))
  }

  sql <- sprintf("SHOW TABLES IN %s.%s", db, sc)
  df <- sfr_query(conn, sql)

  if (nrow(df) == 0) return(character(0))

  name_col <- if ("name" %in% names(df)) "name" else names(df)[2]
  as.character(df[[name_col]])
}


#' List columns/fields of a table
#'
#' @param conn An `sfr_connection` object.
#' @param table_name Character. Name of the table.
#'
#' @returns A data.frame with column names and types.
#'
#' @export
sfr_list_fields <- function(conn, table_name) {
  validate_connection(conn)
  stopifnot(is.character(table_name), length(table_name) == 1L)

  sql <- sprintf("DESCRIBE TABLE %s", table_name)
  df <- sfr_query(conn, sql)
  df
}


#' Check if a table exists
#'
#' @param conn An `sfr_connection` object.
#' @param table_name Character. Name of the table (may be fully qualified
#'   as `DB.SCHEMA.TABLE`).
#'
#' @returns Logical.
#'
#' @export
sfr_table_exists <- function(conn, table_name) {

  validate_connection(conn)
  stopifnot(is.character(table_name), length(table_name) == 1L)

  parts <- strsplit(table_name, "\\.")[[1]]
  if (length(parts) == 3L) {
    db <- parts[1]
    sc <- parts[2]
    bare <- parts[3]
  } else if (length(parts) == 2L) {
    db <- NULL
    sc <- parts[1]
    bare <- parts[2]
  } else {
    db <- NULL
    sc <- NULL
    bare <- parts[1]
  }

  tables <- tryCatch(
    sfr_list_tables(conn, database = db, schema = sc),
    error = function(e) character(0)
  )

  toupper(bare) %in% toupper(tables)
}


#' Read an entire table into a data.frame
#'
#' @param conn An `sfr_connection` object.
#' @param table_name Character. Name of the table.
#' @param limit Integer. Maximum rows to return. Default: no limit.
#'
#' @returns A data.frame.
#'
#' @export
sfr_read_table <- function(conn, table_name, limit = NULL) {
  validate_connection(conn)
  stopifnot(is.character(table_name), length(table_name) == 1L)

  sql <- sprintf("SELECT * FROM %s", table_name)
  if (!is.null(limit)) {
    sql <- paste(sql, "LIMIT", as.integer(limit))
  }
  sfr_query(conn, sql)
}


#' Write a data.frame to a Snowflake table
#'
#' @param conn An `sfr_connection` object.
#' @param table_name Character. Name of the target table.
#' @param value A data.frame to write.
#' @param overwrite Logical. If `TRUE`, replaces the table. If `FALSE`
#'   (default), appends or creates.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_write_table <- function(conn, table_name, value, overwrite = FALSE) {
  validate_connection(conn)
  stopifnot(is.data.frame(value))

  names(value) <- toupper(names(value))

  rownames(value) <- NULL

  py_df <- reticulate::r_to_py(value)
  sp_df <- conn$session$create_dataframe(py_df)

  mode <- if (overwrite) "overwrite" else "append"
  sp_df$write$mode(mode)$save_as_table(table_name)

  cli::cli_inform("Wrote {nrow(value)} rows to {.val {table_name}}.")
  invisible(TRUE)
}
