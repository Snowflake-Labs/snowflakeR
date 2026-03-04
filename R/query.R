# SQL Execution
# =============================================================================

#' Execute a SQL query and return results
#'
#' Runs a SQL query against Snowflake and returns the result as an R
#' data.frame. When an `RSnowflake` DBI connection is available on the
#' `sfr_connection` (see [sfr_dbi_connection()]), the query is executed
#' via the pure-R REST API path. Otherwise, it falls back to the Python
#' Snowpark bridge.
#'
#' Column names are lowercased by default for R friendliness. Use
#' `.keep_case = TRUE` to preserve original Snowflake casing.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param sql Character. SQL query string.
#' @param .keep_case Logical. If `TRUE`, preserve original column name
#'   casing from Snowflake. Default: `FALSE` (lowercase).
#'
#' @returns A data.frame with query results.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' result <- sfr_query(conn, "SELECT CURRENT_TIMESTAMP() AS now")
#' }
#'
#' @export
sfr_query <- function(conn, sql, .keep_case = FALSE) {
  validate_connection(conn)
  stopifnot(is.character(sql), length(sql) == 1L)

  if (!is.null(conn$dbi_con)) {
    df <- DBI::dbGetQuery(conn$dbi_con, sql)
    if (!.keep_case) names(df) <- tolower(names(df))
    return(df)
  }

  bridge <- get_bridge_module("sfr_connect_bridge")
  result <- bridge$query_to_dict(conn$session, sql)

  .bridge_dict_to_df(result, lowercase = !.keep_case)
}


#' Execute a SQL statement for side effects
#'
#' Runs DDL, DML, or other SQL that doesn't return a result set.
#' When an `RSnowflake` DBI connection is available, the statement
#' is executed via the pure-R REST API path. Otherwise falls back to
#' the Python Snowpark bridge.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param sql Character. SQL statement string.
#'
#' @returns Invisibly returns `TRUE` on success.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' sfr_execute(conn, "CREATE TABLE test_table (id INT, name STRING)")
#' }
#'
#' @export
sfr_execute <- function(conn, sql) {
  validate_connection(conn)
  stopifnot(is.character(sql), length(sql) == 1L)

  if (!is.null(conn$dbi_con)) {
    DBI::dbExecute(conn$dbi_con, sql)
    return(invisible(TRUE))
  }

  conn$session$sql(sql)$collect()
  invisible(TRUE)
}
