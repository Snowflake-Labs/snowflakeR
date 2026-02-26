# SQL Execution & dbplyr Integration
# =============================================================================

#' Execute a SQL query and return results
#'
#' Runs a SQL query against Snowflake and returns the result as an R
#' data.frame. The entire pandas/numpy conversion is done on the Python
#' side (via JSON round-trip) to avoid NumPy 1.x/2.x ABI issues that
#' can cause reticulate to fail when converting numpy-backed DataFrames.
#'
#' Column names are lowercased by default for R friendliness. Use
#' `.keep_case = TRUE` to preserve original Snowflake casing (needed
#' for DBI/dbplyr compatibility).
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

  # Run query and convert entirely on Python side (avoids numpy ABI issues)
  bridge <- get_bridge_module("sfr_connect_bridge")
  result <- bridge$query_to_dict(conn$session, sql)

  .bridge_dict_to_df(result, lowercase = !.keep_case)
}


#' Execute a SQL statement for side effects
#'
#' Runs DDL, DML, or other SQL that doesn't return a result set.
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

  conn$session$sql(sql)$collect()
  invisible(TRUE)
}
