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
#' Column names are returned **as-is from Snowflake** by default (UPPER
#' case for unquoted identifiers). This matches the Python connector,
#' Snowpark, and RSnowflake DBI behaviour and ensures consistency with
#' the Model Registry / SPCS inference pipeline.
#'
#' To globally restore the legacy lowercase behaviour, set
#' `options(snowflakeR.lowercase_columns = TRUE)`.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param sql Character. SQL query string.
#' @param .keep_case Logical. If `TRUE` (default), preserve original column
#'   name casing from Snowflake. Set to `FALSE` to lowercase column names.
#'   The default respects the global option `snowflakeR.lowercase_columns`.
#'
#' @returns A data.frame with query results.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' result <- sfr_query(conn, "SELECT CURRENT_TIMESTAMP() AS now")
#' result$NOW
#' }
#'
#' @export
sfr_query <- function(conn, sql,
                      .keep_case = !getOption("snowflakeR.lowercase_columns", FALSE)) {
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


#' Retrieve a previous query's results via RESULT_SCAN
#'
#' Executes `SELECT * FROM TABLE(RESULT_SCAN('<query_id>'))` against
#' Snowflake.  The query ID can be provided explicitly, or looked up
#' from the notebook query tracker by cell execution count or
#' `dataframe_N` name.
#'
#' Requires the query tracker to be installed (automatic when using
#' `setup_notebook()` in a Workspace Notebook).
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param query_id Character. An explicit Snowflake query ID.
#'   If `NULL`, looks up the ID via `cell` or `dataframe`.
#' @param cell Integer. The IPython cell execution count (`In[N]`).
#' @param dataframe Character. The Workspace `dataframe_N` variable name.
#' @param .keep_case Logical. If `TRUE` (default), preserve original column
#'   name casing from Snowflake.
#'
#' @returns A data.frame with the query results.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#'
#' # Explicit query ID
#' df <- sfr_result_scan(conn, query_id = "01c34f1a-0814-fa4a-0000-0c09644ace8a")
#'
#' # By dataframe name (from SQL cell)
#' df <- sfr_result_scan(conn, dataframe = "dataframe_4")
#'
#' # By cell number
#' df <- sfr_result_scan(conn, cell = 3)
#'
#' # Most recent user query
#' df <- sfr_result_scan(conn)
#' }
#'
#' @export
sfr_result_scan <- function(conn,
                            query_id = NULL,
                            cell = NULL,
                            dataframe = NULL,
                            .keep_case = !getOption("snowflakeR.lowercase_columns", FALSE)) {
  validate_connection(conn)

  if (is.null(query_id)) {
    # Look up from the Python query tracker
    qt <- tryCatch(
      reticulate::import("query_tracker"),
      error = function(e) {
        tryCatch(
          reticulate::import("sfnb_multilang.helpers.query_tracker"),
          error = function(e2) NULL
        )
      }
    )

    if (is.null(qt)) {
      cli::cli_abort(c(
        "Query tracker not available.",
        "i" = "Provide an explicit {.arg query_id}, or ensure {.fn setup_notebook} was called."
      ))
    }

    query_id <- if (!is.null(dataframe)) {
      qt$nb_query_id(dataframe = dataframe)
    } else if (!is.null(cell)) {
      qt$nb_query_id(cell = as.integer(cell))
    } else {
      qt$nb_last_query_id()
    }

    if (is.null(query_id)) {
      cli::cli_abort(c(
        "No query ID found.",
        "i" = "Run a SQL or Python cell first, then call {.fn sfr_result_scan}."
      ))
    }

    query_id <- as.character(query_id)
  }

  stopifnot(is.character(query_id), length(query_id) == 1L, nzchar(query_id))

  sql <- sprintf("SELECT * FROM TABLE(RESULT_SCAN('%s'))", query_id)
  sfr_query(conn, sql, .keep_case = .keep_case)
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
