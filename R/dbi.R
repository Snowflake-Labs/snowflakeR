# DBI Methods — Minimum Viable Implementation
# =============================================================================
# Implements the core DBI generics so that sfr_connection objects work with
# standard R database tooling (dbplyr, RStudio connection pane, etc.).
#
# We don't formally subclass DBI::DBIDriver/DBIConnection/DBIResult because
# DBI is only in Suggests (not Imports). Instead, we provide S4 methods that
# register when DBI is available.

# -----------------------------------------------------------------------------
# S3-based helpers that always work (no DBI dependency)
# -----------------------------------------------------------------------------

#' Disconnect a Snowflake connection
#'
#' Closes the underlying Snowpark session.
#'
#' @param conn An `sfr_connection` object.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_disconnect <- function(conn) {
  validate_connection(conn)
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

  # If still NULL, try to get from the live session
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

  # SHOW TABLES returns a 'name' column (lowercased by sfr_query)
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

  # Parse FQN: extract database, schema, and bare table name
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

  # Uppercase column names to follow Snowflake convention.
  # Snowflake stores unquoted identifiers in uppercase; pandas DataFrames
  # often have lowercase names which would be stored case-sensitively.
  names(value) <- toupper(names(value))

  # Reset row names to avoid Snowpark warning about non-standard index
  rownames(value) <- NULL

  py_df <- reticulate::r_to_py(value)
  sp_df <- conn$session$create_dataframe(py_df)

  mode <- if (overwrite) "overwrite" else "append"
  sp_df$write$mode(mode)$save_as_table(table_name)

  cli::cli_inform("Wrote {nrow(value)} rows to {.val {table_name}}.")
  invisible(TRUE)
}


# -----------------------------------------------------------------------------
# DBI generics registration (only when DBI is loaded)
# -----------------------------------------------------------------------------
# DBI uses S4 generics, so we must:
#   1. Register the S3 class with the S4 system via setOldClass()
#   2. Register S4 methods via setMethod() in .onLoad()
# This avoids a hard dependency on DBI (which is in Suggests) while still
# enabling standard R database tooling (dbplyr, etc.).

# Bridge S3 classes into S4 so DBI generics can dispatch on them.
#
# Two layers:
#   - S3 class vectors include "DBIConnection"/"DBIResult" (see connect.R
#     and dbSendQuery below) for dbplyr's S3 dispatch.
#   - setOldClass + setIs tell S4 about the inheritance so that S4 value
#     class checks (e.g. dbSendQuery must return DBIResult) pass via is().
#   - DBIConnection/DBIResult are S4 virtual classes and CANNOT appear in
#     setOldClass, so we use setIs() separately.
setOldClass(c("sfr_connection", "list"))
setIs("sfr_connection", "DBIConnection")

setOldClass(c("sfr_result", "list"))
setIs("sfr_result", "DBIResult")


# =============================================================================
# S4 method definitions — DBI is in Imports, generics are always available
# =============================================================================

# --- Core query / table methods ---

setMethod("dbGetQuery", signature(conn = "sfr_connection"),
          function(conn, statement, ...) sfr_query(conn, statement, .keep_case = TRUE))

setMethod("dbExecute", signature(conn = "sfr_connection"),
          function(conn, statement, ...) sfr_execute(conn, statement))

setMethod("dbListTables", signature(conn = "sfr_connection"),
          function(conn, ...) sfr_list_tables(conn, ...))

setMethod("dbListFields", signature(conn = "sfr_connection"),
          function(conn, name, ...) {
            fields <- sfr_list_fields(conn, name)
            as.character(fields$name)
          })

setMethod("dbExistsTable", signature(conn = "sfr_connection"),
          function(conn, name, ...) sfr_table_exists(conn, name))

setMethod("dbDisconnect", signature(conn = "sfr_connection"),
          function(conn, ...) sfr_disconnect(conn))

setMethod("dbReadTable", signature(conn = "sfr_connection"),
          function(conn, name, ...) sfr_read_table(conn, name))

setMethod("dbWriteTable", signature(conn = "sfr_connection"),
          function(conn, name, value, ..., overwrite = FALSE)
            sfr_write_table(conn, name, value, overwrite = overwrite))


# --- Connection metadata (needed by dbplyr::src_dbi) ---

setMethod("dbGetInfo", "sfr_connection", function(dbObj, ...) {
  list(
    dbname     = dbObj$database %||% "snowflake",
    db.version = "snowflake",
    username   = dbObj$user %||% "",
    host       = dbObj$account %||% "",
    port       = 443L
  )
})

setMethod("dbIsValid", "sfr_connection", function(dbObj, ...) {
  tryCatch({
    !is.null(dbObj$session) &&
      !is.null(dbObj$session$get_current_account())
  }, error = function(e) FALSE)
})


# --- Identifier & string quoting (needed by dbplyr SQL generation) ---

setMethod("dbQuoteIdentifier", signature("sfr_connection", "character"),
          function(conn, x, ...) {
            # Already quoted → pass through
            already_quoted <- grepl("^\".*\"$", x)
            DBI::SQL(ifelse(already_quoted, x,
                            paste0('"', gsub('"', '""', x), '"')))
          })

setMethod("dbQuoteIdentifier", signature("sfr_connection", "SQL"),
          function(conn, x, ...) x)

setMethod("dbQuoteString", signature("sfr_connection", "character"),
          function(conn, x, ...) {
            DBI::SQL(ifelse(is.na(x), "NULL",
                            paste0("'", gsub("'", "''", x), "'")))
          })

setMethod("dbQuoteString", signature("sfr_connection", "SQL"),
          function(conn, x, ...) x)


# --- Send query / fetch (result-set interface for dbplyr collect) ---

setMethod("dbSendQuery", signature("sfr_connection", "character"),
          function(conn, statement, ...) {
            data <- sfr_query(conn, statement, .keep_case = TRUE)
            structure(
              list(statement = statement, data = data, conn = conn),
              class = c("sfr_result", "DBIResult", "list")
            )
          })

setMethod("dbFetch", "sfr_result", function(res, n = -1, ...) {
  if (n >= 0 && n < nrow(res$data)) {
    res$data[seq_len(n), , drop = FALSE]
  } else {
    res$data
  }
})

setMethod("dbClearResult", "sfr_result", function(res, ...) {
  invisible(TRUE)
})

setMethod("dbHasCompleted", "sfr_result", function(res, ...) {
  TRUE
})

setMethod("dbIsValid", "sfr_result", function(dbObj, ...) {
  TRUE
})

setMethod("dbGetInfo", "sfr_result", function(dbObj, ...) {
  list(
    statement     = dbObj$statement,
    row.count     = nrow(dbObj$data),
    has.completed = TRUE
  )
})


# --- S3 methods for dplyr / dbplyr (registered in .onLoad) ---

#' @noRd
dbplyr_edition.sfr_connection <- function(con) 2L

#' @noRd
db_query_fields.sfr_connection <- function(con, sql, ...) {
  # Convert dbplyr table identifiers to SQL strings.
  # in_catalog/in_schema produce dbplyr_catalog/dbplyr_schema objects;
  # format() renders them with backtick-quoting, which we convert to
  # Snowflake's double-quote convention.
  sql_str <- if (inherits(sql, "dbplyr_catalog") ||
                 inherits(sql, "dbplyr_schema") ||
                 inherits(sql, "dbplyr_table_ident") ||
                 inherits(sql, "ident")) {
    gsub("`", "\"", format(sql))
  } else if (inherits(sql, "SQL") || inherits(sql, "sql")) {
    as.character(sql)
  } else {
    as.character(sql)
  }
  # Run a LIMIT 0 query to discover column names.
  # Return names as-is from Snowflake (typically uppercase) so that
  # dbplyr generates SQL with matching case for quoted identifiers.
  bridge <- get_bridge_module("sfr_connect_bridge")
  result <- bridge$query_to_dict(con$session, paste("SELECT * FROM", sql_str, "LIMIT 0"))
  result$columns
}
