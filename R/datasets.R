# Snowflake Datasets
# =============================================================================
# Wraps snowflake.ml.dataset for versioned, immutable ML datasets.
# Datasets capture query results as stage files for reproducible training.


# =============================================================================
# Dataset CRUD
# =============================================================================

#' Create a new Snowflake Dataset
#'
#' Creates an empty dataset in the current database/schema. Add versions
#' with [sfr_create_dataset_version()].
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Dataset name.
#' @param exist_ok Logical. If `FALSE` (default), errors if the dataset
#'   already exists.
#'
#' @returns An `sfr_dataset` object.
#'
#' @export
sfr_create_dataset <- function(conn, name, exist_ok = FALSE) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_datasets_bridge")
  result <- bridge$create_dataset(
    session  = conn$session,
    name     = name,
    exist_ok = exist_ok
  )

  cli::cli_inform("Dataset {.val {name}} created.")
  structure(
    list(
      name = result$name,
      fully_qualified_name = result$fully_qualified_name
    ),
    class = c("sfr_dataset", "list")
  )
}


#' Load an existing Snowflake Dataset
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Dataset name.
#'
#' @returns An `sfr_dataset` object.
#'
#' @export
sfr_get_dataset <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_datasets_bridge")
  result <- bridge$load_dataset(
    session = conn$session,
    name    = name
  )

  structure(
    list(
      name = result$name,
      fully_qualified_name = result$fully_qualified_name
    ),
    class = c("sfr_dataset", "list")
  )
}


#' List datasets in the current database/schema
#'
#' @param conn An `sfr_connection` object.
#'
#' @returns A data.frame with dataset information.
#'
#' @export
sfr_list_datasets <- function(conn) {
  validate_connection(conn)

  bridge <- get_bridge_module("sfr_datasets_bridge")
  result <- bridge$list_datasets(session = conn$session)
  .bridge_dict_to_df(result)
}


#' Delete a dataset and all its versions
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Dataset name.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_delete_dataset <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_datasets_bridge")
  bridge$delete_dataset(session = conn$session, name = name)

  cli::cli_inform("Dataset {.val {name}} deleted.")
  invisible(TRUE)
}


#' @export
print.sfr_dataset <- function(x, ...) {
  cli::cli_text("<{.cls sfr_dataset}> {.val {x$name}}")
  if (!is.null(x$fully_qualified_name)) {
    cli::cli_dl(list("fqn" = x$fully_qualified_name))
  }
  invisible(x)
}


# =============================================================================
# Dataset version operations
# =============================================================================

#' Create a dataset version from a SQL query
#'
#' Materialises the result of a SQL query as an immutable dataset version.
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Dataset name (must already exist).
#' @param version Character. Version name.
#' @param input_sql Character. SQL query whose results become the version data.
#' @param shuffle Logical. Globally shuffle the data. Default: `FALSE`.
#' @param exclude_cols Character vector. Columns to exclude during
#'   training/testing (e.g., timestamps).
#' @param label_cols Character vector. Columns containing labels.
#' @param partition_by Character. SQL expression for partitioning.
#' @param comment Character. Description of this version.
#'
#' @returns An `sfr_dataset` object with the new version info.
#'
#' @export
sfr_create_dataset_version <- function(conn, name, version, input_sql,
                                       shuffle = FALSE,
                                       exclude_cols = NULL,
                                       label_cols = NULL,
                                       partition_by = NULL,
                                       comment = NULL) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)
  stopifnot(is.character(version), length(version) == 1)
  stopifnot(is.character(input_sql), length(input_sql) == 1)

  bridge <- get_bridge_module("sfr_datasets_bridge")
  result <- bridge$create_dataset_version(
    session      = conn$session,
    name         = name,
    version      = version,
    input_sql    = input_sql,
    shuffle      = shuffle,
    exclude_cols = if (!is.null(exclude_cols)) as.list(exclude_cols) else NULL,
    label_cols   = if (!is.null(label_cols)) as.list(label_cols) else NULL,
    partition_by = partition_by,
    comment      = comment
  )

  cli::cli_inform("Dataset {.val {name}} version {.val {version}} created.")
  structure(
    list(
      name = result$name,
      version = result$version,
      fully_qualified_name = result$fully_qualified_name
    ),
    class = c("sfr_dataset", "list")
  )
}


#' List versions of a dataset
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Dataset name.
#' @param detailed Logical. If `TRUE`, returns a data.frame with full
#'   metadata. Default: `FALSE` (returns character vector of version names).
#'
#' @returns A character vector (default) or data.frame (if `detailed = TRUE`).
#'
#' @export
sfr_list_dataset_versions <- function(conn, name, detailed = FALSE) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_datasets_bridge")
  result <- bridge$list_dataset_versions(
    session  = conn$session,
    name     = name,
    detailed = detailed
  )

  if (detailed) {
    .bridge_dict_to_df(result)
  } else {
    as.character(unlist(result))
  }
}


#' Delete a dataset version
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Dataset name.
#' @param version Character. Version to delete.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_delete_dataset_version <- function(conn, name, version) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)
  stopifnot(is.character(version), length(version) == 1)

  bridge <- get_bridge_module("sfr_datasets_bridge")
  bridge$delete_dataset_version(
    session = conn$session,
    name    = name,
    version = version
  )

  cli::cli_inform("Dataset {.val {name}} version {.val {version}} deleted.")
  invisible(TRUE)
}


#' Read a dataset version into an R data.frame
#'
#' Reads the materialised stage files of a dataset version and returns
#' them as a data.frame.
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Dataset name.
#' @param version Character. Version to read.
#'
#' @returns A data.frame.
#'
#' @export
sfr_read_dataset <- function(conn, name, version) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)
  stopifnot(is.character(version), length(version) == 1)

  bridge <- get_bridge_module("sfr_datasets_bridge")
  result <- bridge$read_dataset(
    session = conn$session,
    name    = name,
    version = version
  )

  .bridge_dict_to_df(result)
}
