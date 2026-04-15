# Generic R Executor via Model Registry (Strategy C)
# =============================================================================
# R interface for registering arbitrary R functions as TABLE_FUNCTION models
# in Snowflake Model Registry. Once registered, the R code can be invoked
# from SQL via:
#
#   SELECT * FROM TABLE(MY_EXECUTOR!execute(
#     SELECT ... OVER (PARTITION BY ...)
#   ))
#
# This enables SQL-native dispatch of arbitrary R workloads with automatic
# partitioning, scaling, versioning, and diagnostic return.
#
# Python bridge: sfr_r_executor_bridge.py

# =============================================================================
# Registration
# =============================================================================

#' Register a Generic R Executor in Model Registry
#'
#' Registers an R script as a `TABLE_FUNCTION` model version in Snowflake
#' Model Registry. The script must define an entry function that accepts
#' a data.frame and returns a data.frame. The function is called once per
#' partition.
#'
#' The registered model can be invoked from SQL:
#' ```sql
#' SELECT * FROM TABLE(
#'   MY_EXECUTOR!execute(
#'     SELECT SKU, PARAMS FROM WORK_TABLE
#'   ) OVER (PARTITION BY SKU)
#' )
#' ```
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Model name in the registry.
#' @param version_name Character. Version label. If `NULL`, auto-generated
#'   from timestamp.
#' @param r_script_path Character. Path to the R script on local disk.
#'   Must define the entry function.
#' @param partition_columns Character vector. Column names for partitioning
#'   (default `"SKU"`).
#' @param r_packages Character vector. R packages to load before sourcing
#'   the script (default empty).
#' @param entry_fn Character. Name of the R function to call per partition
#'   (default `"main"`).
#' @param diagnostic_mode Logical. If `TRUE`, the executor returns only
#'   diagnostic columns (status, timing, error) instead of the R function's
#'   result. Useful for long-running tasks that write results elsewhere.
#' @param input_columns Named list of column definitions. Each element is
#'   `list(name = "COL", type = "STRING")`. If `NULL`, partition columns
#'   are used as STRING inputs.
#' @param output_columns Named list of output column definitions. If `NULL`,
#'   diagnostic schema is used.
#' @param conda_deps Character vector. Additional conda dependencies.
#' @param pip_deps Character vector. Additional pip dependencies.
#' @param comment Character. Optional comment.
#'
#' @returns A list with `model_name`, `version_name`, `registration_time_s`.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#'
#' # Register a simple executor
#' result <- sfr_log_executor(
#'   conn,
#'   model_name = "SKU_OPTIMISER",
#'   r_script_path = "optimise_sku.R",
#'   partition_columns = "SKU",
#'   r_packages = c("dplyr", "nloptr"),
#'   entry_fn = "optimise",
#'   diagnostic_mode = TRUE
#' )
#'
#' # Now callable from SQL:
#' # SELECT * FROM TABLE(SKU_OPTIMISER!execute(
#' #   SELECT SKU, PARAMS FROM WORK_MANIFEST
#' # ) OVER (PARTITION BY SKU))
#' }
#' @export
sfr_log_executor <- function(reg,
                             model_name,
                             version_name = NULL,
                             r_script_path,
                             partition_columns = "SKU",
                             r_packages = character(0),
                             entry_fn = "main",
                             diagnostic_mode = FALSE,
                             input_columns = NULL,
                             output_columns = NULL,
                             conda_deps = NULL,
                             pip_deps = NULL,
                             comment = NULL) {
  info <- .resolve_executor_reg(reg)
  bridge <- .get_r_executor_bridge()

  stopifnot(file.exists(r_script_path))

  if (is.null(version_name)) {
    version_name <- format(Sys.time(), "V_%Y%m%d_%H%M%S")
  }

  # Convert R lists to Python-friendly format
  input_cols_py <- if (!is.null(input_columns)) {
    lapply(input_columns, function(x) list(name = x$name, type = x$type))
  }
  output_cols_py <- if (!is.null(output_columns)) {
    lapply(output_columns, function(x) list(name = x$name, type = x$type))
  }

  result <- bridge$registry_log_executor(
    session = info$session,
    model_name = model_name,
    version_name = version_name,
    r_script_path = normalizePath(r_script_path),
    partition_columns = as.list(partition_columns),
    r_packages = as.list(r_packages),
    entry_fn = entry_fn,
    diagnostic_mode = diagnostic_mode,
    input_columns = input_cols_py,
    output_columns = output_cols_py,
    conda_dependencies = conda_deps,
    pip_requirements = pip_deps,
    database_name = info$database,
    schema_name = info$schema,
    comment = comment
  )

  res <- as.list(result)

  cli::cli_inform(c(
    "v" = "Registered executor {.val {model_name}} version {.val {version_name}}",
    "i" = "Entry function: {.fn {entry_fn}}",
    "i" = "Diagnostic mode: {.val {diagnostic_mode}}",
    "i" = "Registration time: {.val {res$registration_time_s}}s"
  ))

  res
}


# =============================================================================
# Run executor via SQL
# =============================================================================

#' Run a Registered R Executor
#'
#' Convenience wrapper that builds and executes the SQL to call a
#' registered R executor model as a TABLE_FUNCTION.
#'
#' @param conn An `sfr_connection` object.
#' @param model_name Character. Registered model name.
#' @param input_sql Character. SQL query providing input data. Must
#'   include the partition column(s).
#' @param partition_by Character. Column name for `PARTITION BY`.
#' @param version_name Character. Version to use. If `NULL`, uses default.
#' @param function_name Character. Method name (default `"execute"`).
#'
#' @returns A data.frame with the executor output.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' results <- sfr_run_executor(
#'   conn,
#'   model_name = "SKU_OPTIMISER",
#'   input_sql = "SELECT SKU, PARAMS FROM WORK_MANIFEST",
#'   partition_by = "SKU"
#' )
#' print(results)
#' }
#' @export
sfr_run_executor <- function(conn,
                             model_name,
                             input_sql,
                             partition_by,
                             version_name = NULL,
                             function_name = "execute") {
  validate_connection(conn)

  # Build the model reference
  model_ref <- if (!is.null(version_name)) {
    sprintf("%s VERSION %s", model_name, version_name)
  } else {
    model_name
  }

  sql <- sprintf(
    "SELECT * FROM TABLE(%s!%s(\n  %s\n) OVER (PARTITION BY %s))",
    model_ref, function_name, input_sql, partition_by
  )

  cli::cli_inform(c("i" = "Executing: {.code {model_name}!{function_name}()}"))
  sfr_query(conn, sql)
}


#' Run Executor with run_batch (SPCS Compute Pool)
#'
#' Uses Model Registry's `run_batch` method to execute the registered
#' R executor on a specified compute pool with automatic scaling.
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Registered model name.
#' @param version_name Character. Version label.
#' @param input_data Data frame or SQL query string providing input data.
#' @param compute_pool Character. SPCS compute pool name.
#' @param function_name Character. Method name (default `"execute"`).
#'
#' @returns A data.frame with executor output.
#'
#' @export
sfr_run_executor_batch <- function(reg,
                                   model_name,
                                   version_name,
                                   input_data,
                                   compute_pool,
                                   function_name = "execute") {
  info <- .resolve_executor_reg(reg)

  # Use the Model Registry Python API for run_batch
  bridge <- get_bridge_module("sfr_registry_bridge")

  mv_info <- bridge$`_get_model_version`(
    info$session, model_name, version_name,
    info$database, info$schema
  )

  mv <- mv_info[[3]]

  if (is.data.frame(input_data)) {
    input_path <- tempfile(fileext = ".csv")
    utils::write.csv(input_data, input_path, row.names = FALSE)
    py_input <- reticulate::import("pandas")$read_csv(input_path)
  } else {
    py_input <- info$session$sql(as.character(input_data))
  }

  result <- mv$run_batch(
    py_input,
    function_name = function_name,
    service_compute_pool = compute_pool
  )

  if (inherits(result, "python.builtin.object")) {
    reticulate::py_to_r(result$to_pandas())
  } else {
    result
  }
}


# =============================================================================
# Version management
# =============================================================================

#' Update R Executor Code
#'
#' Registers a new version of an existing R executor with updated R code.
#' This is the recommended way to update R logic without changing the
#' model infrastructure.
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Existing model name.
#' @param r_script_path Character. Path to the updated R script.
#' @param version_name Character. New version label. If `NULL`,
#'   auto-generated.
#' @param set_default Logical. If `TRUE` (default), sets the new version
#'   as the default.
#' @param ... Additional arguments passed to [sfr_log_executor()].
#'
#' @returns A list with `model_name`, `version_name`, `registration_time_s`.
#'
#' @examples
#' \dontrun{
#' # Update the R code and set as default
#' sfr_update_executor(
#'   conn,
#'   model_name = "SKU_OPTIMISER",
#'   r_script_path = "optimise_sku_v2.R",
#'   set_default = TRUE
#' )
#' }
#' @export
sfr_update_executor <- function(reg,
                                model_name,
                                r_script_path,
                                version_name = NULL,
                                set_default = TRUE,
                                ...) {
  result <- sfr_log_executor(
    reg = reg,
    model_name = model_name,
    version_name = version_name,
    r_script_path = r_script_path,
    ...
  )

  if (set_default) {
    sfr_set_default_model_version(
      reg, model_name, result$version_name
    )
    cli::cli_inform(c(
      "v" = "Default version set to {.val {result$version_name}}"
    ))
  }

  result
}


# =============================================================================
# Internal helpers
# =============================================================================

.get_r_executor_bridge <- function() {
  pkg_python <- system.file("python", package = "snowflakeR")
  reticulate::import_from_path("sfr_r_executor_bridge", path = pkg_python)
}

.resolve_executor_reg <- function(reg) {
  if (inherits(reg, "sfr_model_registry")) {
    list(
      session = reg$session,
      database = reg$database,
      schema = reg$schema
    )
  } else if (inherits(reg, "sfr_connection")) {
    list(
      session = reg$session,
      database = NULL,
      schema = NULL
    )
  } else {
    stop("`reg` must be an sfr_model_registry or sfr_connection object.")
  }
}
