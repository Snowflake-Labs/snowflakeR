# Many-Model Aggregator Wrappers
# =============================================================================
# R interface for registering and running many-model forecast aggregators.
#
# These functions wrap sfr_many_model_bridge.py via reticulate, providing
# an R-native API for the aggregator pattern where a SINGLE registered
# model dispatches to per-partition .rds files on a Snowflake stage.

# =============================================================================
# Model index builder
# =============================================================================

#' Build a Model Index from a Directory Table View
#'
#' Queries the MODEL_INDEX view (or any view with PARTITION_KEY and
#' RELATIVE_PATH columns) and returns a named list mapping partition keys
#' to their stage paths.
#'
#' @param conn An `sfr_connection` object.
#' @param view_fqn Fully-qualified view name, e.g.
#'   `"MR_PRICE_DB.MODELS.MODEL_INDEX"`.
#' @param run_id Optional character. Filter to a specific training run.
#' @param stage_prefix Stage path prefix prepended to RELATIVE_PATH.
#'   Defaults to `"@MR_PRICE_DB.MODELS.MODELS_STAGE/"`.
#'
#' @returns A named list: `list(SKU_001 = "@stage/path.rds", ...)`.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' index <- sfr_build_model_index(conn,
#'   view_fqn = "MR_PRICE_DB.MODELS.MODEL_INDEX",
#'   run_id = "v2_S_2000sku_4c"
#' )
#' cat("Partitions:", length(index), "\n")
#' }
#' @export
sfr_build_model_index <- function(conn,
                                  view_fqn = "MR_PRICE_DB.MODELS.MODEL_INDEX",
                                  run_id = NULL,
                                  stage_prefix = "@MR_PRICE_DB.MODELS.MODELS_STAGE/") {
  session <- .get_session(conn)
  bridge <- .get_many_model_bridge()

  result <- bridge$build_model_index_from_view(
    session = session,
    view_fqn = view_fqn,
    run_id = run_id,
    stage_prefix = stage_prefix
  )

  as.list(result)
}


# =============================================================================
# Aggregator registration
# =============================================================================

#' Register a Many-Model Forecast Aggregator
#'
#' Registers a single `CustomModel` in Snowflake Model Registry that
#' dispatches inference to per-partition `.rds` files stored on an
#' internal stage. The model supports both:
#' \itemize{
#'   \item `predict` (TABLE_FUNCTION / \code{@partitioned_api}) for
#'     `run_batch()` batch inference.
#'   \item `predict_single` (FUNCTION / \code{@inference_api}) for
#'     SPCS service deployment.
#' }
#'
#' Registration is O(1) regardless of partition count: ~13 seconds for
#' 2,000 or 10,000 partitions.
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Model name in the registry.
#' @param version_name Character. Version label.
#' @param model_index Named list from [sfr_build_model_index()]:
#'   `list(partition_key = "stage_path", ...)`.
#' @param partition_columns Character vector. Column names forming the
#'   partition key. Default `"SKU"`.
#' @param r_packages Character vector. R packages loaded at predict time.
#'   Default `"forecast"`.
#' @param horizon Integer. Forecast horizon. Default 12.
#' @param comment Character. Optional comment attached to the version.
#'
#' @returns A list with `model_name`, `version_name`, `n_partitions`,
#'   `registration_time_s`.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' reg  <- sfr_registry(conn, database = "MR_PRICE_DB", schema = "MODELS")
#' index <- sfr_build_model_index(conn, run_id = "v2_S_2000sku_4c")
#'
#' result <- sfr_log_many_model(
#'   reg,
#'   model_name = "R_SKU_FORECAST",
#'   version_name = "V_2k",
#'   model_index = index,
#'   partition_columns = "SKU",
#'   horizon = 12
#' )
#' cat("Registered in", result$registration_time_s, "seconds\n")
#' }
#' @export
sfr_log_many_model <- function(reg,
                               model_name,
                               version_name,
                               model_index,
                               partition_columns = "SKU",
                               r_packages = "forecast",
                               horizon = 12L,
                               comment = NULL) {
  info <- .resolve_reg(reg)
  bridge <- .get_many_model_bridge()

  result <- bridge$registry_log_many_model(
    session = info$session,
    model_name = model_name,
    version_name = version_name,
    model_index = model_index,
    partition_columns = as.list(partition_columns),
    r_packages = as.list(r_packages),
    horizon = as.integer(horizon),
    database_name = info$database,
    schema_name = info$schema,
    comment = comment
  )

  as.list(result)
}


# =============================================================================
# Deploy service for ad-hoc inference
# =============================================================================

#' Deploy a Many-Model Aggregator as an SPCS Service
#'
#' Creates an SPCS inference service from the registered aggregator.
#' Uses the `predict_single` method (FUNCTION type) for online inference.
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Model name in registry.
#' @param version_name Character. Version label.
#' @param service_name Character. Name for the SPCS service.
#' @param compute_pool Character. SPCS compute pool name.
#' @param min_instances Integer. Minimum replicas (default 1).
#' @param max_instances Integer. Maximum replicas (default 1).
#'
#' @returns Invisible. The service name.
#'
#' @examples
#' \dontrun{
#' sfr_deploy_many_model(
#'   reg,
#'   model_name = "R_SKU_FORECAST",
#'   version_name = "V_2k",
#'   service_name = "R_FORECAST_SVC",
#'   compute_pool = "MR_PRICE_POOL"
#' )
#' }
#' @export
sfr_deploy_many_model <- function(reg,
                                  model_name,
                                  version_name,
                                  service_name,
                                  compute_pool,
                                  min_instances = 1L,
                                  max_instances = 1L) {
  info <- .resolve_reg(reg)
  bridge <- .get_registry_bridge()

  mv <- bridge$`_get_model_version`(
    info$session, model_name, version_name,
    info$database, info$schema
  )

  mv[[3]]$create_service(
    service_name = service_name,
    service_compute_pool = compute_pool,
    min_instances = as.integer(min_instances),
    max_instances = as.integer(max_instances),
    block = TRUE
  )

  invisible(service_name)
}


# =============================================================================
# Internal helpers
# =============================================================================

.get_many_model_bridge <- function() {
  pkg_python <- system.file("python", package = "snowflakeR")
  reticulate::import_from_path("sfr_many_model_bridge", path = pkg_python)
}

.get_registry_bridge <- function() {
  pkg_python <- system.file("python", package = "snowflakeR")
  reticulate::import_from_path("sfr_registry_bridge", path = pkg_python)
}

.resolve_reg <- function(reg) {
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

.get_session <- function(conn) {
  if (inherits(conn, "sfr_connection")) {
    conn$session
  } else if (inherits(conn, "sfr_model_registry")) {
    conn$session
  } else {
    stop("`conn` must be an sfr_connection or sfr_model_registry object.")
  }
}
