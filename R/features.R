# Feature Store Wrappers
# =============================================================================
# Two-step create/register pattern mirroring the Python API,
# plus a one-step convenience wrapper.
#
# All operations go through inst/python/sfr_features_bridge.py via reticulate.
# The bridge creates FeatureStore, Entity, and FeatureView objects in Python,
# and returns results as pandas DataFrames / dicts that reticulate converts.

# =============================================================================
# Feature Store connection helper
# =============================================================================

#' Connect to the Feature Store
#'
#' Creates or connects to a Snowflake Feature Store in the specified
#' database/schema. Returns an `sfr_feature_store` object used by other
#' Feature Store functions.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param database Character. Database for the Feature Store. Defaults to the
#'   connection's current database.
#' @param schema Character. Schema for the Feature Store. Defaults to the
#'   connection's current schema.
#' @param warehouse Character. Default warehouse for Feature Store compute.
#'   Defaults to the connection's current warehouse.
#' @param create Logical. If `TRUE`, creates the Feature Store schema and tags
#'   if they don't exist. Default: `FALSE`.
#'
#' @returns An `sfr_feature_store` object.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' fs <- sfr_feature_store(conn, create = TRUE)
#' }
#'
#' @export
sfr_feature_store <- function(conn,
                              database = NULL,
                              schema = NULL,
                              warehouse = NULL,
                              create = FALSE) {
  validate_connection(conn)

  db <- database %||% conn$database
  sc <- schema %||% conn$schema
  wh <- warehouse %||% conn$warehouse

  if (is.null(db)) {
    cli::cli_abort("A {.arg database} is required for the Feature Store.")
  }
  if (is.null(sc)) {
    cli::cli_abort("A {.arg schema} is required for the Feature Store.")
  }
  if (is.null(wh)) {
    cli::cli_abort("A {.arg warehouse} is required for the Feature Store.")
  }

  creation_mode <- if (create) "CREATE_IF_NOT_EXIST" else "FAIL_IF_NOT_EXIST"

  structure(
    list(
      conn = conn,
      database = db,
      schema = sc,
      warehouse = wh,
      creation_mode = creation_mode
    ),
    class = c("sfr_feature_store", "list")
  )
}


#' @export
print.sfr_feature_store <- function(x, ...) {
  cli::cli_text(
    "<{.cls sfr_feature_store}> {.val {x$database}}.{.val {x$schema}}"
  )
  cli::cli_dl(list(
    warehouse = cli::format_inline("{.val {x$warehouse}}"),
    mode = cli::format_inline(
      "{.val {if (x$creation_mode == 'CREATE_IF_NOT_EXIST') 'create' else 'connect'}}"
    )
  ))
  invisible(x)
}


# =============================================================================
# Internal: extract common bridge args from sfr_feature_store
# =============================================================================

#' @noRd
fs_bridge_args <- function(fs) {
  list(
    database = fs$database,
    schema = fs$schema,
    warehouse = fs$warehouse,
    creation_mode = fs$creation_mode
  )
}


# =============================================================================
# Entity operations
# =============================================================================

#' Create and register an entity
#'
#' Creates an entity in the Snowflake Feature Store. Entities define the
#' join keys used to look up features.
#'
#' @param fs An `sfr_feature_store` object from [sfr_feature_store()].
#' @param name Character. Entity name.
#' @param join_keys Character vector. Column names used as join keys.
#' @param desc Character. Description.
#'
#' @returns An `sfr_entity` object.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' fs <- sfr_feature_store(conn, create = TRUE)
#' customer_entity <- sfr_create_entity(fs, "CUSTOMER", "CUSTOMER_ID")
#' }
#'
#' @export
sfr_create_entity <- function(fs, name, join_keys, desc = NULL) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  result <- bridge$register_entity(
    session = fs$conn$session,
    name = name,
    join_keys = as.list(join_keys),
    desc = desc,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse,
    creation_mode = args$creation_mode
  )

  cli::cli_inform("Entity {.val {name}} registered.")

  structure(
    list(
      name = result$name,
      join_keys = as.character(unlist(result$join_keys)),
      desc = result$desc %||% ""
    ),
    class = c("sfr_entity", "list")
  )
}


#' Get a registered entity
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Entity name.
#'
#' @returns An `sfr_entity` object.
#'
#' @export
sfr_get_entity <- function(fs, name) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  result <- bridge$get_entity(
    session = fs$conn$session,
    name = name,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  structure(
    list(
      name = result$name,
      join_keys = as.character(unlist(result$join_keys)),
      desc = result$desc %||% ""
    ),
    class = c("sfr_entity", "list")
  )
}


#' List entities in the Feature Store
#'
#' @param fs An `sfr_feature_store` object.
#'
#' @returns A data.frame listing entities.
#'
#' @export
sfr_list_entities <- function(fs) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  result <- bridge$list_entities(
    session = fs$conn$session,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  .bridge_dict_to_df(result)
}


#' Delete an entity
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Entity name to delete.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_delete_entity <- function(fs, name) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  bridge$delete_entity(
    session = fs$conn$session,
    name = name,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  cli::cli_inform("Entity {.val {name}} deleted.")
  invisible(TRUE)
}


#' Update an entity's description
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Entity name.
#' @param desc Character. New description.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_update_entity <- function(fs, name, desc) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  bridge$update_entity(
    session = fs$conn$session,
    name = name,
    desc = desc,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  cli::cli_inform("Entity {.val {name}} updated.")
  invisible(TRUE)
}


#' @export
print.sfr_entity <- function(x, ...) {
  cli::cli_text("<{.cls sfr_entity}> {.val {x$name}}")
  cli::cli_dl(list(
    join_keys = cli::format_inline("{.val {x$join_keys}}")
  ))
  invisible(x)
}


# =============================================================================
# Feature View: draft creation (local only)
# =============================================================================

#' Create a local draft Feature View
#'
#' Creates an `sfr_feature_view` object locally without registering it in
#' Snowflake. Use [sfr_register_feature_view()] to materialise it, or use
#' [sfr_create_feature_view()] for a one-step convenience.
#'
#' @param name Character. Name for the Feature View.
#' @param entities An `sfr_entity` object or list of `sfr_entity` objects.
#' @param features A `dbplyr` lazy table, SQL string, or table reference
#'   defining the feature transformation logic.
#' @param refresh_freq Character. Refresh frequency (e.g., `"1 hour"`,
#'   `"30 minutes"`). If `NULL`, creates an external (manually maintained)
#'   Feature View.
#' @param warehouse Character. Warehouse for refreshing.
#' @param timestamp_col Character. Timestamp column for point-in-time lookups.
#' @param desc Character. Description.
#'
#' @returns An `sfr_feature_view` object (local draft, not yet registered).
#'
#' @seealso [sfr_register_feature_view()], [sfr_create_feature_view()]
#'
#' @export
sfr_feature_view <- function(name,
                             entities,
                             features,
                             refresh_freq = NULL,
                             warehouse = NULL,
                             timestamp_col = NULL,
                             desc = NULL) {
  # Extract SQL from dbplyr lazy table if needed
  sql <- extract_feature_sql(features)

  # Normalise entities to list
  if (inherits(entities, "sfr_entity")) {
    entities <- list(entities)
  }

  structure(
    list(
      name = name,
      entities = entities,
      sql = sql,
      features_raw = features,
      refresh_freq = refresh_freq,
      warehouse = warehouse,
      timestamp_col = timestamp_col,
      desc = desc,
      registered = FALSE
    ),
    class = c("sfr_feature_view", "list")
  )
}


#' @export
print.sfr_feature_view <- function(x, ...) {
  status <- if (x$registered) "registered" else "draft"
  cli::cli_text("<{.cls sfr_feature_view}> {.val {x$name}} [{status}]")
  if (!is.null(x$desc) && nzchar(x$desc)) {
    cli::cli_text("  {.emph {x$desc}}")
  }
  entity_names <- vapply(x$entities, function(e) e$name, character(1))
  cli::cli_dl(list(
    entities = cli::format_inline("{.val {entity_names}}"),
    refresh = cli::format_inline("{.val {x$refresh_freq %||% 'external'}}")
  ))
  invisible(x)
}


# =============================================================================
# Feature View: registration
# =============================================================================

#' Register a draft Feature View in Snowflake
#'
#' Materialises a locally created Feature View in the Snowflake Feature Store.
#' The Feature View's entities must already be registered.
#'
#' @param fs An `sfr_feature_store` object.
#' @param feature_view An `sfr_feature_view` object from [sfr_feature_view()].
#' @param version Character. Version name (e.g., `"v1"`).
#' @param overwrite Logical. If `TRUE`, overwrites an existing version.
#'   Default: `FALSE`.
#'
#' @returns The `sfr_feature_view` object, updated with registration info.
#'
#' @seealso [sfr_feature_view()], [sfr_create_feature_view()]
#'
#' @export
sfr_register_feature_view <- function(fs, feature_view, version,
                                      overwrite = FALSE) {
  stopifnot(inherits(fs, "sfr_feature_store"))
  stopifnot(inherits(feature_view, "sfr_feature_view"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  entity_names <- vapply(
    feature_view$entities,
    function(e) e$name,
    character(1)
  )

  result <- bridge$register_feature_view(
    session = fs$conn$session,
    name = feature_view$name,
    version = version,
    sql = feature_view$sql,
    entity_names = as.list(entity_names),
    refresh_freq = feature_view$refresh_freq,
    warehouse = args$warehouse,
    timestamp_col = feature_view$timestamp_col,
    desc = feature_view$desc,
    database = args$database,
    schema = args$schema,
    overwrite = overwrite,
    creation_mode = args$creation_mode
  )

  cli::cli_inform(c(
    "v" = "Feature View {.val {feature_view$name}} version {.val {version}} registered."
  ))

  feature_view$registered <- TRUE
  feature_view$version <- version
  feature_view$status <- result$status
  feature_view
}


#' Create and register a Feature View in one step
#'
#' Convenience wrapper that calls [sfr_feature_view()] then
#' [sfr_register_feature_view()] internally.
#'
#' @inheritParams sfr_feature_view
#' @param fs An `sfr_feature_store` object.
#' @param version Character. Version name.
#' @param overwrite Logical. If `TRUE`, overwrites an existing version.
#'
#' @returns A registered `sfr_feature_view` object.
#'
#' @export
sfr_create_feature_view <- function(fs,
                                    name,
                                    version,
                                    entities,
                                    features,
                                    refresh_freq = NULL,
                                    warehouse = NULL,
                                    timestamp_col = NULL,
                                    desc = NULL,
                                    overwrite = FALSE) {
  draft <- sfr_feature_view(
    name = name,
    entities = entities,
    features = features,
    refresh_freq = refresh_freq,
    warehouse = warehouse,
    timestamp_col = timestamp_col,
    desc = desc
  )
  sfr_register_feature_view(fs, draft, version, overwrite = overwrite)
}


# =============================================================================
# Feature View: query / manage
# =============================================================================

#' List Feature Views
#'
#' @param fs An `sfr_feature_store` object.
#' @param entity_name Character. Filter by entity name.
#' @param feature_view_name Character. Filter by Feature View name.
#'
#' @returns A data.frame listing Feature Views.
#'
#' @export
sfr_list_feature_views <- function(fs,
                                   entity_name = NULL,
                                   feature_view_name = NULL) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  result <- bridge$list_feature_views(
    session = fs$conn$session,
    entity_name = entity_name,
    feature_view_name = feature_view_name,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  .bridge_dict_to_df(result)
}


#' Get a Feature View by name and version
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Feature View name.
#' @param version Character. Version to retrieve.
#'
#' @returns An `sfr_feature_view` object.
#'
#' @export
sfr_get_feature_view <- function(fs, name, version) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  result <- bridge$get_feature_view(
    session = fs$conn$session,
    name = name,
    version = version,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  structure(
    list(
      name = result$name,
      version = result$version,
      status = result$status,
      desc = result$desc %||% "",
      timestamp_col = result$timestamp_col,
      entities = list(),
      sql = NULL,
      features_raw = NULL,
      refresh_freq = NULL,
      warehouse = NULL,
      registered = TRUE,
      py_fv = result$`_py_fv`
    ),
    class = c("sfr_feature_view", "list")
  )
}


#' Show versions of a Feature View
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Feature View name.
#'
#' @returns A data.frame with version information.
#'
#' @export
sfr_show_fv_versions <- function(fs, name) {
  stopifnot(inherits(fs, "sfr_feature_store"))
  fvs <- sfr_list_feature_views(fs, feature_view_name = name)
  fvs
}


#' Delete a Feature View
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Feature View name.
#' @param version Character. Version to delete.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_delete_feature_view <- function(fs, name, version) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  bridge$delete_feature_view(
    session = fs$conn$session,
    name = name,
    version = version,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  cli::cli_inform("Feature View {.val {name}} version {.val {version}} deleted.")
  invisible(TRUE)
}


#' Read data from a Feature View
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Feature View name.
#' @param version Character. Version to read.
#'
#' @returns A data.frame with feature data.
#'
#' @export
sfr_read_feature_view <- function(fs, name, version) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  result <- bridge$read_feature_view(
    session = fs$conn$session,
    name = name,
    version = version,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  .bridge_dict_to_df(result)
}


#' Refresh a Feature View
#'
#' Manually triggers a refresh of a managed Feature View.
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Feature View name.
#' @param version Character. Version to refresh.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_refresh_feature_view <- function(fs, name, version) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  bridge$refresh_feature_view(
    session = fs$conn$session,
    name = name,
    version = version,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  cli::cli_inform("Feature View {.val {name}} version {.val {version}} refreshed.")
  invisible(TRUE)
}


#' Suspend a Feature View
#'
#' Pauses automatic refresh of a managed Feature View.
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Feature View name.
#' @param version Character. Version to suspend.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_suspend_feature_view <- function(fs, name, version) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  bridge$suspend_feature_view(
    session = fs$conn$session,
    name = name,
    version = version,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  cli::cli_inform("Feature View {.val {name}} version {.val {version}} suspended.")
  invisible(TRUE)
}


#' Resume a Feature View
#'
#' Resumes automatic refresh of a suspended Feature View.
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Feature View name.
#' @param version Character. Version to resume.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_resume_feature_view <- function(fs, name, version) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  bridge$resume_feature_view(
    session = fs$conn$session,
    name = name,
    version = version,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  cli::cli_inform("Feature View {.val {name}} version {.val {version}} resumed.")
  invisible(TRUE)
}


#' Get refresh history for a Feature View
#'
#' Returns information about past refreshes of a managed Feature View,
#' including timestamps, status, and refresh type (incremental vs full).
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Feature View name.
#' @param version Character. Version to inspect.
#' @param verbose Logical. If `TRUE`, returns more detailed history.
#'   Default: `FALSE`.
#'
#' @returns A data.frame with refresh history.
#'
#' @export
sfr_get_refresh_history <- function(fs, name, version, verbose = FALSE) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  result <- bridge$get_refresh_history(
    session = fs$conn$session,
    name = name,
    version = version,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse,
    verbose = verbose
  )

  .bridge_dict_to_df(result)
}


#' Set up a Feature Store schema
#'
#' Provisions the required schema, roles, and privileges for a Feature Store.
#' This is typically a one-time admin operation.
#'
#' @param conn An `sfr_connection` object.
#' @param database Character. Database for the Feature Store.
#' @param schema Character. Schema for the Feature Store.
#' @param warehouse Character. Default warehouse.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_setup_feature_store <- function(conn, database, schema, warehouse) {
  validate_connection(conn)

  bridge <- get_bridge_module("sfr_features_bridge")
  bridge$setup_feature_store_schema(
    session = conn$session,
    database = database,
    schema = schema,
    warehouse = warehouse
  )

  cli::cli_inform(c(
    "v" = "Feature Store set up in {.val {database}}.{.val {schema}}."
  ))
  invisible(TRUE)
}


# =============================================================================
# Training data generation
# =============================================================================

#' Generate training data from Feature Views
#'
#' Joins spine (label) data with registered Feature Views to produce a
#' training dataset. Supports point-in-time correct joins when
#' `spine_timestamp_col` is provided.
#'
#' @param fs An `sfr_feature_store` object.
#' @param spine A data.frame, dbplyr lazy table, or SQL string for the
#'   spine (label) data.
#' @param features A list of `sfr_feature_view` objects (must be registered,
#'   with `name` and `version` attributes), or a list of lists with `name`
#'   and `version` elements.
#' @param spine_timestamp_col Character. Timestamp column in the spine
#'   for point-in-time joins.
#' @param spine_label_cols Character vector. Label columns in the spine.
#' @param save_as Character. Optional table name to materialise results.
#'
#' @returns A data.frame with training data.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' fs <- sfr_feature_store(conn)
#'
#' training_data <- sfr_generate_training_data(
#'   fs,
#'   spine = "SELECT customer_id, label FROM labels_table",
#'   features = list(
#'     list(name = "CUSTOMER_FEATURES", version = "v1"),
#'     list(name = "TRANSACTION_FEATURES", version = "v1")
#'   ),
#'   spine_label_cols = "label"
#' )
#' }
#'
#' @export
sfr_generate_training_data <- function(fs,
                                       spine,
                                       features,
                                       spine_timestamp_col = NULL,
                                       spine_label_cols = NULL,
                                       save_as = NULL) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  # Convert spine to SQL
  spine_sql <- extract_feature_sql(spine)

  # Convert features to list-of-dicts format for the bridge
  fv_refs <- lapply(features, function(fv) {
    if (inherits(fv, "sfr_feature_view")) {
      list(name = fv$name, version = fv$version)
    } else if (is.list(fv) && all(c("name", "version") %in% names(fv))) {
      list(name = fv$name, version = fv$version)
    } else {
      cli::cli_abort(
        "Each feature must be a registered {.cls sfr_feature_view} or a list with {.field name} and {.field version}."
      )
    }
  })

  result <- bridge$generate_training_set(
    session = fs$conn$session,
    spine_sql = spine_sql,
    feature_view_refs = fv_refs,
    spine_timestamp_col = spine_timestamp_col,
    spine_label_cols = as.list(spine_label_cols),
    save_as = save_as,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  .bridge_dict_to_df(result)
}


#' Retrieve feature values for inference
#'
#' @param fs An `sfr_feature_store` object.
#' @param spine A data.frame, SQL string, or dbplyr lazy table with entity
#'   key values.
#' @param features A list of registered `sfr_feature_view` objects or
#'   lists with `name` and `version`.
#'
#' @returns A data.frame with feature values.
#'
#' @export
sfr_retrieve_features <- function(fs, spine, features) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  spine_sql <- extract_feature_sql(spine)

  fv_refs <- lapply(features, function(fv) {
    if (inherits(fv, "sfr_feature_view")) {
      list(name = fv$name, version = fv$version)
    } else if (is.list(fv) && all(c("name", "version") %in% names(fv))) {
      list(name = fv$name, version = fv$version)
    } else {
      cli::cli_abort(
        "Each feature must be a registered {.cls sfr_feature_view} or a list with {.field name} and {.field version}."
      )
    }
  })

  result <- bridge$retrieve_features(
    session = fs$conn$session,
    spine_sql = spine_sql,
    feature_view_refs = fv_refs,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse
  )

  .bridge_dict_to_df(result)
}


# =============================================================================
# Internal: SQL extraction from features argument
# =============================================================================

#' Extract SQL from a features argument
#'
#' Handles dbplyr lazy tables, character SQL strings, and table references.
#'
#' @param features A dbplyr lazy table, character string, or table reference.
#' @returns A character string containing SQL.
#' @noRd
extract_feature_sql <- function(features) {
  if (is.character(features)) {
    return(features)
  }

  # Try dbplyr SQL extraction
  if (requireNamespace("dbplyr", quietly = TRUE)) {
    sql <- tryCatch(
      as.character(dbplyr::sql_render(features)),
      error = function(e) {
        tryCatch(
          as.character(dbplyr::remote_query(features)),
          error = function(e2) NULL
        )
      }
    )
    if (!is.null(sql)) return(sql)
  }

  cli::cli_abort(c(
    "{.arg features} must be a SQL string, dbplyr lazy table, or table reference.",
    "i" = "Install {.pkg dbplyr} to use dplyr pipelines as features."
  ))
}
