# Administrative Utilities
# =============================================================================
# Helpers for EAI, compute pools, image repos, and other Snowflake objects
# that ML users frequently need to manage.


# =============================================================================
# External Access Integrations (EAI)
# =============================================================================

#' Create an External Access Integration
#'
#' EAIs are required for SPCS containers that need outbound network access
#' (e.g., downloading R packages from CRAN during model inference).
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. EAI name.
#' @param allowed_network_rules Character vector. Network rule names to allow.
#' @param allowed_auth_integrations Character vector. API authentication
#'   integration names. Default: `NULL`.
#' @param enabled Logical. Whether the EAI is enabled. Default: `TRUE`.
#' @param comment Character. Description.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_create_eai <- function(conn, name, allowed_network_rules,
                           allowed_auth_integrations = NULL,
                           enabled = TRUE, comment = NULL) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)
  stopifnot(is.character(allowed_network_rules), length(allowed_network_rules) >= 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  bridge$create_eai(
    session = conn$session,
    name = name,
    allowed_network_rules = as.list(allowed_network_rules),
    allowed_api_authentication_integrations = if (!is.null(allowed_auth_integrations)) as.list(allowed_auth_integrations) else NULL,
    enabled = enabled,
    comment = comment
  )

  cli::cli_inform("External Access Integration {.val {name}} created.")
  invisible(TRUE)
}


#' List External Access Integrations
#'
#' @param conn An `sfr_connection` object.
#'
#' @returns A data.frame.
#'
#' @export
sfr_list_eais <- function(conn) {
  validate_connection(conn)
  bridge <- get_bridge_module("sfr_admin_bridge")
  result <- bridge$list_eais(session = conn$session)
  .bridge_dict_to_df(result)
}


#' Describe an External Access Integration
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. EAI name.
#'
#' @returns A data.frame with integration properties.
#'
#' @export
sfr_describe_eai <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  result <- bridge$describe_eai(session = conn$session, name = name)
  .bridge_dict_to_df(result)
}


#' Delete an External Access Integration
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. EAI name.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_delete_eai <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  bridge$delete_eai(session = conn$session, name = name)

  cli::cli_inform("External Access Integration {.val {name}} deleted.")
  invisible(TRUE)
}


# =============================================================================
# Compute Pools
# =============================================================================

#' Create a compute pool
#'
#' Compute pools provide GPU/CPU resources for SPCS services (model inference).
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Compute pool name.
#' @param instance_family Character. Instance type (e.g.,
#'   `"CPU_X64_XS"`, `"GPU_NV_S"`).
#' @param min_nodes Integer. Minimum node count. Default: `1`.
#' @param max_nodes Integer. Maximum node count. Default: `1`.
#' @param auto_resume Logical. Auto-resume on query. Default: `TRUE`.
#' @param auto_suspend_secs Integer. Seconds of inactivity before
#'   auto-suspend. Default: `3600`.
#' @param comment Character. Description.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_create_compute_pool <- function(conn, name, instance_family,
                                    min_nodes = 1L, max_nodes = 1L,
                                    auto_resume = TRUE,
                                    auto_suspend_secs = 3600L,
                                    comment = NULL) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)
  stopifnot(is.character(instance_family), length(instance_family) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  bridge$create_compute_pool(
    session = conn$session,
    name = name,
    instance_family = instance_family,
    min_nodes = as.integer(min_nodes),
    max_nodes = as.integer(max_nodes),
    auto_resume = auto_resume,
    auto_suspend_secs = as.integer(auto_suspend_secs),
    comment = comment
  )

  cli::cli_inform("Compute pool {.val {name}} created.")
  invisible(TRUE)
}


#' List compute pools
#'
#' @param conn An `sfr_connection` object.
#'
#' @returns A data.frame.
#'
#' @export
sfr_list_compute_pools <- function(conn) {
  validate_connection(conn)
  bridge <- get_bridge_module("sfr_admin_bridge")
  result <- bridge$list_compute_pools(session = conn$session)
  .bridge_dict_to_df(result)
}


#' Describe a compute pool
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Compute pool name.
#'
#' @returns A data.frame with pool properties.
#'
#' @export
sfr_describe_compute_pool <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  result <- bridge$describe_compute_pool(session = conn$session, name = name)
  .bridge_dict_to_df(result)
}


#' Delete a compute pool
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Compute pool name.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_delete_compute_pool <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  bridge$delete_compute_pool(session = conn$session, name = name)

  cli::cli_inform("Compute pool {.val {name}} deleted.")
  invisible(TRUE)
}


#' Suspend a compute pool
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Compute pool name.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_suspend_compute_pool <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  bridge$suspend_compute_pool(session = conn$session, name = name)

  cli::cli_inform("Compute pool {.val {name}} suspended.")
  invisible(TRUE)
}


#' Resume a compute pool
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Compute pool name.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_resume_compute_pool <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  bridge$resume_compute_pool(session = conn$session, name = name)

  cli::cli_inform("Compute pool {.val {name}} resumed.")
  invisible(TRUE)
}


# =============================================================================
# Image Repositories
# =============================================================================

#' Create an image repository
#'
#' Image repositories store container images for SPCS services.
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Image repository name.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_create_image_repo <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  bridge$create_image_repo(session = conn$session, name = name)

  cli::cli_inform("Image repository {.val {name}} created.")
  invisible(TRUE)
}


#' List image repositories
#'
#' @param conn An `sfr_connection` object.
#'
#' @returns A data.frame.
#'
#' @export
sfr_list_image_repos <- function(conn) {
  validate_connection(conn)
  bridge <- get_bridge_module("sfr_admin_bridge")
  result <- bridge$list_image_repos(session = conn$session)
  .bridge_dict_to_df(result)
}


#' Describe an image repository
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Image repository name.
#'
#' @returns A data.frame with repository properties.
#'
#' @export
sfr_describe_image_repo <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  result <- bridge$describe_image_repo(session = conn$session, name = name)
  .bridge_dict_to_df(result)
}


#' Delete an image repository
#'
#' @param conn An `sfr_connection` object.
#' @param name Character. Image repository name.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_delete_image_repo <- function(conn, name) {
  validate_connection(conn)
  stopifnot(is.character(name), length(name) == 1)

  bridge <- get_bridge_module("sfr_admin_bridge")
  bridge$delete_image_repo(session = conn$session, name = name)

  cli::cli_inform("Image repository {.val {name}} deleted.")
  invisible(TRUE)
}
