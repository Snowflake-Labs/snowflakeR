# Feature Store Online Serving Extensions
# =============================================================================
# Extends Feature Store with online serving, feature view updates, and
# store-type aware read/refresh/history.

#' Create an Online Config
#'
#' Configuration object for enabling online feature serving on a Feature View.
#' When passed to [sfr_create_feature_view()] or [sfr_update_feature_view()],
#' creates an Online Feature Table backed by a hybrid table.
#'
#' @param enable Logical. Whether to enable online serving. Default: `TRUE`.
#' @param target_lag Character. Target lag for the online table
#'   (e.g., `"1 minute"`, `"5 minutes"`). The data in the online table will
#'   be at most this far behind the offline source.
#'
#' @returns An `sfr_online_config` object.
#'
#' @examples
#' \dontrun{
#' online <- sfr_online_config(target_lag = "1 minute")
#' }
#'
#' @export
sfr_online_config <- function(enable = TRUE, target_lag = "1 minute") {
  sfr_requires_ml("1.18.0", "Online feature serving")
  stopifnot(is.logical(enable), length(enable) == 1L)
  stopifnot(is.character(target_lag), length(target_lag) == 1L)

  structure(
    list(enable = enable, target_lag = target_lag),
    class = c("sfr_online_config", "list")
  )
}


#' @export
print.sfr_online_config <- function(x, ...) {
  status <- if (x$enable) "enabled" else "disabled"
  cli::cli_text(
    "<{.cls sfr_online_config}> [{status}] target_lag={.val {x$target_lag}}"
  )
  invisible(x)
}


#' Update a Feature View
#'
#' Updates properties of an existing registered Feature View.
#'
#' @param fs An `sfr_feature_store` object.
#' @param name Character. Feature View name.
#' @param version Character. Version to update.
#' @param refresh_freq Character. New refresh frequency (optional).
#' @param warehouse Character. New warehouse (optional).
#' @param desc Character. New description (optional).
#' @param online_config An [sfr_online_config()] object (optional).
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_update_feature_view <- function(fs, name, version,
                                    refresh_freq = NULL,
                                    warehouse = NULL,
                                    desc = NULL,
                                    online_config = NULL) {
  stopifnot(inherits(fs, "sfr_feature_store"))

  online_config_dict <- if (!is.null(online_config)) {
    stopifnot(inherits(online_config, "sfr_online_config"))
    list(enable = online_config$enable, target_lag = online_config$target_lag)
  }

  bridge <- get_bridge_module("sfr_features_bridge")
  args <- fs_bridge_args(fs)

  update_args <- list()
  if (!is.null(refresh_freq)) update_args$refresh_freq <- refresh_freq
  if (!is.null(warehouse)) update_args$warehouse <- warehouse
  if (!is.null(desc)) update_args$desc <- desc

  bridge$update_feature_view(
    session = fs$conn$session,
    name = name,
    version = version,
    update_args = update_args,
    online_config_dict = online_config_dict,
    database = args$database,
    schema = args$schema,
    warehouse = args$warehouse,
    creation_mode = args$creation_mode
  )

  cli::cli_inform(
    "Feature View {.val {name}} version {.val {version}} updated."
  )
  invisible(TRUE)
}
