# Experiment Tracking
# =============================================================================
# Python bridge: inst/python/sfr_experiment_bridge.py (snowflake.ml.experiment).

#' @keywords internal
#' @noRd
validate_sfr_experiment <- function(exp) {
  if (!inherits(exp, "sfr_experiment")) {
    cli::cli_abort(
      "{.arg exp} must be an {.cls sfr_experiment} object."
    )
  }
  validate_connection(exp$conn)
  invisible(exp)
}


# =============================================================================
# Experiment lifecycle
# =============================================================================

#' Create or open an ML experiment
#'
#' Connects experiment tracking to a Snowflake session and activates the
#' named experiment in the underlying `snowflake-ml-python` SDK.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param name Character. Experiment name.
#' @param database Character. Optional database context (reserved for future use).
#' @param schema Character. Optional schema context (reserved for future use).
#'
#' @returns An `sfr_experiment` object with fields `conn`, `name`, `database`,
#'   and `schema`.
#'
#' @export
sfr_experiment <- function(conn, name, database = NULL, schema = NULL) {
  validate_connection(conn)
  sfr_requires_ml("1.19.0", "Experiment Tracking")

  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$set_experiment(
    session = conn$session,
    name = name,
    database_name = database,
    schema_name = schema
  )

  structure(
    list(
      conn = conn,
      name = name,
      database = database %||% conn$database,
      schema = schema %||% conn$schema
    ),
    class = c("sfr_experiment", "list")
  )
}


#' @export
print.sfr_experiment <- function(x, ...) {
  cli::cli_text(
    "<{.cls sfr_experiment}> {.val {x$name}} @ {.val {x$database %||% '<session default>'}}.{.val {x$schema %||% '<session default>'}}"
  )
  invisible(x)
}


#' Start an experiment run
#'
#' @param exp An `sfr_experiment` object from `sfr_experiment()`.
#' @param name Character. Optional run name.
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_start_run <- function(exp, name = NULL) {
  validate_sfr_experiment(exp)
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$start_run(session = exp$conn$session, run_name = name)
  invisible(TRUE)
}


#' End the active experiment run
#'
#' @param exp An `sfr_experiment` object.
#' @param name Character. Optional run name (passed to the Python SDK).
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_end_run <- function(exp, name = NULL) {
  validate_sfr_experiment(exp)
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$end_run(session = exp$conn$session, run_name = name)
  invisible(TRUE)
}


#' Delete a run from the experiment
#'
#' @param exp An `sfr_experiment` object.
#' @param run_name Character. Name of the run to delete.
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_delete_run <- function(exp, run_name) {
  validate_sfr_experiment(exp)
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$delete_run(
    session = exp$conn$session,
    experiment_name = exp$name,
    run_name = run_name
  )
  invisible(TRUE)
}


#' Delete an experiment
#'
#' @param exp An `sfr_experiment` object.
#' @param name Character. Experiment name; defaults to `exp$name`.
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_delete_experiment <- function(exp, name = NULL) {
  validate_sfr_experiment(exp)
  ename <- name %||% exp$name
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$delete_experiment(session = exp$conn$session, name = ename)
  invisible(TRUE)
}


# =============================================================================
# Logging
# =============================================================================

#' Log a single parameter on the active run
#'
#' @param exp An `sfr_experiment` object.
#' @param key Character. Parameter name.
#' @param value Parameter value (scalar; converted for Python).
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_exp_log_param <- function(exp, key, value) {
  validate_sfr_experiment(exp)
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$log_param(session = exp$conn$session, key = key, value = value)
  invisible(TRUE)
}


#' Log multiple parameters on the active run
#'
#' All arguments in `...` must be named; they are passed as a single parameter
#' dictionary to the Python SDK.
#'
#' @param exp An `sfr_experiment` object.
#' @param ... Named parameter values.
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_exp_log_params <- function(exp, ...) {
  validate_sfr_experiment(exp)
  params <- rlang::list2(...)
  nms <- names(params)
  if (length(params) == 0L) {
    cli::cli_abort("{.fn sfr_exp_log_params} requires at least one named argument in {.arg ...}.")
  }
  if (is.null(nms) || any(!nzchar(nms))) {
    cli::cli_abort("All arguments in {.fn sfr_exp_log_params} must be named.")
  }
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$log_params(session = exp$conn$session, params_dict = params)
  invisible(TRUE)
}


#' Log a single metric on the active run
#'
#' @param exp An `sfr_experiment` object.
#' @param key Character. Metric name.
#' @param value Numeric metric value.
#' @param step Integer step (e.g. training step or epoch).
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_exp_log_metric <- function(exp, key, value, step = 0L) {
  validate_sfr_experiment(exp)
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$log_metric(
    session = exp$conn$session,
    key = key,
    value = value,
    step = as.integer(step)
  )
  invisible(TRUE)
}


#' Log multiple metrics on the active run
#'
#' @param exp An `sfr_experiment` object.
#' @param ... Named metric values.
#' @param step Integer step passed to the Python SDK.
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_exp_log_metrics <- function(exp, ..., step = 0L) {
  validate_sfr_experiment(exp)
  metrics <- rlang::list2(...)
  nms <- names(metrics)
  if (length(metrics) == 0L) {
    cli::cli_abort("{.fn sfr_exp_log_metrics} requires at least one named argument in {.arg ...}.")
  }
  if (is.null(nms) || any(!nzchar(nms))) {
    cli::cli_abort("All arguments in {.fn sfr_exp_log_metrics} must be named.")
  }
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$log_metrics(
    session = exp$conn$session,
    metrics_dict = metrics,
    step = as.integer(step)
  )
  invisible(TRUE)
}


#' Log a model to the active run
#'
#' The `model` object is passed through `reticulate` to the Snowflake ML
#' experiment API. Optional `signatures` and `sample_input_data` can be
#' supplied as named arguments in `...`.
#'
#' @param exp An `sfr_experiment` object.
#' @param model A model object acceptable to the Python SDK.
#' @param model_name Character. Registered name for the model within the run.
#' @param ... Optional `signatures` and `sample_input_data` (see Snowflake ML
#'   `ExperimentTracking.log_model` documentation).
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_exp_log_model <- function(exp, model, model_name, ...) {
  validate_sfr_experiment(exp)
  dots <- rlang::list2(...)
  signatures <- dots$signatures %||% NULL
  sample_input_data <- dots$sample_input_data %||% NULL
  py_sample <- if (!is.null(sample_input_data)) {
    reticulate::r_to_py(sample_input_data)
  } else {
    NULL
  }
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$log_model(
    session = exp$conn$session,
    model = model,
    model_name = model_name,
    signatures = signatures,
    sample_input_data = py_sample
  )
  invisible(TRUE)
}


#' Log a local file as a run artifact
#'
#' @param exp An `sfr_experiment` object.
#' @param local_path Character. Path to the file on disk.
#' @param artifact_path Character. Optional path prefix inside the artifact store.
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_exp_log_artifact <- function(exp, local_path, artifact_path = NULL) {
  validate_sfr_experiment(exp)
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$log_artifact(
    session = exp$conn$session,
    local_path = local_path,
    artifact_path = artifact_path
  )
  invisible(TRUE)
}


#' List artifacts for a run
#'
#' @param exp An `sfr_experiment` object.
#' @param run_name Character. Run name.
#' @param artifact_path Character. Optional prefix filter.
#'
#' @returns A value from the Python SDK (typically converted by `reticulate`).
#'
#' @export
sfr_exp_list_artifacts <- function(exp, run_name, artifact_path = NULL) {
  validate_sfr_experiment(exp)
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$list_artifacts(
    session = exp$conn$session,
    run_name = run_name,
    artifact_path = artifact_path
  )
}


#' Download artifacts for a run
#'
#' @param exp An `sfr_experiment` object.
#' @param run_name Character. Run name.
#' @param artifact_path Character. Artifact path within the run.
#' @param target_path Character. Local directory to write files into.
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_exp_download_artifact <- function(exp, run_name, artifact_path, target_path) {
  validate_sfr_experiment(exp)
  bridge <- get_bridge_module("sfr_experiment_bridge")
  bridge$download_artifacts(
    session = exp$conn$session,
    run_name = run_name,
    artifact_path = artifact_path,
    target_path = target_path
  )
  invisible(TRUE)
}


# =============================================================================
# tidymodels integration
# =============================================================================

#' Log each tuning configuration as an experiment run
#'
#' Requires the \pkg{tune} package. For each distinct configuration in
#' `tune::collect_metrics()` output, starts a run, logs hyperparameters as
#' parameters and aggregated metrics via `sfr_exp_log_metric()`.
#'
#' @param exp An `sfr_experiment` object.
#' @param tune_results A `tune_results` object (e.g. from `tune::tune_grid()`).
#' @param prefix Character prefix for generated run names.
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_experiment_from_tune <- function(exp, tune_results, prefix = "run") {
  validate_sfr_experiment(exp)
  rlang::check_installed("tune")
  if (!inherits(tune_results, "tune_results")) {
    cli::cli_abort("{.arg tune_results} must be a {.cls tune_results} object.")
  }

  cm <- tune::collect_metrics(tune_results)
  reserved <- c(".metric", ".estimator", "mean", "n", "std_err")
  param_cols <- setdiff(names(cm), reserved)

  if (length(param_cols) == 0L) {
    cli::cli_abort(
      "No hyperparameter columns found in {.fn tune::collect_metrics} output."
    )
  }

  if (".config" %in% names(cm)) {
    configs <- unique(cm$.config)
    for (cfg in configs) {
      sub <- cm[cm$.config == cfg, , drop = FALSE]
      run_name <- paste0(prefix, "_", cfg)
      sfr_start_run(exp, run_name)
      row1 <- sub[1, , drop = FALSE]
      for (nm in param_cols) {
        if (nm == ".config") {
          next
        }
        val <- row1[[nm]][1L]
        if (length(val) == 1L && !is.na(val)) {
          sfr_exp_log_param(exp, nm, val)
        }
      }
      for (i in seq_len(nrow(sub))) {
        mkey <- paste(sub$.metric[i], sub$.estimator[i], sep = "_")
        sfr_exp_log_metric(exp, mkey, sub$mean[i], step = 0L)
      }
      sfr_end_run(exp, run_name)
    }
  } else {
    key_df <- unique(cm[, param_cols, drop = FALSE])
    for (r in seq_len(nrow(key_df))) {
      run_name <- paste0(prefix, "_", r)
      mask <- rep(TRUE, nrow(cm))
      for (nm in param_cols) {
        mask <- mask & (cm[[nm]] == key_df[[nm]][r])
      }
      sub <- cm[mask, , drop = FALSE]
      sfr_start_run(exp, run_name)
      for (nm in param_cols) {
        val <- key_df[[nm]][r]
        if (length(val) == 1L && !is.na(val)) {
          sfr_exp_log_param(exp, nm, val)
        }
      }
      for (i in seq_len(nrow(sub))) {
        mkey <- paste(sub$.metric[i], sub$.estimator[i], sep = "_")
        sfr_exp_log_metric(exp, mkey, sub$mean[i], step = 0L)
      }
      sfr_end_run(exp, run_name)
    }
  }

  invisible(TRUE)
}


#' Select the best tuning result, log a run, and register the fitted model
#'
#' Fits a \pkg{workflows} workflow with `tune::select_best()` on `train_data`,
#' logs parameters and metrics to the experiment, then calls [sfr_log_model()]
#' on the connection. Requires \pkg{tune}, \pkg{workflows}, and \pkg{parsnip}.
#'
#' @param exp An `sfr_experiment` object.
#' @param tune_results A `tune_results` object.
#' @param workflow An unfitted \pkg{workflows} workflow matching `tune_results`.
#' @param model_name Character. Name for [sfr_log_model()].
#' @param metric Character. Metric passed to `tune::select_best()`.
#' @param train_data Training data for the final fit.
#' @param ... Additional arguments passed to [sfr_log_model()].
#'
#' @returns Invisibly `TRUE`.
#'
#' @export
sfr_experiment_log_best <- function(exp,
                                      tune_results,
                                      workflow,
                                      model_name,
                                      metric = "roc_auc",
                                      train_data,
                                      ...) {
  validate_sfr_experiment(exp)
  rlang::check_installed(c("tune", "workflows", "parsnip"))

  best <- tune::select_best(tune_results, metric = metric)
  finalized <- workflows::finalize_workflow(workflow, best)

  fit_wf <- utils::getS3method("fit", "workflow", envir = asNamespace("workflows"))(
    finalized,
    train_data
  )
  fitted <- workflows::extract_fit_parsnip(fit_wf)

  run_nm <- paste0("best_", metric)
  sfr_start_run(exp, run_nm)

  param_nms <- setdiff(names(best), ".config")
  for (nm in param_nms) {
    sfr_exp_log_param(exp, nm, best[[nm]][1L])
  }

  bm <- tune::show_best(tune_results, metric = metric, n = 1L)
  for (i in seq_len(nrow(bm))) {
    mkey <- paste(bm$.metric[i], bm$.estimator[i], sep = "_")
    sfr_exp_log_metric(exp, mkey, bm$mean[i], step = 0L)
  }

  sfr_end_run(exp, run_nm)

  sfr_log_model(exp$conn, fitted, model_name = model_name, ...)

  invisible(TRUE)
}
