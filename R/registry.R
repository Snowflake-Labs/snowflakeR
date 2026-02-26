# Model Registry Wrappers
# =============================================================================
# Adapted from r_notebook/snowflake_registry.R with sfr_* naming convention.
#
# All functions accept either:
#   - An sfr_model_registry object (explicit db/schema targeting), OR
#   - An sfr_connection object (uses session's current db/schema)
# as the first argument (the `reg` parameter).

# =============================================================================
# Registry context object
# =============================================================================

#' Connect to the Model Registry
#'
#' Creates an `sfr_model_registry` object that targets a specific
#' database/schema for model operations. If you want the registry to use
#' the session's current database/schema, pass an `sfr_connection` directly
#' to registry functions instead.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param database Character. Database for the Model Registry. Defaults to the
#'   connection's current database.
#' @param schema Character. Schema for the Model Registry. Defaults to the
#'   connection's current schema.
#'
#' @returns An `sfr_model_registry` object.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#'
#' # Option A: Explicit registry with target db/schema
#' reg <- sfr_model_registry(conn, database = "ML_DB", schema = "MODELS")
#' sfr_log_model(reg, model, "MY_MODEL", ...)
#'
#' # Option B: Use connection directly (session's current db/schema)
#' sfr_log_model(conn, model, "MY_MODEL", ...)
#' }
#'
#' @export
sfr_model_registry <- function(conn,
                               database = NULL,
                               schema = NULL) {
  validate_connection(conn)

  structure(
    list(
      conn = conn,
      database = database %||% conn$database,
      schema = schema %||% conn$schema
    ),
    class = c("sfr_model_registry", "list")
  )
}


#' @export
print.sfr_model_registry <- function(x, ...) {
  cli::cli_text(
    "<{.cls sfr_model_registry}> {.val {x$database %||% '<session default>'}}.{.val {x$schema %||% '<session default>'}}"
  )
  invisible(x)
}


# =============================================================================
# Internal: resolve registry context from either object type
# =============================================================================

#' Resolve session, database, schema from reg argument
#'
#' Accepts sfr_model_registry or sfr_connection and returns a list with
#' session, database_name, schema_name that can be passed to the Python bridge.
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @returns A list with `session`, `database_name`, `schema_name`.
#' @noRd
resolve_registry_context <- function(reg) {
  if (inherits(reg, "sfr_model_registry")) {
    list(
      session = reg$conn$session,
      database_name = reg$database,
      schema_name = reg$schema
    )
  } else if (inherits(reg, "sfr_connection")) {
    list(
      session = reg$session,
      database_name = NULL,
      schema_name = NULL
    )
  } else {
    cli::cli_abort(
      "{.arg reg} must be an {.cls sfr_model_registry} or {.cls sfr_connection} object."
    )
  }
}


# =============================================================================
# Log model
# =============================================================================

#' Log an R model to the Snowflake Model Registry
#'
#' Saves the R model to an `.rds` file, auto-generates a Python `CustomModel`
#' wrapper (using rpy2), and registers it in the Snowflake Model Registry.
#'
#' @param reg An `sfr_model_registry` object from [sfr_model_registry()], or
#'   an `sfr_connection` object from [sfr_connect()] (uses session defaults).
#' @param model An R model object (anything that can be `saveRDS()`'d).
#' @param model_name Character. Name for the model in the registry.
#' @param version_name Character. Optional version name (auto-generated if
#'   `NULL`).
#' @param predict_fn Character. R function name for inference. Default:
#'   `"predict"`.
#' @param predict_pkgs Character vector. R packages needed at inference time.
#' @param predict_body Character. Optional custom R code for prediction
#'   (advanced). Use template variables `{{MODEL}}`, `{{INPUT}}`, `{{UID}}`,
#'   `{{N}}`.
#' @param input_cols Named list mapping input column names to types.
#'   Valid types: `"integer"`, `"double"`, `"string"`, `"boolean"`.
#' @param output_cols Named list mapping output column names to types.
#' @param conda_deps Character vector. Conda packages for the model
#'   environment. Defaults include `r-base>=4.1`, `rpy2>=3.5`, and
#'   `numpy<2.0`.
#'
#'   **Package version warning:** The SPCS inference container resolves
#'   package versions at image build time. Without `numpy<2.0`, adding
#'   `r-base` and `rpy2` causes the conda solver to pick Python 3.12 +
#'   numpy 2.x. Under numpy 2.x the inference server's JSON
#'   deserialisation produces a `numpy.recarray` instead of a
#'   `pandas.DataFrame`, causing a server-side crash (`recarray has no
#'   attribute fillna`). Pinning `numpy<2.0` also causes the solver to
#'   select Python 3.11 (the same version used by pure-Python model
#'   containers), which completely avoids the bug.
#'   If you override `conda_deps`, ensure you include `numpy<2.0`.
#' @param pip_requirements Character vector. Additional pip packages.
#' @param target_platforms Character. One of `"SNOWPARK_CONTAINER_SERVICES"`,
#'   `"WAREHOUSE"`, or both. Default: `"SNOWPARK_CONTAINER_SERVICES"`.
#'
#'   **Important:** R models require `rpy2` and `r-base` at inference time.
#'   These packages are **not available** in the Snowflake warehouse Anaconda
#'   channel, so `"WAREHOUSE"` inference is not currently supported for R
#'   models. Use `"SNOWPARK_CONTAINER_SERVICES"` (which installs packages in
#'   a container) or test locally with [sfr_predict_local()].
#' @param comment Character. Description of the model.
#' @param metrics Named list. Metrics to attach to the model version.
#' @param sample_input A data.frame. Optional sample input for signature
#'   validation.
#' @param ... Additional arguments passed to the underlying Python
#'   `Registry.log_model()`.
#'
#' @returns An `sfr_model_version` object.
#'
#' @seealso [sfr_predict_local()], [sfr_predict()], [sfr_show_models()]
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' model <- lm(mpg ~ wt, data = mtcars)
#' mv <- sfr_log_model(conn, model, model_name = "MTCARS_MPG",
#'                     input_cols = list(wt = "double"),
#'                     output_cols = list(prediction = "double"))
#' }
#'
#' @export
sfr_log_model <- function(reg,
                          model,
                          model_name,
                          version_name = NULL,
                          predict_fn = "predict",
                          predict_pkgs = character(0),
                          predict_body = NULL,
                          input_cols = NULL,
                          output_cols = NULL,
                          conda_deps = NULL,
                          pip_requirements = NULL,
                          target_platforms = "SNOWPARK_CONTAINER_SERVICES",
                          comment = NULL,
                          metrics = NULL,
                          sample_input = NULL,
                          ...) {
  ctx <- resolve_registry_context(reg)

  # Save model to temp .rds file
  model_path <- tempfile(fileext = ".rds")
  saveRDS(model, model_path)

  # Convert R types to Python-friendly types
  py_predict_pkgs <- as.list(predict_pkgs)
  py_input_cols <- if (!is.null(input_cols)) as.list(input_cols) else NULL
  py_output_cols <- if (!is.null(output_cols)) as.list(output_cols) else NULL
  py_conda <- if (!is.null(conda_deps)) as.list(conda_deps) else NULL
  py_pip <- if (!is.null(pip_requirements)) as.list(pip_requirements) else NULL
  py_target <- as.list(target_platforms)
  py_metrics <- if (!is.null(metrics)) as.list(metrics) else NULL
  py_sample <- if (!is.null(sample_input)) {
    reticulate::r_to_py(sample_input)
  } else {
    NULL
  }

  bridge <- get_bridge_module("sfr_registry_bridge")
  result <- bridge$registry_log_model(
    session = ctx$session,
    model_rds_path = model_path,
    model_name = model_name,
    version_name = version_name,
    predict_function = predict_fn,
    predict_packages = py_predict_pkgs,
    predict_body = predict_body,
    input_cols = py_input_cols,
    output_cols = py_output_cols,
    conda_dependencies = py_conda,
    pip_requirements = py_pip,
    target_platforms = py_target,
    comment = comment,
    metrics = py_metrics,
    sample_input = py_sample,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )

  cli::cli_inform(c(
    "v" = "Model {.val {result$model_name}} version {.val {result$version_name}} registered."
  ))

  structure(
    list(
      model_name = result$model_name,
      version_name = result$version_name,
      py_model_version = result$model_version,
      py_registry = result$registry,
      reg = reg
    ),
    class = c("sfr_model_version", "list")
  )
}


#' @export
print.sfr_model_version <- function(x, ...) {
  cli::cli_text("<{.cls sfr_model_version}>")
  cli::cli_dl(list(
    model = cli::format_inline("{.val {x$model_name}}"),
    version = cli::format_inline("{.val {x$version_name}}")
  ))
  invisible(x)
}


# =============================================================================
# Local predict (pure R, no bridge)
# =============================================================================

#' Test an R model locally
#'
#' Calls the R predict function directly on the model, exactly as the
#' registered model would behave inside Snowflake. Use this to verify
#' predictions before registering.
#'
#' **Note:** This runs entirely in R (no Python bridge). The Python bridge
#' with rpy2 is only used when the model executes inside Snowflake (SPCS).
#' Calling reticulate -> Python -> rpy2 -> R from within R would cause a
#' dual-runtime segfault.
#'
#' @param model An R model object.
#' @param new_data A data.frame with input data.
#' @param predict_fn Character. R function name for inference. Default:
#'   `"predict"`.
#' @param predict_pkgs Character vector. R packages to load before prediction.
#' @param predict_body Character. Optional custom R code. Use template
#'   variables `{{MODEL}}`, `{{INPUT}}`, `{{UID}}`, `{{N}}` (same as used
#'   in the Python bridge).
#' @param input_cols Named list. Input column schema (for validation only).
#' @param output_cols Named list. Output column schema (for validation only).
#'
#' @returns A data.frame with predictions.
#'
#' @seealso [sfr_log_model()]
#'
#' @examples
#' \dontrun{
#' model <- lm(mpg ~ wt, data = mtcars)
#' preds <- sfr_predict_local(model, data.frame(wt = c(2.5, 3.0, 3.5)))
#' }
#'
#' @export
sfr_predict_local <- function(model,
                              new_data,
                              predict_fn = "predict",
                              predict_pkgs = character(0),
                              predict_body = NULL,
                              input_cols = NULL,
                              output_cols = NULL) {
  stopifnot(is.data.frame(new_data))

  # Load any required packages
  for (pkg in predict_pkgs) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      cli::cli_abort("Package {.pkg {pkg}} is required but not installed.")
    }
    library(pkg, character.only = TRUE)
  }

  if (!is.null(predict_body)) {
    # Execute custom R code template (same syntax as the Python bridge)
    uid <- paste0(sample(c(0:9, letters[1:6]), 8, replace = TRUE), collapse = "")
    model_name <- paste0("r_model_", uid)

    # Set up environment
    assign(model_name, model, envir = .GlobalEnv)
    assign(paste0("input_", uid), new_data, envir = .GlobalEnv)

    code <- predict_body
    code <- gsub("\\{\\{MODEL\\}\\}", model_name, code)
    code <- gsub("\\{\\{INPUT\\}\\}", paste0("input_", uid), code)
    code <- gsub("\\{\\{UID\\}\\}", uid, code)
    code <- gsub("\\{\\{N\\}\\}", as.character(nrow(new_data)), code)

    tryCatch(
      {
        eval(parse(text = code), envir = .GlobalEnv)
        result <- get(paste0("result_", uid), envir = .GlobalEnv)
      },
      finally = {
        # Clean up global env
        rm_pattern <- paste0("_", uid, "$")
        to_rm <- grep(rm_pattern, ls(envir = .GlobalEnv), value = TRUE)
        rm(list = c(to_rm, model_name), envir = .GlobalEnv)
      }
    )
    return(as.data.frame(result))
  }

  # Standard predict path
  fn <- match.fun(predict_fn)
  pred <- fn(model, newdata = new_data)

  if (is.data.frame(pred)) {
    return(pred)
  } else if (is.matrix(pred)) {
    return(as.data.frame(pred))
  } else {
    return(data.frame(prediction = as.numeric(pred)))
  }
}


# =============================================================================
# Remote predict
# =============================================================================

#' Run remote inference with a registered model
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Name of the registered model.
#' @param new_data A data.frame with input data.
#' @param version_name Character. Version to use (default: model's default).
#' @param service_name Character. SPCS service name for container inference.
#' @param ... Additional arguments.
#'
#' @returns A data.frame with predictions.
#'
#' @export
sfr_predict <- function(reg,
                        model_name,
                        new_data,
                        version_name = NULL,
                        service_name = NULL,
                        ...) {
  ctx <- resolve_registry_context(reg)

  bridge <- get_bridge_module("sfr_registry_bridge")
  py_input <- reticulate::r_to_py(new_data)

  # registry_predict writes JSON to a temp file to bypass the
  # basic_string::substr C++ crash in reticulate/rpy2 that occurs when
  # large strings cross the Python <-> R bridge (Workspace Notebooks).
  json_path <- bridge$registry_predict(
    session = ctx$session,
    model_name = model_name,
    version_name = version_name,
    input_data = py_input,
    service_name = service_name,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )

  on.exit(unlink(json_path), add = TRUE)
  result <- jsonlite::fromJSON(json_path)
  .bridge_dict_to_df(result)
}


# =============================================================================
# List / get / delete models
# =============================================================================

#' List models in the registry
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#'
#' @returns A data.frame listing registered models.
#'
#' @export
sfr_show_models <- function(reg) {
  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  result <- bridge$registry_show_models(
    session = ctx$session,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  .bridge_dict_to_df(result)
}


#' Get a model reference from the registry
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Name of the model.
#'
#' @returns An `sfr_model` object.
#'
#' @export
sfr_get_model <- function(reg, model_name) {
  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  info <- bridge$registry_get_model(
    session = ctx$session,
    model_name = model_name,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )

  structure(
    list(
      name = info$name,
      comment = info$comment,
      versions = as.character(info$versions),
      default_version = info$default_version,
      py_model = info$model,
      py_registry = info$registry,
      reg = reg
    ),
    class = c("sfr_model", "list")
  )
}


#' @export
print.sfr_model <- function(x, ...) {
  cli::cli_text("<{.cls sfr_model}> {.val {x$name}}")
  cli::cli_dl(list(
    versions = cli::format_inline("{.val {x$versions}}"),
    default = cli::format_inline("{.val {x$default_version %||% 'none'}}")
  ))
  if (!is.null(x$comment) && nzchar(x$comment)) {
    cli::cli_text("  {.emph {x$comment}}")
  }
  invisible(x)
}


#' Show versions of a model
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Name of the model.
#'
#' @returns A data.frame with version information.
#'
#' @export
sfr_show_model_versions <- function(reg, model_name) {
  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  result <- bridge$registry_show_versions(
    session = ctx$session,
    model_name = model_name,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  .bridge_dict_to_df(result)
}


#' Get a specific model version
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Name of the model.
#' @param version_name Character. Version to retrieve.
#'
#' @returns An `sfr_model_version` object.
#'
#' @export
sfr_get_model_version <- function(reg, model_name, version_name) {
  ctx <- resolve_registry_context(reg)
  model <- sfr_get_model(reg, model_name)
  py_mv <- model$py_model$version(version_name)

  structure(
    list(
      model_name = model_name,
      version_name = version_name,
      py_model_version = py_mv,
      py_registry = model$py_registry,
      reg = reg
    ),
    class = c("sfr_model_version", "list")
  )
}


#' Delete a model from the registry
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Name of the model to delete.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_delete_model <- function(reg, model_name) {
  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  bridge$registry_delete_model(
    session = ctx$session,
    model_name = model_name,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  cli::cli_inform("Model {.val {model_name}} deleted.")
  invisible(TRUE)
}


# =============================================================================
# Metrics
# =============================================================================

#' Set a metric on a model version
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character.
#' @param version_name Character.
#' @param metric_name Character. Name of the metric.
#' @param metric_value Numeric or character. Value of the metric.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_set_model_metric <- function(reg, model_name, version_name,
                                 metric_name, metric_value) {
  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  bridge$registry_set_metric(
    session = ctx$session,
    model_name = model_name,
    version_name = version_name,
    metric_name = metric_name,
    metric_value = metric_value,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  invisible(TRUE)
}


#' Show metrics for a model version
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character.
#' @param version_name Character.
#'
#' @returns A named list of metrics.
#'
#' @export
sfr_show_model_metrics <- function(reg, model_name, version_name) {
  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  result <- bridge$registry_show_metrics(
    session = ctx$session,
    model_name = model_name,
    version_name = version_name,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  as.list(result)
}


#' Set the default version of a model
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character.
#' @param version_name Character.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_set_default_model_version <- function(reg, model_name, version_name) {
  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  bridge$registry_set_default_version(
    session = ctx$session,
    model_name = model_name,
    version_name = version_name,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  cli::cli_inform(
    "Default version for {.val {model_name}} set to {.val {version_name}}."
  )
  invisible(TRUE)
}


# =============================================================================
# Deploy / undeploy
# =============================================================================

#' Deploy a model as an SPCS service
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character.
#' @param version_name Character.
#' @param service_name Character. Name for the SPCS service.
#' @param compute_pool Character. Compute pool to use.
#' @param image_repo Character. Image repository.
#' @param max_instances Integer. Max service instances. Default: 1.
#' @param force Logical. If `TRUE` and the service already exists, drop it
#'   first and redeploy. Default: `FALSE`.
#'
#' @returns Invisibly returns a list with deployment info.
#'
#' @export
sfr_deploy_model <- function(reg, model_name, version_name,
                             service_name, compute_pool, image_repo,
                             max_instances = 1L, force = FALSE) {
  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  result <- bridge$registry_create_service(
    session = ctx$session,
    model_name = model_name,
    version_name = version_name,
    service_name = service_name,
    compute_pool = compute_pool,
    image_repo = image_repo,
    max_instances = as.integer(max_instances),
    force = isTRUE(force),
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  cli::cli_inform(c(
    "v" = "Service {.val {service_name}} deployed for {.val {model_name}}/{.val {version_name}}."
  ))
  invisible(as.list(result))
}


#' Undeploy a model service
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character.
#' @param version_name Character.
#' @param service_name Character.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @export
sfr_undeploy_model <- function(reg, model_name, version_name, service_name) {
  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  bridge$registry_delete_service(
    session = ctx$session,
    model_name = model_name,
    version_name = version_name,
    service_name = service_name,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  cli::cli_inform("Service {.val {service_name}} removed.")
  invisible(TRUE)
}


# =============================================================================
# Service status helpers
# =============================================================================

# Internal: build a fully-qualified service name.
#
# Falls back to conn$database / conn$schema when the registry context
# doesn't supply them (i.e., when `reg` is a plain sfr_connection).
#
# @param ctx  List from resolve_registry_context().
# @param conn An sfr_connection.
# @param service_name Character. Unqualified service name.
# @returns Character. The (possibly qualified) service name, uppercased.
# @noRd
.resolve_service_fqn <- function(ctx, conn, service_name) {
  db <- ctx$database_name %||% conn$database
  sc <- ctx$schema_name %||% conn$schema
  svc <- toupper(service_name)

  if (!is.null(db) && !is.null(sc)) {
    paste0(toupper(db), ".", toupper(sc), ".", svc)
  } else {
    svc
  }
}


# Internal: parse the JSON returned by SYSTEM$GET_SERVICE_STATUS.
#
# Snowflake returns a JSON array of per-container status objects, e.g.:
#   [{"status":"READY","message":"Running","containerName":"model-inference",...}]
#
# We derive an overall status from the individual container statuses:
#   - Any container FAILED  → overall FAILED
#   - All containers READY  → overall READY
#   - Otherwise             → status of the first container (usually PENDING)
#
# @param json_str Character. Raw JSON string.
# @returns A list with `status` (character), `message` (character or NA),
#   and `containers` (data.frame or NULL).
# @noRd
.parse_service_status_json <- function(json_str) {
  if (is.na(json_str) || !nzchar(trimws(json_str))) {
    return(list(
      status     = "UNKNOWN",
      message    = "Empty response from SYSTEM$GET_SERVICE_STATUS",
      containers = NULL
    ))
  }

  parsed <- jsonlite::fromJSON(json_str)

  # Empty array "[]": service exists but no containers provisioned yet.
  # jsonlite::fromJSON("[]") returns list() (length 0) or a 0-row data.frame.
  is_empty <- (is.data.frame(parsed) && nrow(parsed) == 0) ||
              (is.list(parsed) && length(parsed) == 0)
  if (is_empty) {
    return(list(
      status     = "PENDING",
      message    = "No containers provisioned yet",
      containers = NULL
    ))
  }

  # Normal case: jsonlite converts the JSON array into a data.frame
  if (is.data.frame(parsed) && nrow(parsed) > 0 && "status" %in% names(parsed)) {
    statuses <- toupper(as.character(parsed$status))
    if (any(statuses == "FAILED")) {
      overall <- "FAILED"
    } else if (all(statuses %in% c("READY", "RUNNING"))) {
      overall <- "READY"
    } else {
      overall <- statuses[1]
    }
    msg <- if ("message" %in% names(parsed)) as.character(parsed$message[1]) else NA_character_
    return(list(status = overall, message = msg, containers = parsed))
  }

  # Edge case: parsed as a list of lists (e.g., single-element array)
  if (is.list(parsed) && length(parsed) > 0) {
    first <- if (is.list(parsed[[1]])) parsed[[1]] else parsed
    return(list(
      status     = toupper(as.character(first$status %||% "UNKNOWN")),
      message    = as.character(first$message %||% NA_character_),
      containers = NULL
    ))
  }

  list(
    status     = "UNKNOWN",
    message    = paste("Unexpected JSON format:", substr(json_str, 1, 200)),
    containers = NULL
  )
}


#' Get the current status of an SPCS service
#'
#' Queries `SYSTEM$GET_SERVICE_STATUS()` and returns a structured result.
#' Useful for one-off status checks without starting a polling loop.
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param service_name Character. Name of the SPCS service.
#'
#' @returns A list with:
#'   \describe{
#'     \item{status}{Character. Overall service status
#'       (e.g. `"READY"`, `"PENDING"`, `"FAILED"`).}
#'     \item{message}{Character. Human-readable status message, or `NA`.}
#'     \item{containers}{A data.frame of per-container statuses, or `NULL`
#'       if the response couldn't be parsed as a table.}
#'     \item{fqn}{Character. The fully-qualified service name used in the
#'       query.}
#'   }
#'
#' @examples
#' \dontrun{
#' st <- sfr_get_service_status(reg, "my_svc")
#' st$status   # "READY"
#' st$message  # "Running"
#' }
#'
#' @export
sfr_get_service_status <- function(reg, service_name) {
  ctx  <- resolve_registry_context(reg)
  conn <- if (inherits(reg, "sfr_model_registry")) reg$conn else reg
  stopifnot(is.character(service_name), length(service_name) == 1L)

  svc_fqn <- .resolve_service_fqn(ctx, conn, service_name)

  sql <- paste0(
    "SELECT SYSTEM$GET_SERVICE_STATUS('", svc_fqn, "') AS status_json"
  )

  # sfr_query() lowercases column names by default, so the alias
  # STATUS_JSON (Snowflake uppercases it) becomes status_json in R.
  result   <- sfr_query(conn, sql)
  json_str <- as.character(result$status_json[1])

  out <- .parse_service_status_json(json_str)
  out$fqn <- svc_fqn
  out
}


#' Wait for an SPCS service to be ready
#'
#' Polls [sfr_get_service_status()] until the service reaches `READY`/`RUNNING`
#' or a terminal failure state is detected. Useful after [sfr_deploy_model()]
#' since SPCS services take several minutes to provision.
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object. Used to
#'   resolve the database and schema where the service lives.
#' @param service_name Character. Name of the SPCS service.
#' @param timeout_min Numeric. Maximum minutes to wait. Default: 10.
#' @param poll_sec Numeric. Seconds between status checks. Default: 15.
#' @param verbose Logical. Print status updates? Default: `TRUE`.
#'
#' @returns `TRUE` (invisibly) if the service is running; raises an error on
#'   terminal failure or timeout.
#'
#' @examples
#' \dontrun{
#' sfr_deploy_model(reg, "MY_MODEL", "V1", "my_svc", "ML_POOL", "my_repo")
#' sfr_wait_for_service(reg, "my_svc", timeout_min = 15)
#' }
#'
#' @export
sfr_wait_for_service <- function(reg,
                                 service_name,
                                 timeout_min = 10,
                                 poll_sec = 15,
                                 verbose = TRUE) {
  stopifnot(is.character(service_name), length(service_name) == 1L)

  # Resolve the FQN once for display purposes
  ctx  <- resolve_registry_context(reg)
  conn <- if (inherits(reg, "sfr_model_registry")) reg$conn else reg
  svc_fqn <- .resolve_service_fqn(ctx, conn, service_name)

  deadline   <- Sys.time() + timeout_min * 60
  start_time <- Sys.time()
  poll_count <- 0L

  if (verbose) {
    cli::cli_inform(c(
      "i" = "Waiting for service {.val {svc_fqn}} (timeout: {timeout_min} min) ..."
    ))
  }

  repeat {
    poll_count <- poll_count + 1L

    # Query the service status; wrap in tryCatch so transient errors
    # (e.g., service not yet visible in metadata) don't abort the loop.
    st <- tryCatch(
      sfr_get_service_status(reg, service_name),
      error = function(e) {
        list(
          status  = "QUERY_ERROR",
          message = conditionMessage(e),
          fqn     = svc_fqn
        )
      }
    )

    elapsed_min <- round(as.numeric(difftime(
      Sys.time(), start_time, units = "mins"
    )), 1)

    if (verbose) {
      status_label <- st$status
      if (!is.null(st$message) && !is.na(st$message)) {
        status_label <- paste0(st$status, " — ", st$message)
      }
      cli::cli_inform("  [{elapsed_min} min] Status: {.val {status_label}}")
    }

    # Terminal success
    if (toupper(st$status) %in% c("READY", "RUNNING")) {
      if (verbose) {
        cli::cli_inform(c("v" = "Service {.val {svc_fqn}} is running."))
      }
      return(invisible(TRUE))
    }

    # Terminal failure
    if (toupper(st$status) %in% c("FAILED", "DELETED")) {
      detail <- if (!is.null(st$message) && !is.na(st$message)) {
        st$message
      } else {
        "Check service logs for details."
      }
      cli::cli_abort(c(
        "x" = "Service {.val {svc_fqn}} entered state {.val {st$status}}.",
        "i" = detail
      ))
    }

    # Timeout
    if (Sys.time() >= deadline) {
      cli::cli_abort(c(
        "x" = "Timeout ({timeout_min} min) waiting for service {.val {svc_fqn}}.",
        "i" = "Last status: {.val {st$status}}.",
        "i" = "Increase {.arg timeout_min} or check SPCS logs."
      ))
    }

    Sys.sleep(poll_sec)
  }
}


#' Benchmark SPCS model inference
#'
#' Runs `n` inference requests against a deployed SPCS service and reports
#' timing statistics. Use this to validate that the service is responding
#' correctly and to measure throughput.
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Registered model name.
#' @param new_data A data.frame with input data.
#' @param service_name Character. SPCS service name.
#' @param n Integer. Number of inference iterations. Default: 10.
#' @param version_name Character. Model version (default: model's default).
#' @param verbose Logical. Print per-iteration results? Default: `TRUE`.
#'
#' @returns A list (invisibly) with:
#'   - `timings`: numeric vector of per-iteration seconds
#'   - `mean_sec`: mean latency
#'   - `median_sec`: median latency
#'   - `min_sec`, `max_sec`: range
#'   - `total_sec`: total wall time
#'   - `n`: iterations completed
#'   - `last_result`: data.frame from the last prediction
#'
#' @examples
#' \dontrun{
#' bench <- sfr_benchmark_inference(
#'   reg, "MY_MODEL", test_data,
#'   service_name = "my_svc", n = 20
#' )
#' }
#'
#' @export
sfr_benchmark_inference <- function(reg,
                                    model_name,
                                    new_data,
                                    service_name,
                                    n = 10L,
                                    version_name = NULL,
                                    verbose = TRUE) {
  stopifnot(is.data.frame(new_data), is.numeric(n), n >= 1)
  n <- as.integer(n)

  if (verbose) {
    cli::cli_inform(c(
      "i" = "Running {n} inference iterations against service {.val {service_name}} ..."
    ))
  }

  timings <- numeric(n)
  last_result <- NULL

  for (i in seq_len(n)) {
    t0 <- proc.time()["elapsed"]

    result <- tryCatch(
      sfr_predict(
        reg,
        model_name = model_name,
        new_data = new_data,
        version_name = version_name,
        service_name = service_name
      ),
      error = function(e) {
        cli::cli_warn("Iteration {i}/{n} failed: {conditionMessage(e)}")
        NULL
      }
    )

    t1 <- proc.time()["elapsed"]
    timings[i] <- t1 - t0

    if (!is.null(result)) {
      last_result <- result
    }

    if (verbose) {
      status <- if (!is.null(result)) "OK" else "FAIL"
      cli::cli_inform(
        "  [{i}/{n}] {status} -- {round(timings[i], 3)}s"
      )
    }
  }

  # Compute stats (exclude failed iterations for stats)
  successful <- timings[timings > 0]
  stats <- list(
    timings    = timings,
    mean_sec   = mean(successful),
    median_sec = stats::median(successful),
    min_sec    = min(successful),
    max_sec    = max(successful),
    total_sec  = sum(timings),
    n          = n,
    last_result = last_result
  )

  if (verbose) {
    cli::cli_rule("Benchmark Results")
    cli::cli_inform(c(
      "i" = "Iterations: {n}",
      "i" = "Mean:   {round(stats$mean_sec, 3)}s",
      "i" = "Median: {round(stats$median_sec, 3)}s",
      "i" = "Min:    {round(stats$min_sec, 3)}s",
      "i" = "Max:    {round(stats$max_sec, 3)}s",
      "i" = "Total:  {round(stats$total_sec, 1)}s"
    ))
  }

  invisible(stats)
}
