# Model monitoring (ML Observability)
# =============================================================================
# SQL table functions use Snowflake names MODEL_MONITOR_DRIFT_METRIC,
# MODEL_MONITOR_PERFORMANCE_METRIC, and MODEL_MONITOR_STAT_METRIC.

#' @keywords internal
#' @noRd
.validate_registry_connection <- function(reg) {
  if (inherits(reg, "sfr_model_registry")) {
    validate_connection(reg$conn)
  } else {
    validate_connection(reg)
  }
}


#' @keywords internal
#' @noRd
.sfr_sql_string_literal <- function(x) {
  paste0("'", gsub("'", "''", as.character(x)), "'")
}


#' @keywords internal
#' @noRd
.sfr_monitor_timestamp_sql <- function(t) {
  if (inherits(t, "POSIXct") || inherits(t, "POSIXt")) {
    paste0(
      "TO_TIMESTAMP_TZ(",
      .sfr_sql_string_literal(strftime(t, "%Y-%m-%d %H:%M:%S", tz = "UTC")),
      ")"
    )
  } else if (inherits(t, "Date")) {
    paste0("TO_TIMESTAMP_TZ(", .sfr_sql_string_literal(format(t)), ")")
  } else {
    as.character(t)
  }
}


#' @keywords internal
#' @noRd
.sfr_monitor_granularity_sql <- function(granularity) {
  if (is.null(granularity)) {
    return("NULL")
  }
  g <- trimws(as.character(granularity))
  ug <- toupper(g)
  if (ug %in% c("NULL")) {
    return("NULL")
  }
  if (ug == "ALL") {
    return("'ALL'")
  }
  if (grepl("^'", g)) {
    return(g)
  }
  if (grepl("^[0-9]+[[:space:]]+", g, ignore.case = TRUE)) {
    return(.sfr_sql_string_literal(g))
  }
  paste0("'1 ", ug, "'")
}


#' @keywords internal
#' @noRd
.sfr_monitor_segment_json_sql <- function(segment) {
  if (is.null(segment)) {
    return(NULL)
  }
  col <- segment$column %||% segment[["column"]]
  val <- segment$value %||% segment[["value"]]
  if (is.null(col) || is.null(val)) {
    cli::cli_abort(
      "{.arg segment} must include {.field column} and {.field value}."
    )
  }
  j <- jsonlite::toJSON(
    list(SEGMENTS = list(list(column = col, value = as.character(val)))),
    auto_unbox = TRUE
  )
  .sfr_sql_string_literal(as.character(j))
}


#' @keywords internal
#' @noRd
.df_as_tibble <- function(df) {
  class(df) <- c("tbl_df", "tbl", "data.frame")
  df
}


#' @keywords internal
#' @noRd
.monitor_source_to_py_list <- function(x) {
  out <- list(
    source = x$source,
    timestamp_column = x$timestamp_column,
    prediction_score_columns = as.list(x$prediction_score_columns)
  )
  if (!is.null(x$actual_score_columns)) {
    out$actual_score_columns <- as.list(x$actual_score_columns)
  }
  out$id_columns <- if (!is.null(x$id_columns)) {
    as.list(x$id_columns)
  } else {
    list()
  }
  out
}


#' @keywords internal
#' @noRd
.monitor_config_to_py_list <- function(x) {
  list(
    model_name = x$model_name,
    version_name = x$version_name,
    function_name = x$function_name %||% "predict",
    warehouse = x$warehouse,
    aggregation_window = x$aggregation_window
  )
}


# =============================================================================
# S3: source and monitor config
# =============================================================================

#' Model monitor source configuration
#'
#' Describes the table (or view) and columns used as the monitor data source.
#' Pass the result to `sfr_add_monitor()` as `source_config`.
#'
#' @param source Character. Table or view name (optionally fully qualified).
#' @param timestamp_column Character. Timestamp column in the source.
#' @param prediction_score_columns Character vector of prediction score columns.
#' @param actual_score_columns Optional character vector of actual score columns.
#' @param id_columns Optional character vector of ID columns (defaults to empty
#'   when passed to the Python registry).
#'
#' @returns An object of class `sfr_monitor_source`.
#'
#' @examples
#' \dontrun{
#' src <- sfr_monitor_source(
#'   "MY_DB.MY_SCH.INFERENCE_LOG",
#'   "EVENT_TIME",
#'   prediction_score_columns = "PREDICTION"
#' )
#' }
#'
#' @export
sfr_monitor_source <- function(source,
                               timestamp_column,
                               prediction_score_columns,
                               actual_score_columns = NULL,
                               id_columns = NULL) {
  structure(
    list(
      source = as.character(source),
      timestamp_column = as.character(timestamp_column),
      prediction_score_columns = unname(as.character(prediction_score_columns)),
      actual_score_columns = if (!is.null(actual_score_columns)) {
        unname(as.character(actual_score_columns))
      } else {
        NULL
      },
      id_columns = if (!is.null(id_columns)) {
        unname(as.character(id_columns))
      } else {
        NULL
      }
    ),
    class = c("sfr_monitor_source", "list")
  )
}


#' @export
print.sfr_monitor_source <- function(x, ...) {
  cli::cli_text("<{.cls sfr_monitor_source}>")
  cli::cli_dl(list(
    source = cli::format_inline("{.val {x$source}}"),
    timestamp_column = cli::format_inline("{.val {x$timestamp_column}}"),
    prediction_score_columns = cli::format_inline(
      "{.val {x$prediction_score_columns}}"
    )
  ))
  invisible(x)
}


#' Model monitor registry configuration
#'
#' Specifies the model version and warehouse used when registering a monitor
#' via `sfr_add_monitor()`.
#'
#' @param model_name Character. Registered model name.
#' @param version_name Character. Model version name.
#' @param function_name Character. Monitored prediction function name. Default:
#'   `"predict"`.
#' @param warehouse Character. Warehouse for background monitor compute.
#' @param aggregation_window Character. Aggregation window, e.g. `"1 day"`.
#'
#' @returns An object of class `sfr_monitor_config`.
#'
#' @examples
#' \dontrun{
#' cfg <- sfr_monitor_config("MY_MODEL", "v1", warehouse = "ML_WH")
#' }
#'
#' @export
sfr_monitor_config <- function(model_name,
                               version_name,
                               function_name = "predict",
                               warehouse,
                               aggregation_window = "1 day") {
  structure(
    list(
      model_name = as.character(model_name),
      version_name = as.character(version_name),
      function_name = as.character(function_name),
      warehouse = as.character(warehouse),
      aggregation_window = as.character(aggregation_window)
    ),
    class = c("sfr_monitor_config", "list")
  )
}


#' @export
print.sfr_monitor_config <- function(x, ...) {
  cli::cli_text("<{.cls sfr_monitor_config}>")
  cli::cli_dl(list(
    model = cli::format_inline("{.val {x$model_name}}"),
    version = cli::format_inline("{.val {x$version_name}}"),
    warehouse = cli::format_inline("{.val {x$warehouse}}")
  ))
  invisible(x)
}


# =============================================================================
# Registry CRUD (Python bridge)
# =============================================================================

#' Add a model monitor
#'
#' Registers a model monitor in the Snowflake Model Registry using the Python
#' SDK. Requires `snowflake-ml-python` \eqn{\geq}{>=} 1.7.1.
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param monitor_name Character. Name for the monitor.
#' @param source_config An object from `sfr_monitor_source()`.
#' @param monitor_config An object from `sfr_monitor_config()`.
#'
#' @returns Invisibly `TRUE` on success.
#'
#' @examples
#' \dontrun{
#' reg <- sfr_model_registry(conn, "ML_DB", "MODELS")
#' src <- sfr_monitor_source("ML_DB.MY_SCH.LOG", "TS", "SCORE")
#' cfg <- sfr_monitor_config("M", "v1", warehouse = "ML_WH")
#' sfr_add_monitor(reg, "MON1", src, cfg)
#' }
#'
#' @export
sfr_add_monitor <- function(reg,
                            monitor_name,
                            source_config,
                            monitor_config) {
  .validate_registry_connection(reg)
  if (!inherits(source_config, "sfr_monitor_source")) {
    cli::cli_abort("{.arg source_config} must be from {.fn sfr_monitor_source}.")
  }
  if (!inherits(monitor_config, "sfr_monitor_config")) {
    cli::cli_abort("{.arg monitor_config} must be from {.fn sfr_monitor_config}.")
  }

  sfr_requires_ml("1.7.1", "Model Monitoring")

  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  bridge$registry_add_monitor(
    session = ctx$session,
    monitor_name = monitor_name,
    source_config_dict = .monitor_source_to_py_list(source_config),
    monitor_config_dict = .monitor_config_to_py_list(monitor_config),
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )

  cli::cli_inform("Model monitor {.val {monitor_name}} created.")
  invisible(TRUE)
}


#' Get a model monitor reference
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param name Character. Monitor name (if provided, `model_name` and
#'   `version_name` are ignored).
#' @param model_name Character. Model name (use with `version_name`).
#' @param version_name Character. Model version name.
#'
#' @returns A list with at least `name` and `status`.
#'
#' @examples
#' \dontrun{
#' sfr_get_monitor(reg, name = "MY_MON")
#' sfr_get_monitor(reg, model_name = "M", version_name = "v1")
#' }
#'
#' @export
sfr_get_monitor <- function(reg,
                            name = NULL,
                            model_name = NULL,
                            version_name = NULL) {
  .validate_registry_connection(reg)
  sfr_requires_ml("1.7.1", "Model Monitoring")

  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  result <- bridge$registry_get_monitor(
    session = ctx$session,
    name = name,
    model_name = model_name,
    version_name = version_name,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  as.list(result)
}


#' List model monitors in the registry
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#'
#' @returns A `data.frame` of monitors (columns depend on the Python SDK).
#'
#' @examples
#' \dontrun{
#' sfr_show_model_monitors(reg)
#' }
#'
#' @export
sfr_show_model_monitors <- function(reg) {
  .validate_registry_connection(reg)
  sfr_requires_ml("1.7.1", "Model Monitoring")

  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  result <- bridge$registry_show_monitors(
    session = ctx$session,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  .bridge_dict_to_df(result)
}


#' Delete a model monitor
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param monitor_name Character. Monitor to delete.
#'
#' @returns Invisibly `TRUE`.
#'
#' @examples
#' \dontrun{
#' sfr_delete_monitor(reg, "MY_MON")
#' }
#'
#' @export
sfr_delete_monitor <- function(reg, monitor_name) {
  .validate_registry_connection(reg)
  sfr_requires_ml("1.7.1", "Model Monitoring")

  ctx <- resolve_registry_context(reg)
  bridge <- get_bridge_module("sfr_registry_bridge")
  bridge$registry_delete_monitor(
    session = ctx$session,
    monitor_name = monitor_name,
    database_name = ctx$database_name,
    schema_name = ctx$schema_name
  )
  cli::cli_inform("Model monitor {.val {monitor_name}} deleted.")
  invisible(TRUE)
}


# =============================================================================
# SQL metric queries
# =============================================================================

#' Query drift metrics for a model monitor
#'
#' Runs `SELECT * FROM TABLE(MODEL_MONITOR_DRIFT_METRIC(...))`.
#'
#' @param conn An `sfr_connection` object.
#' @param monitor_name Character. Model monitor name.
#' @param metric Character. Drift metric (e.g. `JENSEN_SHANNON`).
#' @param column Character. Feature, prediction, or actual column.
#' @param granularity Character. Default `"DAY"` (mapped to `'1 DAY'`). See
#'   Snowflake documentation for other values.
#' @param start_time Start timestamp (Date/POSIXct or SQL expression).
#' @param end_time End timestamp (Date/POSIXct or SQL expression).
#' @param segment Optional named list with `column` and `value` for segmented
#'   queries (JSON `SEGMENTS` passed as the last argument).
#'
#' @returns A `data.frame` of results.
#'
#' @examples
#' \dontrun{
#' sfr_monitor_drift(
#'   conn, "MY_MON", "JENSEN_SHANNON", "SCORE",
#'   start_time = Sys.Date() - 30, end_time = Sys.Date()
#' )
#' }
#'
#' @export
sfr_monitor_drift <- function(conn,
                                monitor_name,
                                metric,
                                column,
                                granularity = "DAY",
                                start_time,
                                end_time,
                                segment = NULL) {
  mn <- .sfr_sql_string_literal(monitor_name)
  met <- .sfr_sql_string_literal(metric)
  col <- .sfr_sql_string_literal(column)
  gran <- .sfr_monitor_granularity_sql(granularity)
  st <- .sfr_monitor_timestamp_sql(start_time)
  en <- .sfr_monitor_timestamp_sql(end_time)
  seg <- .sfr_monitor_segment_json_sql(segment)

  inner <- paste(c(mn, met, col, gran, st, en), collapse = ", ")
  if (!is.null(seg)) {
    inner <- paste0(inner, ", ", seg)
  }
  sql <- paste0(
    "SELECT * FROM TABLE(MODEL_MONITOR_DRIFT_METRIC(",
    inner,
    "))"
  )
  sfr_query(conn, sql)
}


#' Query performance metrics for a model monitor
#'
#' Runs `SELECT * FROM TABLE(MODEL_MONITOR_PERFORMANCE_METRIC(...))`.
#'
#' @param conn An `sfr_connection` object.
#' @param monitor_name Character.
#' @param metric Character (e.g. `RMSE`, `ROC_AUC`).
#' @param granularity Character. Default `"DAY"`.
#' @param start_time,end_time Timestamp bounds (Date/POSIXct or SQL expression).
#' @param segment Optional list with `column` and `value`.
#'
#' @returns A `data.frame`.
#'
#' @examples
#' \dontrun{
#' sfr_monitor_performance(
#'   conn, "MY_MON", "RMSE",
#'   start_time = Sys.Date() - 7, end_time = Sys.Date()
#' )
#' }
#'
#' @export
sfr_monitor_performance <- function(conn,
                                    monitor_name,
                                    metric,
                                    granularity = "DAY",
                                    start_time,
                                    end_time,
                                    segment = NULL) {
  mn <- .sfr_sql_string_literal(monitor_name)
  met <- .sfr_sql_string_literal(metric)
  gran <- .sfr_monitor_granularity_sql(granularity)
  st <- .sfr_monitor_timestamp_sql(start_time)
  en <- .sfr_monitor_timestamp_sql(end_time)
  seg <- .sfr_monitor_segment_json_sql(segment)

  inner <- paste(c(mn, met, gran, st, en), collapse = ", ")
  if (!is.null(seg)) {
    inner <- paste0(inner, ", ", seg)
  }
  sql <- paste0(
    "SELECT * FROM TABLE(MODEL_MONITOR_PERFORMANCE_METRIC(",
    inner,
    "))"
  )
  sfr_query(conn, sql)
}


#' Query count / stat metrics for a model monitor
#'
#' Runs `SELECT * FROM TABLE(MODEL_MONITOR_STAT_METRIC(...))` (Snowflake
#' function name `MODEL_MONITOR_STAT_METRIC`).
#'
#' @param conn An `sfr_connection` object.
#' @param monitor_name Character.
#' @param metric Character. `COUNT` or `COUNT_NULL`.
#' @param column Character. Column to measure.
#' @param granularity Character. Default `"DAY"`.
#' @param start_time,end_time Timestamp bounds.
#'
#' @returns A `data.frame`.
#'
#' @examples
#' \dontrun{
#' sfr_monitor_stats(
#'   conn, "MY_MON", "COUNT", "SCORE",
#'   start_time = Sys.Date() - 7, end_time = Sys.Date()
#' )
#' }
#'
#' @export
sfr_monitor_stats <- function(conn,
                              monitor_name,
                              metric,
                              column,
                              granularity = "DAY",
                              start_time,
                              end_time) {
  mn <- .sfr_sql_string_literal(monitor_name)
  met <- .sfr_sql_string_literal(metric)
  col <- .sfr_sql_string_literal(column)
  gran <- .sfr_monitor_granularity_sql(granularity)
  st <- .sfr_monitor_timestamp_sql(start_time)
  en <- .sfr_monitor_timestamp_sql(end_time)
  sql <- paste0(
    "SELECT * FROM TABLE(MODEL_MONITOR_STAT_METRIC(",
    paste(c(mn, met, col, gran, st, en), collapse = ", "),
    "))"
  )
  sfr_query(conn, sql)
}


# =============================================================================
# SQL lifecycle
# =============================================================================

#' Suspend a model monitor
#'
#' @param conn An `sfr_connection` object.
#' @param monitor_name Character.
#'
#' @returns Invisibly `TRUE`.
#'
#' @examples
#' \dontrun{
#' sfr_suspend_monitor(conn, "MY_MON")
#' }
#'
#' @export
sfr_suspend_monitor <- function(conn, monitor_name) {
  sql <- paste0("ALTER MODEL MONITOR ", monitor_name, " SUSPEND")
  sfr_execute(conn, sql)
  invisible(TRUE)
}


#' Resume a model monitor
#'
#' @inheritParams sfr_suspend_monitor
#'
#' @returns Invisibly `TRUE`.
#'
#' @examples
#' \dontrun{
#' sfr_resume_monitor(conn, "MY_MON")
#' }
#'
#' @export
sfr_resume_monitor <- function(conn, monitor_name) {
  sql <- paste0("ALTER MODEL MONITOR ", monitor_name, " RESUME")
  sfr_execute(conn, sql)
  invisible(TRUE)
}


#' Describe a model monitor
#'
#' @param conn An `sfr_connection` object.
#' @param monitor_name Character.
#'
#' @returns A `data.frame` from `DESCRIBE MODEL MONITOR`.
#'
#' @examples
#' \dontrun{
#' sfr_describe_monitor(conn, "MY_MON")
#' }
#'
#' @export
sfr_describe_monitor <- function(conn, monitor_name) {
  sql <- paste0("DESCRIBE MODEL MONITOR ", monitor_name)
  sfr_query(conn, sql)
}


# =============================================================================
# Segment management (SQL)
# =============================================================================

#' Add a segment column to a model monitor
#'
#' @param conn An `sfr_connection` object.
#' @param monitor_name Character.
#' @param column Character. Segment column name (string column in source data).
#'
#' @returns Invisibly `TRUE`.
#'
#' @examples
#' \dontrun{
#' sfr_add_monitor_segment(conn, "MY_MON", "REGION")
#' }
#'
#' @export
sfr_add_monitor_segment <- function(conn, monitor_name, column) {
  sql <- paste0(
    "ALTER MODEL MONITOR ",
    monitor_name,
    " ADD segment_column = ",
    .sfr_sql_string_literal(column)
  )
  sfr_execute(conn, sql)
  invisible(TRUE)
}


#' Drop a segment column from a model monitor
#'
#' @inheritParams sfr_add_monitor_segment
#'
#' @returns Invisibly `TRUE`.
#'
#' @examples
#' \dontrun{
#' sfr_drop_monitor_segment(conn, "MY_MON", "REGION")
#' }
#'
#' @export
sfr_drop_monitor_segment <- function(conn, monitor_name, column) {
  sql <- paste0(
    "ALTER MODEL MONITOR ",
    monitor_name,
    " DROP segment_column = ",
    .sfr_sql_string_literal(column)
  )
  sfr_execute(conn, sql)
  invisible(TRUE)
}


# =============================================================================
# Vetiver-style metric frame
# =============================================================================

#' Fetch monitor metrics in a vetiver-like tibble
#'
#' Queries drift, performance, or stat metrics and returns a small tibble-class
#' data frame with columns `.index`, `.metric`, and `.estimate` for plotting
#' or tidymodels workflows.
#'
#' @param conn An `sfr_connection` object.
#' @param monitor_name Character.
#' @param metric Character. Metric name for the chosen `metric_type`.
#' @param metric_type One of `"drift"`, `"performance"`, or `"stats"`.
#' @param start_time,end_time Passed through to the underlying query function.
#' @param column Required for `drift` and `stats`; ignored for `performance`.
#' @param granularity Passed through.
#'
#' @returns A `data.frame` with classes `tbl_df`, `tbl`, `data.frame`.
#'
#' @examples
#' \dontrun{
#' sfr_monitor_to_vetiver(
#'   conn, "MY_MON", "RMSE", metric_type = "performance",
#'   start_time = Sys.Date() - 7, end_time = Sys.Date()
#' )
#' }
#'
#' @export
sfr_monitor_to_vetiver <- function(conn,
                                   monitor_name,
                                   metric,
                                   metric_type = "drift",
                                   start_time,
                                   end_time,
                                   column = NULL,
                                   granularity = "DAY") {
  mt <- tolower(metric_type)
  df <- switch(
    mt,
    drift = {
      if (is.null(column)) {
        cli::cli_abort("{.arg column} is required when {.arg metric_type} is {.val drift}.")
      }
      sfr_monitor_drift(
        conn, monitor_name, metric, column,
        granularity = granularity,
        start_time = start_time,
        end_time = end_time
      )
    },
    performance = sfr_monitor_performance(
      conn, monitor_name, metric,
      granularity = granularity,
      start_time = start_time,
      end_time = end_time
    ),
    stats = {
      if (is.null(column)) {
        cli::cli_abort("{.arg column} is required when {.arg metric_type} is {.val stats}.")
      }
      sfr_monitor_stats(
        conn, monitor_name, metric, column,
        granularity = granularity,
        start_time = start_time,
        end_time = end_time
      )
    },
    cli::cli_abort(
      "{.arg metric_type} must be one of {.val drift}, {.val performance}, or {.val stats}."
    )
  )

  nms <- tolower(names(df))
  names(df) <- nms

  est <- df[["metric_value"]]
  idx <- df[["event_timestamp"]]
  mname <- if ("metric_name" %in% nms) {
    df[["metric_name"]]
  } else {
    rep(metric, length.out = nrow(df))
  }

  out <- data.frame(
    .index = idx,
    .metric = mname,
    .estimate = est,
    stringsAsFactors = FALSE
  )
  .df_as_tibble(out)
}


#' Store vetiver metrics as model registry metrics
#'
#' Takes output from `vetiver_compute_metrics()` (vetiver package) and stores
#' each metric as a model version metric via [sfr_set_model_metric()].
#'
#' @param reg An `sfr_model_registry` or `sfr_connection` object.
#' @param model_name Character. Model name.
#' @param version_name Character. Version name.
#' @param vetiver_metrics A data.frame from `vetiver_compute_metrics()` with
#'   columns `.index`, `.metric`, `.estimator`, `.estimate`.
#' @param prefix Character. Prefix for metric names to distinguish vetiver
#'   metrics from other metrics. Default: `"vetiver_"`.
#'
#' @returns Invisibly returns the number of metrics stored.
#'
#' @export
sfr_vetiver_to_metrics <- function(reg, model_name, version_name,
                                    vetiver_metrics, prefix = "vetiver_") {
  required_cols <- c(".index", ".metric", ".estimate")
  missing <- setdiff(required_cols, names(vetiver_metrics))
  if (length(missing) > 0) {
    cli::cli_abort(c(
      "{.arg vetiver_metrics} is missing required columns: {.val {missing}}.",
      "i" = "Expected output from {.fn vetiver::vetiver_compute_metrics}."
    ))
  }

  n <- 0L
  for (i in seq_len(nrow(vetiver_metrics))) {
    row <- vetiver_metrics[i, ]
    metric_name <- paste0(prefix, row$.metric, "_", format(row$.index, "%Y%m%d"))
    sfr_set_model_metric(
      reg, model_name, version_name,
      metric_name = metric_name,
      metric_value = row$.estimate
    )
    n <- n + 1L
  }

  cli::cli_inform(
    "Stored {n} vetiver metric{?s} on {.val {model_name}}/{.val {version_name}}."
  )
  invisible(n)
}
