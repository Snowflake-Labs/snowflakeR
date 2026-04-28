# Standalone R workflow helpers for the parallel SPCS demo.
#
# Purpose:
# - Run clean-room setup and demo orchestration outside Workspace notebooks
#   (for example, in RStudio / Positron).
# - Mirror the notebook helper flow with explicit, testable functions.
#
# Typical usage:
#   source("snowflakeR/inst/notebooks/parallel_spcs_workflow.R")
#   conn <- snowflakeR::sfr_connect()
#   cfg  <- parallel_lab_load_config("snowflakeR/inst/notebooks/snowflaker_parallel_spcs_config.yaml")
#   parallel_lab_validate_clean_room(cfg)
#   parallel_lab_setup(conn, cfg, create_series = TRUE)
#   out <- parallel_lab_run_tasks_demo(conn, cfg, run = TRUE)

`%||%` <- function(x, y) {
  if (is.null(x) || (is.character(x) && !nzchar(x))) y else x
}

.abort <- function(...) {
  stop(sprintf(...), call. = FALSE)
}

.flatten_foreach_results <- function(x, key = "unit_id") {
  out <- list()
  .walk <- function(node) {
    if (is.list(node) && !is.null(node[[key]])) {
      out[[length(out) + 1L]] <<- node
    } else if (is.list(node)) {
      for (item in node) .walk(item)
    }
  }
  .walk(x)
  out
}

parallel_lab_load_config <- function(config_path = "snowflaker_parallel_spcs_config.yaml") {
  if (!requireNamespace("yaml", quietly = TRUE)) {
    .abort("Package 'yaml' is required. Install with install.packages('yaml').")
  }
  if (!file.exists(config_path)) {
    .abort("Config file not found: %s", config_path)
  }

  raw <- yaml::read_yaml(config_path)
  if (is.null(raw) || !is.list(raw)) {
    .abort("Invalid YAML structure in: %s", config_path)
  }

  ctx <- raw$context %||% list()
  lab <- raw$parallel_lab %||% list()
  sch <- lab$schemas %||% list()

  defaults <- list(
    database = "SFLAB_EP_DEMO",
    warehouse = "",
    compute_pool = "",
    image_uri = "",
    dosnowflake_stage_name = "DOSNOWFLAKE_STAGE",
    queue_table = "DOSNOWFLAKE_QUEUE",
    create_synthetic_series_table = TRUE,
    demo_forecast_n_skus = 2000L,
    demo_tasks_chunks_per_job = 10L,
    demo_queue_n_workers = 10L,
    demo_queue_chunks_per_job = 10L,
    clean_room = TRUE,
    schemas = list(
      source_data = "SOURCE_DATA",
      config = "CONFIG",
      models = "MODELS"
    )
  )

  out <- defaults
  out$database <- lab$database %||% defaults$database
  out$warehouse <- lab$warehouse %||% ctx$warehouse %||% defaults$warehouse
  out$compute_pool <- lab$compute_pool %||% defaults$compute_pool
  out$image_uri <- lab$image_uri %||% defaults$image_uri
  out$dosnowflake_stage_name <- lab$dosnowflake_stage_name %||% defaults$dosnowflake_stage_name
  out$queue_table <- lab$queue_table %||% defaults$queue_table
  out$create_synthetic_series_table <- isTRUE(lab$create_synthetic_series_table %||% defaults$create_synthetic_series_table)
  out$demo_forecast_n_skus <- as.integer(lab$demo_forecast_n_skus %||% defaults$demo_forecast_n_skus)
  out$demo_tasks_chunks_per_job <- as.integer(lab$demo_tasks_chunks_per_job %||% defaults$demo_tasks_chunks_per_job)
  out$demo_queue_n_workers <- as.integer(lab$demo_queue_n_workers %||% defaults$demo_queue_n_workers)
  out$demo_queue_chunks_per_job <- as.integer(lab$demo_queue_chunks_per_job %||% defaults$demo_queue_chunks_per_job)
  out$instance_family <- lab$instance_family %||% "CPU_X64_S"
  out$clean_room <- if (is.null(lab$clean_room)) TRUE else isTRUE(lab$clean_room)

  out$schemas <- list(
    source_data = sch$source_data %||% defaults$schemas$source_data,
    config = sch$config %||% defaults$schemas$config,
    models = sch$models %||% defaults$schemas$models
  )
  out$config_path <- normalizePath(config_path, winslash = "/", mustWork = TRUE)
  out
}

parallel_lab_contains_forbidden_refs <- function(cfg, forbidden = character(0)) {
  vals <- c(
    cfg$database, cfg$warehouse, cfg$compute_pool, cfg$image_uri,
    cfg$dosnowflake_stage_name, cfg$queue_table,
    cfg$schemas$source_data, cfg$schemas$config, cfg$schemas$models
  )
  vals <- toupper(as.character(vals[!is.na(vals)]))
  out <- forbidden[vapply(forbidden, function(x) any(grepl(toupper(x), vals, fixed = TRUE)), logical(1))]
  unique(out)
}

parallel_lab_validate_clean_room <- function(cfg, require_runtime = TRUE) {
  if (isTRUE(cfg$clean_room %||% TRUE)) {
    hits <- parallel_lab_contains_forbidden_refs(cfg)
    if (length(hits) > 0) {
      .abort("Config contains forbidden identifiers: %s", paste(hits, collapse = ", "))
    }
  }

  if (!isTRUE(require_runtime)) return(invisible(TRUE))
  if (!nzchar(cfg$compute_pool %||% "")) .abort("parallel_lab.compute_pool is required")
  if (!nzchar(cfg$image_uri %||% "")) .abort("parallel_lab.image_uri is required")
  invisible(TRUE)
}

parallel_lab_stage_at <- function(cfg) {
  sprintf("@%s.%s.%s", cfg$database, cfg$schemas$source_data, cfg$dosnowflake_stage_name)
}

parallel_lab_queue_fqn <- function(cfg) {
  sprintf("%s.%s.%s", cfg$database, cfg$schemas$config, cfg$queue_table)
}

parallel_lab_sql_bootstrap <- function(cfg) {
  db <- cfg$database
  sch_src <- cfg$schemas$source_data
  sch_cfg <- cfg$schemas$config
  sch_models <- cfg$schemas$models
  stg <- cfg$dosnowflake_stage_name
  qtbl <- cfg$queue_table

  # CREATE DATABASE requires account-level CREATE DATABASE (fails for restricted
  # demo roles). When clean_room is FALSE, assume the database already exists
  # (e.g. provision_sc_demo.py / admin DDL).
  db_stmt <- if (isTRUE(cfg$clean_room %||% TRUE)) {
    sprintf("CREATE DATABASE IF NOT EXISTS %s", db)
  } else {
    NULL
  }

  c(
    db_stmt,
    sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", db, sch_src),
    sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", db, sch_cfg),
    sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", db, sch_models),
    sprintf("CREATE STAGE IF NOT EXISTS %s.%s.%s", db, sch_src, stg),
    paste0(
      "CREATE HYBRID TABLE IF NOT EXISTS ", db, ".", sch_cfg, ".", qtbl, " (",
      "QUEUE_ID NUMBER AUTOINCREMENT PRIMARY KEY, ",
      "JOB_ID VARCHAR NOT NULL, ",
      "CHUNK_ID VARCHAR NOT NULL, ",
      "STAGE_PATH VARCHAR NOT NULL, ",
      "STATUS VARCHAR DEFAULT 'PENDING', ",
      "WORKER_ID VARCHAR, ",
      "CLAIMED_AT TIMESTAMP_NTZ, ",
      "COMPLETED_AT TIMESTAMP_NTZ, ",
      "ERROR_MSG VARCHAR, ",
      "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
    ),
    paste0(
      "CREATE OR REPLACE TABLE ", db, ".", sch_cfg, ".JOB_MANIFEST (",
      "JOB_ID VARCHAR, JOB_TYPE VARCHAR, UNIT_ID VARCHAR, STATUS VARCHAR, ",
      "WORKER_INDEX INTEGER, STARTED_AT TIMESTAMP_NTZ, COMPLETED_AT TIMESTAMP_NTZ, ",
      "ERROR_MSG VARCHAR, UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
    ),
    paste0(
      "CREATE OR REPLACE TABLE ", db, ".", sch_models, ".TRAINING_RESULTS (",
      "JOB_ID VARCHAR, UNIT_ID VARCHAR, WORKER_INDEX INTEGER, ",
      "RMSE FLOAT, MAE FLOAT, AIC FLOAT, N_OBS INTEGER, TRAINING_SECS FLOAT, ",
      "COMPLETED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
    ),
    paste0(
      "CREATE TABLE IF NOT EXISTS ", db, ".", sch_models, ".TRAINING_METRICS (",
      "MODEL_RUN_ID VARCHAR, ",
      "UNIT_ID VARCHAR, ",
      "ARIMA_MODEL VARCHAR, ",
      "AICC FLOAT, ",
      "RMSE FLOAT, ",
      "MAPE FLOAT, ",
      "MASE FLOAT, ",
      "N_OBS INTEGER, ",
      "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
    ),
    sprintf(
      "ALTER STAGE %s.%s.%s SET DIRECTORY = (ENABLE = TRUE)",
      db, sch_src, stg
    ),
    paste0(
      "CREATE OR REPLACE VIEW ", db, ".", sch_models, ".MODEL_INDEX AS ",
      "SELECT ",
      "  SPLIT_PART(RELATIVE_PATH, '/', 2) AS RUN_ID, ",
      "  REPLACE(SPLIT_PART(RELATIVE_PATH, '/', -1), '.rds', '') AS PARTITION_KEY, ",
      "  RELATIVE_PATH ",
      "FROM DIRECTORY(@", db, ".", sch_src, ".", stg, ") ",
      "WHERE RELATIVE_PATH LIKE 'models/%/%.rds'"
    ),
    paste0(
      "CREATE TABLE IF NOT EXISTS ", db, ".", sch_models, ".FORECAST_RESULTS (",
      "RUN_ID VARCHAR, UNIT_ID VARCHAR, HORIZON INTEGER, ",
      "FORECAST_DATE DATE, POINT_FORECAST FLOAT, LO_80 FLOAT, HI_80 FLOAT, ",
      "LO_95 FLOAT, HI_95 FLOAT, STATUS VARCHAR, ",
      "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
    )
  )
}

parallel_lab_sql_seed_series_events <- function(
    cfg,
    n_units = 120L,
    n_days = 1095L,
    start_date = "2022-01-01"
) {
  n_units <- as.integer(n_units)
  n_days  <- as.integer(n_days)
  if (n_units < 1L || n_days < 1L) .abort("n_units and n_days must be >= 1")

  # Daily time-series (matches ts(frequency=7) in the worker).
  # Each SKU gets:
  #   - Per-SKU base level (50-200)
  #   - Weekly seasonality via SIN (amplitude 5-20, day-of-week cycle)
  #   - Annual seasonality via SIN (amplitude 10-40, phase-shifted per SKU)
  #   - Piecewise linear trend with 2 changepoints (slope changes at day 400, 800)
  #   - Occasional outliers (~2% of days, 2-3x normal noise)
  #   - Random noise (hash-based, ±12)
  tbl <- sprintf("%s.%s.SERIES_EVENTS", cfg$database, cfg$schemas$source_data)
  paste0(
    "CREATE OR REPLACE TABLE ", tbl, " AS ",
    "WITH units AS (",
    "  SELECT TO_VARCHAR(SEQ4()) AS UNIT_ID ",
    "  FROM TABLE(GENERATOR(ROWCOUNT => ", n_units, "))",
    "), days AS (",
    "  SELECT SEQ4() AS d FROM TABLE(GENERATOR(ROWCOUNT => ", n_days, "))",
    ") ",
    "SELECT u.UNIT_ID, ",
    "       DATEADD('day', d.d, DATE '", start_date, "') AS OBS_DATE, ",
    "       GREATEST(0, ROUND(",
    "         (50 + MOD(ABS(HASH(u.UNIT_ID, 0)), 150))",
    # Weekly seasonality (period = 7 days)
    "         + (5 + MOD(ABS(HASH(u.UNIT_ID, 10)), 15))",
    "           * SIN(2 * 3.14159 * (d.d + MOD(ABS(HASH(u.UNIT_ID, 11)), 7))",
    "                 / 7.0)",
    # Annual seasonality (period = 365.25 days)
    "         + (10 + MOD(ABS(HASH(u.UNIT_ID, 1)), 30))",
    "           * SIN(2 * 3.14159 * (d.d + MOD(ABS(HASH(u.UNIT_ID, 2)), 365))",
    "                 / 365.25)",
    # Piecewise trend: slope0 for d<400, slope1 for 400<=d<800, slope2 for d>=800
    "         + d.d * MOD(ABS(HASH(u.UNIT_ID, 3)), 30) * 0.005",
    "         + IFF(d.d >= 400, (d.d - 400) * (MOD(ABS(HASH(u.UNIT_ID, 4)), 40) - 20) * 0.003, 0)",
    "         + IFF(d.d >= 800, (d.d - 800) * (MOD(ABS(HASH(u.UNIT_ID, 5)), 30) - 15) * 0.004, 0)",
    # Noise: ±12 base, with ~2% outlier spikes (3x amplitude)
    "         + (MOD(ABS(HASH(u.UNIT_ID, d.d)), 240) - 120) * 0.1",
    "         * IFF(MOD(ABS(HASH(u.UNIT_ID, d.d, 99)), 100) < 2, 3.0, 1.0)",
    "       , 2)) AS Y ",
    "FROM units u CROSS JOIN days d"
  )
}

parallel_lab_setup <- function(
    conn,
    cfg,
    create_series = isTRUE(cfg$create_synthetic_series_table),
    n_units = 120L,
    n_days = 1095L
) {
  if (!requireNamespace("snowflakeR", quietly = TRUE)) {
    .abort("Package 'snowflakeR' is required.")
  }
  if (!inherits(conn, "Snowflake")) {
    warning("Connection does not inherit 'Snowflake'; continuing anyway.")
  }

  parallel_lab_validate_clean_room(cfg, require_runtime = FALSE)
  sql <- parallel_lab_sql_bootstrap(cfg)
  for (q in sql) {
    snowflakeR::sfr_execute(conn, q)
  }

  if (isTRUE(create_series)) {
    snowflakeR::sfr_execute(conn, parallel_lab_sql_seed_series_events(
      cfg, n_units = n_units, n_days = n_days))
    message("Created SERIES_EVENTS synthetic table.")
  } else {
    message("Skipped SERIES_EVENTS creation (create_series = FALSE).")
  }

  snowflakeR::sfr_use(conn, database = cfg$database, schema = cfg$schemas$source_data)
}

parallel_lab_run_tasks_demo <- function(
    conn, cfg, run = FALSE,
    model_run_id = NULL,
    arima_stepwise = TRUE,
    forecast_h = 30L,
    save_models = TRUE
) {
  if (!isTRUE(run)) {
    message("Dry-run only. Set run = TRUE to execute tasks demo.")
    return(invisible(NULL))
  }
  parallel_lab_validate_clean_room(cfg)

  if (!requireNamespace("foreach", quietly = TRUE) || !requireNamespace("forecast", quietly = TRUE)) {
    .abort("Packages 'foreach' and 'forecast' are required for demo execution.")
  }
  `%dopar%` <- foreach::`%dopar%`

  if (is.null(model_run_id) || !nzchar(model_run_id)) {
    model_run_id <- sprintf("tasks_%s", format(Sys.time(), "%Y%m%d_%H%M"))
  }
  forecast_h <- as.integer(forecast_h)
  arima_stepwise <- isTRUE(arima_stepwise)

  n_skus <- as.integer(cfg$demo_forecast_n_skus)
  n_chunks <- as.integer(cfg$demo_tasks_chunks_per_job)
  stage <- parallel_lab_stage_at(cfg)
  pool <- cfg$compute_pool
  img <- cfg$image_uri
  series_table <- sprintf("%s.%s.SERIES_EVENTS", cfg$database, cfg$schemas$source_data)
  if (nzchar(cfg$warehouse %||% "")) {
    snowflakeR::sfr_execute(conn, sprintf("USE WAREHOUSE %s", cfg$warehouse))
  }
  snowflakeR::sfr_use(conn, database = cfg$database, schema = cfg$schemas$source_data)

  unit_ids <- snowflakeR::sfr_query(conn, sprintf(
    "SELECT DISTINCT UNIT_ID FROM %s ORDER BY UNIT_ID LIMIT %d",
    series_table, n_skus
  ))$UNIT_ID
  message(sprintf("Fetched %d UNIT_IDs from %s", length(unit_ids), series_table))

  data_query <- list(
    table      = series_table,
    key_column = "UNIT_ID",
    key_arg    = "unit_id",
    columns    = c("UNIT_ID", "OBS_DATE", "Y"),
    order_by   = "UNIT_ID, OBS_DATE",
    warehouse  = cfg$warehouse,
    database   = cfg$database
  )

  reg_args <- list(
    conn,
    mode = "tasks",
    compute_pool = pool,
    image_uri = img,
    stage = stage,
    chunks_per_job = n_chunks,
    instance_family = cfg$instance_family %||% "CPU_X64_S",
    data_query = data_query,
    save_models = save_models,
    model_key_arg = "unit_id",
    model_run_id = model_run_id
  )
  if (nzchar(cfg$warehouse %||% "")) {
    reg_args$warehouse <- cfg$warehouse
  }
  do.call(snowflakeR::registerDoSnowflake, reg_args)

  message(sprintf("model_run_id = %s | stepwise=%s h=%d | save_models=%s",
                  model_run_id, arima_stepwise, forecast_h, save_models))

  t0 <- Sys.time()
  out <- foreach::foreach(
    unit_id = unit_ids,
    .combine = list,
    .multicombine = TRUE,
    .packages = "forecast"
  ) %dopar% {
    suppressPackageStartupMessages(library(forecast))
    ts_full <- ts(unit_data$Y, frequency = 7)
    n <- length(ts_full)
    holdout_n <- min(30L, as.integer(floor(n / 4)))

    model <- auto.arima(ts_full, stepwise = arima_stepwise)
    fc <- forecast(model, h = forecast_h)
    acc <- forecast::accuracy(model)

    holdout_mape <- tryCatch({
      ts_train <- window(ts_full, end = time(ts_full)[n - holdout_n])
      ts_test  <- window(ts_full, start = time(ts_full)[n - holdout_n + 1])
      m_ho <- auto.arima(ts_train, stepwise = arima_stepwise)
      fc_ho <- forecast(m_ho, h = holdout_n)
      mean(abs((ts_test - fc_ho$mean) / ts_test)) * 100
    }, error = function(e) NA_real_)

    list(
      unit_id   = unit_id,
      model     = as.character(fc$method),
      forecast  = as.numeric(fc$mean),
      n_obs     = nrow(unit_data),
      model_obj = model,
      aicc      = model$aicc,
      rmse      = acc[1, "RMSE"],
      mape      = holdout_mape,
      mape_train = acc[1, "MAPE"],
      mase      = acc[1, "MASE"]
    )
  }

  dt <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  out <- .flatten_foreach_results(out)
  message(sprintf("tasks demo complete: %d forecasts in %.1fs", length(out), dt))
  message(sprintf("stage used: %s | run_id: %s", stage, model_run_id))
  list(
    results      = out,
    model_run_id = model_run_id,
    elapsed      = dt,
    params       = list(arima_stepwise = arima_stepwise, forecast_h = forecast_h,
                        n_skus = length(unit_ids), n_chunks = n_chunks)
  )
}

parallel_lab_run_queue_demo <- function(
    conn, cfg, run = FALSE,
    model_run_id = NULL,
    arima_stepwise = FALSE,
    forecast_h = 30L,
    save_models = TRUE
) {
  if (!isTRUE(run)) {
    message("Dry-run only. Set run = TRUE to execute queue demo.")
    return(invisible(NULL))
  }
  parallel_lab_validate_clean_room(cfg)

  if (!requireNamespace("foreach", quietly = TRUE) || !requireNamespace("forecast", quietly = TRUE)) {
    .abort("Packages 'foreach' and 'forecast' are required for demo execution.")
  }
  `%dopar%` <- foreach::`%dopar%`

  if (is.null(model_run_id) || !nzchar(model_run_id)) {
    model_run_id <- sprintf("queue_%s", format(Sys.time(), "%Y%m%d_%H%M"))
  }
  forecast_h <- as.integer(forecast_h)
  arima_stepwise <- isTRUE(arima_stepwise)

  n_skus <- as.integer(cfg$demo_forecast_n_skus)
  n_workers <- as.integer(cfg$demo_queue_n_workers)
  n_chunks <- as.integer(cfg$demo_tasks_chunks_per_job)
  stage <- parallel_lab_stage_at(cfg)
  queue <- parallel_lab_queue_fqn(cfg)
  series_table <- sprintf("%s.%s.SERIES_EVENTS", cfg$database, cfg$schemas$source_data)
  if (nzchar(cfg$warehouse %||% "")) {
    snowflakeR::sfr_execute(conn, sprintf("USE WAREHOUSE %s", cfg$warehouse))
  }
  snowflakeR::sfr_use(conn, database = cfg$database, schema = cfg$schemas$source_data)

  unit_ids <- snowflakeR::sfr_query(conn, sprintf(
    "SELECT DISTINCT UNIT_ID FROM %s ORDER BY UNIT_ID LIMIT %d",
    series_table, n_skus
  ))$UNIT_ID
  message(sprintf("Fetched %d UNIT_IDs from %s", length(unit_ids), series_table))

  data_query <- list(
    table      = series_table,
    key_column = "UNIT_ID",
    key_arg    = "unit_id",
    columns    = c("UNIT_ID", "OBS_DATE", "Y"),
    order_by   = "UNIT_ID, OBS_DATE",
    warehouse  = cfg$warehouse,
    database   = cfg$database
  )

  snowflakeR::registerDoSnowflake(
    conn,
    mode = "queue",
    compute_pool = cfg$compute_pool,
    image_uri = cfg$image_uri,
    stage = stage,
    queue_table = queue,
    n_workers = n_workers,
    chunks_per_job = n_chunks,
    data_query = data_query,
    save_models = save_models,
    model_key_arg = "unit_id",
    model_run_id = model_run_id
  )

  message(sprintf("model_run_id = %s | stepwise=%s h=%d | save_models=%s",
                  model_run_id, arima_stepwise, forecast_h, save_models))

  t0 <- Sys.time()
  out <- foreach::foreach(
    unit_id = unit_ids,
    .combine = list,
    .multicombine = TRUE,
    .packages = "forecast"
  ) %dopar% {
    suppressPackageStartupMessages(library(forecast))
    ts_data <- ts(unit_data$Y, frequency = 7)
    model <- auto.arima(ts_data, stepwise = arima_stepwise)
    fc <- forecast(model, h = forecast_h)
    acc <- forecast::accuracy(model)
    list(
      unit_id   = unit_id,
      model     = as.character(fc$method),
      forecast  = as.numeric(fc$mean),
      n_obs     = nrow(unit_data),
      model_obj = model,
      aicc      = model$aicc,
      rmse      = acc[1, "RMSE"],
      mape      = acc[1, "MAPE"],
      mase      = acc[1, "MASE"]
    )
  }

  dt <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  out <- .flatten_foreach_results(out)
  message(sprintf("queue demo complete: %d forecasts in %.1fs", length(out), dt))
  list(
    results      = out,
    model_run_id = model_run_id,
    elapsed      = dt,
    params       = list(arima_stepwise = arima_stepwise, forecast_h = forecast_h,
                        n_skus = length(unit_ids), n_chunks = n_chunks)
  )
}


# =============================================================================
# Queue demo: time-boxed dynamic feeder
# =============================================================================
#
# Runs the queue with a *dynamic feeder* that adds chunks to the work queue
# as workers pull them, stopping once the time budget expires.  This is the
# key differentiator vs. Tasks:
#   - Tasks: fixed batch — all chunks dispatched upfront, wait for all.
#   - Queue: streaming — chunks fed on demand, can stop early or run forever.
#
# After the time budget, already-running chunks finish and their results are
# collected from stage.  Models are saved exactly as in the Tasks flow.

parallel_lab_run_queue_timeboxed <- function(
    conn, cfg, run = FALSE,
    time_limit_sec,
    model_run_id = NULL,
    arima_stepwise = TRUE,
    forecast_h = 30L,
    save_models = TRUE,
    n_chunks = 20L,
    unit_ids = NULL,
    model_contest = FALSE
) {
  if (!isTRUE(run)) {
    message("Dry-run only. Set run = TRUE to execute queue demo.")
    return(invisible(NULL))
  }
  parallel_lab_validate_clean_room(cfg)

  if (missing(time_limit_sec) || is.null(time_limit_sec) || time_limit_sec <= 0) {
    .abort("time_limit_sec must be a positive number (e.g. tasks_out$elapsed)")
  }

  if (!requireNamespace("foreach", quietly = TRUE) ||
      !requireNamespace("forecast", quietly = TRUE) ||
      !requireNamespace("iterators", quietly = TRUE)) {
    .abort("Packages 'foreach', 'forecast', and 'iterators' are required.")
  }

  if (is.null(model_run_id) || !nzchar(model_run_id)) {
    model_run_id <- sprintf("queue_%s", format(Sys.time(), "%Y%m%d_%H%M"))
  }
  forecast_h     <- as.integer(forecast_h)
  arima_stepwise <- isTRUE(arima_stepwise)
  n_chunks       <- as.integer(n_chunks)

  n_skus    <- as.integer(cfg$demo_forecast_n_skus)
  n_workers <- as.integer(cfg$demo_queue_n_workers)
  stage     <- parallel_lab_stage_at(cfg)
  queue_fqn <- parallel_lab_queue_fqn(cfg)
  pool      <- cfg$compute_pool
  img       <- cfg$image_uri
  inst_fam  <- cfg$instance_family %||% "CPU_X64_S"
  series_table <- sprintf("%s.%s.SERIES_EVENTS",
                          cfg$database, cfg$schemas$source_data)

  if (nzchar(cfg$warehouse %||% "")) {
    snowflakeR::sfr_execute(conn, sprintf("USE WAREHOUSE %s", cfg$warehouse))
  }
  snowflakeR::sfr_use(conn, database = cfg$database,
                      schema = cfg$schemas$source_data)

  if (is.null(unit_ids) || length(unit_ids) == 0) {
    unit_ids <- snowflakeR::sfr_query(conn, sprintf(
      "SELECT DISTINCT UNIT_ID FROM %s ORDER BY UNIT_ID LIMIT %d",
      series_table, n_skus
    ))$UNIT_ID
    message(sprintf("Fetched %d UNIT_IDs from %s", length(unit_ids), series_table))
  } else {
    message(sprintf("Using %d caller-supplied UNIT_IDs", length(unit_ids)))
  }

  data_query <- list(
    table      = series_table,
    key_column = "UNIT_ID",
    key_arg    = "unit_id",
    columns    = c("UNIT_ID", "OBS_DATE", "Y"),
    order_by   = "UNIT_ID, OBS_DATE",
    warehouse  = cfg$warehouse,
    database   = cfg$database
  )

  # Spread SKUs across n_chunks (recycling if n_chunks * chunk_size > n_skus)
  units_per_chunk <- ceiling(length(unit_ids) / n_chunks)
  n_total <- units_per_chunk * n_chunks
  queue_unit_ids <- rep_len(unit_ids, n_total)

  # Build the foreach expression — bquote substitutes parameter values
  if (isTRUE(model_contest)) {
    forecast_expr <- bquote({
      suppressPackageStartupMessages(library(forecast))
      ts_full <- ts(unit_data$Y, frequency = 7)
      n <- length(ts_full)
      holdout_n <- min(30L, as.integer(floor(n / 4)))

      ts_train <- window(ts_full, end = time(ts_full)[n - holdout_n])
      ts_test  <- window(ts_full, start = time(ts_full)[n - holdout_n + 1])

      .try_model <- function(fit_fn, train, h) {
        tryCatch({
          m <- fit_fn(train)
          fc <- forecast(m, h = h)
          test_mape <- mean(abs((ts_test - fc$mean) / ts_test)) * 100
          list(model = m, mape = test_mape, method = as.character(fc$method))
        }, error = function(e) list(model = NULL, mape = Inf, method = "failed"))
      }

      cands <- list(
        arima_ex = .try_model(function(d) auto.arima(d, stepwise = FALSE), ts_train, holdout_n),
        ets_auto = .try_model(function(d) ets(d), ts_train, holdout_n),
        tbats_auto = .try_model(function(d) tbats(d), ts_train, holdout_n)
      )

      best_name <- names(which.min(vapply(cands, function(x) x$mape, numeric(1))))

      refit_fns <- list(
        arima_ex   = function(d) auto.arima(d, stepwise = FALSE),
        ets_auto   = function(d) ets(d),
        tbats_auto = function(d) tbats(d)
      )
      model <- refit_fns[[best_name]](ts_full)
      fc_full <- forecast(model, h = .(forecast_h))
      method_str <- paste0(best_name, ": ", as.character(fc_full$method))

      acc <- forecast::accuracy(model)
      best_holdout_mape <- cands[[best_name]]$mape
      list(
        unit_id   = unit_id,
        model     = method_str,
        forecast  = as.numeric(fc_full$mean),
        n_obs     = nrow(unit_data),
        model_obj = model,
        aicc      = if (!is.null(model$aicc)) model$aicc else NA_real_,
        rmse      = acc[1, "RMSE"],
        mape      = best_holdout_mape,
        mape_train = acc[1, "MAPE"],
        mase      = acc[1, "MASE"]
      )
    })
  } else {
    forecast_expr <- bquote({
      suppressPackageStartupMessages(library(forecast))
      ts_full <- ts(unit_data$Y, frequency = 7)
      n <- length(ts_full)
      holdout_n <- min(30L, as.integer(floor(n / 4)))

      model <- auto.arima(ts_full, stepwise = .(arima_stepwise))
      fc    <- forecast(model, h = .(forecast_h))
      acc   <- forecast::accuracy(model)

      holdout_mape <- tryCatch({
        ts_train <- window(ts_full, end = time(ts_full)[n - holdout_n])
        ts_test  <- window(ts_full, start = time(ts_full)[n - holdout_n + 1])
        m_ho <- auto.arima(ts_train, stepwise = .(arima_stepwise))
        fc_ho <- forecast(m_ho, h = holdout_n)
        mean(abs((ts_test - fc_ho$mean) / ts_test)) * 100
      }, error = function(e) NA_real_)

      list(
        unit_id   = unit_id,
        model     = as.character(fc$method),
        forecast  = as.numeric(fc$mean),
        n_obs     = nrow(unit_data),
        model_obj = model,
        aicc      = model$aicc,
        rmse      = acc[1, "RMSE"],
        mape      = holdout_mape,
        mape_train = acc[1, "MAPE"],
        mase      = acc[1, "MASE"]
      )
    })
  }

  # Serialize chunks to stage
  bridge <- snowflakeR:::get_bridge_module("sfr_queue_bridge")
  job_id <- paste0(sprintf("%04x", sample(0:65535, 4, replace = TRUE)),
                   collapse = "-")

  queue_fo   <- foreach::foreach(unit_id = queue_unit_ids, .packages = "forecast")
  queue_it   <- iterators::iter(queue_fo)
  queue_args <- as.list(queue_it)

  queue_opts <- list(
    stage          = cfg$dosnowflake_stage_name,
    chunks_per_job = n_chunks,
    data_query     = data_query,
    save_models    = save_models,
    model_key_arg  = "unit_id",
    model_run_id   = model_run_id
  )

  mode_label <- if (isTRUE(model_contest)) "contest(ARIMA+ETS+TBATS)"
                else sprintf("stepwise=%s", arima_stepwise)
  message(sprintf(
    "Serializing %d chunks (%d units/chunk) to stage | model_run_id=%s | %s h=%d",
    n_chunks, units_per_chunk, model_run_id, mode_label, forecast_h
  ))
  job <- snowflakeR:::.serialize_job_to_stage(
    conn, job_id, forecast_expr, queue_args, queue_fo, parent.frame(), queue_opts
  )

  bridge$create_queue_table(session = conn$session, fqn = queue_fqn)

  message(sprintf(
    "Starting time-boxed queue feeder (limit=%.0fs, %d workers, %d available chunks)...",
    time_limit_sec, n_workers, n_chunks
  ))
  tb_result <- bridge$run_time_boxed_queue(
    session         = conn$session,
    compute_pool    = pool,
    image_uri       = img,
    job_id          = job_id,
    stage_path      = job$stage_path,
    total_chunks    = n_chunks,
    n_workers       = n_workers,
    time_limit_sec  = time_limit_sec,
    queue_fqn       = queue_fqn,
    instance_family = inst_fam,
    skus_per_chunk  = as.integer(units_per_chunk)
  )

  chunks_fed  <- as.integer(tb_result$chunks_fed)
  chunks_done <- as.integer(tb_result$chunks_done)
  elapsed     <- as.numeric(tb_result$elapsed_sec)

  message(sprintf(
    "Time-boxed queue: %d/%d chunks done in %.1fs (%.1f units/s)",
    chunks_done, chunks_fed, elapsed, as.numeric(tb_result$skus_per_sec)
  ))

  # --- Collect results from completed chunks on stage ---
  message("Collecting results from stage...")
  Sys.sleep(10)  # brief pause for stage sync

  tmp_dir <- tempfile(pattern = "queue_timeboxed_results_")
  dir.create(tmp_dir, recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)

  all_results <- list()
  for (i in seq_len(chunks_fed)) {
    chunk_id <- sprintf("%03d", i)
    result_stage <- paste0(job$stage_path, "/results/result_", chunk_id, ".rds")
    tryCatch({
      snowflakeR:::.dosnowflake_stage_get(conn, result_stage, tmp_dir)
      result_file <- file.path(tmp_dir, paste0("result_", chunk_id, ".rds"))
      if (file.exists(result_file)) {
        chunk_result <- readRDS(result_file)
        for (j in seq_along(chunk_result$results)) {
          all_results[[length(all_results) + 1L]] <- chunk_result$results[[j]]
        }
        unlink(result_file)
      }
    }, error = function(e) {
      # Chunk not yet written or failed — skip silently
    })
  }

  n_ok <- sum(vapply(all_results, function(r) !inherits(r, "error"), logical(1)))
  message(sprintf("Collected %d results (%d OK, %d errors)",
                  length(all_results), n_ok, length(all_results) - n_ok))

  list(
    results      = all_results,
    model_run_id = model_run_id,
    elapsed      = elapsed,
    params       = list(
      arima_stepwise = arima_stepwise,
      model_contest  = isTRUE(model_contest),
      forecast_h     = forecast_h,
      n_skus         = length(all_results),
      n_chunks       = n_chunks,
      chunks_fed     = chunks_fed,
      chunks_done    = chunks_done,
      time_limit_sec = time_limit_sec
    )
  )
}


# =============================================================================
# Benchmark: tasks vs time-boxed queue comparison
# =============================================================================

parallel_lab_run_benchmark <- function(conn, cfg, run = FALSE) {
  if (!isTRUE(run)) {
    message("Dry-run only. Set run = TRUE to execute benchmark.")
    return(invisible(NULL))
  }
  parallel_lab_validate_clean_room(cfg)

  if (!requireNamespace("foreach", quietly = TRUE) ||
      !requireNamespace("forecast", quietly = TRUE)) {
    .abort("Packages 'foreach' and 'forecast' are required for benchmark.")
  }
  `%dopar%` <- foreach::`%dopar%`

  n_skus    <- as.integer(cfg$demo_forecast_n_skus)
  n_tasks_ch <- as.integer(cfg$demo_tasks_chunks_per_job)
  n_queue_ch <- as.integer(cfg$demo_queue_chunks_per_job)
  n_workers <- as.integer(cfg$demo_queue_n_workers)
  stage     <- parallel_lab_stage_at(cfg)
  queue_fqn <- parallel_lab_queue_fqn(cfg)
  pool      <- cfg$compute_pool
  img       <- cfg$image_uri
  inst_fam  <- cfg$instance_family %||% "CPU_X64_S"
  series_table <- sprintf("%s.%s.SERIES_EVENTS", cfg$database, cfg$schemas$source_data)

  if (nzchar(cfg$warehouse %||% "")) {
    snowflakeR::sfr_execute(conn, sprintf("USE WAREHOUSE %s", cfg$warehouse))
  }
  snowflakeR::sfr_use(conn, database = cfg$database, schema = cfg$schemas$source_data)

  unit_ids <- snowflakeR::sfr_query(conn, sprintf(
    "SELECT DISTINCT UNIT_ID FROM %s ORDER BY UNIT_ID LIMIT %d",
    series_table, n_skus
  ))$UNIT_ID
  message(sprintf("Fetched %d UNIT_IDs from %s", length(unit_ids), series_table))

  data_query <- list(
    table      = series_table,
    key_column = "UNIT_ID",
    key_arg    = "unit_id",
    columns    = c("UNIT_ID", "OBS_DATE", "Y"),
    order_by   = "UNIT_ID, OBS_DATE",
    warehouse  = cfg$warehouse,
    database   = cfg$database
  )

  message(paste(rep("=", 72), collapse = ""))
  message(sprintf("BENCHMARK: %d units | pool=%s (%s) | tasks=%d chunks | queue=%d workers",
                  length(unit_ids), pool, inst_fam, n_tasks_ch, n_workers))
  message(paste(rep("=", 72), collapse = ""))

  # The expression workers execute — unit_data is injected by the
  # manifest-driven bulk-read in the worker before mclapply runs.
  forecast_expr <- quote({
    suppressPackageStartupMessages(library(forecast))
    ts_data <- ts(unit_data$Y, frequency = 7)
    model <- auto.arima(ts_data)
    fc <- forecast(model, h = 30)
    acc <- forecast::accuracy(model)
    list(
      unit_id  = unit_id,
      model    = as.character(fc$method),
      forecast = as.numeric(fc$mean),
      n_obs    = nrow(unit_data),
      aicc     = model$aicc,
      rmse     = acc[1, "RMSE"],
      mape     = acc[1, "MAPE"],
      mase     = acc[1, "MASE"]
    )
  })

  # ---- Phase 1: Tasks ----
  message("\n--- Phase 1: Tasks mode ---")
  reg_args <- list(
    conn,
    mode = "tasks",
    compute_pool = pool,
    image_uri = img,
    stage = stage,
    chunks_per_job = n_tasks_ch,
    instance_family = inst_fam,
    data_query = data_query
  )
  if (nzchar(cfg$warehouse %||% "")) reg_args$warehouse <- cfg$warehouse
  do.call(snowflakeR::registerDoSnowflake, reg_args)

  t0_tasks <- Sys.time()
  tasks_out <- foreach::foreach(
    unit_id = unit_ids,
    .combine = list,
    .multicombine = TRUE,
    .packages = "forecast"
  ) %dopar% {
    suppressPackageStartupMessages(library(forecast))
    ts_data <- ts(unit_data$Y, frequency = 7)
    model <- auto.arima(ts_data)
    fc <- forecast(model, h = 30)
    list(unit_id = unit_id, model = as.character(fc$method),
         forecast = as.numeric(fc$mean), n_obs = nrow(unit_data))
  }
  tasks_elapsed <- as.numeric(difftime(Sys.time(), t0_tasks, units = "secs"))
  tasks_rate <- length(unit_ids) / tasks_elapsed
  message(sprintf(
    "Tasks complete: %d units in %.1fs (%.1f units/s)",
    length(unit_ids), tasks_elapsed, tasks_rate
  ))

  # ---- Phase 2: Queue (time-boxed feeder) ----
  message("\n--- Phase 2: Queue mode (time-boxed feeder) ---")
  message(sprintf("Time limit = %.0fs (= tasks wall time)", tasks_elapsed))

  units_per_chunk <- ceiling(length(unit_ids) / n_queue_ch)
  n_queue_total <- units_per_chunk * n_queue_ch
  queue_unit_ids <- rep_len(unit_ids, n_queue_total)

  bridge <- snowflakeR:::get_bridge_module("sfr_queue_bridge")
  job_id <- paste0(sprintf("%04x", sample(0:65535, 4, replace = TRUE)),
                   collapse = "-")

  message(sprintf("Serializing %d chunks (%d units/chunk) to stage...",
                  n_queue_ch, units_per_chunk))

  queue_fo <- foreach::foreach(unit_id = queue_unit_ids, .packages = "forecast")
  queue_it <- iterators::iter(queue_fo)
  queue_args <- as.list(queue_it)

  queue_opts <- list(
    stage = cfg$dosnowflake_stage_name,
    chunks_per_job = n_queue_ch,
    data_query = data_query
  )
  job <- snowflakeR:::.serialize_job_to_stage(
    conn, job_id, forecast_expr, queue_args, queue_fo, parent.frame(), queue_opts
  )

  bridge$create_queue_table(session = conn$session, fqn = queue_fqn)

  message("Starting time-boxed queue feeder...")
  result <- bridge$run_time_boxed_queue(
    session         = conn$session,
    compute_pool    = pool,
    image_uri       = img,
    job_id          = job_id,
    stage_path      = job$stage_path,
    total_chunks    = as.integer(n_queue_ch),
    n_workers       = as.integer(n_workers),
    time_limit_sec  = tasks_elapsed,
    queue_fqn       = queue_fqn,
    instance_family = inst_fam,
    skus_per_chunk  = as.integer(units_per_chunk)
  )

  queue_done  <- as.integer(result$chunks_done)
  queue_units_n <- as.integer(result$skus_processed)
  queue_elapsed <- as.numeric(result$elapsed_sec)
  queue_rate <- as.numeric(result$skus_per_sec)

  # ---- Summary ----
  message("\n", paste(rep("=", 72), collapse = ""))
  message("BENCHMARK RESULTS")
  message(paste(rep("=", 72), collapse = ""))
  message(sprintf("  Pool:           %s (%s)", pool, inst_fam))
  message(sprintf("  Source table:   %s (%d units)", series_table, length(unit_ids)))
  message(sprintf("  Tasks:          %d units in %.1fs  (%.1f units/s, %d chunks)",
                  length(unit_ids), tasks_elapsed, tasks_rate, n_tasks_ch))
  message(sprintf("  Queue (timed):  %d units in %.1fs  (%.1f units/s, %d/%d chunks done)",
                  queue_units_n, queue_elapsed, queue_rate,
                  queue_done, n_queue_ch))
  message(sprintf("  Queue/Tasks throughput ratio: %.0f%%",
                  100 * queue_rate / tasks_rate))
  message(paste(rep("=", 72), collapse = ""))

  invisible(list(
    tasks = list(units = length(unit_ids), elapsed = tasks_elapsed,
                 rate = tasks_rate, chunks = n_tasks_ch),
    queue = list(units = queue_units_n, elapsed = queue_elapsed,
                 rate = queue_rate, chunks_done = queue_done,
                 chunks_total = n_queue_ch)
  ))
}


# =============================================================================
# Plotting helpers
# =============================================================================

parallel_lab_plot_series <- function(conn, cfg, unit_ids = NULL, n = 6) {
  if (!requireNamespace("ggplot2", quietly = TRUE) || !requireNamespace("tidyr", quietly = TRUE)) {
    .abort("Packages 'ggplot2' and 'tidyr' are required for plotting.")
  }
  series_table <- sprintf("%s.%s.SERIES_EVENTS", cfg$database, cfg$schemas$source_data)

  if (is.null(unit_ids)) {
    unit_ids <- snowflakeR::sfr_query(conn, sprintf(
      "SELECT DISTINCT UNIT_ID FROM %s ORDER BY UNIT_ID LIMIT %d",
      series_table, as.integer(n)
    ))$UNIT_ID
  }

  sql_in <- paste(sprintf("'%s'", unit_ids), collapse = ", ")
  df <- snowflakeR::sfr_query(conn, sprintf(
    "SELECT UNIT_ID, OBS_DATE, Y FROM %s WHERE UNIT_ID IN (%s) ORDER BY UNIT_ID, OBS_DATE",
    series_table, sql_in
  ))
  df$OBS_DATE <- as.Date(df$OBS_DATE)

  ggplot2::ggplot(df, ggplot2::aes(x = OBS_DATE, y = Y)) +
    ggplot2::geom_line(colour = "#2563eb", linewidth = 0.5) +
    ggplot2::facet_wrap(~ UNIT_ID, scales = "free_y", ncol = 2) +
    ggplot2::labs(title = "Time Series Samples", x = "Date", y = "Observed Y") +
    ggplot2::theme_minimal(base_size = 11)
}

parallel_lab_plot_training_results <- function(run_results, top_n = 15) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    .abort("Package 'ggplot2' is required for plotting.")
  }

  rows <- lapply(run_results$results, function(r) {
    if (inherits(r, "error")) return(NULL)
    data.frame(unit_id = as.character(r$unit_id),
               model = r$model %||% "unknown",
               n_obs = r$n_obs %||% NA_integer_,
               stringsAsFactors = FALSE)
  })
  df <- do.call(rbind, Filter(Negate(is.null), rows))

  counts <- as.data.frame(table(model = df$model), stringsAsFactors = FALSE)
  names(counts)[2] <- "count"
  counts <- counts[order(-counts$count), ]
  n_types <- nrow(counts)

  if (n_types > top_n) {
    top <- counts[seq_len(top_n), ]
    other_total <- sum(counts$count[(top_n + 1):nrow(counts)])
    other_row <- data.frame(
      model = sprintf("Other (%d types)", n_types - top_n),
      count = other_total, stringsAsFactors = FALSE)
    counts <- rbind(top, other_row)
  }

  counts$model <- factor(counts$model, levels = rev(counts$model))
  is_other <- grepl("^Other ", counts$model)

  ggplot2::ggplot(counts, ggplot2::aes(x = model, y = count)) +
    ggplot2::geom_col(fill = ifelse(is_other, "#94a3b8", "#2563eb")) +
    ggplot2::geom_text(ggplot2::aes(label = count),
                       hjust = -0.15, size = 3, colour = "#334155") +
    ggplot2::coord_flip() +
    ggplot2::scale_y_continuous(expand = ggplot2::expansion(mult = c(0, 0.15))) +
    ggplot2::labs(
      title = sprintf("Model Selection (%s)", run_results$model_run_id),
      subtitle = sprintf("%d SKUs, %d unique model types", nrow(df), n_types),
      x = NULL, y = "Count") +
    ggplot2::theme_minimal(base_size = 11)
}

parallel_lab_plot_forecasts <- function(conn, cfg, run_results, unit_ids = NULL, n = 4) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    .abort("Package 'ggplot2' is required for plotting.")
  }
  series_table <- sprintf("%s.%s.SERIES_EVENTS", cfg$database, cfg$schemas$source_data)

  all_results <- run_results$results
  valid <- Filter(function(r) !inherits(r, "error"), all_results)
  if (is.null(unit_ids)) {
    unit_ids <- head(vapply(valid, function(r) as.character(r$unit_id), character(1)), n)
  }

  sql_in <- paste(sprintf("'%s'", unit_ids), collapse = ", ")
  hist_df <- snowflakeR::sfr_query(conn, sprintf(
    "SELECT UNIT_ID, OBS_DATE, Y FROM %s WHERE UNIT_ID IN (%s) ORDER BY UNIT_ID, OBS_DATE",
    series_table, sql_in
  ))
  hist_df$OBS_DATE <- as.Date(hist_df$OBS_DATE)
  hist_df$type <- "historical"

  fc_rows <- lapply(valid, function(r) {
    uid <- as.character(r$unit_id)
    if (!uid %in% unit_ids) return(NULL)
    fc_vals <- r$forecast
    h <- length(fc_vals)
    last_date <- max(hist_df$OBS_DATE[hist_df$UNIT_ID == uid])
    fc_dates <- seq.Date(last_date, by = "month", length.out = h + 1)[-1]
    data.frame(UNIT_ID = uid, OBS_DATE = fc_dates, Y = fc_vals,
               type = "forecast", stringsAsFactors = FALSE)
  })
  fc_df <- do.call(rbind, Filter(Negate(is.null), fc_rows))

  plot_df <- rbind(hist_df, fc_df)

  ggplot2::ggplot(plot_df, ggplot2::aes(x = OBS_DATE, y = Y, colour = type, linetype = type)) +
    ggplot2::geom_line(linewidth = 0.6) +
    ggplot2::facet_wrap(~ UNIT_ID, scales = "free_y", ncol = 2) +
    ggplot2::scale_colour_manual(values = c(historical = "#2563eb", forecast = "#dc2626")) +
    ggplot2::scale_linetype_manual(values = c(historical = "solid", forecast = "dashed")) +
    ggplot2::labs(title = sprintf("Forecasts (%s)", run_results$model_run_id),
                  x = "Date", y = "Y") +
    ggplot2::theme_minimal(base_size = 11)
}


# =============================================================================
# Training metrics: persist, rank, plot
# =============================================================================

parallel_lab_save_metrics <- function(conn, cfg, run_results) {
  metrics_tbl <- sprintf("%s.%s.TRAINING_METRICS",
                         cfg$database, cfg$schemas$models)
  run_id <- run_results$model_run_id

  rows <- Filter(Negate(is.null), lapply(run_results$results, function(r) {
    if (inherits(r, "error")) return(NULL)
    data.frame(
      MODEL_RUN_ID = run_id,
      UNIT_ID      = as.character(r$unit_id),
      ARIMA_MODEL  = r$model %||% "unknown",
      AICC         = r$aicc %||% NA_real_,
      RMSE         = r$rmse %||% NA_real_,
      MAPE         = r$mape %||% NA_real_,
      MASE         = r$mase %||% NA_real_,
      N_OBS        = r$n_obs %||% NA_integer_,
      stringsAsFactors = FALSE
    )
  }))

  if (length(rows) == 0) {
    message("No valid results to save.")
    return(invisible(0L))
  }

  df <- do.call(rbind, rows)

  .safe <- function(x) {
    if (is.null(x) || is.na(x)) "NULL"
    else if (is.numeric(x)) sprintf("%f", x)
    else sprintf("'%s'", gsub("'", "''", as.character(x)))
  }

  batch_size <- 500L
  n_inserted <- 0L

  for (start in seq(1L, nrow(df), by = batch_size)) {
    end <- min(start + batch_size - 1L, nrow(df))
    batch <- df[start:end, , drop = FALSE]
    vals <- vapply(seq_len(nrow(batch)), function(j) {
      r <- batch[j, , drop = FALSE]
      sprintf("(%s,%s,%s,%s,%s,%s,%s,%s)",
              .safe(r$MODEL_RUN_ID), .safe(r$UNIT_ID), .safe(r$ARIMA_MODEL),
              .safe(r$AICC), .safe(r$RMSE), .safe(r$MAPE), .safe(r$MASE),
              if (is.na(r$N_OBS)) "NULL" else as.character(r$N_OBS))
    }, character(1))

    sql <- sprintf(
      "INSERT INTO %s (MODEL_RUN_ID,UNIT_ID,ARIMA_MODEL,AICC,RMSE,MAPE,MASE,N_OBS) VALUES %s",
      metrics_tbl, paste(vals, collapse = ",")
    )
    snowflakeR::sfr_execute(conn, sql)
    n_inserted <- n_inserted + nrow(batch)
  }

  message(sprintf("Saved %d training metrics to %s (run=%s)",
                  n_inserted, metrics_tbl, run_id))
  invisible(n_inserted)
}

parallel_lab_rank_skus_by_accuracy <- function(
    run_results = NULL,
    conn = NULL, cfg = NULL,
    model_run_id = NULL,
    metric = "mape",
    descending = TRUE
) {
  if (!is.null(run_results)) {
    rows <- Filter(Negate(is.null), lapply(run_results$results, function(r) {
      if (inherits(r, "error")) return(NULL)
      data.frame(
        unit_id = as.character(r$unit_id),
        value   = r[[metric]] %||% NA_real_,
        stringsAsFactors = FALSE
      )
    }))
    if (length(rows) == 0) return(character(0))
    df <- do.call(rbind, rows)
    df <- df[!is.na(df$value), ]
    df <- df[order(df$value, decreasing = descending), ]
    return(df$unit_id)
  }

  if (!is.null(conn) && !is.null(cfg)) {
    metrics_tbl <- sprintf("%s.%s.TRAINING_METRICS",
                           cfg$database, cfg$schemas$models)
    run_filter <- if (!is.null(model_run_id)) {
      sprintf("WHERE MODEL_RUN_ID = '%s'", gsub("'", "''", model_run_id))
    } else {
      sprintf(
        "WHERE MODEL_RUN_ID = (SELECT MODEL_RUN_ID FROM %s ORDER BY CREATED_AT DESC LIMIT 1)",
        metrics_tbl
      )
    }
    col <- toupper(metric)
    sql <- sprintf(
      "SELECT UNIT_ID FROM %s %s AND %s IS NOT NULL ORDER BY %s %s",
      metrics_tbl, run_filter, col, col,
      if (descending) "DESC" else "ASC"
    )
    df <- snowflakeR::sfr_query(conn, sql)
    return(df$UNIT_ID)
  }

  .abort("Provide either run_results or conn + cfg (+ optional model_run_id)")
}

parallel_lab_plot_accuracy <- function(run_results, metric = "mape", bins = 30) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    .abort("Package 'ggplot2' is required for plotting.")
  }

  rows <- Filter(Negate(is.null), lapply(run_results$results, function(r) {
    if (inherits(r, "error")) return(NULL)
    data.frame(
      unit_id = as.character(r$unit_id),
      value   = r[[metric]] %||% NA_real_,
      stringsAsFactors = FALSE
    )
  }))
  if (length(rows) == 0) {
    message("No valid results for accuracy plot.")
    return(invisible(NULL))
  }
  df <- do.call(rbind, rows)
  df <- df[!is.na(df$value), ]

  label <- toupper(metric)
  med <- median(df$value, na.rm = TRUE)
  p90 <- quantile(df$value, 0.9, na.rm = TRUE)

  ggplot2::ggplot(df, ggplot2::aes(x = value)) +
    ggplot2::geom_histogram(bins = bins, fill = "#2563eb", colour = "white", alpha = 0.85) +
    ggplot2::geom_vline(xintercept = med, linetype = "dashed", colour = "#16a34a", linewidth = 0.7) +
    ggplot2::geom_vline(xintercept = p90, linetype = "dotted", colour = "#dc2626", linewidth = 0.7) +
    ggplot2::annotate("text", x = med, y = Inf, label = sprintf("median=%.2f", med),
                      vjust = 2, hjust = -0.1, size = 3, colour = "#16a34a") +
    ggplot2::annotate("text", x = p90, y = Inf, label = sprintf("p90=%.2f", p90),
                      vjust = 3.5, hjust = -0.1, size = 3, colour = "#dc2626") +
    ggplot2::labs(
      title = sprintf("Holdout Accuracy: %s (%s)", label, run_results$model_run_id),
      subtitle = sprintf("%d SKUs | median=%.2f, p90=%.2f (out-of-sample)", nrow(df), med, p90),
      x = paste(label, "(holdout)"), y = "Count") +
    ggplot2::theme_minimal(base_size = 11)
}

parallel_lab_plot_accuracy_comparison <- function(
    tasks_results, queue_results,
    metric = "mape", bins = 30,
    matched_only = TRUE
) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    .abort("Package 'ggplot2' is required for plotting.")
  }

  .extract <- function(run_results, label) {
    rows <- Filter(Negate(is.null), lapply(run_results$results, function(r) {
      if (inherits(r, "error")) return(NULL)
      data.frame(
        unit_id = as.character(r$unit_id),
        value   = r[[metric]] %||% NA_real_,
        stringsAsFactors = FALSE
      )
    }))
    if (length(rows) == 0) return(NULL)
    df <- do.call(rbind, rows)
    df <- df[!is.na(df$value), ]
    df$run <- label
    df
  }

  tasks_label <- sprintf("Tasks (%s)", tasks_results$model_run_id)
  queue_label <- sprintf("Queue (%s)", queue_results$model_run_id)

  df_tasks <- .extract(tasks_results, tasks_label)
  df_queue <- .extract(queue_results, queue_label)

  if (is.null(df_tasks) || is.null(df_queue)) {
    message("Insufficient data for comparison plot.")
    return(invisible(NULL))
  }

  if (isTRUE(matched_only)) {
    common_ids <- intersect(df_tasks$unit_id, df_queue$unit_id)
    df_tasks <- df_tasks[df_tasks$unit_id %in% common_ids, ]
    scope_note <- sprintf("%d matched SKUs", length(common_ids))
  } else {
    scope_note <- sprintf("Tasks: %d, Queue: %d SKUs", nrow(df_tasks), nrow(df_queue))
  }

  df <- rbind(df_tasks, df_queue)
  df$run <- factor(df$run, levels = c(tasks_label, queue_label))

  label <- toupper(metric)

  stats <- do.call(rbind, lapply(split(df, df$run), function(d) {
    data.frame(
      run    = d$run[1],
      med    = median(d$value, na.rm = TRUE),
      p90    = quantile(d$value, 0.9, na.rm = TRUE),
      stringsAsFactors = FALSE
    )
  }))

  ggplot2::ggplot(df, ggplot2::aes(x = value)) +
    ggplot2::geom_histogram(bins = bins, fill = "#2563eb", colour = "white", alpha = 0.85) +
    ggplot2::geom_vline(data = stats, ggplot2::aes(xintercept = med),
                        linetype = "dashed", colour = "#16a34a", linewidth = 0.7) +
    ggplot2::geom_vline(data = stats, ggplot2::aes(xintercept = p90),
                        linetype = "dotted", colour = "#dc2626", linewidth = 0.7) +
    ggplot2::geom_text(data = stats,
                       ggplot2::aes(x = med, y = Inf, label = sprintf("med=%.2f", med)),
                       vjust = 2, hjust = -0.1, size = 2.8, colour = "#16a34a") +
    ggplot2::geom_text(data = stats,
                       ggplot2::aes(x = p90, y = Inf, label = sprintf("p90=%.2f", p90)),
                       vjust = 3.5, hjust = -0.1, size = 2.8, colour = "#dc2626") +
    ggplot2::facet_wrap(~ run, ncol = 1, scales = "free_y") +
    ggplot2::labs(
      title = sprintf("Accuracy Comparison: Holdout %s (out-of-sample)", label),
      subtitle = scope_note,
      x = paste(label, "(holdout)"), y = "Count") +
    ggplot2::theme_minimal(base_size = 11)
}


# =============================================================================
# Model Registry helpers
# =============================================================================

parallel_lab_refresh_directory <- function(conn, cfg) {
  stg <- sprintf("%s.%s.%s", cfg$database, cfg$schemas$source_data,
                 cfg$dosnowflake_stage_name)
  snowflakeR::sfr_execute(conn, sprintf("ALTER STAGE %s REFRESH", stg))
  message(sprintf("Refreshed directory table for stage %s", stg))
}

parallel_lab_list_runs <- function(conn, cfg) {
  view <- sprintf("%s.%s.MODEL_INDEX", cfg$database, cfg$schemas$models)
  snowflakeR::sfr_query(conn, sprintf(
    "SELECT RUN_ID, COUNT(*) AS N_MODELS FROM %s GROUP BY RUN_ID ORDER BY RUN_ID",
    view
  ))
}

parallel_lab_register_models <- function(
    conn, cfg, model_run_id,
    model_name = NULL,
    version_name = NULL,
    forecast_h = 30L,
    description = NULL
) {
  if (is.null(model_name)) {
    model_name <- "PARALLEL_FORECAST_MODEL"
  }
  if (is.null(version_name)) {
    version_name <- model_run_id
  }

  parallel_lab_refresh_directory(conn, cfg)

  stage_prefix <- sprintf("@%s.%s.%s/",
                          cfg$database, cfg$schemas$source_data,
                          cfg$dosnowflake_stage_name)
  view_fqn <- sprintf("%s.%s.MODEL_INDEX", cfg$database, cfg$schemas$models)

  model_index <- snowflakeR::sfr_build_model_index(
    conn, view_fqn = view_fqn,
    run_id = model_run_id, stage_prefix = stage_prefix
  )

  n_models <- length(model_index)
  if (n_models == 0) {
    .abort("No model files found for run_id = %s", model_run_id)
  }
  message(sprintf("Found %d model files for run_id = %s", n_models, model_run_id))

  if (n_models < 200) {
    warning(sprintf(
      paste0("Only %d partition keys (<200). The Ray shuffle bug (SNOW-2193288) ",
             "may cause run_batch to fail. Consider using >= 200 unique ",
             "partition keys to be safe."),
      n_models
    ), call. = FALSE)
  }

  desc <- description %||% sprintf(
    "Many-model R forecast aggregator | %d partitions | run=%s | h=%d",
    n_models, model_run_id, forecast_h
  )

  reg <- snowflakeR::sfr_model_registry(
    conn, database = cfg$database, schema = cfg$schemas$models
  )

  result <- snowflakeR::sfr_log_many_model(
    reg,
    model_name       = model_name,
    version_name     = version_name,
    model_index      = model_index,
    partition_columns = "UNIT_ID",
    r_packages       = "forecast",
    horizon          = as.integer(forecast_h),
    comment          = desc
  )

  message(sprintf("Registered many-model aggregator: %s / %s (%d partitions, %.1fs)",
                  result$model_name, result$version_name,
                  result$n_partitions, result$registration_time_s))

  invisible(list(
    model_name   = result$model_name,
    version_name = result$version_name,
    n_models     = result$n_partitions,
    run_id       = model_run_id,
    method       = "sfr_log_many_model",
    reg_time_s   = result$registration_time_s
  ))
}


# =============================================================================
# Batch inference via Model Registry run_batch (server-side, partitioned)
# =============================================================================

parallel_lab_run_inference <- function(
    conn, cfg, model_run_id,
    model_name = NULL,
    version_name = NULL,
    forecast_h = 30L,
    unit_ids = NULL,
    output_table = NULL
) {
  if (is.null(model_name)) model_name <- "PARALLEL_FORECAST_MODEL"
  if (is.null(version_name)) version_name <- model_run_id

  series_table <- sprintf("%s.%s.SERIES_EVENTS",
                          cfg$database, cfg$schemas$source_data)

  if (is.null(output_table)) {
    output_table <- sprintf("%s.%s.FORECAST_RESULTS",
                            cfg$database, cfg$schemas$models)
  }

  # Build input DataFrame: one row per UNIT_ID + dummy SALES column
  # (the model doesn't use SALES — it loads .rds from stage and re-forecasts)
  if (is.null(unit_ids)) {
    unit_ids <- snowflakeR::sfr_query(conn, sprintf(
      "SELECT DISTINCT UNIT_ID FROM %s ORDER BY UNIT_ID",
      series_table
    ))$UNIT_ID
  }

  n_keys <- length(unit_ids)
  message(sprintf("Preparing batch inference: %d units, model=%s/%s",
                  n_keys, model_name, version_name))

  if (n_keys < 200) {
    warning(sprintf(
      paste0("Only %d partition keys (<200). The Ray shuffle bug (SNOW-2193288) ",
             "may cause run_batch to fail. Consider using >= 200 unique ",
             "partition keys."),
      n_keys
    ), call. = FALSE)
  }

  input_df <- data.frame(
    UNIT_ID = as.character(unit_ids),
    SALES   = rep(0.0, n_keys),
    stringsAsFactors = FALSE
  )

  run_ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
  output_stage <- sprintf("@%s.%s.%s/batch_inference/%s_%s/",
                          cfg$database, cfg$schemas$source_data,
                          cfg$dosnowflake_stage_name, version_name, run_ts)

  reg <- snowflakeR::sfr_model_registry(
    conn, database = cfg$database, schema = cfg$schemas$models
  )

  message(sprintf("Launching run_batch on pool %s (partition_column=UNIT_ID)...",
                  cfg$compute_pool))
  t0 <- Sys.time()

  result_df <- snowflakeR::sfr_run_batch(
    reg,
    model_name       = model_name,
    version_name     = version_name,
    new_data         = input_df,
    compute_pool     = cfg$compute_pool,
    function_name    = "predict",
    partition_column = "UNIT_ID",
    output_stage     = output_stage
  )

  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  message(sprintf("Batch inference complete: %.1fs", elapsed))

  if (is.data.frame(result_df) && nrow(result_df) > 0 &&
      !("status" %in% names(result_df) && result_df$status[1] %in%
        c("job_completed", "no_output_files") && nrow(result_df) == 1)) {

    uid_col <- if ("UNIT_ID" %in% names(result_df)) "UNIT_ID"
               else if ("PARTITION_KEY" %in% names(result_df)) "PARTITION_KEY"
               else names(result_df)[1]

    n_units <- length(unique(result_df[[uid_col]]))
    message(sprintf("Got %d result rows for %d units", nrow(result_df), n_units))

    snowflakeR::sfr_execute(conn, sprintf(
      paste0(
        "CREATE TABLE IF NOT EXISTS %s (",
        "RUN_ID VARCHAR, UNIT_ID VARCHAR, HORIZON INTEGER, ",
        "FORECAST_DATE DATE, POINT_FORECAST FLOAT, ",
        "LO_80 FLOAT, HI_80 FLOAT, LO_95 FLOAT, HI_95 FLOAT, ",
        "STATUS VARCHAR, ",
        "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
      ),
      output_table
    ))
    existing_cols <- tryCatch({
      snowflakeR::sfr_query(conn, sprintf(
        "SELECT COLUMN_NAME FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND TABLE_SCHEMA = '%s'",
        cfg$database, "FORECAST_RESULTS", cfg$schemas$models
      ))$COLUMN_NAME
    }, error = function(e) character(0))
    if (length(existing_cols) > 0 && !("LO_95" %in% existing_cols)) {
      message("Recreating FORECAST_RESULTS table with updated schema...")
      snowflakeR::sfr_execute(conn, sprintf("DROP TABLE IF EXISTS %s", output_table))
      snowflakeR::sfr_execute(conn, sprintf(
        paste0(
          "CREATE TABLE %s (",
          "RUN_ID VARCHAR, UNIT_ID VARCHAR, HORIZON INTEGER, ",
          "FORECAST_DATE DATE, POINT_FORECAST FLOAT, ",
          "LO_80 FLOAT, HI_80 FLOAT, LO_95 FLOAT, HI_95 FLOAT, ",
          "STATUS VARCHAR, ",
          "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())"
        ),
        output_table
      ))
    }

    .safe_num <- function(x) {
      if (is.null(x) || length(x) == 0) return("NULL")
      v <- suppressWarnings(as.numeric(x))
      if (is.na(v)) "NULL" else sprintf("%f", v)
    }
    .safe_int <- function(x) {
      if (is.null(x) || length(x) == 0) return("NULL")
      v <- suppressWarnings(as.integer(x))
      if (is.na(v)) "NULL" else as.character(v)
    }

    batch_size <- 500L
    n_rows <- nrow(result_df)
    n_inserted <- 0L

    for (start in seq(1L, n_rows, by = batch_size)) {
      end <- min(start + batch_size - 1L, n_rows)
      batch <- result_df[start:end, ]
      vals <- character(nrow(batch))
      for (j in seq_len(nrow(batch))) {
        r <- batch[j, , drop = FALSE]
        uid  <- gsub("'", "''", as.character(r[[uid_col]]))
        per  <- .safe_int(r[["PERIOD"]])
        fc   <- .safe_num(r[["FORECAST"]])
        lo80 <- .safe_num(r[["LOWER_80"]])
        hi80 <- .safe_num(r[["UPPER_80"]])
        lo95 <- .safe_num(r[["LOWER_95"]])
        hi95 <- .safe_num(r[["UPPER_95"]])
        st   <- gsub("'", "''", as.character(r[["STATUS"]] %||% "OK"))
        hzn  <- if (per == "NULL") "NULL" else per
        fdt  <- if (per == "NULL") "NULL"
                else sprintf("'%s'", as.character(
                  Sys.Date() + as.integer(per) * 30))
        vals[j] <- sprintf(
          "('%s','%s',%s,%s,%s,%s,%s,%s,%s,'%s')",
          gsub("'", "''", model_run_id), uid, hzn, fdt,
          fc, lo80, hi80, lo95, hi95, st
        )
      }
      sql <- sprintf(
        "INSERT INTO %s (RUN_ID,UNIT_ID,HORIZON,FORECAST_DATE,POINT_FORECAST,LO_80,HI_80,LO_95,HI_95,STATUS) VALUES %s",
        output_table, paste(vals, collapse = ",")
      )
      snowflakeR::sfr_execute(conn, sql)
      n_inserted <- n_inserted + nrow(batch)
    }
    message(sprintf("Inserted %d forecast rows into %s", n_inserted, output_table))
  } else {
    message("No results returned from run_batch. Check service logs.")
  }

  invisible(result_df)
}
