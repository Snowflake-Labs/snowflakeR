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
    )
  )
}

parallel_lab_sql_seed_series_events <- function(
    cfg,
    n_units = 120L,
    n_days = 365L,
    start_date = "2023-01-01"
) {
  n_units <- as.integer(n_units)
  n_days <- as.integer(n_days)
  if (n_units < 1L || n_days < 1L) .abort("n_units and n_days must be >= 1")

  tbl <- sprintf("%s.%s.SERIES_EVENTS", cfg$database, cfg$schemas$source_data)
  paste0(
    "CREATE OR REPLACE TABLE ", tbl, " AS ",
    "WITH units AS (",
    "  SELECT LPAD(TO_VARCHAR(SEQ4()), 6, '0') AS UNIT_ID ",
    "  FROM TABLE(GENERATOR(ROWCOUNT => ", n_units, "))",
    "), days AS (",
    "  SELECT SEQ4() AS d FROM TABLE(GENERATOR(ROWCOUNT => ", n_days, "))",
    ") ",
    "SELECT u.UNIT_ID, ",
    "       DATEADD('day', d.d, DATE '", start_date, "') AS OBS_DATE, ",
    "       10 + MOD(ABS(HASH(u.UNIT_ID, d.d)), 80) * 0.05 + SIN(d.d / 25.0) AS Y ",
    "FROM units u CROSS JOIN days d"
  )
}

parallel_lab_setup <- function(
    conn,
    cfg,
    create_series = isTRUE(cfg$create_synthetic_series_table),
    n_units = 120L,
    n_days = 365L
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
    snowflakeR::sfr_execute(conn, parallel_lab_sql_seed_series_events(cfg, n_units = n_units, n_days = n_days))
    message("Created SERIES_EVENTS synthetic table.")
  } else {
    message("Skipped SERIES_EVENTS creation (create_series = FALSE).")
  }

  snowflakeR::sfr_use(conn, database = cfg$database, schema = cfg$schemas$source_data)
}

parallel_lab_run_tasks_demo <- function(conn, cfg, run = FALSE) {
  if (!isTRUE(run)) {
    message("Dry-run only. Set run = TRUE to execute tasks demo.")
    return(invisible(NULL))
  }
  parallel_lab_validate_clean_room(cfg)

  if (!requireNamespace("foreach", quietly = TRUE) || !requireNamespace("forecast", quietly = TRUE)) {
    .abort("Packages 'foreach' and 'forecast' are required for demo execution.")
  }
  `%dopar%` <- foreach::`%dopar%`

  n_skus <- as.integer(cfg$demo_forecast_n_skus)
  n_chunks <- as.integer(cfg$demo_tasks_chunks_per_job)
  stage <- parallel_lab_stage_at(cfg)
  pool <- cfg$compute_pool
  img <- cfg$image_uri
  if (nzchar(cfg$warehouse %||% "")) {
    snowflakeR::sfr_execute(conn, sprintf("USE WAREHOUSE %s", cfg$warehouse))
  }
  snowflakeR::sfr_use(conn, database = cfg$database, schema = cfg$schemas$source_data)

  reg_args <- list(
    conn,
    mode = "tasks",
    compute_pool = pool,
    image_uri = img,
    stage = stage,
    chunks_per_job = n_chunks
  )
  if (nzchar(cfg$warehouse %||% "")) {
    reg_args$warehouse <- cfg$warehouse
  }
  do.call(snowflakeR::registerDoSnowflake, reg_args)

  skus <- sprintf("SKU_DEMO_%05d", seq_len(n_skus))
  t0 <- Sys.time()
  out <- foreach::foreach(
    sku = skus,
    .combine = list,
    .multicombine = TRUE,
    .packages = "forecast"
  ) %dopar% {
    suppressPackageStartupMessages(library(forecast))
    seed_str <- gsub("[^0-9]", "", sku)
    if (nchar(seed_str) == 0) seed_str <- "42"
    set.seed(as.integer(substr(seed_str, 1, 8)) %% 10000)
    n <- 36L
    trend <- seq(100, 200, length.out = n)
    seasonal <- 20 * sin(2 * pi * (1:n) / 12)
    noise <- rnorm(n, 0, 10)
    quantities <- pmax(round(trend + seasonal + noise), 1)
    ts_data <- ts(quantities, frequency = 12)
    model <- auto.arima(ts_data)
    fc <- forecast(model, h = 6)
    list(
      sku = sku,
      model = as.character(fc$method),
      forecast = as.numeric(fc$mean)
    )
  }

  dt <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  message(sprintf("tasks demo complete: %d forecasts in %.1fs", length(out), dt))
  message(sprintf("stage used: %s", stage))
  out
}

parallel_lab_run_queue_demo <- function(conn, cfg, run = FALSE) {
  if (!isTRUE(run)) {
    message("Dry-run only. Set run = TRUE to execute queue demo.")
    return(invisible(NULL))
  }
  parallel_lab_validate_clean_room(cfg)

  if (!requireNamespace("foreach", quietly = TRUE) || !requireNamespace("forecast", quietly = TRUE)) {
    .abort("Packages 'foreach' and 'forecast' are required for demo execution.")
  }
  `%dopar%` <- foreach::`%dopar%`

  n_skus <- as.integer(cfg$demo_forecast_n_skus)
  n_workers <- as.integer(cfg$demo_queue_n_workers)
  n_chunks <- as.integer(cfg$demo_queue_chunks_per_job)
  stage <- parallel_lab_stage_at(cfg)
  queue <- parallel_lab_queue_fqn(cfg)
  if (nzchar(cfg$warehouse %||% "")) {
    snowflakeR::sfr_execute(conn, sprintf("USE WAREHOUSE %s", cfg$warehouse))
  }
  snowflakeR::sfr_use(conn, database = cfg$database, schema = cfg$schemas$source_data)

  snowflakeR::registerDoSnowflake(
    conn,
    mode = "queue",
    compute_pool = cfg$compute_pool,
    image_uri = cfg$image_uri,
    stage = stage,
    queue_table = queue,
    n_workers = n_workers,
    chunks_per_job = n_chunks
  )

  skus <- sprintf("SKU_DEMO_%05d", seq_len(n_skus))
  t0 <- Sys.time()
  out <- foreach::foreach(
    sku = skus,
    .combine = list,
    .multicombine = TRUE,
    .packages = "forecast"
  ) %dopar% {
    suppressPackageStartupMessages(library(forecast))
    seed_str <- gsub("[^0-9]", "", sku)
    if (nchar(seed_str) == 0) seed_str <- "42"
    set.seed(as.integer(substr(seed_str, 1, 8)) %% 10000)
    n <- 36L
    trend <- seq(100, 200, length.out = n)
    seasonal <- 20 * sin(2 * pi * (1:n) / 12)
    noise <- rnorm(n, 0, 10)
    quantities <- pmax(round(trend + seasonal + noise), 1)
    ts_data <- ts(quantities, frequency = 12)
    model <- auto.arima(ts_data)
    fc <- forecast(model, h = 6)
    list(
      sku = sku,
      model = as.character(fc$method),
      forecast = as.numeric(fc$mean)
    )
  }

  dt <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  message(sprintf("queue demo complete: %d forecasts in %.1fs", length(out), dt))
  out
}
