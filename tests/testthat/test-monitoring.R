# Model monitoring (registry_monitoring.R)
# =============================================================================

mock_conn <- function() {
  structure(
    list(
      session = "fake_session",
      account = "test",
      database = "DB",
      schema = "SC",
      warehouse = "WH",
      auth_method = "test",
      environment = "test",
      created_at = Sys.time(),
      dbi_con = NULL
    ),
    class = c("sfr_connection", "list")
  )
}


# -- constructors -------------------------------------------------------------

test_that("sfr_monitor_source has correct class and fields", {
  src <- sfr_monitor_source(
    "DB.SC.TBL",
    "TS",
    prediction_score_columns = c("P1", "P2"),
    actual_score_columns = "A1",
    id_columns = "ID"
  )
  expect_s3_class(src, "sfr_monitor_source")
  expect_equal(src$source, "DB.SC.TBL")
  expect_equal(src$timestamp_column, "TS")
  expect_equal(src$prediction_score_columns, c("P1", "P2"))
  expect_equal(src$actual_score_columns, "A1")
  expect_equal(src$id_columns, "ID")
})


test_that("sfr_monitor_config has correct class and fields", {
  cfg <- sfr_monitor_config("M1", "v2", warehouse = "ML_WH", function_name = "score")
  expect_s3_class(cfg, "sfr_monitor_config")
  expect_equal(cfg$model_name, "M1")
  expect_equal(cfg$version_name, "v2")
  expect_equal(cfg$warehouse, "ML_WH")
  expect_equal(cfg$function_name, "score")
  expect_equal(cfg$aggregation_window, "1 day")
})


test_that("print.sfr_monitor_source and print.sfr_monitor_config run", {
  src <- sfr_monitor_source("T", "TS", "P")
  cfg <- sfr_monitor_config("M", "v1", warehouse = "W")
  expect_message(print(src), "sfr_monitor_source")
  expect_message(print(cfg), "sfr_monitor_config")
})


# -- SQL: drift / performance / stats ----------------------------------------

test_that("sfr_monitor_drift builds expected SQL", {
  sql_seen <- NULL
  local_mocked_bindings(
    sfr_query = function(conn, sql, .keep_case = TRUE) {
      sql_seen <<- sql
      data.frame()
    },
    .package = "snowflakeR"
  )

  sfr_monitor_drift(
    mock_conn(),
    "MY_MON",
    "JENSEN_SHANNON",
    "MODEL_PREDICTION",
    granularity = "DAY",
    start_time = as.Date("2024-01-01"),
    end_time = as.Date("2024-01-02")
  )

  expect_true(grepl("MODEL_MONITOR_DRIFT_METRIC", sql_seen))
  expect_true(grepl("'MY_MON'", sql_seen))
  expect_true(grepl("'JENSEN_SHANNON'", sql_seen))
  expect_true(grepl("'MODEL_PREDICTION'", sql_seen))
  expect_true(grepl("'1 DAY'", sql_seen))
})


test_that("sfr_monitor_performance builds expected SQL", {
  sql_seen <- NULL
  local_mocked_bindings(
    sfr_query = function(conn, sql, .keep_case = TRUE) {
      sql_seen <<- sql
      data.frame()
    },
    .package = "snowflakeR"
  )

  sfr_monitor_performance(
    mock_conn(),
    "MY_MON",
    "RMSE",
    granularity = "DAY",
    start_time = "CURRENT_DATE()",
    end_time = "CURRENT_DATE()"
  )

  expect_true(grepl("MODEL_MONITOR_PERFORMANCE_METRIC", sql_seen))
  expect_true(grepl("'MY_MON'", sql_seen))
  expect_true(grepl("'RMSE'", sql_seen))
})


test_that("sfr_monitor_stats builds MODEL_MONITOR_STAT_METRIC SQL", {
  sql_seen <- NULL
  local_mocked_bindings(
    sfr_query = function(conn, sql, .keep_case = TRUE) {
      sql_seen <<- sql
      data.frame()
    },
    .package = "snowflakeR"
  )

  sfr_monitor_stats(
    mock_conn(),
    "MY_MON",
    "COUNT",
    "SCORE",
    start_time = as.Date("2024-01-01"),
    end_time = as.Date("2024-01-02")
  )

  expect_true(grepl("MODEL_MONITOR_STAT_METRIC", sql_seen))
  expect_true(grepl("'COUNT'", sql_seen))
  expect_true(grepl("'SCORE'", sql_seen))
})


test_that("sfr_monitor_drift includes segment JSON when segment is set", {
  sql_seen <- NULL
  local_mocked_bindings(
    sfr_query = function(conn, sql, .keep_case = TRUE) {
      sql_seen <<- sql
      data.frame()
    },
    .package = "snowflakeR"
  )

  sfr_monitor_drift(
    mock_conn(),
    "MY_MON",
    "WASSERSTEIN",
    "X",
    start_time = as.Date("2024-01-01"),
    end_time = as.Date("2024-01-02"),
    segment = list(column = "REGION", value = "US")
  )

  expect_true(grepl("SEGMENTS", sql_seen))
  expect_true(grepl("REGION", sql_seen))
  expect_true(grepl("US", sql_seen))
})


# -- suspend / resume ----------------------------------------------------------

test_that("sfr_suspend_monitor and sfr_resume_monitor build ALTER SQL", {
  ex_sql <- character()
  local_mocked_bindings(
    sfr_execute = function(conn, sql) {
      ex_sql <<- c(ex_sql, sql)
      invisible(TRUE)
    },
    .package = "snowflakeR"
  )

  sfr_suspend_monitor(mock_conn(), "MY_MON")
  sfr_resume_monitor(mock_conn(), "MY_MON")

  expect_equal(ex_sql[1], "ALTER MODEL MONITOR MY_MON SUSPEND")
  expect_equal(ex_sql[2], "ALTER MODEL MONITOR MY_MON RESUME")
})


# -- sfr_add_monitor ------------------------------------------------------------

test_that("sfr_add_monitor rejects invalid reg", {
  src <- sfr_monitor_source("T", "TS", "P")
  cfg <- sfr_monitor_config("M", "v1", warehouse = "W")
  expect_error(
    sfr_add_monitor("not_reg", "MON", src, cfg),
    "sfr_connection"
  )
})


test_that("sfr_add_monitor rejects wrong config types", {
  mock_reg <- mock_conn()
  cfg <- sfr_monitor_config("M", "v1", warehouse = "W")
  expect_error(
    sfr_add_monitor(mock_reg, "MON", list(a = 1), cfg),
    "sfr_monitor_source"
  )
})


test_that("sfr_add_monitor calls sfr_requires_ml and bridge", {
  req_n <- 0L
  bridge_called <- FALSE
  local_mocked_bindings(
    sfr_requires_ml = function(min, feature) {
      req_n <<- req_n + 1L
      invisible(TRUE)
    },
    get_bridge_module = function(name) {
      list(
        registry_add_monitor = function(...) {
          bridge_called <<- TRUE
          list(name = "MON", status = "created")
        }
      )
    },
    .package = "snowflakeR"
  )

  src <- sfr_monitor_source("T", "TS", "P")
  cfg <- sfr_monitor_config("M", "v1", warehouse = "W")
  expect_message(
    sfr_add_monitor(mock_conn(), "MON", src, cfg),
    "MON"
  )
  expect_equal(req_n, 1L)
  expect_true(bridge_called)
})


# -- sfr_monitor_to_vetiver -----------------------------------------------------

test_that("sfr_monitor_to_vetiver drift dispatch returns vetiver-like columns", {
  local_mocked_bindings(
    sfr_query = function(conn, sql, .keep_case = TRUE) {
      data.frame(
        EVENT_TIMESTAMP = as.POSIXct("2024-06-01", tz = "UTC"),
        METRIC_VALUE = 0.42,
        METRIC_NAME = "JENSEN_SHANNON",
        stringsAsFactors = FALSE
      )
    },
    .package = "snowflakeR"
  )

  out <- sfr_monitor_to_vetiver(
    mock_conn(),
    "MY_MON",
    "JENSEN_SHANNON",
    metric_type = "drift",
    start_time = Sys.Date(),
    end_time = Sys.Date(),
    column = "FEAT"
  )
  expect_named(out, c(".index", ".metric", ".estimate"))
  expect_s3_class(out, "tbl_df")
  expect_equal(out$.estimate, 0.42)
})


test_that("sfr_monitor_to_vetiver performance dispatch returns vetiver-like columns", {
  local_mocked_bindings(
    sfr_query = function(conn, sql, .keep_case = TRUE) {
      data.frame(
        EVENT_TIMESTAMP = as.POSIXct("2024-06-01", tz = "UTC"),
        METRIC_VALUE = 1.1,
        METRIC_NAME = "RMSE",
        stringsAsFactors = FALSE
      )
    },
    .package = "snowflakeR"
  )

  out <- sfr_monitor_to_vetiver(
    mock_conn(),
    "MY_MON",
    "RMSE",
    metric_type = "performance",
    start_time = Sys.Date(),
    end_time = Sys.Date()
  )
  expect_named(out, c(".index", ".metric", ".estimate"))
  expect_equal(out$.metric, "RMSE")
})


test_that("sfr_monitor_to_vetiver stats dispatch returns vetiver-like columns", {
  local_mocked_bindings(
    sfr_query = function(conn, sql, .keep_case = TRUE) {
      data.frame(
        EVENT_TIMESTAMP = as.POSIXct("2024-06-01", tz = "UTC"),
        METRIC_VALUE = 100,
        METRIC_NAME = "COUNT",
        stringsAsFactors = FALSE
      )
    },
    .package = "snowflakeR"
  )

  out <- sfr_monitor_to_vetiver(
    mock_conn(),
    "MY_MON",
    "COUNT",
    metric_type = "stats",
    start_time = Sys.Date(),
    end_time = Sys.Date(),
    column = "C"
  )
  expect_named(out, c(".index", ".metric", ".estimate"))
  expect_equal(out$.metric, "COUNT")
})


# -- sfr_vetiver_to_metrics ----------------------------------------------------

test_that("sfr_vetiver_to_metrics validates required columns", {
  bad_df <- data.frame(x = 1, y = 2)
  mock_conn <- structure(
    list(session = NULL, account = "test", database = "DB",
         schema = "SC", warehouse = "WH", auth_method = "test",
         environment = "test", created_at = Sys.time()),
    class = c("sfr_connection", "list")
  )
  expect_error(
    sfr_vetiver_to_metrics(mock_conn, "M", "v1", bad_df),
    "missing required columns"
  )
})

test_that("sfr_vetiver_to_metrics calls sfr_set_model_metric for each row", {
  mock_conn <- structure(
    list(session = NULL, account = "test", database = "DB",
         schema = "SC", warehouse = "WH", auth_method = "test",
         environment = "test", created_at = Sys.time()),
    class = c("sfr_connection", "list")
  )

  metrics_df <- data.frame(
    .index = as.Date(c("2026-01-01", "2026-01-02")),
    .metric = c("rmse", "rmse"),
    .estimator = c("standard", "standard"),
    .estimate = c(0.5, 0.6)
  )

  call_count <- 0L
  local_mocked_bindings(
    sfr_set_model_metric = function(...) { call_count <<- call_count + 1L },
    .package = "snowflakeR"
  )

  n <- sfr_vetiver_to_metrics(mock_conn, "M", "v1", metrics_df)
  expect_equal(n, 2L)
  expect_equal(call_count, 2L)
})
