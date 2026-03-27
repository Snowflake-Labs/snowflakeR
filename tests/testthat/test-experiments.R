# Unit tests for experiment tracking (R/experiments.R)
# =============================================================================

mock_sfr_conn <- function() {
  structure(
    list(
      session = "fake_session",
      account = "test",
      database = "DB",
      schema = "SC",
      warehouse = "WH",
      auth_method = "test",
      environment = "test",
      created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )
}


test_that("sfr_experiment creates correct S3 object and fields", {
  local_mocked_bindings(
    sfr_requires_ml = function(min, feature) invisible(TRUE),
    get_bridge_module = function(module_name) {
      list(set_experiment = function(...) invisible(NULL))
    },
    .package = "snowflakeR"
  )

  conn <- mock_sfr_conn()
  exp <- sfr_experiment(conn, "MY_EXP", database = "E_DB", schema = "E_SC")

  expect_s3_class(exp, "sfr_experiment")
  expect_equal(exp$name, "MY_EXP")
  expect_equal(exp$database, "E_DB")
  expect_equal(exp$schema, "E_SC")
  expect_identical(exp$conn, conn)
})


test_that("print.sfr_experiment outputs expected class and name", {
  local_mocked_bindings(
    sfr_requires_ml = function(min, feature) invisible(TRUE),
    get_bridge_module = function(module_name) {
      list(set_experiment = function(...) invisible(NULL))
    },
    .package = "snowflakeR"
  )

  conn <- mock_sfr_conn()
  exp <- sfr_experiment(conn, "PRINT_EXP")
  expect_message(print(exp), "sfr_experiment")
  expect_message(print(exp), "PRINT_EXP")
})


test_that("sfr_start_run validates experiment class", {
  local_mocked_bindings(
    get_bridge_module = function(module_name) {
      list(start_run = function(...) invisible(NULL))
    },
    .package = "snowflakeR"
  )

  expect_error(
    sfr_start_run("not_an_experiment"),
    "sfr_experiment"
  )
})


test_that("sfr_end_run validates experiment class", {
  local_mocked_bindings(
    get_bridge_module = function(module_name) {
      list(end_run = function(...) invisible(NULL))
    },
    .package = "snowflakeR"
  )

  expect_error(
    sfr_end_run(list()),
    "sfr_experiment"
  )
})


test_that("sfr_exp_log_param validates experiment class", {
  local_mocked_bindings(
    get_bridge_module = function(module_name) {
      list(log_param = function(...) invisible(NULL))
    },
    .package = "snowflakeR"
  )

  expect_error(
    sfr_exp_log_param(1L, "k", 1),
    "sfr_experiment"
  )
})


test_that("sfr_exp_log_params converts ... to named list for bridge", {
  captured <- NULL
  local_mocked_bindings(
    sfr_requires_ml = function(min, feature) invisible(TRUE),
    get_bridge_module = function(module_name) {
      list(
        set_experiment = function(...) invisible(NULL),
        log_params = function(session, params_dict) {
          captured <<- params_dict
          invisible(NULL)
        }
      )
    },
    .package = "snowflakeR"
  )

  conn <- mock_sfr_conn()
  exp <- sfr_experiment(conn, "E")
  sfr_exp_log_params(exp, alpha = 0.1, beta = 2L)

  expect_type(captured, "list")
  expect_equal(captured$alpha, 0.1)
  expect_equal(captured$beta, 2L)
})


test_that("sfr_exp_log_metric validates experiment class", {
  local_mocked_bindings(
    get_bridge_module = function(module_name) {
      list(log_metric = function(...) invisible(NULL))
    },
    .package = "snowflakeR"
  )

  expect_error(
    sfr_exp_log_metric(NULL, "loss", 0.5),
    "sfr_experiment"
  )
})


test_that("sfr_experiment_from_tune rejects non-experiment exp", {
  fake_tune <- structure(
    list(),
    class = c("tune_results", "tbl_df", "tbl", "data.frame")
  )

  expect_error(
    sfr_experiment_from_tune("bad", fake_tune),
    "sfr_experiment"
  )
})
