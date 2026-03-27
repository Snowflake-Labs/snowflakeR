# Unit tests for Feature Store online serving (R/features_online.R)

test_that("sfr_online_config creates correct S3 object", {
  local_mocked_bindings(sfr_requires_ml = function(...) invisible(TRUE))

  oc <- sfr_online_config(enable = TRUE, target_lag = "2 minutes")
  expect_s3_class(oc, "sfr_online_config")
  expect_true(oc$enable)
  expect_equal(oc$target_lag, "2 minutes")
})


test_that("sfr_online_config validates inputs", {
  local_mocked_bindings(sfr_requires_ml = function(...) invisible(TRUE))

  expect_error(sfr_online_config(enable = "yes"))
  expect_error(sfr_online_config(target_lag = 123))
})


test_that("print.sfr_online_config outputs expected format", {
  local_mocked_bindings(sfr_requires_ml = function(...) invisible(TRUE))

  oc <- sfr_online_config()
  expect_message(print(oc), "sfr_online_config")
  expect_message(print(oc), "enabled")
})


test_that("sfr_update_feature_view validates feature store", {
  expect_error(sfr_update_feature_view(list(), "FV", "v1"), "sfr_feature_store")
})


test_that("sfr_update_feature_view validates online_config class", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )
  fs <- sfr_feature_store(mock_conn)

  expect_error(
    sfr_update_feature_view(fs, "FV", "v1", online_config = list(enable = TRUE)),
    "sfr_online_config"
  )
})
