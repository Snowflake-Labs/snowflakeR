# Unit tests for version compatibility infrastructure (R/version.R)

test_that("sfr_ml_version returns NULL when Python not available", {
  local_mocked_bindings(
    import = function(...) stop("no python"),
    .package = "reticulate"
  )
  expect_null(sfr_ml_version())
})


test_that("sfr_ml_version returns numeric_version when available", {
  mock_md <- list(version = function(pkg) "1.27.0")
  local_mocked_bindings(
    import = function(...) mock_md,
    .package = "reticulate"
  )
  v <- sfr_ml_version()
  expect_s3_class(v, "numeric_version")
  expect_equal(as.character(v), "1.27.0")
})


test_that("sfr_requires_ml passes when version sufficient", {
  local_mocked_bindings(
    sfr_ml_version = function() numeric_version("1.27.0")
  )
  expect_invisible(sfr_requires_ml("1.5.0", "Core features"))
  expect_true(sfr_requires_ml("1.27.0", "Latest feature"))
})


test_that("sfr_requires_ml aborts when version too old", {
  local_mocked_bindings(
    sfr_ml_version = function() numeric_version("1.5.0")
  )
  expect_error(
    sfr_requires_ml("1.24.0", "Tile-based aggregation"),
    "1.24.0"
  )
})


test_that("sfr_requires_ml aborts when not installed", {
  local_mocked_bindings(
    sfr_ml_version = function() NULL
  )
  expect_error(
    sfr_requires_ml("1.7.1", "Model Monitoring"),
    "not installed"
  )
})


test_that("sfr_check_features returns data.frame with expected columns", {
  local_mocked_bindings(
    sfr_ml_version = function() numeric_version("1.19.0")
  )
  df <- sfr_check_features()
  expect_s3_class(df, "data.frame")
  expect_true(all(c("feature", "min_version", "available", "installed") %in% names(df)))
  expect_equal(df$installed[1], "1.19.0")
})


test_that("sfr_check_features marks availability correctly", {
  local_mocked_bindings(
    sfr_ml_version = function() numeric_version("1.19.0")
  )
  df <- sfr_check_features()

  core_row <- df[df$feature == "Core Feature Store + Model Registry", ]
  expect_true(core_row$available)

  tile_row <- df[df$feature == "Tile-based aggregation", ]
  expect_false(tile_row$available)
})


test_that("sfr_check_features handles no Python", {
  local_mocked_bindings(
    sfr_ml_version = function() NULL
  )
  df <- sfr_check_features()
  expect_true(all(!df$available))
  expect_equal(df$installed[1], "not installed")
})
