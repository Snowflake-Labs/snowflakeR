# Unit tests for doSnowflake tasks mode (Phase 1)
# =============================================================================
# Tests for option resolution, DAG naming, and registration.
# Actual SPCS execution requires a live Snowflake environment.

skip_if_not_installed("foreach")
skip_if_not_installed("iterators")

library(foreach)

# Helper: mock connection
mock_conn <- function() {
  structure(list(
    environment = "test",
    account     = "test_account",
    database    = "TEST_DB",
    schema      = "PUBLIC",
    warehouse   = "test_wh",
    session     = NULL
  ), class = "sfr_connection")
}

# ---------------------------------------------------------------------------
# Option resolution
# ---------------------------------------------------------------------------

test_that(".resolve_task_options requires compute_pool", {
  data <- list(options = list(image_uri = "/db/schema/repo/worker:latest"))
  expect_error(
    snowflakeR:::.resolve_task_options(data),
    "compute_pool.*required"
  )
})

test_that(".resolve_task_options requires image_uri", {
  data <- list(options = list(compute_pool = "MY_POOL"))
  expect_error(
    snowflakeR:::.resolve_task_options(data),
    "image_uri.*required"
  )
})

test_that(".resolve_task_options merges defaults correctly", {
  data <- list(options = list(
    compute_pool = "MY_POOL",
    image_uri    = "/db/schema/repo/worker:latest"
  ))
  opts <- snowflakeR:::.resolve_task_options(data)

  expect_equal(opts$compute_pool, "MY_POOL")
  expect_equal(opts$image_uri, "/db/schema/repo/worker:latest")
  expect_equal(opts$stage, "DOSNOWFLAKE_STAGE")
  expect_equal(opts$timeout_min, 30)
  expect_equal(opts$poll_sec, 5)
  expect_equal(opts$chunks_per_job, "auto")
  expect_equal(opts$result_sync_wait_sec, 45)
  expect_equal(opts$result_sync_poll_sec, 3)
})

test_that(".resolve_task_options respects user overrides", {
  data <- list(options = list(
    compute_pool   = "MY_POOL",
    image_uri      = "/db/schema/repo/worker:latest",
    stage          = "CUSTOM_STAGE",
    timeout_min    = 60,
    poll_sec       = 10,
    chunks_per_job = 4L
  ))
  opts <- snowflakeR:::.resolve_task_options(data)

  expect_equal(opts$stage, "CUSTOM_STAGE")
  expect_equal(opts$timeout_min, 60)
  expect_equal(opts$poll_sec, 10)
  expect_equal(opts$chunks_per_job, 4L)
})

# ---------------------------------------------------------------------------
# Job ID generation
# ---------------------------------------------------------------------------

test_that(".generate_job_id returns a string", {
  id <- snowflakeR:::.generate_job_id()
  expect_type(id, "character")
  expect_true(nchar(id) > 0)
})

test_that(".generate_job_id returns unique values", {
  ids <- replicate(100, snowflakeR:::.generate_job_id())
  expect_equal(length(unique(ids)), 100L)
})

# ---------------------------------------------------------------------------
# Registration with tasks mode
# ---------------------------------------------------------------------------

test_that("registerDoSnowflake with tasks mode sets correct backend name", {
  conn <- mock_conn()
  suppressMessages(
    registerDoSnowflake(conn, mode = "tasks",
                        compute_pool = "POOL",
                        image_uri = "/db/schema/repo/img:latest")
  )
  expect_equal(foreach::getDoParName(), "doSnowflake")
})

test_that("registerDoSnowflake with tasks mode accepts all options", {
  conn <- mock_conn()
  expect_message(
    registerDoSnowflake(conn, mode = "tasks",
                        compute_pool = "POOL",
                        image_uri = "/db/schema/repo/img:latest",
                        stage = "MY_STAGE",
                        timeout_min = 60,
                        poll_sec = 10,
                        chunks_per_job = 5L),
    "tasks"
  )
})

# ---------------------------------------------------------------------------
# Tasks info function
# ---------------------------------------------------------------------------

test_that(".doSnowflakeTasksInfo returns correct metadata", {
  data <- list(options = list(chunks_per_job = 8L))

  expect_equal(snowflakeR:::.doSnowflakeTasksInfo(data, "name"), "doSnowflake")
  expect_equal(snowflakeR:::.doSnowflakeTasksInfo(data, "workers"), 8L)
  expect_true(nzchar(snowflakeR:::.doSnowflakeTasksInfo(data, "version")))
})

test_that(".doSnowflakeTasksInfo auto workers defaults to 10", {
  data <- list(options = list())
  expect_equal(snowflakeR:::.doSnowflakeTasksInfo(data, "workers"), 10L)
})

# ---------------------------------------------------------------------------
# Package install line generation
# ---------------------------------------------------------------------------

test_that(".generate_package_install_lines handles CRAN packages", {
  lines <- snowflakeR:::.generate_package_install_lines(
    list(cran = c("randomForest", "glmnet"))
  )
  expect_true(grepl("install.packages", lines))
  expect_true(grepl("randomForest", lines))
  expect_true(grepl("glmnet", lines))
})

test_that(".generate_package_install_lines handles pip packages", {
  lines <- snowflakeR:::.generate_package_install_lines(
    list(pip = c("nevergrad", "scikit-learn"))
  )
  expect_true(grepl("pip3 install", lines))
  expect_true(grepl("nevergrad", lines))
})

test_that(".generate_package_install_lines handles empty input", {
  lines <- snowflakeR:::.generate_package_install_lines(list())
  expect_equal(lines, "")
})
