# Unit tests for doSnowflake Queue Backend
# =============================================================================

skip_if_not_installed("foreach")
skip_if_not_installed("iterators")

library(foreach)

mock_conn <- function() {
  structure(list(
    environment = "test",
    account     = "test_account",
    database    = "test_db",
    schema      = "CONFIG",
    warehouse   = "test_wh",
    session     = NULL
  ), class = "sfr_connection")
}

# ---------------------------------------------------------------------------
# sfr_parcel_iterator
# ---------------------------------------------------------------------------

test_that("sfr_parcel_iterator creates correct number of parcels", {
  keys <- paste0("SKU_", 1:200)
  parcels <- sfr_parcel_iterator(keys, parcel_size = 50)
  expect_equal(length(parcels), 4L)
  expect_equal(length(parcels[[1]]), 50L)
  expect_equal(length(parcels[[4]]), 50L)
})

test_that("sfr_parcel_iterator handles remainder correctly", {
  keys <- paste0("SKU_", 1:73)
  parcels <- sfr_parcel_iterator(keys, parcel_size = 25)
  expect_equal(length(parcels), 3L)
  expect_equal(length(parcels[[1]]), 25L)
  expect_equal(length(parcels[[2]]), 25L)
  expect_equal(length(parcels[[3]]), 23L)
})

test_that("sfr_parcel_iterator handles empty input", {
  parcels <- sfr_parcel_iterator(character(0), parcel_size = 10)
  expect_equal(length(parcels), 0L)
})

test_that("sfr_parcel_iterator handles single-element input", {
  parcels <- sfr_parcel_iterator("SKU_1", parcel_size = 10)
  expect_equal(length(parcels), 1L)
  expect_equal(parcels[[1]], "SKU_1")
})

test_that("sfr_parcel_iterator preserves all keys", {
  keys <- paste0("SKU_", 1:100)
  parcels <- sfr_parcel_iterator(keys, parcel_size = 30)
  recovered <- unlist(parcels)
  expect_equal(recovered, keys)
})

test_that("sfr_parcel_iterator handles parcel_size larger than keys", {
  keys <- paste0("SKU_", 1:5)
  parcels <- sfr_parcel_iterator(keys, parcel_size = 100)
  expect_equal(length(parcels), 1L)
  expect_equal(parcels[[1]], keys)
})

# ---------------------------------------------------------------------------
# Queue option resolution
# ---------------------------------------------------------------------------

test_that(".resolve_queue_options errors for ephemeral without compute_pool", {
  data <- list(options = list(worker_type = "ephemeral"))
  expect_error(snowflakeR:::.resolve_queue_options(data), "compute_pool")
})

test_that(".resolve_queue_options errors for ephemeral without image_uri", {
  data <- list(options = list(
    worker_type = "ephemeral",
    compute_pool = "MY_POOL"
  ))
  expect_error(snowflakeR:::.resolve_queue_options(data), "image_uri")
})

test_that(".resolve_queue_options succeeds for persistent without compute_pool", {
  data <- list(options = list(worker_type = "persistent"))
  opts <- snowflakeR:::.resolve_queue_options(data)
  expect_equal(opts$worker_type, "persistent")
  expect_equal(opts$compute_pool, "")
})

test_that(".resolve_queue_options sets defaults correctly", {
  data <- list(options = list(
    worker_type  = "ephemeral",
    compute_pool = "MY_POOL",
    image_uri    = "/db/schema/repo/worker:latest"
  ))
  opts <- snowflakeR:::.resolve_queue_options(data)
  expect_equal(opts$queue_fqn, "CONFIG.DOSNOWFLAKE_QUEUE")
  expect_equal(opts$stage, "DOSNOWFLAKE_STAGE")
  expect_equal(opts$n_workers, 4L)
  expect_equal(opts$timeout_min, 30)
  expect_equal(opts$poll_sec, 5)
  expect_equal(opts$stale_timeout_sec, 600)
  expect_equal(opts$chunks_per_job, "auto")
})

test_that(".resolve_queue_options rejects invalid worker_type", {
  data <- list(options = list(worker_type = "invalid"))
  expect_error(snowflakeR:::.resolve_queue_options(data), "ephemeral")
})

test_that(".resolve_queue_options respects user overrides", {
  data <- list(options = list(
    worker_type  = "ephemeral",
    compute_pool = "MY_POOL",
    image_uri    = "/db/schema/repo/worker:latest",
    n_workers    = 8,
    queue_fqn    = "CUSTOM.MY_QUEUE",
    timeout_min  = 60,
    poll_sec     = 10,
    stale_timeout_sec = 300,
    chunks_per_job = 20
  ))
  opts <- snowflakeR:::.resolve_queue_options(data)
  expect_equal(opts$n_workers, 8L)
  expect_equal(opts$queue_fqn, "CUSTOM.MY_QUEUE")
  expect_equal(opts$timeout_min, 60)
  expect_equal(opts$poll_sec, 10)
  expect_equal(opts$stale_timeout_sec, 300)
  expect_equal(opts$chunks_per_job, 20)
})

# ---------------------------------------------------------------------------
# Queue info function
# ---------------------------------------------------------------------------

test_that(".doSnowflakeQueueInfo returns correct worker count", {
  data <- list(options = list(n_workers = 6))
  expect_equal(snowflakeR:::.doSnowflakeQueueInfo(data, "workers"), 6L)
})

test_that(".doSnowflakeQueueInfo returns name", {
  data <- list(options = list())
  expect_equal(snowflakeR:::.doSnowflakeQueueInfo(data, "name"), "doSnowflake")
})

test_that(".doSnowflakeQueueInfo returns version", {
  data <- list(options = list())
  v <- snowflakeR:::.doSnowflakeQueueInfo(data, "version")
  expect_true(nzchar(v))
})

# ---------------------------------------------------------------------------
# Registration with queue mode
# ---------------------------------------------------------------------------

test_that("registerDoSnowflake registers queue backend with persistent workers", {
  conn <- mock_conn()
  expect_message(
    registerDoSnowflake(conn, mode = "queue",
                        worker_type = "persistent")
  )
  expect_equal(foreach::getDoParName(), "doSnowflake")
})

test_that("registerDoSnowflake errors for queue+ephemeral without required args", {
  conn <- mock_conn()
  expect_error(
    suppressMessages(
      foreach::foreach(i = 1:2, .combine = c) %dopar% i
    )
  )
})
