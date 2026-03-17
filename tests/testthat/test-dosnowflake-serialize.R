# Unit tests for doSnowflake serialization layer
# =============================================================================
# These tests exercise the local serialization/deserialization round-trip
# without requiring a Snowflake connection.

# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

test_that(".chunk_iterations splits evenly", {
  arg_list <- lapply(1:10, function(i) list(i = i))
  chunks <- snowflakeR:::.chunk_iterations(arg_list, 2L)

  expect_length(chunks, 2L)
  expect_length(chunks[[1]], 5L)
  expect_length(chunks[[2]], 5L)
  # Round-robin: chunk 1 gets odd indices, chunk 2 gets even
  expect_equal(attr(chunks[[1]], "indices"), c(1L, 3L, 5L, 7L, 9L))
  expect_equal(attr(chunks[[2]], "indices"), c(2L, 4L, 6L, 8L, 10L))
})

test_that(".chunk_iterations handles uneven splits", {
  arg_list <- lapply(1:7, function(i) list(i = i))
  chunks <- snowflakeR:::.chunk_iterations(arg_list, 3L)

  expect_length(chunks, 3L)
  total_items <- sum(vapply(chunks, length, integer(1)))
  expect_equal(total_items, 7L)

  all_indices <- sort(unlist(lapply(chunks, attr, "indices")))
  expect_equal(all_indices, 1:7)
})

test_that(".chunk_iterations caps at n_tasks", {
  arg_list <- lapply(1:3, function(i) list(i = i))
  chunks <- snowflakeR:::.chunk_iterations(arg_list, 10L)

  expect_length(chunks, 3L)
})

test_that(".chunk_iterations handles single chunk", {
  arg_list <- lapply(1:5, function(i) list(i = i))
  chunks <- snowflakeR:::.chunk_iterations(arg_list, 1L)

  expect_length(chunks, 1L)
  expect_length(chunks[[1]], 5L)
  expect_equal(attr(chunks[[1]], "indices"), 1:5)
})

# ---------------------------------------------------------------------------
# Chunk count resolution
# ---------------------------------------------------------------------------

test_that(".resolve_n_chunks auto defaults to min(n, 10)", {
  expect_equal(snowflakeR:::.resolve_n_chunks(5, "auto"), 5L)
  expect_equal(snowflakeR:::.resolve_n_chunks(100, "auto"), 10L)
})

test_that(".resolve_n_chunks respects explicit integer", {
  expect_equal(snowflakeR:::.resolve_n_chunks(100, 4L), 4L)
  expect_equal(snowflakeR:::.resolve_n_chunks(3, 10L), 3L)
})

test_that(".resolve_n_chunks handles invalid values", {
  expect_equal(snowflakeR:::.resolve_n_chunks(10, 0L), 1L)
  expect_equal(snowflakeR:::.resolve_n_chunks(10, -1L), 1L)
  expect_equal(snowflakeR:::.resolve_n_chunks(10, NA), 1L)
})

# ---------------------------------------------------------------------------
# Local serialization round-trip
# ---------------------------------------------------------------------------

test_that("serialize/execute/collect round-trip produces correct results", {
  job_dir <- tempfile(pattern = "dosnowflake_test_")
  on.exit(unlink(job_dir, recursive = TRUE), add = TRUE)

  expr <- quote(i^2)
  arg_list <- lapply(1:10, function(i) list(i = i))

  job <- snowflakeR:::.serialize_job_local(
    job_dir, "test-uuid-001", expr, arg_list,
    n_chunks = 3L
  )

  expect_equal(job$n_chunks, 3L)
  expect_true(file.exists(file.path(job_dir, "export.rds")))
  expect_true(file.exists(file.path(job_dir, "expr.rds")))
  expect_true(file.exists(file.path(job_dir, "manifest.json")))

  task_files <- list.files(file.path(job_dir, "tasks"), pattern = "\\.rds$")
  expect_length(task_files, 3L)

  snowflakeR:::.execute_job_local(job_dir)

  result_files <- list.files(file.path(job_dir, "results"), pattern = "\\.rds$")
  expect_length(result_files, 3L)

  results <- snowflakeR:::.collect_results_local(job_dir, 3L)
  expect_length(results, 10L)
  expect_equal(unlist(results), (1:10)^2)
})

test_that("round-trip with exported variables works", {
  job_dir <- tempfile(pattern = "dosnowflake_test_")
  on.exit(unlink(job_dir, recursive = TRUE), add = TRUE)

  multiplier <- 100
  expr <- quote(i * multiplier)
  arg_list <- lapply(1:5, function(i) list(i = i))

  snowflakeR:::.serialize_job_local(
    job_dir, "test-uuid-002", expr, arg_list,
    export_list = list(multiplier = multiplier),
    n_chunks = 2L
  )

  snowflakeR:::.execute_job_local(job_dir)
  results <- snowflakeR:::.collect_results_local(job_dir, 2L)

  expect_equal(unlist(results), c(100, 200, 300, 400, 500))
})

test_that("round-trip with errors preserves error objects", {
  job_dir <- tempfile(pattern = "dosnowflake_test_")
  on.exit(unlink(job_dir, recursive = TRUE), add = TRUE)

  expr <- quote({
    if (i == 3) stop("boom")
    i
  })
  arg_list <- lapply(1:5, function(i) list(i = i))

  snowflakeR:::.serialize_job_local(
    job_dir, "test-uuid-003", expr, arg_list,
    n_chunks = 1L
  )

  snowflakeR:::.execute_job_local(job_dir)
  results <- snowflakeR:::.collect_results_local(job_dir, 1L)

  expect_length(results, 5L)
  expect_equal(results[[1]], 1)
  expect_equal(results[[2]], 2)
  expect_true(inherits(results[[3]], "error"))
  expect_equal(results[[4]], 4)
  expect_equal(results[[5]], 5)
})

# ---------------------------------------------------------------------------
# Manifest structure
# ---------------------------------------------------------------------------

test_that("manifest.json has correct structure", {
  job_dir <- tempfile(pattern = "dosnowflake_test_")
  on.exit(unlink(job_dir, recursive = TRUE), add = TRUE)

  expr <- quote(i + 1)
  arg_list <- lapply(1:3, function(i) list(i = i))

  snowflakeR:::.serialize_job_local(
    job_dir, "test-uuid-manifest", expr, arg_list,
    packages = c("stats", "randomForest"),
    n_chunks = 2L
  )

  manifest <- jsonlite::fromJSON(file.path(job_dir, "manifest.json"))
  expect_equal(manifest$job_id, "test-uuid-manifest")
  expect_equal(manifest$n_chunks, 2L)
  expect_equal(manifest$n_tasks, 3L)
  expect_true(nzchar(manifest$expr_text))
  expect_equal(manifest$packages, c("stats", "randomForest"))
  expect_true(nzchar(manifest$created_at))
})

# ---------------------------------------------------------------------------
# Stage path resolution
# ---------------------------------------------------------------------------

test_that(".resolve_stage_path handles @-prefixed paths", {
  mock_conn <- structure(list(database = "DB", schema = "SCH"),
                         class = "sfr_connection")
  path <- snowflakeR:::.resolve_stage_path(mock_conn, "@MY_DB.MY_SCHEMA.MY_STAGE", "abc-123")
  expect_equal(path, "@MY_DB.MY_SCHEMA.MY_STAGE/job_abc-123")
})

test_that(".resolve_stage_path builds FQN from connection context", {
  mock_conn <- structure(list(database = "TESTDB", schema = "PUBLIC"),
                         class = "sfr_connection")
  path <- snowflakeR:::.resolve_stage_path(mock_conn, "DOSNOWFLAKE_STAGE", "uuid-456")
  expect_equal(path, "@TESTDB.PUBLIC.DOSNOWFLAKE_STAGE/job_uuid-456")
})

# ---------------------------------------------------------------------------
# Multiple chunks preserve iteration order
# ---------------------------------------------------------------------------

test_that("results are correctly ordered across many chunks", {
  job_dir <- tempfile(pattern = "dosnowflake_test_")
  on.exit(unlink(job_dir, recursive = TRUE), add = TRUE)

  expr <- quote(i * 10)
  arg_list <- lapply(1:20, function(i) list(i = i))

  snowflakeR:::.serialize_job_local(
    job_dir, "test-order", expr, arg_list,
    n_chunks = 7L
  )

  snowflakeR:::.execute_job_local(job_dir)
  results <- snowflakeR:::.collect_results_local(job_dir, 7L)

  expect_length(results, 20L)
  expect_equal(unlist(results), (1:20) * 10)
})
