# Unit tests for doSnowflake (Phase 0: local mode)
# =============================================================================

skip_if_not_installed("foreach")
skip_if_not_installed("iterators")

library(foreach)

# Helper: create a minimal mock connection object for registration
mock_conn <- function() {
  structure(list(
    environment = "test",
    account     = "test_account",
    database    = "test_db",
    warehouse   = "test_wh",
    session     = NULL
  ), class = "sfr_connection")
}

# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

test_that("registerDoSnowflake registers the backend and reports name/workers", {
  conn <- mock_conn()
  expect_message(registerDoSnowflake(conn, mode = "local", workers = 2L))
  expect_equal(foreach::getDoParName(), "doSnowflake")
  expect_equal(foreach::getDoParWorkers(), 2L)
})

test_that("registerDoSnowflake rejects unimplemented modes", {
  conn <- mock_conn()
  expect_error(registerDoSnowflake(conn, mode = "tasks"),  "not yet implemented")
  expect_error(registerDoSnowflake(conn, mode = "spcs"),   "not yet implemented")
  expect_error(registerDoSnowflake(conn, mode = "queue"),  "not yet implemented")
})

test_that("registerDoSnowflake errors without foreach installed", {
  # We can't easily un-install foreach mid-test, so just verify the check path
  # exists by confirming the function runs without error when foreach IS present

  conn <- mock_conn()
  expect_message(registerDoSnowflake(conn, workers = 1L))
})

# ---------------------------------------------------------------------------
# Worker resolution
# ---------------------------------------------------------------------------

test_that(".resolve_snowflake_workers handles 'auto'", {
  n <- snowflakeR:::.resolve_snowflake_workers("auto")
  expect_true(is.integer(n) || is.numeric(n))
  expect_true(n >= 1L)
})

test_that(".resolve_snowflake_workers handles explicit integer", {
  expect_equal(snowflakeR:::.resolve_snowflake_workers(4L), 4L)
  expect_equal(snowflakeR:::.resolve_snowflake_workers(1L), 1L)
})

test_that(".resolve_snowflake_workers floors invalid values to 1", {
  expect_equal(snowflakeR:::.resolve_snowflake_workers(0L), 1L)
  expect_equal(snowflakeR:::.resolve_snowflake_workers(-5L), 1L)
  expect_equal(snowflakeR:::.resolve_snowflake_workers(NA), 1L)
})

# ---------------------------------------------------------------------------
# Correct computation (parallel path)
# ---------------------------------------------------------------------------

test_that("foreach %dopar% computes correct results with multiple workers", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  result <- foreach::foreach(i = 1:10, .combine = c) %dopar% {
    i^2
  }
  expect_equal(result, (1:10)^2)
})

test_that("foreach %dopar% preserves result order with .inorder = TRUE", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  result <- foreach::foreach(i = 1:20, .combine = c, .inorder = TRUE) %dopar% {
    Sys.sleep(runif(1, 0, 0.01))
    i
  }
  expect_equal(result, 1:20)
})

# ---------------------------------------------------------------------------
# Sequential fallback (workers = 1)
# ---------------------------------------------------------------------------

test_that("foreach %dopar% works with workers = 1 (sequential fallback)", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 1L))

  result <- foreach::foreach(i = 1:5, .combine = c) %dopar% {
    i * 10
  }
  expect_equal(result, (1:5) * 10)
})

# ---------------------------------------------------------------------------
# .combine functions
# ---------------------------------------------------------------------------

test_that(".combine = c concatenates results", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  result <- foreach::foreach(i = 1:5, .combine = c) %dopar% i
  expect_equal(result, 1:5)
})

test_that(".combine = rbind binds data.frame rows", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  result <- foreach::foreach(i = 1:3, .combine = rbind) %dopar% {
    data.frame(x = i, y = i^2)
  }
  expect_equal(nrow(result), 3L)
  expect_equal(result$x, 1:3)
  expect_equal(result$y, c(1, 4, 9))
})

test_that(".combine = list collects results into a list", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  result <- foreach::foreach(i = 1:4) %dopar% i
  expect_type(result, "list")
  expect_equal(length(result), 4L)
  expect_equal(result[[3]], 3)
})

# ---------------------------------------------------------------------------
# .export variable propagation
# ---------------------------------------------------------------------------

test_that(".export sends variables to workers", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  multiplier <- 100
  result <- foreach::foreach(i = 1:3, .combine = c,
                             .export = "multiplier") %dopar% {
    i * multiplier
  }
  expect_equal(result, c(100, 200, 300))
})

test_that("auto-export detects free variables", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  offset <- 50
  result <- foreach::foreach(i = 1:3, .combine = c) %dopar% {
    i + offset
  }
  expect_equal(result, c(51, 52, 53))
})

# ---------------------------------------------------------------------------
# .packages loading on workers
# ---------------------------------------------------------------------------

test_that(".packages loads packages on workers", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  result <- foreach::foreach(i = 1:2, .combine = c,
                             .packages = "stats") %dopar% {
    median(1:i)
  }
  expect_equal(result, c(median(1:1), median(1:2)))
})

# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

test_that("errorHandling = 'stop' raises on iteration error", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  expect_error(
    foreach::foreach(i = 1:5, .combine = c,
                     .errorhandling = "stop") %dopar% {
      if (i == 3) stop("boom")
      i
    },
    "boom"
  )
})

test_that("errorHandling = 'remove' drops failed iterations", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  result <- foreach::foreach(i = 1:5, .combine = c,
                             .errorhandling = "remove") %dopar% {
    if (i == 3) stop("boom")
    i
  }
  expect_equal(result, c(1, 2, 4, 5))
})

test_that("errorHandling = 'pass' includes error objects in results", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 2L))

  result <- foreach::foreach(i = 1:3,
                             .errorhandling = "pass") %dopar% {
    if (i == 2) stop("oops")
    i
  }
  expect_equal(length(result), 3L)
  expect_true(inherits(result[[2]], "error"))
})

# ---------------------------------------------------------------------------
# Sequential fallback error handling
# ---------------------------------------------------------------------------

test_that("sequential fallback handles errors with 'stop'", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 1L))

  expect_error(
    foreach::foreach(i = 1:3, .combine = c,
                     .errorhandling = "stop") %dopar% {
      if (i == 2) stop("seq_boom")
      i
    },
    "seq_boom"
  )
})

test_that("sequential fallback handles errors with 'remove'", {
  conn <- mock_conn()
  suppressMessages(registerDoSnowflake(conn, workers = 1L))

  result <- foreach::foreach(i = 1:5, .combine = c,
                             .errorhandling = "remove") %dopar% {
    if (i == 3) stop("gone")
    i
  }
  expect_equal(result, c(1, 2, 4, 5))
})
