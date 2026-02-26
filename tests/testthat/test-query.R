# Unit tests for query module
# =============================================================================

test_that("sfr_query validates connection", {
  expect_error(
    sfr_query("not_a_connection", "SELECT 1"),
    "sfr_connection"
  )
})

test_that("sfr_execute validates connection", {
  expect_error(
    sfr_execute("not_a_connection", "CREATE TABLE x (id INT)"),
    "sfr_connection"
  )
})

test_that("sfr_query rejects non-string sql", {
  fake_conn <- structure(
    list(session = NULL, account = "test"),
    class = c("sfr_connection", "list")
  )
  expect_error(sfr_query(fake_conn, 42))
})
