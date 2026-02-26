# Unit tests for connection module
# =============================================================================

test_that("sfr_connect requires account", {
  # Without snowflakeauth and without active session, should fail
  # if no account provided
  expect_error(
    sfr_connect(
      account = NULL,
      .use_snowflakeauth = FALSE
    ),
    "account"
  )
})

test_that("validate_connection rejects non-connection objects", {
  expect_error(
    snowflakeR:::validate_connection("not a connection"),
    "sfr_connection"
  )
  expect_error(
    snowflakeR:::validate_connection(42),
    "sfr_connection"
  )
})

test_that("sfr_has_connection returns logical", {
  result <- sfr_has_connection(.use_snowflakeauth = FALSE)
  expect_type(result, "logical")
})

test_that("is_sfr_connection identifies connection objects", {
  fake_conn <- structure(
    list(session = NULL, account = "test"),
    class = c("sfr_connection", "list")
  )
  expect_true(snowflakeR:::is_sfr_connection(fake_conn))
  expect_false(snowflakeR:::is_sfr_connection("not a connection"))
})
