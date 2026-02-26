# Unit tests for DBI methods (R/dbi.R)
# These tests use mock connection objects and do not require Snowflake access.

test_that("sfr_disconnect validates connection", {
  expect_error(sfr_disconnect(list()), "sfr_connection")
})


test_that("sfr_list_tables validates connection", {
  expect_error(sfr_list_tables(list()), "sfr_connection")
})


test_that("sfr_list_tables requires database and schema", {
  mock_conn <- structure(
    list(
      session = NULL,
      account = "test",
      database = NULL,
      schema = NULL,
      warehouse = "WH",
      auth_method = "test",
      environment = "test",
      created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  expect_error(sfr_list_tables(mock_conn), "database.*schema")
})


test_that("sfr_list_fields validates connection", {
  expect_error(sfr_list_fields(list(), "T"), "sfr_connection")
})


test_that("sfr_list_fields validates table_name type", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  expect_error(sfr_list_fields(mock_conn, 123))
})


test_that("sfr_table_exists validates connection", {
  expect_error(sfr_table_exists(list(), "T"), "sfr_connection")
})


test_that("sfr_read_table validates connection", {
  expect_error(sfr_read_table(list(), "T"), "sfr_connection")
})


test_that("sfr_write_table validates connection", {
  expect_error(sfr_write_table(list(), "T", data.frame()), "sfr_connection")
})


test_that("sfr_write_table requires data.frame", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  expect_error(sfr_write_table(mock_conn, "T", "not_a_df"))
})
