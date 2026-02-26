# Unit tests for Datasets (R/datasets.R)
# These tests use mock connection objects and do not require Snowflake access.

mock_conn <- structure(
  list(
    session = NULL,
    account = "test",
    database = "TEST_DB",
    schema = "TEST_SCHEMA",
    warehouse = "WH",
    auth_method = "test",
    environment = "test",
    created_at = Sys.time()
  ),
  class = c("sfr_connection", "list")
)


# ---------------------------------------------------------------------------
# sfr_create_dataset
# ---------------------------------------------------------------------------

test_that("sfr_create_dataset validates connection", {
  expect_error(sfr_create_dataset(list(), "DS"), "sfr_connection")
})

test_that("sfr_create_dataset validates name argument", {
  expect_error(sfr_create_dataset(mock_conn, 123))
  expect_error(sfr_create_dataset(mock_conn, c("a", "b")))
})


# ---------------------------------------------------------------------------
# sfr_get_dataset
# ---------------------------------------------------------------------------

test_that("sfr_get_dataset validates connection", {
  expect_error(sfr_get_dataset(list(), "DS"), "sfr_connection")
})

test_that("sfr_get_dataset validates name argument", {
  expect_error(sfr_get_dataset(mock_conn, 123))
})


# ---------------------------------------------------------------------------
# sfr_list_datasets
# ---------------------------------------------------------------------------

test_that("sfr_list_datasets validates connection", {
  expect_error(sfr_list_datasets(list()), "sfr_connection")
})


# ---------------------------------------------------------------------------
# sfr_delete_dataset
# ---------------------------------------------------------------------------

test_that("sfr_delete_dataset validates connection", {
  expect_error(sfr_delete_dataset(list(), "DS"), "sfr_connection")
})

test_that("sfr_delete_dataset validates name argument", {
  expect_error(sfr_delete_dataset(mock_conn, 123))
})


# ---------------------------------------------------------------------------
# sfr_create_dataset_version
# ---------------------------------------------------------------------------

test_that("sfr_create_dataset_version validates connection", {
  expect_error(
    sfr_create_dataset_version(list(), "DS", "v1", "SELECT 1"),
    "sfr_connection"
  )
})

test_that("sfr_create_dataset_version validates arguments", {
  expect_error(sfr_create_dataset_version(mock_conn, 123, "v1", "SELECT 1"))
  expect_error(sfr_create_dataset_version(mock_conn, "DS", 123, "SELECT 1"))
  expect_error(sfr_create_dataset_version(mock_conn, "DS", "v1", 123))
})


# ---------------------------------------------------------------------------
# sfr_list_dataset_versions
# ---------------------------------------------------------------------------

test_that("sfr_list_dataset_versions validates connection", {
  expect_error(sfr_list_dataset_versions(list(), "DS"), "sfr_connection")
})

test_that("sfr_list_dataset_versions validates name", {
  expect_error(sfr_list_dataset_versions(mock_conn, 123))
})


# ---------------------------------------------------------------------------
# sfr_delete_dataset_version
# ---------------------------------------------------------------------------

test_that("sfr_delete_dataset_version validates connection", {
  expect_error(sfr_delete_dataset_version(list(), "DS", "v1"), "sfr_connection")
})

test_that("sfr_delete_dataset_version validates arguments", {
  expect_error(sfr_delete_dataset_version(mock_conn, 123, "v1"))
  expect_error(sfr_delete_dataset_version(mock_conn, "DS", 123))
})


# ---------------------------------------------------------------------------
# sfr_read_dataset
# ---------------------------------------------------------------------------

test_that("sfr_read_dataset validates connection", {
  expect_error(sfr_read_dataset(list(), "DS", "v1"), "sfr_connection")
})

test_that("sfr_read_dataset validates arguments", {
  expect_error(sfr_read_dataset(mock_conn, 123, "v1"))
  expect_error(sfr_read_dataset(mock_conn, "DS", 123))
})


# ---------------------------------------------------------------------------
# print.sfr_dataset
# ---------------------------------------------------------------------------

test_that("print.sfr_dataset works", {
  ds <- structure(
    list(name = "MY_DATASET", fully_qualified_name = "DB.SCHEMA.MY_DATASET"),
    class = c("sfr_dataset", "list")
  )
  expect_message(print(ds), "MY_DATASET")
})
