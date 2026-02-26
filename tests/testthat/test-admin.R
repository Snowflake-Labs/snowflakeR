# Unit tests for Admin Utilities (R/admin.R)
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


# =============================================================================
# EAI
# =============================================================================

test_that("sfr_create_eai validates connection", {
  expect_error(sfr_create_eai(list(), "EAI", "rule1"), "sfr_connection")
})

test_that("sfr_create_eai validates arguments", {
  expect_error(sfr_create_eai(mock_conn, 123, "rule1"))
  expect_error(sfr_create_eai(mock_conn, "EAI", 123))
})

test_that("sfr_list_eais validates connection", {
  expect_error(sfr_list_eais(list()), "sfr_connection")
})

test_that("sfr_describe_eai validates connection", {
  expect_error(sfr_describe_eai(list(), "EAI"), "sfr_connection")
})

test_that("sfr_describe_eai validates name", {
  expect_error(sfr_describe_eai(mock_conn, 123))
})

test_that("sfr_delete_eai validates connection", {
  expect_error(sfr_delete_eai(list(), "EAI"), "sfr_connection")
})

test_that("sfr_delete_eai validates name", {
  expect_error(sfr_delete_eai(mock_conn, 123))
})


# =============================================================================
# Compute Pools
# =============================================================================

test_that("sfr_create_compute_pool validates connection", {
  expect_error(
    sfr_create_compute_pool(list(), "POOL", "CPU_X64_XS"),
    "sfr_connection"
  )
})

test_that("sfr_create_compute_pool validates arguments", {
  expect_error(sfr_create_compute_pool(mock_conn, 123, "CPU_X64_XS"))
  expect_error(sfr_create_compute_pool(mock_conn, "POOL", 123))
})

test_that("sfr_list_compute_pools validates connection", {
  expect_error(sfr_list_compute_pools(list()), "sfr_connection")
})

test_that("sfr_describe_compute_pool validates connection", {
  expect_error(sfr_describe_compute_pool(list(), "POOL"), "sfr_connection")
})

test_that("sfr_delete_compute_pool validates connection", {
  expect_error(sfr_delete_compute_pool(list(), "POOL"), "sfr_connection")
})

test_that("sfr_suspend_compute_pool validates connection", {
  expect_error(sfr_suspend_compute_pool(list(), "POOL"), "sfr_connection")
})

test_that("sfr_resume_compute_pool validates connection", {
  expect_error(sfr_resume_compute_pool(list(), "POOL"), "sfr_connection")
})


# =============================================================================
# Image Repositories
# =============================================================================

test_that("sfr_create_image_repo validates connection", {
  expect_error(sfr_create_image_repo(list(), "REPO"), "sfr_connection")
})

test_that("sfr_create_image_repo validates name", {
  expect_error(sfr_create_image_repo(mock_conn, 123))
})

test_that("sfr_list_image_repos validates connection", {
  expect_error(sfr_list_image_repos(list()), "sfr_connection")
})

test_that("sfr_describe_image_repo validates connection", {
  expect_error(sfr_describe_image_repo(list(), "REPO"), "sfr_connection")
})

test_that("sfr_delete_image_repo validates connection", {
  expect_error(sfr_delete_image_repo(list(), "REPO"), "sfr_connection")
})

test_that("sfr_delete_image_repo validates name", {
  expect_error(sfr_delete_image_repo(mock_conn, 123))
})
