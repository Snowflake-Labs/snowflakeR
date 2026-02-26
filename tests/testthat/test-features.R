# Unit tests for Feature Store wrappers (R/features.R)
# These tests use mock objects and do not require Snowflake access.

test_that("sfr_feature_view creates draft object with correct class", {
  entity <- structure(
    list(name = "CUSTOMER", join_keys = "CUSTOMER_ID", desc = ""),
    class = c("sfr_entity", "list")
  )

  fv <- sfr_feature_view(
    name = "MY_FV",
    entities = entity,
    features = "SELECT * FROM features_table",
    refresh_freq = "1 hour",
    desc = "Test feature view"
  )

  expect_s3_class(fv, "sfr_feature_view")
  expect_equal(fv$name, "MY_FV")
  expect_equal(fv$sql, "SELECT * FROM features_table")
  expect_false(fv$registered)
  expect_equal(fv$refresh_freq, "1 hour")
})


test_that("sfr_feature_view handles single entity", {
  entity <- structure(
    list(name = "E1", join_keys = "ID", desc = ""),
    class = c("sfr_entity", "list")
  )

  fv <- sfr_feature_view("FV", entities = entity, features = "SELECT 1")
  expect_length(fv$entities, 1)
  expect_s3_class(fv$entities[[1]], "sfr_entity")
})


test_that("sfr_feature_view handles list of entities", {
  e1 <- structure(
    list(name = "E1", join_keys = "ID1", desc = ""),
    class = c("sfr_entity", "list")
  )
  e2 <- structure(
    list(name = "E2", join_keys = "ID2", desc = ""),
    class = c("sfr_entity", "list")
  )

  fv <- sfr_feature_view("FV", entities = list(e1, e2), features = "SELECT 1")
  expect_length(fv$entities, 2)
})


test_that("print.sfr_feature_view outputs expected format", {
  entity <- structure(
    list(name = "E1", join_keys = "ID", desc = ""),
    class = c("sfr_entity", "list")
  )
  fv <- sfr_feature_view("MY_FV", entities = entity, features = "SELECT 1")

  expect_message(print(fv), "sfr_feature_view")
  expect_message(print(fv), "draft")
})


test_that("print.sfr_entity outputs expected format", {
  entity <- structure(
    list(name = "MY_ENTITY", join_keys = c("A", "B"), desc = "test"),
    class = c("sfr_entity", "list")
  )

  expect_message(print(entity), "sfr_entity")
  expect_message(print(entity), "MY_ENTITY")
})


test_that("sfr_feature_store validates connection", {
  expect_error(
    sfr_feature_store(list()),
    "sfr_connection"
  )
})


test_that("sfr_feature_store requires database, schema, warehouse", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = NULL,
      schema = NULL, warehouse = NULL, auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  expect_error(sfr_feature_store(mock_conn), "database")
})


test_that("sfr_feature_store creates correct S3 object", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  fs <- sfr_feature_store(mock_conn)
  expect_s3_class(fs, "sfr_feature_store")
  expect_equal(fs$database, "DB")
  expect_equal(fs$schema, "SC")
  expect_equal(fs$warehouse, "WH")
  expect_equal(fs$creation_mode, "FAIL_IF_NOT_EXIST")
})


test_that("sfr_feature_store with create = TRUE sets mode", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  fs <- sfr_feature_store(mock_conn, create = TRUE)
  expect_equal(fs$creation_mode, "CREATE_IF_NOT_EXIST")
})


test_that("sfr_create_entity requires sfr_feature_store", {
  expect_error(sfr_create_entity(list(), "E", "ID"), "sfr_feature_store")
})


test_that("sfr_list_entities requires sfr_feature_store", {
  expect_error(sfr_list_entities(list()), "sfr_feature_store")
})


test_that("sfr_register_feature_view requires sfr_feature_store", {
  entity <- structure(
    list(name = "E1", join_keys = "ID", desc = ""),
    class = c("sfr_entity", "list")
  )
  fv <- sfr_feature_view("FV", entities = entity, features = "SELECT 1")

  expect_error(sfr_register_feature_view(list(), fv, "v1"), "sfr_feature_store")
})


test_that("sfr_register_feature_view validates feature_view class", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )
  fs <- sfr_feature_store(mock_conn)

  expect_error(sfr_register_feature_view(fs, list(), "v1"), "sfr_feature_view")
})


test_that("sfr_generate_training_data requires sfr_feature_store", {
  expect_error(
    sfr_generate_training_data(list(), "SELECT 1", list()),
    "sfr_feature_store"
  )
})


test_that("extract_feature_sql handles character input", {
  expect_equal(
    extract_feature_sql("SELECT 1"),
    "SELECT 1"
  )
})


test_that("extract_feature_sql rejects non-SQL non-dbplyr input", {
  expect_error(extract_feature_sql(42), "must be a SQL string")
})
