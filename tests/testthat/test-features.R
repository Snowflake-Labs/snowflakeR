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
    feature_df = "SELECT * FROM features_table",
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

  fv <- sfr_feature_view("FV", entities = entity, feature_df = "SELECT 1")
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

  fv <- sfr_feature_view("FV", entities = list(e1, e2), feature_df = "SELECT 1")
  expect_length(fv$entities, 2)
})


test_that("print.sfr_feature_view outputs expected format", {
  entity <- structure(
    list(name = "E1", join_keys = "ID", desc = ""),
    class = c("sfr_entity", "list")
  )
  fv <- sfr_feature_view("MY_FV", entities = entity, feature_df = "SELECT 1")

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
  fv <- sfr_feature_view("FV", entities = entity, feature_df = "SELECT 1")

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


test_that("sfr_feature_view stores feature_df_raw field", {
  entity <- structure(
    list(name = "E1", join_keys = "ID", desc = ""),
    class = c("sfr_entity", "list")
  )
  fv <- sfr_feature_view("FV", entities = entity, feature_df = "SELECT 1")
  expect_equal(fv$feature_df_raw, "SELECT 1")
})


test_that("sfr_feature creates correct S3 object", {
  f <- sfr_feature("AMOUNT", "FLOAT", "SUM", "1 hour")
  expect_s3_class(f, "sfr_feature")
  expect_equal(f$name, "AMOUNT")
  expect_equal(f$dtype, "FLOAT")
  expect_equal(f$agg, "SUM")
  expect_equal(f$window, "1 hour")
})


test_that("sfr_feature validates character inputs", {
  expect_error(sfr_feature(123, "FLOAT", "SUM", "1h"))
  expect_error(sfr_feature("A", 123, "SUM", "1h"))
})


test_that("print.sfr_feature outputs expected format", {
  f <- sfr_feature("AMOUNT", "FLOAT", "SUM", "1 hour")
  expect_message(print(f), "sfr_feature")
  expect_message(print(f), "SUM")
})


test_that("sfr_feature_view accepts features and feature_granularity", {
  entity <- structure(
    list(name = "E1", join_keys = "ID", desc = ""),
    class = c("sfr_entity", "list")
  )
  f1 <- sfr_feature("AMOUNT", "FLOAT", "SUM", "1 hour")
  f2 <- sfr_feature("COUNT", "NUMBER", "COUNT", "1 hour")

  local_mocked_bindings(
    sfr_requires_ml = function(min, feature) invisible(TRUE),
    .package = "snowflakeR"
  )

  fv <- sfr_feature_view(
    "TILED_FV", entities = entity, feature_df = "SELECT * FROM t",
    features = list(f1, f2), feature_granularity = "1h"
  )
  expect_equal(length(fv$features), 2)
  expect_equal(fv$feature_granularity, "1h")
})


test_that("sfr_feature_view requires features when feature_granularity is set", {
  entity <- structure(
    list(name = "E1", join_keys = "ID", desc = ""),
    class = c("sfr_entity", "list")
  )
  local_mocked_bindings(
    sfr_requires_ml = function(min, feature) invisible(TRUE),
    .package = "snowflakeR"
  )

  expect_error(
    sfr_feature_view("FV", entities = entity, feature_df = "SELECT 1",
                     feature_granularity = "1h"),
    "features"
  )
})
