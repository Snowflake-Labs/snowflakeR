# Unit tests for registry module
# =============================================================================

# -- sfr_model_registry constructor -------------------------------------------

test_that("sfr_model_registry validates connection", {
  expect_error(
    sfr_model_registry("not_a_conn"),
    "sfr_connection"
  )
})


test_that("sfr_model_registry creates correct S3 object", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  mr <- sfr_model_registry(mock_conn)
  expect_s3_class(mr, "sfr_model_registry")
  expect_equal(mr$database, "DB")
  expect_equal(mr$schema, "SC")
  expect_identical(mr$conn, mock_conn)
})


test_that("sfr_model_registry overrides database/schema", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  mr <- sfr_model_registry(mock_conn, database = "OTHER_DB", schema = "OTHER_SC")
  expect_equal(mr$database, "OTHER_DB")
  expect_equal(mr$schema, "OTHER_SC")
})


test_that("print.sfr_model_registry outputs expected format", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  mr <- sfr_model_registry(mock_conn)
  expect_message(print(mr), "sfr_model_registry")
})


# -- resolve_registry_context -------------------------------------------------

test_that("resolve_registry_context works with sfr_model_registry", {
  mock_conn <- structure(
    list(
      session = "fake_session", account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  mr <- sfr_model_registry(mock_conn, database = "ML_DB", schema = "MODELS")
  ctx <- resolve_registry_context(mr)

  expect_equal(ctx$session, "fake_session")
  expect_equal(ctx$database_name, "ML_DB")
  expect_equal(ctx$schema_name, "MODELS")
})


test_that("resolve_registry_context works with sfr_connection", {
  mock_conn <- structure(
    list(
      session = "fake_session", account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  ctx <- resolve_registry_context(mock_conn)

  expect_equal(ctx$session, "fake_session")
  expect_null(ctx$database_name)
  expect_null(ctx$schema_name)
})


test_that("resolve_registry_context rejects invalid types", {
  expect_error(
    resolve_registry_context("not_valid"),
    "sfr_model_registry.*sfr_connection"
  )
  expect_error(
    resolve_registry_context(list()),
    "sfr_model_registry.*sfr_connection"
  )
})


# -- sfr_log_model validation -------------------------------------------------

test_that("sfr_log_model rejects invalid first arg", {
  model <- lm(mpg ~ wt, data = mtcars)
  expect_error(
    sfr_log_model("not_a_conn", model, model_name = "test"),
    "sfr_model_registry.*sfr_connection"
  )
})


test_that("sfr_log_model accepts sfr_connection (backward compat)", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  # Will fail at the bridge call (NULL session), but should pass R validation
  model <- lm(mpg ~ wt, data = mtcars)
  expect_error(
    sfr_log_model(mock_conn, model, model_name = "test")
  )
})


# -- Other function validation ------------------------------------------------

test_that("sfr_show_models rejects invalid first arg", {
  expect_error(
    sfr_show_models("not_a_conn"),
    "sfr_model_registry.*sfr_connection"
  )
})


test_that("sfr_predict rejects invalid first arg", {
  expect_error(
    sfr_predict("not_a_conn", "model", data.frame(x = 1)),
    "sfr_model_registry.*sfr_connection"
  )
})


# -- S3 print methods ---------------------------------------------------------

test_that("sfr_model_version prints cleanly", {
  mv <- structure(
    list(
      model_name = "TEST_MODEL",
      version_name = "V1",
      py_model_version = NULL,
      py_registry = NULL,
      reg = NULL
    ),
    class = c("sfr_model_version", "list")
  )
  expect_message(print(mv), "sfr_model_version")
})


test_that("sfr_model prints cleanly", {
  m <- structure(
    list(
      name = "TEST_MODEL",
      comment = "A test model",
      versions = c("V1", "V2"),
      default_version = "V1",
      py_model = NULL,
      py_registry = NULL,
      reg = NULL
    ),
    class = c("sfr_model", "list")
  )
  expect_message(print(m), "sfr_model")
})


# -- .resolve_service_fqn -----------------------------------------------------

test_that(".resolve_service_fqn uses registry context db/schema", {
  ctx <- list(database_name = "ML_DB", schema_name = "MODELS")
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "CONN_DB",
      schema = "CONN_SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  fqn <- .resolve_service_fqn(ctx, mock_conn, "my_svc")
  expect_equal(fqn, "ML_DB.MODELS.MY_SVC")
})


test_that(".resolve_service_fqn falls back to conn fields when ctx is NULL", {
  # This is the plain-connection case: resolve_registry_context returns NULLs
  ctx <- list(database_name = NULL, schema_name = NULL)
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "TESTDB",
      schema = "TESTSCHEMA", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  fqn <- .resolve_service_fqn(ctx, mock_conn, "mpg_service")
  expect_equal(fqn, "TESTDB.TESTSCHEMA.MPG_SERVICE")
})


test_that(".resolve_service_fqn returns bare name when no db/schema anywhere", {
  ctx <- list(database_name = NULL, schema_name = NULL)
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = NULL,
      schema = NULL, warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  fqn <- .resolve_service_fqn(ctx, mock_conn, "my_svc")
  expect_equal(fqn, "MY_SVC")
})


# -- .parse_service_status_json ------------------------------------------------

test_that(".parse_service_status_json handles READY single-container JSON", {
  json <- '[{"status":"READY","message":"Running","containerName":"model-inference","instanceId":"0"}]'
  out <- .parse_service_status_json(json)

  expect_equal(out$status, "READY")
  expect_equal(out$message, "Running")
  expect_true(is.data.frame(out$containers))
  expect_equal(nrow(out$containers), 1)
})


test_that(".parse_service_status_json handles PENDING status", {
  json <- '[{"status":"PENDING","message":"Waiting for compute","containerName":"model-inference","instanceId":"0"}]'
  out <- .parse_service_status_json(json)

  expect_equal(out$status, "PENDING")
  expect_equal(out$message, "Waiting for compute")
})


test_that(".parse_service_status_json handles FAILED status", {
  json <- '[{"status":"FAILED","message":"Image pull error","containerName":"model-inference","instanceId":"0"}]'
  out <- .parse_service_status_json(json)

  expect_equal(out$status, "FAILED")
  expect_equal(out$message, "Image pull error")
})


test_that(".parse_service_status_json derives overall status from multiple containers", {
  # All READY → READY
  json_all_ready <- '[{"status":"READY","message":"Running"},{"status":"READY","message":"Running"}]'
  expect_equal(.parse_service_status_json(json_all_ready)$status, "READY")

  # One FAILED → FAILED
  json_one_fail <- '[{"status":"READY","message":"Running"},{"status":"FAILED","message":"OOM"}]'
  expect_equal(.parse_service_status_json(json_one_fail)$status, "FAILED")

  # Mixed READY + PENDING → PENDING (first non-ready)
  json_mixed <- '[{"status":"PENDING","message":"Starting"},{"status":"READY","message":"Running"}]'
  expect_equal(.parse_service_status_json(json_mixed)$status, "PENDING")
})


test_that(".parse_service_status_json handles empty/NA input", {
  expect_equal(.parse_service_status_json(NA_character_)$status, "UNKNOWN")
  expect_equal(.parse_service_status_json("")$status, "UNKNOWN")
  expect_equal(.parse_service_status_json("  ")$status, "UNKNOWN")
})


test_that(".parse_service_status_json handles empty array (no containers yet)", {
  # SYSTEM$GET_SERVICE_STATUS returns "[]" when service exists but
  # no containers are provisioned yet (e.g. suspended or just created)
  out <- .parse_service_status_json("[]")
  expect_equal(out$status, "PENDING")
  expect_equal(out$message, "No containers provisioned yet")
  expect_null(out$containers)
})


test_that(".parse_service_status_json handles RUNNING as READY", {
  json <- '[{"status":"RUNNING","message":"Active"}]'
  out <- .parse_service_status_json(json)
  # RUNNING is in the READY/RUNNING success set
  expect_equal(out$status, "READY")
})


# -- sfr_get_service_status validation ----------------------------------------

test_that("sfr_get_service_status rejects invalid first arg", {
  expect_error(
    sfr_get_service_status("not_valid", "svc"),
    "sfr_model_registry.*sfr_connection"
  )
})


test_that("sfr_get_service_status validates service_name type", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  expect_error(sfr_get_service_status(mock_conn, 123))
  expect_error(sfr_get_service_status(mock_conn, c("a", "b")))
})


# -- sfr_wait_for_service validation ------------------------------------------

test_that("sfr_wait_for_service validates service_name type", {
  mock_conn <- structure(
    list(
      session = NULL, account = "test", database = "DB",
      schema = "SC", warehouse = "WH", auth_method = "test",
      environment = "test", created_at = Sys.time()
    ),
    class = c("sfr_connection", "list")
  )

  expect_error(sfr_wait_for_service(mock_conn, 123))
  expect_error(sfr_wait_for_service(mock_conn, c("a", "b")))
})
