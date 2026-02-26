# Unit tests for helper functions
# =============================================================================

test_that("rprint works with data.frames", {
  expect_output(rprint(mtcars[1:3, ]), "mpg")
})

test_that("rview shows limited rows", {
  # rview prints data.frame to stdout and summary message via cli to stderr
  expect_output(rview(mtcars, n = 2), "Mazda")
})

test_that("rglimpse shows structure", {
  # cli messages go to stderr; dplyr::glimpse goes to stdout
  if (requireNamespace("dplyr", quietly = TRUE)) {
    expect_output(rglimpse(mtcars), "Rows")
  } else {
    expect_message(rglimpse(mtcars), "Rows")
  }
})

test_that("rcat outputs text", {
  expect_output(rcat("hello", " ", "world"), "hello world")
})
