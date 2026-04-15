# doSnowflake: Tasks+Jobs Backend (Phase 1)
# =============================================================================
# Implements the foreach backend contract for mode = "tasks".
# Orchestrates SPCS job services via Snowflake Task graphs using the
# Python bridge (sfr_tasks_bridge.py).


#' Tasks-mode backend: SPCS jobs orchestrated by Snowflake Task graphs
#'
#' Satisfies the foreach backend contract: `function(obj, expr, envir, data)`.
#' Serializes iterations to a Snowflake stage, builds a Task DAG with one
#' SPCS job per chunk, polls for completion, and collects results.
#' @noRd
.doSnowflakeTasks <- function(obj, expr, envir, data) {
  if (!inherits(obj, "foreach")) {
    stop("obj must be a foreach object", call. = FALSE)
  }

  conn <- data$conn
  opts <- .resolve_task_options(data)

  it <- iterators::iter(obj)
  accumulator <- foreach::makeAccum(it)
  arg_list <- as.list(it)
  n_tasks <- length(arg_list)

  if (n_tasks == 0L) {
    return(foreach::getResult(it))
  }

  # Generate a unique job ID
  job_id <- .generate_job_id()
  dag_name <- paste0("DOSNOWFLAKE_", toupper(gsub("-", "", job_id)))

  cli::cli_inform(c(
    "i" = "doSnowflake tasks mode: dispatching {n_tasks} iteration{?s} to SPCS.",
    "i" = "Job ID: {.val {job_id}}",
    "i" = "Compute pool: {.val {opts$compute_pool}}"
  ))

  # 1. Serialize job to stage
  cli::cli_inform("Serializing {n_tasks} iteration{?s} to stage...")
  job <- .serialize_job_to_stage(conn, job_id, expr, arg_list,
                                 obj, envir, opts)

  # 2. Build and execute Task DAG via Python bridge
  cli::cli_inform("Building Task graph with {job$n_chunks} chunk{?s}...")
  bridge <- get_bridge_module("sfr_tasks_bridge")

  bridge$create_and_run_dag(
    session      = conn$session,
    dag_name     = dag_name,
    job_id       = job_id,
    n_chunks     = as.integer(job$n_chunks),
    stage_path   = job$stage_path,
    compute_pool = opts$compute_pool,
    image_uri    = opts$image_uri
  )

  # 3. Poll task graph for completion
  cli::cli_inform("Task graph deployed. Polling for completion...")
  .poll_task_graph(bridge, conn, dag_name,
                   timeout_min = opts$timeout_min,
                   poll_sec = opts$poll_sec)

  # 4. Collect results from stage
  cli::cli_inform("Collecting results from stage...")
  raw_results <- .collect_results_from_stage(
    conn,
    job$stage_path,
    job$n_chunks,
    sync_wait_sec = opts$result_sync_wait_sec,
    sync_poll_sec = opts$result_sync_poll_sec
  )

  for (i in seq_along(raw_results)) {
    accumulator(list(raw_results[[i]]), i)
  }

  # 5. Cleanup
  tryCatch(bridge$cleanup_dag(session = conn$session, dag_name = dag_name),
           error = function(e) NULL)
  .cleanup_job_stage(conn, job$stage_path)

  # Error handling
  err_val <- foreach::getErrorValue(it)
  err_idx <- foreach::getErrorIndex(it)
  if (identical(obj$errorHandling, "stop") && !is.null(err_val)) {
    msg <- sprintf(
      "doSnowflake: error in iteration %d: %s",
      err_idx, conditionMessage(err_val)
    )
    stop(msg, call. = FALSE)
  }

  cli::cli_inform(c("v" = "doSnowflake job {.val {job_id}} completed."))
  foreach::getResult(it)
}


# =============================================================================
# Tasks-mode info function
# =============================================================================

#' @noRd
.doSnowflakeTasksInfo <- function(data, item) {
  switch(item,
    workers = .resolve_n_chunks(
      100L,
      data$options$chunks_per_job %||% "auto"
    ),
    name    = "doSnowflake",
    version = as.character(utils::packageVersion("snowflakeR")),
    NULL
  )
}


# =============================================================================
# Option resolution
# =============================================================================

#' Resolve tasks-mode options from user-supplied ... args
#'
#' Merges user-supplied options with sensible defaults.
#'
#' @param data Backend data list from registerDoSnowflake().
#' @returns Named list of resolved options.
#' @noRd
.resolve_task_options <- function(data) {
  user_opts <- data$options
  if (is.null(user_opts)) user_opts <- list()

  compute_pool <- user_opts$compute_pool
  if (is.null(compute_pool) || !nzchar(compute_pool)) {
    cli::cli_abort(c(
      "{.arg compute_pool} is required for {.val tasks} mode.",
      "i" = "Pass it to {.fn registerDoSnowflake}: {.code registerDoSnowflake(conn, mode = 'tasks', compute_pool = 'MY_POOL', image_uri = '...')}"
    ))
  }

  image_uri <- user_opts$image_uri
  if (is.null(image_uri) || !nzchar(image_uri)) {
    cli::cli_abort(c(
      "{.arg image_uri} is required for {.val tasks} mode.",
      "i" = "Build the worker image first with {.fn sfr_dosnowflake_build_image}, then pass the URI."
    ))
  }

  list(
    compute_pool   = compute_pool,
    image_uri      = image_uri,
    stage          = user_opts$stage %||% "DOSNOWFLAKE_STAGE",
    timeout_min    = as.numeric(user_opts$timeout_min %||% 30),
    poll_sec       = as.numeric(user_opts$poll_sec %||% 5),
    chunks_per_job = user_opts$chunks_per_job %||% "auto",
    result_sync_wait_sec = as.numeric(user_opts$result_sync_wait_sec %||% 45),
    result_sync_poll_sec = as.numeric(user_opts$result_sync_poll_sec %||% 3)
  )
}


# =============================================================================
# Task graph polling
# =============================================================================

#' Poll a Task graph until it completes or times out
#' @param bridge The sfr_tasks_bridge Python module.
#' @param conn sfr_connection.
#' @param dag_name Character. Name of the root task.
#' @param timeout_min Numeric. Maximum minutes to wait.
#' @param poll_sec Numeric. Seconds between polls.
#' @noRd
.poll_task_graph <- function(bridge, conn, dag_name,
                             timeout_min = 30, poll_sec = 5) {
  deadline <- Sys.time() + timeout_min * 60
  last_state <- ""

  repeat {
    if (Sys.time() > deadline) {
      cli::cli_abort(c(
        "doSnowflake: timed out after {timeout_min} minute{?s} waiting for Task graph.",
        "i" = "DAG: {.val {dag_name}}",
        "i" = "Increase {.arg timeout_min} or check Snowsight for details."
      ))
    }

    status <- bridge$get_dag_status(session = conn$session, dag_name = dag_name)
    state <- as.character(status$state)

    if (state != last_state) {
      cli::cli_inform("Task graph state: {.val {state}}")
      last_state <- state
    }

    if (state == "SUCCEEDED") {
      return(invisible(TRUE))
    }

    if (state == "FAILED") {
      err_msg <- if (!is.null(status$error)) as.character(status$error) else "unknown error"
      cli::cli_abort(c(
        "doSnowflake: Task graph failed.",
        "x" = "Error: {err_msg}",
        "i" = "DAG: {.val {dag_name}}. Check Snowsight for per-chunk details."
      ))
    }

    Sys.sleep(poll_sec)
  }
}


# =============================================================================
# Job ID generation
# =============================================================================

#' Generate a UUID for a job, using uuid package if available
#' @returns Character UUID string.
#' @noRd
.generate_job_id <- function() {
  if (requireNamespace("uuid", quietly = TRUE)) {
    return(uuid::UUIDgenerate())
  }
  # Fallback: pseudo-random hex string
  paste0(
    sprintf("%04x", sample(0:65535, 4, replace = TRUE)),
    collapse = "-"
  )
}
