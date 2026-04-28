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
    session              = conn$session,
    dag_name             = dag_name,
    job_id               = job_id,
    n_chunks             = as.integer(job$n_chunks),
    stage_path           = job$stage_path,
    compute_pool         = opts$compute_pool,
    image_uri            = opts$image_uri,
    warehouse            = opts$warehouse,
    instance_family      = opts$instance_family,
    containers_per_node  = as.integer(opts$containers_per_node %||% 1L)
  )

  # 3. Poll task graph for completion (use chunk child status — root can flip
  #    SUCCEEDED before SPCS jobs finish and write result_*.rds to stage)
  cli::cli_inform("Task graph deployed. Polling for completion...")
  .poll_task_graph(
    bridge, conn, dag_name,
    timeout_min = opts$timeout_min,
    poll_sec = opts$poll_sec,
    n_chunks = as.integer(job$n_chunks)
  )

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

  wh <- user_opts$warehouse
  if (is.null(wh) || !length(wh) || !nzchar(as.character(wh)[[1L]])) {
    warehouse <- NULL
  } else {
    warehouse <- as.character(wh)[[1L]]
  }

  list(
    compute_pool   = compute_pool,
    image_uri      = image_uri,
    warehouse      = warehouse,
    instance_family     = user_opts$instance_family %||% "CPU_X64_S",
    containers_per_node = as.integer(user_opts$containers_per_node %||% 1L),
    stage          = user_opts$stage %||% "DOSNOWFLAKE_STAGE",
    timeout_min    = as.numeric(user_opts$timeout_min %||% 30),
    poll_sec       = as.numeric(user_opts$poll_sec %||% 5),
    chunks_per_job = user_opts$chunks_per_job %||% "auto",
    result_sync_wait_sec = as.numeric(user_opts$result_sync_wait_sec %||% 45),
    result_sync_poll_sec = as.numeric(user_opts$result_sync_poll_sec %||% 3),
    data_query     = user_opts$data_query,
    save_models    = isTRUE(user_opts$save_models),
    model_key_arg  = user_opts$model_key_arg,
    model_run_id   = user_opts$model_run_id
  )
}


# =============================================================================
# Task graph polling
# =============================================================================

#' Normalize child-status payload from sfr_tasks_bridge (Python -> R)
#' @noRd
.task_dag_child_counts <- function(bridge, conn, dag_name) {
  st <- tryCatch(
    bridge$get_dag_child_status(session = conn$session, dag_name = dag_name),
    error = function(e) structure(list(.error = e), class = "task_child_status_error")
  )
  if (inherits(st, "task_child_status_error")) {
    return(list(
      succeeded = 0L,
      failed = 0L,
      total = 0L,
      chunks = list(),
      unavailable = TRUE
    ))
  }
  counts <- st[["counts"]]
  if (is.null(counts)) {
    counts <- st$counts
  }
  if (inherits(counts, "python.builtin.dict")) {
    counts <- reticulate::py_to_r(counts)
  }
  chunks <- st[["chunks"]]
  if (is.null(chunks)) {
    chunks <- st$chunks
  }
  if (inherits(chunks, "python.builtin.list")) {
    chunks <- reticulate::py_to_r(chunks)
  }
  list(
    succeeded = as.integer(counts$succeeded %||% 0L),
    failed = as.integer(counts$failed %||% 0L),
    total = as.integer(counts$total %||% 0L),
    chunks = if (is.list(chunks)) chunks else list(),
    unavailable = FALSE
  )
}


#' Abort with per-chunk error lines when any child FAILED
#' @noRd
.cli_abort_task_children <- function(cc, dag_name) {
  lines <- character()
  for (ch in cc$chunks) {
    if (!is.list(ch)) {
      next
    }
    st <- ch[["state"]]
    if (is.null(st)) {
      st <- ch[["STATE"]]
    }
    if (is.null(st)) {
      st <- ch$state
    }
    if (!identical(as.character(st), "FAILED")) {
      next
    }
    nm <- ch[["name"]]
    if (is.null(nm)) {
      nm <- ch[["NAME"]]
    }
    if (is.null(nm)) {
      nm <- ch$name
    }
    err <- ch[["error"]]
    if (is.null(err)) {
      err <- ch[["ERROR_MESSAGE"]]
    }
    if (is.null(err)) {
      err <- ch$error
    }
    lines <- c(lines, paste0(as.character(nm), ": ", as.character(err %||% "")))
  }
  if (length(lines) == 0L) {
    lines <- "(no per-chunk error text returned)"
  }
  cli::cli_abort(c(
    "doSnowflake: one or more chunk tasks FAILED.",
    "i" = "DAG: {.val {dag_name}}",
    "x" = paste(lines, collapse = "\n")
  ))
}


#' Poll a Task graph until it completes or times out
#'
#' When `n_chunks` is set, completion requires that many **child** tasks in
#' `TASK_HISTORY(ROOT_TASK_NAME => dag_name)` to be `SUCCEEDED`. Relying only
#' on the root row can return `SUCCEEDED` before SPCS workers finish, which
#' then makes stage `GET` fail with 253006 (no `result_*.rds` yet).
#'
#' @param bridge The sfr_tasks_bridge Python module.
#' @param conn sfr_connection.
#' @param dag_name Character. Name of the root task.
#' @param timeout_min Numeric. Maximum minutes to wait.
#' @param poll_sec Numeric. Seconds between polls.
#' @param n_chunks Integer or NULL. When non-NULL, poll child chunk tasks.
#' @noRd
.poll_task_graph <- function(bridge, conn, dag_name,
                             timeout_min = 30, poll_sec = 5,
                             n_chunks = NULL) {
  deadline <- Sys.time() + timeout_min * 60
  history_grace_sec <- 90
  last_root <- ""
  last_child_line <- ""
  root_succeeded_nochild_since <- NULL

  repeat {
    if (Sys.time() > deadline) {
      cli::cli_abort(c(
        "doSnowflake: timed out after {timeout_min} minute{?s} waiting for Task graph.",
        "i" = "DAG: {.val {dag_name}}",
        "i" = "Increase {.arg timeout_min} or check Snowsight for details."
      ))
    }

    status <- bridge$get_dag_status(session = conn$session, dag_name = dag_name)
    root_state <- as.character(status$state)

    if (root_state != last_root) {
      cli::cli_inform("Task graph (root) state: {.val {root_state}}")
      last_root <- root_state
    }

    if (!is.null(n_chunks) && !is.na(n_chunks) && n_chunks >= 1L) {
      cc <- .task_dag_child_counts(bridge, conn, dag_name)
      line <- paste0(
        "Chunk tasks: ", cc$succeeded, "/", n_chunks, " SUCCEEDED",
        " (total in history: ", cc$total, ", failed: ", cc$failed, ")"
      )
      if (line != last_child_line) {
        cli::cli_inform(line)
        last_child_line <- line
      }

      if (isTRUE(cc$unavailable)) {
        if (identical(root_state, "SUCCEEDED")) {
          cli::cli_warn(c(
            "Child TASK_HISTORY is unavailable in this account/session.",
            "i" = "Falling back to root-task completion and stage result synchronization."
          ))
          return(invisible(TRUE))
        }
        Sys.sleep(poll_sec)
        next
      }

      if (cc$failed > 0L) {
        .cli_abort_task_children(cc, dag_name)
      }

      if (cc$succeeded >= n_chunks) {
        return(invisible(TRUE))
      }

      if (identical(root_state, "FAILED")) {
        err_msg <- if (!is.null(status$error)) {
          as.character(status$error)
        } else {
          "unknown error"
        }
        cli::cli_abort(c(
          "doSnowflake: Task graph root FAILED.",
          "x" = "Error: {err_msg}",
          "i" = "DAG: {.val {dag_name}}. Check Snowsight for per-chunk details."
        ))
      }

      if (identical(root_state, "SUCCEEDED") && cc$total < 1L) {
        if (is.null(root_succeeded_nochild_since)) {
          root_succeeded_nochild_since <- Sys.time()
          cli::cli_inform(c(
            "Root task is SUCCEEDED but child TASK_HISTORY is still empty.",
            "i" = "Waiting up to {history_grace_sec}s for history visibility..."
          ))
        }

        if (as.numeric(difftime(
          Sys.time(),
          root_succeeded_nochild_since,
          units = "secs"
        )) > history_grace_sec) {
          cli::cli_abort(c(
            "doSnowflake: Root task SUCCEEDED but TASK_HISTORY still shows no child tasks.",
            "i" = "DAG: {.val {dag_name}}",
            "i" = "Verify the DAG was deployed in the current database/schema."
          ))
        }
      } else {
        root_succeeded_nochild_since <- NULL
      }
    } else {
      if (identical(root_state, "SUCCEEDED")) {
        return(invisible(TRUE))
      }

      if (identical(root_state, "FAILED")) {
        err_msg <- if (!is.null(status$error)) {
          as.character(status$error)
        } else {
          "unknown error"
        }
        cli::cli_abort(c(
          "doSnowflake: Task graph failed.",
          "x" = "Error: {err_msg}",
          "i" = "DAG: {.val {dag_name}}. Check Snowsight for per-chunk details."
        ))
      }
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
