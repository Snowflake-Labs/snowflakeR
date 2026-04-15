# doSnowflake: Serialization Layer
# =============================================================================
# Handles serializing foreach iterations to Snowflake stages and collecting
# results back.  Shared across all remote modes (tasks, spcs, queue).
#
# Stage layout for a job:
#   @<stage>/job_<uuid>/
#     export.rds          -- shared export environment
#     manifest.json       -- job metadata (expr text, packages, chunk map)
#     tasks/
#       task_001.rds      -- serialized iteration args for chunk 1
#       task_002.rds      -- ...
#     results/
#       result_001.rds    -- worker output for chunk 1
#       result_002.rds    -- ...


# =============================================================================
# Stage PUT/GET primitives
# =============================================================================

#' PUT a local file to a Snowflake stage
#' @param conn sfr_connection.
#' @param local_path Character. Absolute path to the local file.
#' @param stage_path Character. Stage path including the @-prefix directory.
#' @noRd
.dosnowflake_stage_put <- function(conn, local_path, stage_path) {
  sql <- sprintf(
    "PUT 'file://%s' '%s' AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
    local_path, stage_path
  )
  sfr_execute(conn, sql)
}

#' GET a file from a Snowflake stage to a local directory
#' @param conn sfr_connection.
#' @param stage_path Character. Full stage path to the file (with @-prefix).
#' @param local_dir Character. Local directory to download into.
#' @noRd
.dosnowflake_stage_get <- function(conn, stage_path, local_dir) {
  sql <- sprintf("GET '%s' 'file://%s'", stage_path, local_dir)
  sfr_execute(conn, sql)
}


# =============================================================================
# Job serialization
# =============================================================================

#' Serialize a foreach job to a Snowflake stage
#'
#' Chunks the iteration argument list, serializes each chunk and the shared
#' export environment as .rds files, writes a JSON manifest, and PUTs
#' everything to the stage.
#'
#' @param conn sfr_connection.
#' @param job_id Character. UUID for this job.
#' @param expr The quoted R expression to evaluate per iteration.
#' @param arg_list List of named lists -- one per iteration.
#' @param obj The foreach object (for packages, export, noexport, etc.).
#' @param envir The calling environment.
#' @param opts Resolved options list (must contain `stage`).
#' @returns A list with `n_chunks`, `stage_path`, `chunk_map`.
#' @noRd
.serialize_job_to_stage <- function(conn, job_id, expr, arg_list, obj,
                                    envir, opts) {
  n_tasks <- length(arg_list)
  n_chunks <- .resolve_n_chunks(n_tasks, opts$chunks_per_job)
  chunk_map <- .chunk_iterations(arg_list, n_chunks)

  stage_base <- .resolve_stage_path(conn, opts$stage, job_id)

  tmp_dir <- tempfile(pattern = "dosnowflake_")
  dir.create(tmp_dir, recursive = TRUE)
  dir.create(file.path(tmp_dir, "tasks"), recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)

  # 1. Serialize export environment
  export_env <- .build_export_env(obj, expr, envir)
  export_list <- as.list(export_env)
  export_file <- file.path(tmp_dir, "export.rds")
  saveRDS(export_list, export_file)
  .dosnowflake_stage_put(conn, export_file,
                         paste0(stage_base, "/"))

  # 2. Serialize each chunk
  for (i in seq_along(chunk_map)) {
    chunk_id <- sprintf("%03d", i)
    chunk_data <- list(
      chunk_id  = chunk_id,
      args      = chunk_map[[i]],
      indices   = attr(chunk_map[[i]], "indices")
    )
    chunk_file <- file.path(tmp_dir, "tasks", paste0("task_", chunk_id, ".rds"))
    saveRDS(chunk_data, chunk_file)
    .dosnowflake_stage_put(conn, chunk_file,
                           paste0(stage_base, "/tasks/"))
  }

  # 3. Write and upload manifest
  manifest <- list(
    job_id     = job_id,
    n_chunks   = n_chunks,
    n_tasks    = n_tasks,
    expr_text  = deparse(expr, width.cutoff = 500L),
    packages   = obj$packages,
    created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  )
  manifest_file <- file.path(tmp_dir, "manifest.json")
  writeLines(jsonlite::toJSON(manifest, auto_unbox = TRUE, pretty = TRUE),
             manifest_file)
  .dosnowflake_stage_put(conn, manifest_file,
                         paste0(stage_base, "/"))

  # 4. Upload the expression itself as an .rds for the worker
  expr_file <- file.path(tmp_dir, "expr.rds")
  saveRDS(expr, expr_file)
  .dosnowflake_stage_put(conn, expr_file,
                         paste0(stage_base, "/"))

  # 5. Upload latest worker_queue.R to stage root so volume-mounted
  #    containers always run the current version (no image rebuild needed)
  worker_script <- system.file("workers", "worker_queue.R",
                               package = "snowflakeR")
  if (nzchar(worker_script) && file.exists(worker_script)) {
    stage_root <- sub("/job_[^/]+$", "/", stage_base)
    tryCatch(
      .dosnowflake_stage_put(conn, worker_script, stage_root),
      error = function(e) {
        cli::cli_warn("Could not upload worker script to stage: {conditionMessage(e)}")
      }
    )
  }

  list(
    n_chunks   = n_chunks,
    stage_path = stage_base,
    chunk_map  = chunk_map
  )
}


# =============================================================================
# Result collection
# =============================================================================

#' Count result_*.rds files visible under a job results prefix (via LIST)
#' @noRd
.n_stage_result_rds_files <- function(conn, result_prefix) {
  prefix <- gsub("/+$", "", result_prefix)
  path_for_list <- paste0(prefix, "/")
  sql <- sprintf("LIST '%s'", path_for_list)
  rows <- tryCatch(
    sfr_query(conn, sql, .keep_case = FALSE),
    error = function(e) {
      cli::cli_warn("LIST stage results failed: {conditionMessage(e)}")
      data.frame()
    }
  )
  if (NROW(rows) == 0L) {
    return(0L)
  }
  nm_col <- if ("name" %in% names(rows)) {
    "name"
  } else if ("NAME" %in% names(rows)) {
    "NAME"
  } else {
    names(rows)[1L]
  }
  nm <- rows[[nm_col]]
  base <- basename(as.character(nm))
  as.integer(sum(grepl("^result_[0-9]+\\.rds$", base)))
}


#' Wait until LIST shows enough result files (SPCS volume / stage propagation)
#'
#' Internal benchmarks sleep here: worker output may not be visible to
#' GET immediately after the Task graph reports SUCCEEDED.
#' @noRd
.wait_for_stage_result_rds <- function(conn, stage_path, n_chunks,
                                       max_wait_sec, poll_sec) {
  result_prefix <- paste0(gsub("/+$", "", stage_path), "/results")
  t0 <- Sys.time()
  last_n <- -1L
  repeat {
    n <- .n_stage_result_rds_files(conn, result_prefix)
    if (n != last_n) {
      cli::cli_inform(
        "Result files visible on stage (LIST): {n}/{n_chunks}"
      )
      last_n <- n
    }
    if (n >= n_chunks) {
      return(invisible(TRUE))
    }
    if (as.numeric(difftime(Sys.time(), t0, units = "secs")) > max_wait_sec) {
      cli::cli_warn(c(
        "!" = paste0(
          "Only ", n, "/", n_chunks,
          " result file(s) visible after ",
          max_wait_sec, "s (LIST). GET may still fail."
        ),
        "i" = "Increase {.arg result_sync_wait_sec} in {.fn registerDoSnowflake} if needed."
      ))
      return(invisible(FALSE))
    }
    Sys.sleep(poll_sec)
  }
}


#' Collect results from stage after worker completion
#'
#' Downloads result .rds files from the stage, deserializes them, and
#' returns a flat list ordered by original iteration index.
#'
#' @param conn sfr_connection.
#' @param stage_path Character. Base stage path for the job.
#' @param n_chunks Integer. Number of chunks to collect.
#' @param sync_wait_sec Max seconds to poll LIST before GET (stage propagation).
#' @param sync_poll_sec Seconds between LIST polls.
#' @returns A list of results, one per original iteration, in order.
#' @noRd
.collect_results_from_stage <- function(conn, stage_path, n_chunks,
                                         sync_wait_sec = 45,
                                         sync_poll_sec = 3) {
  cli::cli_inform(
    "Waiting for result files on stage (LIST, up to {sync_wait_sec}s)..."
  )
  .wait_for_stage_result_rds(
    conn, stage_path, n_chunks,
    max_wait_sec = sync_wait_sec,
    poll_sec = sync_poll_sec
  )

  tmp_dir <- tempfile(pattern = "dosnowflake_results_")
  dir.create(tmp_dir, recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)

  all_results <- list()

  for (i in seq_len(n_chunks)) {
    chunk_id <- sprintf("%03d", i)
    result_stage <- paste0(stage_path, "/results/result_", chunk_id, ".rds")
    .dosnowflake_stage_get(conn, result_stage, tmp_dir)

    result_file <- file.path(tmp_dir, paste0("result_", chunk_id, ".rds"))
    chunk_result <- readRDS(result_file)

    # chunk_result is a list with $results (list) and $indices (integer vector)
    for (j in seq_along(chunk_result$results)) {
      idx <- chunk_result$indices[j]
      all_results[[idx]] <- chunk_result$results[[j]]
    }

    unlink(result_file)
  }

  all_results
}


# =============================================================================
# Stage cleanup
# =============================================================================

#' Remove all stage files for a completed job
#' @param conn sfr_connection.
#' @param stage_path Character. Base stage path for the job.
#' @noRd
.cleanup_job_stage <- function(conn, stage_path) {
  tryCatch(
    sfr_execute(conn, sprintf("REMOVE '%s/'", stage_path)),
    error = function(e) {
      cli::cli_warn("Failed to clean up stage files at {.path {stage_path}}: {conditionMessage(e)}")
    }
  )
}


# =============================================================================
# Chunking
# =============================================================================

#' Split iterations into chunks for parallel workers
#'
#' Uses contiguous assignment: iterations 1-5 go to chunk 1,
#' 6-10 to chunk 2, etc. Each chunk element is tagged with an
#' `indices` attribute tracking the original iteration positions.
#'
#' @param arg_list List of named lists from the foreach iterator.
#' @param n_chunks Integer. Number of chunks to create.
#' @returns A list of length `n_chunks`, each element a list of arg sets
#'   with an `indices` attribute.
#' @noRd
.chunk_iterations <- function(arg_list, n_chunks) {
  n <- length(arg_list)
  n_chunks <- min(n_chunks, n)

  assignments <- rep(seq_len(n_chunks), length.out = n)

  chunks <- vector("list", n_chunks)
  for (k in seq_len(n_chunks)) {
    which_k <- which(assignments == k)
    chunk_args <- arg_list[which_k]
    attr(chunk_args, "indices") <- which_k
    chunks[[k]] <- chunk_args
  }

  chunks
}

#' Determine the number of chunks for a job
#' @param n_tasks Integer. Total iteration count.
#' @param chunks_per_job Integer, or "auto".
#' @returns Integer.
#' @noRd
.resolve_n_chunks <- function(n_tasks, chunks_per_job = "auto") {
  if (is.character(chunks_per_job) && tolower(chunks_per_job) == "auto") {
    return(min(n_tasks, 10L))
  }
  chunks_per_job <- suppressWarnings(as.integer(chunks_per_job))
  if (is.na(chunks_per_job) || chunks_per_job < 1L) chunks_per_job <- 1L
  min(chunks_per_job, n_tasks)
}


# =============================================================================
# Stage path resolution
# =============================================================================

#' Build the full stage path for a job
#'
#' Resolves the stage name relative to the current database/schema context
#' and appends the job subdirectory.
#'
#' @param conn sfr_connection.
#' @param stage Character. Stage name (e.g. "DOSNOWFLAKE_STAGE") or
#'   fully qualified with @ prefix.
#' @param job_id Character. UUID for this job.
#' @returns Character. Full stage path like `@DB.SCHEMA.STAGE/job_<uuid>`.
#' @noRd
.resolve_stage_path <- function(conn, stage, job_id) {
  stage <- trimws(stage)

  # Already fully qualified with @-prefix
  if (startsWith(stage, "@")) {
    return(paste0(stage, "/job_", job_id))
  }

  # Build FQN from session context
  db <- conn$database
  schema <- conn$schema
  if (is.null(db) || is.null(schema)) {
    db <- tryCatch({
      res <- sfr_query(conn, "SELECT CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SCH")
      list(db = res$DB[1], schema = res$SCH[1])
    }, error = function(e) NULL)
    if (!is.null(db)) {
      schema <- db$schema
      db <- db$db
    }
  }

  if (!is.null(db) && !is.null(schema)) {
    paste0("@", db, ".", schema, ".", stage, "/job_", job_id)
  } else {
    paste0("@", stage, "/job_", job_id)
  }
}


# =============================================================================
# Local serialization helpers (for testing without Snowflake)
# =============================================================================

#' Serialize a job to a local directory (for unit testing)
#'
#' Same logic as `.serialize_job_to_stage()` but writes to a local directory
#' instead of a Snowflake stage.
#'
#' @param job_dir Character. Local directory to write into.
#' @param job_id Character. UUID for this job.
#' @param expr Quoted expression.
#' @param arg_list List of arg sets.
#' @param packages Character vector of required packages.
#' @param export_list Named list of exported variables.
#' @param n_chunks Integer. Number of chunks.
#' @returns A list with `n_chunks`, `chunk_map`.
#' @noRd
.serialize_job_local <- function(job_dir, job_id, expr, arg_list,
                                 packages = character(0),
                                 export_list = list(),
                                 n_chunks = NULL) {
  if (is.null(n_chunks)) n_chunks <- .resolve_n_chunks(length(arg_list))
  chunk_map <- .chunk_iterations(arg_list, n_chunks)

  dir.create(file.path(job_dir, "tasks"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(job_dir, "results"), recursive = TRUE, showWarnings = FALSE)

  saveRDS(export_list, file.path(job_dir, "export.rds"))
  saveRDS(expr, file.path(job_dir, "expr.rds"))

  for (i in seq_along(chunk_map)) {
    chunk_id <- sprintf("%03d", i)
    chunk_data <- list(
      chunk_id = chunk_id,
      args     = chunk_map[[i]],
      indices  = attr(chunk_map[[i]], "indices")
    )
    saveRDS(chunk_data, file.path(job_dir, "tasks", paste0("task_", chunk_id, ".rds")))
  }

  manifest <- list(
    job_id     = job_id,
    n_chunks   = n_chunks,
    n_tasks    = length(arg_list),
    expr_text  = deparse(expr, width.cutoff = 500L),
    packages   = packages,
    created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  )
  writeLines(jsonlite::toJSON(manifest, auto_unbox = TRUE, pretty = TRUE),
             file.path(job_dir, "manifest.json"))

  list(n_chunks = n_chunks, chunk_map = chunk_map)
}

#' Simulate worker execution locally (for unit testing)
#'
#' Reads task files, evaluates the expression, writes result files.
#'
#' @param job_dir Character. Local directory with serialized job.
#' @noRd
.execute_job_local <- function(job_dir) {
  export_list <- readRDS(file.path(job_dir, "export.rds"))
  expr <- readRDS(file.path(job_dir, "expr.rds"))

  task_files <- sort(list.files(file.path(job_dir, "tasks"),
                                pattern = "^task_.*\\.rds$",
                                full.names = TRUE))

  for (task_file in task_files) {
    chunk <- readRDS(task_file)
    chunk_id <- chunk$chunk_id
    results <- vector("list", length(chunk$args))

    for (j in seq_along(chunk$args)) {
      e <- list2env(export_list, parent = .GlobalEnv)
      args <- chunk$args[[j]]
      for (nm in names(args)) assign(nm, args[[nm]], envir = e)
      results[[j]] <- tryCatch(eval(expr, envir = e), error = function(err) err)
    }

    result_data <- list(results = results, indices = chunk$indices)
    saveRDS(result_data, file.path(job_dir, "results",
                                   paste0("result_", chunk_id, ".rds")))
  }
}

#' Collect results from a local job directory (for unit testing)
#'
#' @param job_dir Character. Local directory with result files.
#' @param n_chunks Integer.
#' @returns Ordered list of results.
#' @noRd
.collect_results_local <- function(job_dir, n_chunks) {
  all_results <- list()

  for (i in seq_len(n_chunks)) {
    chunk_id <- sprintf("%03d", i)
    result_file <- file.path(job_dir, "results", paste0("result_", chunk_id, ".rds"))
    chunk_result <- readRDS(result_file)

    for (j in seq_along(chunk_result$results)) {
      idx <- chunk_result$indices[j]
      all_results[[idx]] <- chunk_result$results[[j]]
    }
  }

  all_results
}
