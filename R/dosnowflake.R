# doSnowflake: foreach Parallel Backend for Snowflake
# =============================================================================
# Implements the foreach %dopar% contract so that R users can run parallel
# loops on Snowflake: local socket clusters, Snowflake Task + SPCS job
# dispatch ("tasks"), and Hybrid Table queue + SPCS workers ("queue").
# Stage-based "spcs" mode is reserved for a future release.


# =============================================================================
# Cluster cache -- avoids respawning R worker processes on every %dopar% call
# =============================================================================
.dosnowflake_env <- new.env(parent = emptyenv())
.dosnowflake_env$cluster   <- NULL
.dosnowflake_env$n_workers <- 0L

.get_or_create_cluster <- function(n_workers) {
  if (!is.null(.dosnowflake_env$cluster) &&
      .dosnowflake_env$n_workers == n_workers) {
    return(.dosnowflake_env$cluster)
  }
  .shutdown_cluster()
  cl <- parallel::makeCluster(n_workers)
  .dosnowflake_env$cluster   <- cl
  .dosnowflake_env$n_workers <- n_workers
  cl
}

.shutdown_cluster <- function() {
  if (!is.null(.dosnowflake_env$cluster)) {
    tryCatch(
      parallel::stopCluster(.dosnowflake_env$cluster),
      error = function(e) NULL
    )
    .dosnowflake_env$cluster   <- NULL
    .dosnowflake_env$n_workers <- 0L
  }
  invisible(NULL)
}

#' Stop the cached doSnowflake cluster
#'
#' Shuts down the reusable socket cluster that `registerDoSnowflake()`
#' maintains across `\%dopar\%` calls.
#'
#' @returns Invisibly returns `NULL`.
#' @export
stopDoSnowflake <- function() {
  .shutdown_cluster()
  cli::cli_inform("doSnowflake cluster stopped.")
  invisible(NULL)
}


#' Register the doSnowflake parallel backend
#'
#' Registers a `foreach` parallel backend that dispatches iterations to
#' Snowflake compute.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param mode Character.
#'   * `"local"` -- socket cluster in the current container (default).
#'   * `"tasks"` -- Snowflake Task graph + SPCS jobs.
#'   * `"queue"` -- Hybrid Table queue with SPCS workers (ephemeral or persistent).
#'   * `"spcs"` -- stage-based SPCS job dispatch (not yet implemented).
#' @param workers Integer or `"auto"`. Number of parallel workers.
#'   `"auto"` (default) detects available CPU cores and reserves one for
#'   the main thread.
#' @param ... Backend-specific options.
#'
#'   For `mode = "tasks"`:
#'   * `compute_pool` (required) -- name of the SPCS compute pool.
#'   * `image_uri` (required) -- worker Docker image URI in image repo.
#'   * `warehouse` -- optional Snowflake warehouse for the Task DAG root task.
#'     When omitted, Snowflake may create a **serverless** task graph, which
#'     requires the `EXECUTE MANAGED TASK` account privilege on the session role.
#'     Set this (for example to the same warehouse as your lab `context`) so the
#'     graph is **user-managed** and runs without that privilege.
#'   * `stage` -- stage name (default `"DOSNOWFLAKE_STAGE"`).
#'   * `timeout_min` -- max minutes to wait for completion (default 30).
#'   * `poll_sec` -- seconds between status polls (default 5).
#'   * `chunks_per_job` -- number of SPCS jobs to create (default `"auto"`).
#'   * `result_sync_wait_sec` -- max seconds to poll `LIST` on the stage
#'     before `GET`ing chunk results (default `45`). SPCS volume writes can
#'     lag behind Task graph `SUCCEEDED`; increase if you see error 253006.
#'   * `result_sync_poll_sec` -- seconds between `LIST` polls (default `3`).
#'
#'   For `mode = "queue"`:
#'   * `compute_pool` (required for ephemeral) -- name of the SPCS compute pool.
#'   * `image_uri` (required for ephemeral) -- worker Docker image URI.
#'   * `worker_type` -- `"ephemeral"` (default) or `"persistent"`.
#'   * `n_workers` -- number of worker instances (default 4).
#'   * `queue_fqn` -- Hybrid Table name (default `"CONFIG.DOSNOWFLAKE_QUEUE"`).
#'   * `stage` -- stage name (default `"DOSNOWFLAKE_STAGE"`).
#'   * `timeout_min` -- max minutes to wait (default 30).
#'   * `poll_sec` -- seconds between status polls (default 5).
#'   * `stale_timeout_sec` -- seconds before re-queuing stale items (default 600).
#'   * `chunks_per_job` -- number of chunks to create (default `"auto"`).
#'   * `pre_warm` -- logical. Wait for all workers to be READY before
#'     enqueuing work (default `FALSE`). Useful for benchmarking or when
#'     consistent timing is important. For smaller jobs, leaving this FALSE
#'     lets early workers start immediately.
#'   * `instance_family` -- character. SPCS instance family for resource
#'     sizing (default `"CPU_X64_S"`). Resources are auto-sized:
#'     XS=1c/4G, S=3c/12G, M=6c/24G, L=12c/48G, XL=24c/96G.
#'   * `result_sync_wait_sec`, `result_sync_poll_sec` -- same as for
#'     `mode = "tasks"` (stage result collection after workers finish).
#'
#' @details
#' **Local** (default): iterations run on a cached `parallel::makeCluster()`
#' socket cluster in the current R session (Workspace container or local).
#' No SPCS objects are required. Call [stopDoSnowflake()] to shut down the
#' cluster explicitly.
#'
#' **Tasks**: iterations are chunked and executed via Snowflake Tasks and SPCS
#' jobs. Requires a compute pool, worker image, and stage; see
#' [sfr_dosnowflake_setup()] and the `mode = "tasks"` arguments above.
#'
#' **Queue**: iterations are written to a Hybrid Table queue and processed by
#' SPCS workers (ephemeral or persistent). Requires queue DDL, images, and
#' pool configuration; see [sfr_dosnowflake_setup()] and the shipped notebooks
#' under `inst/notebooks/` (`workspace_parallel_spcs_*.ipynb`,
#' `snowflaker_parallel_spcs_config.yaml`).
#'
#' **Spcs**: reserved for future stage-only job dispatch; not implemented (an
#' error is raised if selected).
#'
#' Related infrastructure: [sfr_dosnowflake_build_image()] for worker images;
#' [crew_controller_spcs()] and [sfr_execute_r_script()] for other SPCS
#' parallel patterns beyond `foreach`.
#'
#' @returns Invisibly returns `TRUE`.
#'
#' @examples
#' \dontrun{
#' library(foreach)
#' conn <- sfr_connect()
#' registerDoSnowflake(conn)
#'
#' result <- foreach(i = 1:10, .combine = c) %dopar% {
#'   i^2
#' }
#' }
#'
#' @export
registerDoSnowflake <- function(conn,
                                mode = c("local", "tasks", "spcs", "queue"),
                                workers = "auto",
                                ...) {
  if (!requireNamespace("foreach", quietly = TRUE)) {
    cli::cli_abort(c(
      "Package {.pkg foreach} is required for {.fn registerDoSnowflake}.",
      "i" = "Install it with {.code install.packages('foreach')}."
    ))
  }
  if (!requireNamespace("iterators", quietly = TRUE)) {
    cli::cli_abort(c(
      "Package {.pkg iterators} is required for {.fn registerDoSnowflake}.",
      "i" = "Install it with {.code install.packages('iterators')}."
    ))
  }

  mode <- match.arg(mode)

  if (mode == "spcs") {
    cli::cli_abort(c(
      "Mode {.val spcs} is not yet implemented.",
      "i" = "Currently {.val local}, {.val tasks}, and {.val queue} modes are available.",
      "i" = "Mode {.val spcs} is planned for a future release."
    ))
  }

  backend_data <- list(
    conn    = conn,
    mode    = mode,
    workers = workers,
    options = list(...)
  )

  backend_fn <- switch(mode,
    local = .doSnowflakeLocal,
    tasks = .doSnowflakeTasks,
    queue = .doSnowflakeQueue
  )

  info_fn <- switch(mode,
    local = .doSnowflakeInfo,
    tasks = .doSnowflakeTasksInfo,
    queue = .doSnowflakeQueueInfo
  )

  foreach::setDoPar(
    fun  = backend_fn,
    data = backend_data,
    info = info_fn
  )

  if (mode == "local") {
    resolved_workers <- .resolve_snowflake_workers(workers)
    # Pre-warm the cluster so the first %dopar% call doesn't pay startup cost
    if (resolved_workers > 1L) {
      .get_or_create_cluster(resolved_workers)
    }
    cli::cli_inform(
      "Registered {.fn doSnowflake} backend (mode = {.val {mode}}, workers = {.val {resolved_workers}})."
    )
  } else {
    cli::cli_inform(
      "Registered {.fn doSnowflake} backend (mode = {.val {mode}})."
    )
  }
  invisible(TRUE)
}


# =============================================================================
# Info function
# =============================================================================

#' Return backend metadata for foreach
#' @noRd
.doSnowflakeInfo <- function(data, item) {
  switch(item,
    workers = .resolve_snowflake_workers(data$workers),
    name    = "doSnowflake",
    version = as.character(utils::packageVersion("snowflakeR")),
    NULL
  )
}


# =============================================================================
# Local backend function
# =============================================================================

#' Local-mode backend: socket cluster within the current container
#'
#' Satisfies the foreach backend contract: `function(obj, expr, envir, data)`.
#' @noRd
.doSnowflakeLocal <- function(obj, expr, envir, data) {
  if (!inherits(obj, "foreach")) {
    stop("obj must be a foreach object", call. = FALSE)
  }

  it <- iterators::iter(obj)
  accumulator <- foreach::makeAccum(it)

  arg_list <- as.list(it)
  n_tasks  <- length(arg_list)

  n_workers <- .resolve_snowflake_workers(data$workers)
  n_workers <- min(n_workers, n_tasks)

  if (n_workers <= 1L) {
    .dosnowflake_sequential(obj, expr, envir, arg_list, accumulator)
  } else {
    .dosnowflake_parallel(obj, expr, envir, arg_list, accumulator, n_workers)
  }

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

  foreach::getResult(it)
}


# =============================================================================
# Sequential path (workers = 1 or single-task fallback)
# =============================================================================

#' @noRd
.dosnowflake_sequential <- function(obj, expr, envir, arg_list, accumulator) {
  for (pkg in obj$packages) {
    library(pkg, character.only = TRUE)
  }

  xpr <- tryCatch(
    compiler::compile(expr, env = envir, options = compiler::getCompilerOption("optimize")),
    error = function(e) expr
  )

  for (i in seq_along(arg_list)) {
    args <- arg_list[[i]]
    for (nm in names(args)) {
      assign(nm, args[[nm]], pos = envir, inherits = FALSE)
    }
    r <- tryCatch(eval(xpr, envir = envir), error = function(e) e)
    accumulator(list(r), i)
  }
}


# =============================================================================
# Parallel path (cached cluster + iteration chunking)
# =============================================================================

#' @noRd
.dosnowflake_parallel <- function(obj, expr, envir, arg_list,
                                  accumulator, n_workers) {
  cl <- .get_or_create_cluster(n_workers)

  export_env <- .build_export_env(obj, expr, envir)
  if (length(ls(export_env)) > 0L) {
    parallel::clusterExport(cl, ls(export_env), envir = export_env)
  }

  for (pkg in obj$packages) {
    parallel::clusterCall(cl, library, pkg, character.only = TRUE)
  }

  chunks <- .chunk_arg_list(arg_list, n_workers)

  chunk_results <- parallel::clusterApply(cl, chunks, function(chunk) {
    lapply(chunk, function(args) {
      e <- new.env(parent = .GlobalEnv)
      for (nm in names(args)) assign(nm, args[[nm]], pos = e)
      tryCatch(eval(expr, envir = e), error = function(e) e)
    })
  })

  # Flatten chunks back to original iteration order
  idx <- 1L
  for (chunk_res in chunk_results) {
    for (r in chunk_res) {
      accumulator(list(r), idx)
      idx <- idx + 1L
    }
  }
}


#' Split an arg_list into n contiguous chunks for parallel dispatch
#' @noRd
.chunk_arg_list <- function(arg_list, n_chunks) {
  n <- length(arg_list)
  n_chunks <- min(n_chunks, n)
  splits <- .balanced_split_indices(n, n_chunks)
  lapply(splits, function(idxs) arg_list[idxs])
}

#' Compute balanced contiguous index ranges
#' @noRd
.balanced_split_indices <- function(n, k) {
  base_size <- n %/% k
  remainder <- n %% k
  starts <- integer(k)
  ends   <- integer(k)
  offset <- 0L
  for (i in seq_len(k)) {
    size <- base_size + (if (i <= remainder) 1L else 0L)
    starts[i] <- offset + 1L
    ends[i]   <- offset + size
    offset     <- offset + size
  }
  mapply(seq, starts, ends, SIMPLIFY = FALSE)
}


# =============================================================================
# Worker count resolution
# =============================================================================

#' Resolve the number of parallel workers
#'
#' When `workers` is `"auto"`, detects available CPU cores via
#' `parallel::detectCores()` and reserves one for the main R thread.
#' A positive integer is used as-is.
#'
#' Workspace Notebooks run inside SPCS containers whose vCPU count varies
#' by instance family (CPU_X64_XS = 1, CPU_X64_S = 3, CPU_X64_M = 6, etc.).
#' Auto-detection adapts to whatever the container exposes.
#'
#' @param workers Integer or `"auto"`.
#' @returns Integer number of workers to use (always >= 1).
#' @noRd
.resolve_snowflake_workers <- function(workers) {
  if (is.character(workers) && tolower(workers) == "auto") {
    detected <- tryCatch(
      parallel::detectCores(logical = FALSE),
      error = function(e) NA_integer_
    )
    if (is.na(detected) || detected < 1L) detected <- 2L
    return(max(1L, detected - 1L))
  }

  workers <- suppressWarnings(as.integer(workers))
  if (is.na(workers) || workers < 1L) workers <- 1L
  workers
}


# =============================================================================
# Export environment builder
# =============================================================================

#' Build an environment containing variables the workers need
#'
#' Collects variables listed in `.export` from the calling environment.
#' If `.export` is NULL, uses `foreach::getexports()` for automatic
#' detection of free variables in the expression.
#'
#' @param obj foreach object.
#' @param expr The quoted expression.
#' @param envir The calling environment.
#' @returns An environment containing variables to export.
#' @noRd
.build_export_env <- function(obj, expr, envir) {
  export_env <- new.env(parent = emptyenv())

  export_names <- obj$export
  if (is.null(export_names)) {
    export_names <- tryCatch(
      foreach::getexports(expr, envir, bad = obj$noexport),
      error = function(e) character(0)
    )
    # Supplement with all.vars() -- getexports can miss variables
    # that are resolved from the calling environment
    extra <- setdiff(all.vars(expr), export_names)
    export_names <- c(export_names, extra)
  }

  noexport <- if (is.null(obj$noexport)) character(0) else obj$noexport
  export_names <- setdiff(export_names, noexport)

  # Also exclude foreach iteration variable names
  iter_names <- names(obj$args)
  export_names <- setdiff(export_names, iter_names)

  for (nm in export_names) {
    val <- tryCatch(
      get(nm, envir = envir, inherits = TRUE),
      error = function(e) NULL
    )
    if (!is.null(val)) {
      assign(nm, val, pos = export_env)
    }
  }

  export_env
}
