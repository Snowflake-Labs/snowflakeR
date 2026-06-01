# doSnowflake: Queue Backend (Phase 2)
# =============================================================================
# Implements the foreach backend contract for mode = "queue".
# Uses a Hybrid Table queue for work distribution with both persistent
# worker pools and ephemeral EXECUTE JOB SERVICE workers.


#' Queue-mode backend: Hybrid Table queue + SPCS workers
#'
#' Satisfies the foreach backend contract: `function(obj, expr, envir, data)`.
#' Serializes iterations to a Snowflake stage, enqueues chunks into a Hybrid
#' Table, optionally launches ephemeral workers, polls for completion, and
#' collects results.
#' @noRd
.doSnowflakeQueue <- function(obj, expr, envir, data) {
  if (!inherits(obj, "foreach")) {
    stop("obj must be a foreach object", call. = FALSE)
  }

  conn <- data$conn
  opts <- .resolve_queue_options(data)

  it <- iterators::iter(obj)
  accumulator <- foreach::makeAccum(it)
  arg_list <- as.list(it)
  n_tasks <- length(arg_list)

  if (n_tasks == 0L) {
    return(foreach::getResult(it))
  }

  job_id <- .generate_job_id()
  bridge <- get_bridge_module("sfr_queue_bridge")

  cli::cli_inform(c(
    "i" = "doSnowflake queue mode: dispatching {n_tasks} iteration{?s}.",
    "i" = "Job ID: {.val {job_id}}",
    "i" = "Worker type: {.val {opts$worker_type}}",
    "i" = "Chunks: {.val {opts$chunks_per_job}}"
  ))

  # 1. Ensure queue table exists
  bridge$create_queue_table(
    session = conn$session,
    fqn     = opts$queue_fqn
  )

  # 2. Serialize job to stage
  cli::cli_inform("Serializing {n_tasks} iteration{?s} to stage...")
  job <- .serialize_job_to_stage(conn, job_id, expr, arg_list,
                                 obj, envir, opts)

  # 3. Enqueue chunks
  cli::cli_inform("Enqueuing {job$n_chunks} chunk{?s} to Hybrid Table queue...")
  bridge$enqueue_chunks(
    session   = conn$session,
    job_id    = job_id,
    n_chunks  = as.integer(job$n_chunks),
    stage_path = job$stage_path,
    queue_fqn = opts$queue_fqn
  )

  # 4. Launch workers if ephemeral mode (persistent pools should already be running)
  if (opts$worker_type == "ephemeral") {
    cli::cli_inform("Launching {opts$n_workers} ephemeral worker{?s}...")
    bridge$create_ephemeral_workers(
      session        = conn$session,
      compute_pool   = opts$compute_pool,
      image_uri      = opts$image_uri,
      job_id         = job_id,
      stage_path     = job$stage_path,
      n_workers      = as.integer(opts$n_workers),
      queue_fqn      = opts$queue_fqn,
      instance_family = opts$instance_family,
      warehouse      = opts$warehouse
    )

    # 4b. Optionally wait for all workers to be READY before they start claiming
    if (opts$pre_warm) {
      cli::cli_inform("Pre-warming: waiting for all {opts$n_workers} worker{?s} to be READY...")
      bridge$wait_for_workers_ready(
        session    = conn$session,
        n_expected = as.integer(opts$n_workers),
        timeout_sec = 300L,
        poll_sec    = 5L
      )
      cli::cli_inform("All workers ready.")
    }
  }

  # 5. Poll queue until all chunks complete
  cli::cli_inform("Polling queue for completion (timeout: {opts$timeout_min}min)...")
  t_start <- Sys.time()

  final_status <- bridge$poll_until_complete(
    session          = conn$session,
    job_id           = job_id,
    timeout_sec      = as.integer(opts$timeout_min * 60),
    poll_sec         = as.integer(opts$poll_sec),
    queue_fqn        = opts$queue_fqn,
    stale_timeout_sec = as.integer(opts$stale_timeout_sec),
    service_name     = opts$service_name %||% "",
    compute_pool     = opts$compute_pool %||% "",
    image_uri        = opts$image_uri %||% "",
    n_workers        = as.integer(opts$n_workers %||% 0)
  )

  elapsed <- as.numeric(difftime(Sys.time(), t_start, units = "secs"))
  cli::cli_inform(c(
    "i" = "Queue polling complete in {round(elapsed, 1)}s.",
    "i" = "Done: {final_status$done}, Failed: {final_status$failed}"
  ))

  if (!is.null(final_status$error) && final_status$error == "timeout") {
    cli::cli_abort(c(
      "doSnowflake: queue timed out after {opts$timeout_min} minute{?s}.",
      "i" = "Job: {.val {job_id}}",
      "i" = "Pending: {final_status$pending}, Running: {final_status$running}"
    ))
  }

  if (final_status$failed > 0) {
    cli::cli_warn(c(
      "!" = "{final_status$failed} chunk{?s} failed during execution.",
      "i" = "Check queue table {.val {opts$queue_fqn}} for error details."
    ))
  }

  # 6. Collect results from stage
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

  # 7. Cleanup
  tryCatch(
    bridge$cleanup_job(session = conn$session, job_id = job_id,
                       queue_fqn = opts$queue_fqn),
    error = function(e) NULL
  )
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

  cli::cli_inform(c("v" = "doSnowflake queue job {.val {job_id}} completed."))
  foreach::getResult(it)
}


# =============================================================================
# Queue-mode info function
# =============================================================================

#' @noRd
.doSnowflakeQueueInfo <- function(data, item) {
  switch(item,
    workers = as.integer(data$options$n_workers %||% 4),
    name    = "doSnowflake",
    version = as.character(utils::packageVersion("snowflakeR")),
    NULL
  )
}


# =============================================================================
# Option resolution
# =============================================================================

#' Resolve queue-mode options from user-supplied ... args
#' @noRd
.resolve_queue_options <- function(data) {
  user_opts <- data$options
  if (is.null(user_opts)) user_opts <- list()

  worker_type <- user_opts$worker_type %||% "ephemeral"
  if (!worker_type %in% c("ephemeral", "persistent")) {
    cli::cli_abort(c(
      "{.arg worker_type} must be {.val ephemeral} or {.val persistent}.",
      "i" = "Got: {.val {worker_type}}"
    ))
  }

  if (worker_type == "ephemeral") {
    compute_pool <- user_opts$compute_pool
    if (is.null(compute_pool) || !nzchar(compute_pool)) {
      cli::cli_abort(c(
        "{.arg compute_pool} is required for queue mode with ephemeral workers.",
        "i" = 'registerDoSnowflake(conn, mode = "queue", compute_pool = "MY_POOL", image_uri = "...")'
      ))
    }

    image_uri <- user_opts$image_uri
    if (is.null(image_uri) || !nzchar(image_uri)) {
      cli::cli_abort(c(
        "{.arg image_uri} is required for queue mode with ephemeral workers.",
        "i" = "Build the worker image first, then pass the URI."
      ))
    }
  } else {
    compute_pool <- user_opts$compute_pool %||% ""
    image_uri <- user_opts$image_uri %||% ""
  }

  list(
    worker_type       = worker_type,
    compute_pool      = compute_pool,
    image_uri         = image_uri,
    queue_fqn         = user_opts$queue_fqn %||% "CONFIG.DOSNOWFLAKE_QUEUE",
    stage             = user_opts$stage %||% "DOSNOWFLAKE_STAGE",
    n_workers         = as.integer(user_opts$n_workers %||% 4),
    timeout_min       = as.numeric(user_opts$timeout_min %||% 30),
    poll_sec          = as.numeric(user_opts$poll_sec %||% 5),
    stale_timeout_sec = as.numeric(user_opts$stale_timeout_sec %||% 600),
    chunks_per_job    = user_opts$chunks_per_job %||% "auto",
    pre_warm          = isTRUE(user_opts$pre_warm),
    service_name      = user_opts$service_name %||% NULL,
    instance_family   = user_opts$instance_family %||% "CPU_X64_S",
    warehouse         = user_opts$warehouse %||% "",
    result_sync_wait_sec = as.numeric(user_opts$result_sync_wait_sec %||% 45),
    result_sync_poll_sec = as.numeric(user_opts$result_sync_poll_sec %||% 3)
  )
}


# =============================================================================
# Worker pool management (exported functions)
# =============================================================================

#' Create a persistent worker pool for queue-based dispatch
#'
#' Launches a long-running SPCS service with multiple replicas that
#' continuously poll the Hybrid Table queue for work.
#'
#' @param conn An `sfr_connection` object.
#' @param service_name Character. Name for the SPCS service.
#' @param compute_pool Character. Name of the SPCS compute pool.
#' @param image_uri Character. Docker image URI.
#' @param replicas Integer. Number of worker instances (default 2).
#' @param job_id Character. Job to process ('*' for any, default '*').
#' @param queue_fqn Character. Queue table name.
#' @param ... Additional options passed to the bridge.
#'
#' @returns Invisibly returns the service name.
#' @export
sfr_create_worker_pool <- function(conn,
                                   service_name,
                                   compute_pool,
                                   image_uri,
                                   replicas = 2L,
                                   job_id = "*",
                                   queue_fqn = "CONFIG.DOSNOWFLAKE_QUEUE",
                                   ...) {
  bridge <- get_bridge_module("sfr_queue_bridge")

  bridge$create_queue_table(session = conn$session, fqn = queue_fqn)

  bridge$create_worker_pool(
    session      = conn$session,
    service_name = service_name,
    compute_pool = compute_pool,
    image_uri    = image_uri,
    replicas     = as.integer(replicas),
    job_id       = job_id,
    queue_fqn    = queue_fqn
  )

  cli::cli_inform(c(
    "v" = "Worker pool {.val {service_name}} created with {replicas} replica{?s}.",
    "i" = "Workers are polling {.val {queue_fqn}} for work."
  ))
  invisible(service_name)
}


#' Get the status of a worker pool
#'
#' @param conn An `sfr_connection` object.
#' @param service_name Character. Name of the SPCS service.
#'
#' @returns A list with name, status, and instance count.
#' @export
sfr_worker_pool_status <- function(conn, service_name) {
  bridge <- get_bridge_module("sfr_queue_bridge")
  result <- bridge$get_pool_status(
    session      = conn$session,
    service_name = service_name
  )
  as.list(result)
}


#' Stop and remove a worker pool
#'
#' @param conn An `sfr_connection` object.
#' @param service_name Character. Name of the SPCS service.
#'
#' @returns Invisibly returns TRUE.
#' @export
sfr_stop_worker_pool <- function(conn, service_name) {
  bridge <- get_bridge_module("sfr_queue_bridge")
  bridge$stop_worker_pool(
    session      = conn$session,
    service_name = service_name
  )
  cli::cli_inform(c("v" = "Worker pool {.val {service_name}} stopped."))
  invisible(TRUE)
}


# =============================================================================
# Parcel iterator helper
# =============================================================================

#' Create a parcel iterator for bulk I/O in doSnowflake
#'
#' Groups a vector of partition keys (e.g. SKU IDs) into parcels of size
#' `parcel_size`. Each parcel is a character vector that the foreach body
#' can process as a batch, enabling a single stage read and write per
#' parcel instead of per-SKU.
#'
#' @param keys Character vector of partition keys.
#' @param parcel_size Integer. Number of keys per parcel (default 50).
#'
#' @returns A list of character vectors, one per parcel.
#'
#' @examples
#' \dontrun{
#' skus <- paste0("SKU_", 1:200)
#' parcels <- sfr_parcel_iterator(skus, parcel_size = 50)
#' # parcels is a list of 4 character vectors, each with 50 SKU IDs
#'
#' library(foreach)
#' results <- foreach(parcel = parcels, .combine = rbind) %dopar% {
#'   # Bulk read data for all SKUs in parcel
#'   # Train models for each
#'   # Bulk write results
#' }
#' }
#'
#' @export
sfr_parcel_iterator <- function(keys, parcel_size = 50L) {
  keys <- as.character(keys)
  n <- length(keys)
  if (n == 0L) return(list())

  parcel_size <- max(1L, as.integer(parcel_size))
  n_parcels <- ceiling(n / parcel_size)

  parcels <- vector("list", n_parcels)
  for (i in seq_len(n_parcels)) {
    start <- (i - 1L) * parcel_size + 1L
    end <- min(i * parcel_size, n)
    parcels[[i]] <- keys[start:end]
  }

  parcels
}


# =============================================================================
# Dynamic scaling functions
# =============================================================================

#' Add workers to a running queue job
#'
#' Launches additional ephemeral SPCS workers that claim from the same
#' queue. Use this to speed up a long-running job mid-flight.
#'
#' @param conn An `sfr_connection` object.
#' @param job_id Character. Job ID to work on.
#' @param n Integer. Number of additional workers.
#' @param compute_pool Character. Compute pool name.
#' @param image_uri Character. Worker Docker image URI.
#' @param queue_fqn Character. Queue table name.
#' @param instance_family Character. Instance family for resource sizing.
#'
#' @returns Invisibly returns the number of workers added.
#' @export
sfr_add_workers <- function(conn, job_id, n,
                            compute_pool,
                            image_uri,
                            queue_fqn = "CONFIG.DOSNOWFLAKE_QUEUE",
                            instance_family = "CPU_X64_S") {
  bridge <- get_bridge_module("sfr_queue_bridge")

  bridge$create_ephemeral_workers(
    session         = conn$session,
    compute_pool    = compute_pool,
    image_uri       = image_uri,
    job_id          = job_id,
    stage_path      = "",
    n_workers       = as.integer(n),
    queue_fqn       = queue_fqn,
    instance_family = instance_family
  )

  cli::cli_inform(c(
    "v" = "Added {n} worker{?s} to job {.val {job_id}}.",
    "i" = "New workers will begin claiming from {.val {queue_fqn}}."
  ))
  invisible(as.integer(n))
}


#' Scale a persistent worker pool
#'
#' Alters the MIN/MAX instances of an existing SPCS service to scale
#' the worker pool up or down.
#'
#' @param conn An `sfr_connection` object.
#' @param service_name Character. Name of the SPCS service.
#' @param replicas Integer. New number of replicas.
#'
#' @returns Invisibly returns the service name.
#' @export
sfr_scale_worker_pool <- function(conn, service_name, replicas) {
  bridge <- get_bridge_module("sfr_queue_bridge")

  bridge$scale_worker_pool(
    session      = conn$session,
    service_name = service_name,
    replicas     = as.integer(replicas)
  )

  cli::cli_inform(c(
    "v" = "Scaled {.val {service_name}} to {replicas} replica{?s}."
  ))
  invisible(service_name)
}


#' Get queue status for a running job
#'
#' Returns pending, running, done, and failed counts plus throughput
#' estimates for a queue-based job.
#'
#' @param conn An `sfr_connection` object.
#' @param job_id Character. Job ID to check.
#' @param queue_fqn Character. Queue table name.
#'
#' @returns A list with total, pending, running, done, failed counts
#'   and throughput_per_min estimate.
#' @export
sfr_queue_status <- function(conn, job_id,
                             queue_fqn = "CONFIG.DOSNOWFLAKE_QUEUE") {
  bridge <- get_bridge_module("sfr_queue_bridge")

  status <- bridge$poll_job_status(
    session   = conn$session,
    job_id    = job_id,
    queue_fqn = queue_fqn
  )

  result <- as.list(status)

  # Estimate throughput from completed items
  tryCatch({
    throughput <- bridge$estimate_throughput(
      session   = conn$session,
      job_id    = job_id,
      queue_fqn = queue_fqn
    )
    result$throughput_per_min <- throughput$throughput_per_min
    result$eta_minutes <- throughput$eta_minutes
  }, error = function(e) NULL)

  result
}
