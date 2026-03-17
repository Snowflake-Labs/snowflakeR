# doSnowflake: foreach Parallel Backend for Snowflake
# =============================================================================
# Implements the foreach %dopar% contract so that R users can run parallel
# loops on Snowflake compute.  Phase 0 provides local (in-container)
# parallelism via socket clusters; remote modes (tasks, spcs, queue)
# are planned for subsequent phases.


#' Register the doSnowflake parallel backend
#'
#' Registers a `foreach` parallel backend that dispatches iterations to
#' Snowflake compute.
#'
#' In **local** mode (the default and currently the only implemented mode),
#' iterations execute on a `parallel::makeCluster()` socket cluster within
#' the current R process's container.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param mode Character.
#'   * `"local"` -- socket cluster in the current container (default).
#'   * `"tasks"` -- Snowflake Task graph + SPCS jobs.
#'   * `"spcs"` -- stage-based SPCS job dispatch (not yet implemented).
#'   * `"queue"` -- persistent worker pool with queue (not yet implemented).
#' @param workers Integer or `"auto"`. Number of parallel workers.
#'   `"auto"` (default) detects available CPU cores and reserves one for
#'   the main thread.
#' @param ... Backend-specific options. For `mode = "tasks"`:
#'   * `compute_pool` (required) -- name of the SPCS compute pool.
#'   * `image_uri` (required) -- worker Docker image URI in image repo.
#'   * `stage` -- stage name (default `"DOSNOWFLAKE_STAGE"`).
#'   * `timeout_min` -- max minutes to wait for completion (default 30).
#'   * `poll_sec` -- seconds between status polls (default 5).
#'   * `chunks_per_job` -- number of SPCS jobs to create (default `"auto"`).
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

  if (mode %in% c("spcs", "queue")) {
    cli::cli_abort(c(
      "Mode {.val {mode}} is not yet implemented.",
      "i" = "Currently {.val local} and {.val tasks} modes are available.",
      "i" = "Modes {.val spcs} and {.val queue} are planned for future releases."
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
    tasks = .doSnowflakeTasks
  )

  info_fn <- switch(mode,
    local = .doSnowflakeInfo,
    tasks = .doSnowflakeTasksInfo
  )

  foreach::setDoPar(
    fun  = backend_fn,
    data = backend_data,
    info = info_fn
  )

  if (mode == "local") {
    resolved_workers <- .resolve_snowflake_workers(workers)
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
# Parallel path (socket cluster)
# =============================================================================

#' @noRd
.dosnowflake_parallel <- function(obj, expr, envir, arg_list,
                                  accumulator, n_workers) {
  cl <- parallel::makeCluster(n_workers)
  on.exit(parallel::stopCluster(cl), add = TRUE)

  # Export variables that the expression needs
  export_env <- .build_export_env(obj, expr, envir)
  if (length(ls(export_env)) > 0L) {
    parallel::clusterExport(cl, ls(export_env), envir = export_env)
  }

  # Load required packages on workers
  for (pkg in obj$packages) {
    parallel::clusterCall(cl, library, pkg, character.only = TRUE)
  }

  # Execute iterations across workers with load balancing
  results <- parallel::clusterApplyLB(cl, arg_list, function(args) {
    e <- new.env(parent = .GlobalEnv)
    for (nm in names(args)) assign(nm, args[[nm]], pos = e)
    tryCatch(eval(expr, envir = e), error = function(e) e)
  })

  for (i in seq_along(results)) {
    accumulator(list(results[[i]]), i)
  }
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
