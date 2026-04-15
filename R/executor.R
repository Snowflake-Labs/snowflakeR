# Generic R Executor (Strategy B)
# =============================================================================
# R interface for launching arbitrary R scripts on SPCS via EXECUTE JOB SERVICE.
#
# Scripts are loaded dynamically from a mounted Snowflake stage at runtime,
# eliminating Docker image rebuilds when R code changes. The executor
# container calls a user-specified entry function with a config list.
#
# Python bridge: sfr_executor_bridge.py

# =============================================================================
# Script upload
# =============================================================================

#' Upload an R Script to a Snowflake Stage
#'
#' Uploads a local R script to a Snowflake stage, ready for execution by
#' the generic R executor. The script must define an entry function
#' (default `main(config)`) that the executor will call.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param script_path Character. Local path to the R script.
#' @param stage Character. Stage name (default `"DOSNOWFLAKE_STAGE"`).
#' @param stage_dir Character. Directory within the stage (default
#'   `"scripts"`).
#'
#' @returns The stage-relative path for use in `sfr_execute_r_script()`,
#'   e.g. `"/stage/scripts/my_script.R"`.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' stage_path <- sfr_upload_r_script(conn, "my_analysis.R")
#' # stage_path == "/stage/scripts/my_analysis.R"
#' }
#' @export
sfr_upload_r_script <- function(conn,
                                script_path,
                                stage = "DOSNOWFLAKE_STAGE",
                                stage_dir = "scripts") {
  validate_connection(conn)
  stopifnot(file.exists(script_path))

  bridge <- .get_executor_bridge()
  result <- bridge$upload_r_script(
    session = conn$session,
    local_script_path = normalizePath(script_path),
    stage_name = paste0("@", stage),
    stage_dir = stage_dir
  )

  as.character(result)
}


# =============================================================================
# Executor launch
# =============================================================================

#' Execute an R Script on SPCS
#'
#' Launches a generic R executor on Snowflake Snowpark Container Services
#' that loads and runs the specified R script from a mounted stage. The
#' R script must define an entry function (default `main(config)`) that
#' receives a configuration list.
#'
#' This avoids Docker image rebuilds -- update the R script on the stage
#' and re-launch.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param r_script_stage_path Character. Path to the R script on the
#'   mounted stage, e.g. `"/stage/scripts/my_script.R"`.
#' @param compute_pool Character. SPCS compute pool name.
#' @param image_uri Character. Docker image URI for the executor container.
#' @param config Named list. Configuration passed to the R entry function
#'   as a list. Written as JSON to the stage.
#' @param replicas Integer. Number of parallel workers (default 1).
#' @param entry_fn Character. Name of the entry function (default `"main"`).
#' @param output_mode Character. `"stage"`, `"table"`, `"stdout"`, or
#'   `"none"` (default `"stage"`).
#' @param output_format Character. Output file format for stage results:
#'   `"rds"` (default, any R object), `"parquet"` (needs arrow pkg),
#'   `"csv"`, or `"json"`/`"ndjson"`. Parquet, CSV, and NDJSON can be
#'   read directly by Snowflake `COPY INTO` or External Tables.
#' @param stage Character. Stage to mount in the container (default
#'   `"DOSNOWFLAKE_STAGE"`).
#' @param instance_family Character. SPCS instance family for resource
#'   sizing (default `"CPU_X64_S"`).
#' @param eai Character. External Access Integration name for internet
#'   access (default `""`).
#' @param async Logical. If `TRUE` (default), launch asynchronously.
#'   If `FALSE`, block until completion.
#' @param job_name Character. Custom job service name. Auto-generated
#'   if empty.
#'
#' @returns A list with `job_id`, `job_name`, `replicas`, `status`,
#'   `output_path`.
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#'
#' # Upload script
#' sfr_upload_r_script(conn, "optimise_sku.R")
#'
#' # Launch with 4 replicas
#' job <- sfr_execute_r_script(
#'   conn,
#'   r_script_stage_path = "/stage/scripts/optimise_sku.R",
#'   compute_pool = "MY_POOL",
#'   image_uri = "/DB/SCHEMA/REPO/r_executor:latest",
#'   config = list(max_iter = 1000, tolerance = 0.01),
#'   replicas = 4
#' )
#' cat("Job launched:", job$job_name, "\n")
#'
#' # Poll for completion
#' status <- sfr_executor_status(conn, job$job_name)
#' }
#' @export
sfr_execute_r_script <- function(conn,
                                 r_script_stage_path,
                                 compute_pool,
                                 image_uri,
                                 config = list(),
                                 replicas = 1L,
                                 entry_fn = "main",
                                 output_mode = "stage",
                                 output_format = "rds",
                                 stage = "DOSNOWFLAKE_STAGE",
                                 instance_family = "CPU_X64_S",
                                 eai = "",
                                 async = TRUE,
                                 job_name = "") {
  validate_connection(conn)

  bridge <- .get_executor_bridge()

  result <- bridge$launch_executor(
    session = conn$session,
    compute_pool = compute_pool,
    image_uri = image_uri,
    r_script_stage_path = r_script_stage_path,
    stage_name = paste0("@", stage),
    config = config,
    replicas = as.integer(replicas),
    entry_fn = entry_fn,
    output_mode = output_mode,
    output_format = output_format,
    instance_family = instance_family,
    eai_name = eai,
    async_mode = async,
    job_name = job_name
  )

  as.list(result)
}


# =============================================================================
# Status and results
# =============================================================================

#' Poll Executor Job Status
#'
#' Checks the current status of an R executor job launched by
#' [sfr_execute_r_script()].
#'
#' @param conn An `sfr_connection` object.
#' @param job_name Character. The job service name returned by
#'   `sfr_execute_r_script()$job_name`.
#' @param timeout Integer. Maximum seconds to wait (default 3600).
#' @param poll_interval Integer. Seconds between polls (default 10).
#'
#' @returns A list with `status`, `elapsed_sec`, and optionally
#'   `instances`.
#'
#' @export
sfr_executor_status <- function(conn,
                                job_name,
                                timeout = 3600L,
                                poll_interval = 10L) {
  validate_connection(conn)

  bridge <- .get_executor_bridge()
  result <- bridge$poll_executor_status(
    session = conn$session,
    job_name = job_name,
    timeout_sec = as.integer(timeout),
    poll_sec = as.integer(poll_interval)
  )

  as.list(result)
}


#' Collect Executor Results from Stage
#'
#' Downloads result files written by executor workers from the stage
#' and loads them into R.
#'
#' @param conn An `sfr_connection` object.
#' @param job A list returned by [sfr_execute_r_script()], or a
#'   character string with the output path.
#' @param stage Character. Stage name (default `"DOSNOWFLAKE_STAGE"`).
#'
#' @returns A list of result objects (one per worker).
#'
#' @export
sfr_collect_executor_results <- function(conn, job, stage = "DOSNOWFLAKE_STAGE") {
  validate_connection(conn)

  if (is.list(job)) {
    output_path <- job$output_path
  } else {
    output_path <- as.character(job)
  }

  bridge <- .get_executor_bridge()
  local_dir <- tempfile("executor_results_")
  dir.create(local_dir)

  files <- bridge$collect_results(
    session = conn$session,
    stage_name = paste0("@", stage),
    output_path = output_path,
    local_dir = local_dir
  )

  file_paths <- as.character(files)

  results <- lapply(file_paths, function(f) {
    ext <- tolower(tools::file_ext(f))
    switch(ext,
      rds = readRDS(f),
      parquet = {
        if (requireNamespace("arrow", quietly = TRUE)) {
          arrow::read_parquet(f)
        } else {
          warning("arrow package needed to read .parquet; returning path")
          f
        }
      },
      csv = utils::read.csv(f, stringsAsFactors = FALSE),
      ndjson = , json = jsonlite::stream_in(file(f), verbose = FALSE),
      readLines(f)
    )
  })

  names(results) <- basename(file_paths)
  results
}


# =============================================================================
# Internal helpers
# =============================================================================

.get_executor_bridge <- function() {
  pkg_python <- system.file("python", package = "snowflakeR")
  reticulate::import_from_path("sfr_executor_bridge", path = pkg_python)
}
