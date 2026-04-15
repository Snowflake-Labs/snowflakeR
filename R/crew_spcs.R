# crew.spcs: mirai/crew Integration with SPCS Backend (Strategy A)
# =============================================================================
# Provides a crew launcher plugin that dispatches mirai tasks to SPCS
# containers. Workers connect back to the controller via TCP (NNG protocol).
#
# Requires: crew, mirai, nanonext (on the controller side)
# Python bridge: sfr_crew_bridge.py
#
# Usage:
#   controller <- crew_controller_spcs(conn, compute_pool, image)
#   controller$start()
#   results <- future_map(items, my_fn)
#   controller$terminate()

# =============================================================================
# Controller helper
# =============================================================================

#' Create a crew Controller with SPCS Workers
#'
#' Convenience function that creates a `crew` controller configured to
#' launch workers on Snowflake SPCS. Workers are launched as ephemeral
#' EXECUTE JOB SERVICE instances that connect back to the controller
#' via TCP.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param compute_pool Character. SPCS compute pool for workers.
#' @param image Character. Docker image URI with R + crew + mirai
#'   pre-installed.
#' @param workers Integer. Maximum number of concurrent workers (default 4).
#' @param seconds_idle Numeric. Seconds before idle workers are terminated
#'   (default 120).
#' @param host Character. Controller hostname. If `"auto"`, determines
#'   the correct host based on the execution environment.
#' @param port Integer. Controller listener port (default 5555).
#' @param instance_family Character. SPCS instance family (default
#'   `"CPU_X64_S"`).
#' @param eai Character. External Access Integration name.
#'
#' @returns A `crew_controller` object.
#'
#' @examples
#' \dontrun{
#' library(crew)
#' library(snowflakeR)
#'
#' conn <- sfr_connect()
#' controller <- crew_controller_spcs(
#'   conn, compute_pool = "MY_POOL",
#'   image = "/DB/SCHEMA/REPO/r_crew:latest",
#'   workers = 8
#' )
#' controller$start()
#'
#' # Submit tasks
#' controller$push(quote(1 + 1))
#' controller$pop()
#'
#' controller$terminate()
#' }
#' @export
crew_controller_spcs <- function(conn,
                                 compute_pool,
                                 image,
                                 workers = 4L,
                                 seconds_idle = 120,
                                 host = "auto",
                                 port = 5555L,
                                 instance_family = "CPU_X64_S",
                                 eai = "") {
  validate_connection(conn)

  rlang::check_installed("crew", reason = "for SPCS worker orchestration")
  rlang::check_installed("mirai", reason = "for async task dispatch")

  launcher <- crew_launcher_spcs$new(
    conn = conn,
    compute_pool = compute_pool,
    image_uri = image,
    instance_family = instance_family,
    eai_name = eai
  )

  crew::crew_controller(
    launcher = launcher,
    workers = as.integer(workers),
    seconds_idle = seconds_idle,
    host = if (host == "auto") NULL else host,
    port = as.integer(port)
  )
}


# =============================================================================
# Launcher R6 class
# =============================================================================

#' @title crew Launcher for SPCS
#' @description R6 class that implements the crew launcher interface
#'   for Snowflake SPCS. Each `launch_worker()` call creates an
#'   EXECUTE JOB SERVICE that runs `mirai::daemon()` connecting back
#'   to the controller.
#' @export
crew_launcher_spcs <- NULL  # defined below after checking crew availability

.onLoad_crew <- function() {
  # Only define the class if crew is available
  if (!requireNamespace("crew", quietly = TRUE)) {
    return()
  }

  cls <- R6::R6Class(
    classname = "crew_launcher_spcs",
    inherit = crew::crew_class_launcher,

    public = list(
      conn = NULL,
      compute_pool = NULL,
      image_uri = NULL,
      instance_family = NULL,
      eai_name = NULL,
      worker_handles = NULL,

      initialize = function(conn,
                           compute_pool,
                           image_uri,
                           instance_family = "CPU_X64_S",
                           eai_name = "",
                           ...) {
        super$initialize(...)
        self$conn <- conn
        self$compute_pool <- compute_pool
        self$image_uri <- image_uri
        self$instance_family <- instance_family
        self$eai_name <- eai_name
        self$worker_handles <- list()
      },

      launch_worker = function(call) {
        bridge <- .get_crew_bridge()

        controller_url <- call  # crew passes the connection URL

        result <- bridge$launch_crew_worker(
          session = self$conn$session,
          compute_pool = self$compute_pool,
          image_uri = self$image_uri,
          controller_url = controller_url,
          instance_family = self$instance_family,
          eai_name = self$eai_name
        )

        handle <- as.list(result)
        self$worker_handles[[handle$worker_name]] <- handle
        invisible(handle)
      },

      terminate_worker = function(handle) {
        bridge <- .get_crew_bridge()
        bridge$terminate_worker(
          session = self$conn$session,
          job_name = handle$job_name
        )
        self$worker_handles[[handle$worker_name]] <- NULL
        invisible(TRUE)
      }
    )
  )

  assign("crew_launcher_spcs", cls, envir = parent.env(environment()))
}


# =============================================================================
# SPCS Controller Service
# =============================================================================

#' Create a crew Controller as an SPCS Service
#'
#' For scenarios where the controller itself needs to run inside SPCS
#' (e.g., long-running orchestration without a Workspace session), this
#' function creates a persistent SPCS service with a TCP endpoint that
#' workers can connect to.
#'
#' @param conn An `sfr_connection` object.
#' @param compute_pool Character. SPCS compute pool.
#' @param image Character. Docker image URI.
#' @param service_name Character. Service name (default
#'   `"CREW_CONTROLLER"`).
#' @param port Integer. TCP listener port (default 5555).
#' @param r_script_stage_path Character. Optional R script for the
#'   controller to run.
#' @param stage Character. Optional stage to mount.
#'
#' @returns A list with `service_name`, `dns_name`, `controller_url`.
#'
#' @export
sfr_crew_controller_service <- function(conn,
                                        compute_pool,
                                        image,
                                        service_name = "CREW_CONTROLLER",
                                        port = 5555L,
                                        r_script_stage_path = "",
                                        stage = "") {
  validate_connection(conn)

  bridge <- .get_crew_bridge()

  result <- bridge$create_controller_service(
    session = conn$session,
    compute_pool = compute_pool,
    image_uri = image,
    service_name = service_name,
    listener_port = as.integer(port),
    r_script_stage_path = r_script_stage_path,
    stage_name = if (nzchar(stage)) paste0("@", stage) else ""
  )

  as.list(result)
}


#' Launch crew Workers for an Existing Controller
#'
#' Launches a batch of SPCS worker containers that connect to an
#' existing crew controller.
#'
#' @param conn An `sfr_connection` object.
#' @param controller_url Character. TCP URL of the controller,
#'   e.g. `"tcp://crew_controller.abc123.svc.spcs.internal:5555"`.
#' @param compute_pool Character. SPCS compute pool.
#' @param image Character. Docker image URI.
#' @param n_workers Integer. Number of workers (default 4).
#' @param instance_family Character. Instance family (default
#'   `"CPU_X64_S"`).
#'
#' @returns A list with `job_name`, `n_workers`, `status`.
#'
#' @export
sfr_crew_launch_workers <- function(conn,
                                    controller_url,
                                    compute_pool,
                                    image,
                                    n_workers = 4L,
                                    instance_family = "CPU_X64_S") {
  validate_connection(conn)

  bridge <- .get_crew_bridge()

  result <- bridge$launch_crew_workers_batch(
    session = conn$session,
    compute_pool = compute_pool,
    image_uri = image,
    controller_url = controller_url,
    n_workers = as.integer(n_workers),
    instance_family = instance_family
  )

  as.list(result)
}


# =============================================================================
# Internal helpers
# =============================================================================

.get_crew_bridge <- function() {
  pkg_python <- system.file("python", package = "snowflakeR")
  reticulate::import_from_path("sfr_crew_bridge", path = pkg_python)
}
