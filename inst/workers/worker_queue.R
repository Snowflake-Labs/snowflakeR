#!/usr/bin/env Rscript
# doSnowflake Queue Worker
# =============================================================================
# Runs inside an SPCS container (persistent service or ephemeral job).
# Polls a Hybrid Table queue for work, claims chunks, evaluates the R
# expression, and writes results back to stage.
#
# Two modes controlled by WORKER_MODE env var:
#   "queue"     -- persistent: loop until SIGTERM, backoff on empty queue
#   "ephemeral" -- exit after MAX_EMPTY_POLLS consecutive empty polls
#
# Environment variables:
#   WORKER_MODE  -- "queue" (default) or "ephemeral"
#   JOB_ID       -- UUID of the job to process ("*" for any)
#   QUEUE_FQN    -- Fully-qualified queue table (e.g. CONFIG.DOSNOWFLAKE_QUEUE)
#   STAGE_PATH   -- Base stage path (optional, for volume mounts)

library(DBI)
library(RSnowflake)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORKER_MODE      <- Sys.getenv("WORKER_MODE", "queue")
JOB_ID           <- Sys.getenv("JOB_ID", "*")
QUEUE_FQN        <- Sys.getenv("QUEUE_FQN", "CONFIG.DOSNOWFLAKE_QUEUE")
MAX_EMPTY_POLLS  <- as.integer(Sys.getenv("MAX_EMPTY_POLLS", "5"))
BACKOFF_BASE_SEC <- 2
BACKOFF_MAX_SEC  <- 30
WAREHOUSE        <- Sys.getenv("SNOWFLAKE_WAREHOUSE", "")
STAGE_MOUNT      <- Sys.getenv("STAGE_MOUNT", "")

# ---------------------------------------------------------------------------
# Stage I/O -- volume mount (filesystem) or GET/PUT fallback
# ---------------------------------------------------------------------------

.mount_path <- function(stage_path) {
  # Convert "@DB.SCHEMA.STAGE/job_xxx/tasks/file.rds" -> "/stage/job_xxx/tasks/file.rds"
  parts <- sub("^@[^/]+/", "", stage_path)
  file.path(STAGE_MOUNT, parts)
}

stage_get <- function(con, stage_path, local_dir) {
  if (nzchar(STAGE_MOUNT)) {
    src <- .mount_path(stage_path)
    dest <- file.path(local_dir, basename(src))
    file.copy(src, dest, overwrite = TRUE)
    return(invisible(NULL))
  }
  sql <- sprintf("GET '%s' 'file://%s'", stage_path, local_dir)
  DBI::dbExecute(con, sql)
}

stage_put <- function(con, local_path, stage_path) {
  if (nzchar(STAGE_MOUNT)) {
    dest_dir <- .mount_path(stage_path)
    dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
    dest <- file.path(dest_dir, basename(local_path))
    file.copy(local_path, dest, overwrite = TRUE)
    return(invisible(NULL))
  }
  sql <- sprintf(
    "PUT 'file://%s' '%s' AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
    local_path, stage_path
  )
  DBI::dbExecute(con, sql)
}

# ---------------------------------------------------------------------------
# Queue operations via SQL
# ---------------------------------------------------------------------------

claim_work <- function(con, job_id, worker_id, queue_fqn) {
  job_filter <- if (job_id == "*") "" else sprintf("AND JOB_ID = '%s'", job_id)
  claim_tag <- as.character(runif(1))

  DBI::dbExecute(con, sprintf("
    UPDATE %s
    SET STATUS = 'RUNNING',
        WORKER_ID = '%s',
        CLAIMED_AT = CURRENT_TIMESTAMP(),
        ERROR_MSG = '%s'
    WHERE QUEUE_ID = (
      SELECT QUEUE_ID FROM %s
      WHERE STATUS = 'PENDING' %s
      ORDER BY CREATED_AT
      LIMIT 1
    )
  ", queue_fqn, worker_id, claim_tag, queue_fqn, job_filter))

  result <- DBI::dbGetQuery(con, sprintf("
    SELECT QUEUE_ID, JOB_ID, CHUNK_ID, STAGE_PATH
    FROM %s
    WHERE ERROR_MSG = '%s' AND STATUS = 'RUNNING'
    LIMIT 1
  ", queue_fqn, claim_tag))

  if (nrow(result) == 0) return(NULL)

  DBI::dbExecute(con, sprintf("
    UPDATE %s SET ERROR_MSG = NULL WHERE QUEUE_ID = '%s'
  ", queue_fqn, result$QUEUE_ID[1]))

  list(
    queue_id   = result$QUEUE_ID[1],
    job_id     = result$JOB_ID[1],
    chunk_id   = result$CHUNK_ID[1],
    stage_path = result$STAGE_PATH[1]
  )
}

mark_done <- function(con, queue_id, queue_fqn) {
  DBI::dbExecute(con, sprintf("
    UPDATE %s
    SET STATUS = 'DONE', COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE QUEUE_ID = '%s'
  ", queue_fqn, queue_id))
}

mark_failed <- function(con, queue_id, error_msg, queue_fqn) {
  safe_msg <- gsub("'", "''", substr(error_msg, 1, 1000))
  DBI::dbExecute(con, sprintf("
    UPDATE %s
    SET STATUS = 'FAILED',
        COMPLETED_AT = CURRENT_TIMESTAMP(),
        ERROR_MSG = '%s'
    WHERE QUEUE_ID = '%s'
  ", queue_fqn, safe_msg, queue_id))
}

# ---------------------------------------------------------------------------
# Process one chunk
# ---------------------------------------------------------------------------

process_chunk <- function(con, claim, shared_cache) {
  sp <- claim$stage_path
  chunk_id <- claim$chunk_id
  tmp_dir <- tempdir()

  if (is.null(shared_cache$export_list) || shared_cache$job_id != claim$job_id) {
    cat(sprintf("[worker] Loading shared data for job %s\n", claim$job_id))
    stage_get(con, paste0(sp, "/export.rds"), tmp_dir)
    shared_cache$export_list <- readRDS(file.path(tmp_dir, "export.rds"))

    stage_get(con, paste0(sp, "/expr.rds"), tmp_dir)
    shared_cache$expr <- readRDS(file.path(tmp_dir, "expr.rds"))

    stage_get(con, paste0(sp, "/manifest.json"), tmp_dir)
    manifest <- jsonlite::fromJSON(file.path(tmp_dir, "manifest.json"))
    for (pkg in manifest$packages) {
      suppressPackageStartupMessages(library(pkg, character.only = TRUE))
    }
    shared_cache$job_id <- claim$job_id
  }

  task_file <- paste0("task_", chunk_id, ".rds")
  stage_get(con, paste0(sp, "/tasks/", task_file), tmp_dir)
  chunk <- readRDS(file.path(tmp_dir, task_file))

  cat(sprintf("[worker] Processing chunk %s (%d iterations)\n",
              chunk_id, length(chunk$args)))

  results <- vector("list", length(chunk$args))
  for (j in seq_along(chunk$args)) {
    e <- list2env(shared_cache$export_list, parent = .GlobalEnv)
    args <- chunk$args[[j]]
    for (nm in names(args)) assign(nm, args[[nm]], envir = e)

    results[[j]] <- tryCatch(
      eval(shared_cache$expr, envir = e),
      error = function(err) {
        cat(sprintf("[worker] Error in iteration %d: %s\n",
                    chunk$indices[j], conditionMessage(err)))
        err
      }
    )
  }

  result_data <- list(results = results, indices = chunk$indices)
  result_file <- paste0("result_", chunk_id, ".rds")
  result_path <- file.path(tmp_dir, result_file)
  saveRDS(result_data, result_path)
  stage_put(con, result_path, paste0(sp, "/results/"))
  unlink(result_path)

  cat(sprintf("[worker] Chunk %s complete\n", chunk_id))
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

main <- function() {
  worker_id <- sprintf("w_%s_%d", Sys.info()[["nodename"]], Sys.getpid())
  cat(sprintf("[worker] Starting queue worker: id=%s, mode=%s, job=%s\n",
              worker_id, WORKER_MODE, JOB_ID))

  con <- tryCatch(
    DBI::dbConnect(RSnowflake::Snowflake()),
    error = function(e) {
      cat(sprintf("[worker] Connection failed: %s\n", conditionMessage(e)))
      stop("Cannot connect to Snowflake")
    }
  )
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  shared_cache <- list(export_list = NULL, expr = NULL, job_id = NULL)
  empty_count <- 0L
  backoff_sec <- BACKOFF_BASE_SEC

  # Graceful shutdown flag
  running <- TRUE
  tryCatch(
    {
      while (running) {
        claim <- tryCatch(
          claim_work(con, JOB_ID, worker_id, QUEUE_FQN),
          error = function(e) {
            cat(sprintf("[worker] Claim error: %s\n", conditionMessage(e)))
            NULL
          }
        )

        if (is.null(claim)) {
          empty_count <- empty_count + 1L
          if (WORKER_MODE == "ephemeral" && empty_count >= MAX_EMPTY_POLLS) {
            cat(sprintf("[worker] No work after %d polls, exiting (ephemeral)\n",
                        empty_count))
            break
          }
          cat(sprintf("[worker] Queue empty, backoff %.0fs (empty=%d)\n",
                      backoff_sec, empty_count))
          Sys.sleep(backoff_sec)
          backoff_sec <- min(backoff_sec * 1.5, BACKOFF_MAX_SEC)
          next
        }

        empty_count <- 0L
        backoff_sec <- BACKOFF_BASE_SEC

        tryCatch(
          {
            process_chunk(con, claim, shared_cache)
            mark_done(con, claim$queue_id, QUEUE_FQN)
          },
          error = function(e) {
            cat(sprintf("[worker] Chunk %s failed: %s\n",
                        claim$chunk_id, conditionMessage(e)))
            tryCatch(
              mark_failed(con, claim$queue_id, conditionMessage(e), QUEUE_FQN),
              error = function(e2) {
                cat(sprintf("[worker] Could not mark failed: %s\n",
                            conditionMessage(e2)))
              }
            )
          }
        )
      }
    },
    interrupt = function(e) {
      cat("[worker] Received interrupt, shutting down\n")
    }
  )

  cat("[worker] Queue worker exiting\n")
}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

tryCatch(
  main(),
  error = function(e) {
    cat(sprintf("[worker] FATAL: %s\n", conditionMessage(e)))
    quit(status = 1)
  }
)
