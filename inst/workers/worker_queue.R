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
#   WORKER_MODE    -- "queue" (default) or "ephemeral"
#   JOB_ID         -- UUID of the job to process ("*" for any)
#   QUEUE_FQN      -- Fully-qualified queue table (e.g. CONFIG.DOSNOWFLAKE_QUEUE)
#   STAGE_PATH     -- Base stage path (optional, for volume mounts)
#   LEASE_MINUTES  -- Lease duration for claimed chunks (default 10)

library(DBI)
library(RSnowflake)
library(parallel)

`%||%` <- function(x, y) if (is.null(x) || (is.character(x) && !nzchar(x))) y else x

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORKER_MODE      <- Sys.getenv("WORKER_MODE", "queue")
JOB_ID           <- Sys.getenv("JOB_ID", "*")
QUEUE_FQN        <- Sys.getenv("QUEUE_FQN", "CONFIG.DOSNOWFLAKE_QUEUE")
MAX_EMPTY_POLLS  <- as.integer(Sys.getenv("MAX_EMPTY_POLLS", "5"))
LEASE_MINUTES    <- as.integer(Sys.getenv("LEASE_MINUTES", "10"))
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
# Queue operations via SQL -- lease-based compare-and-swap (CAS)
#
# Two-step claim prevents silent race losses that caused stuck PENDING chunks:
#   1. SELECT a candidate row (PENDING or expired lease)
#   2. UPDATE with WHERE re-checking claimability (CAS)
#   3. If n_updated == 0, return "RETRY" (not NULL) so caller retries
#      immediately without incrementing empty_count
# ---------------------------------------------------------------------------

esc <- function(x) gsub("'", "''", as.character(x))

claim_work <- function(con, job_id, worker_id, queue_fqn, lease_min) {
  job_filter <- if (job_id == "*") "" else sprintf("AND JOB_ID = '%s'", job_id)

  candidate <- DBI::dbGetQuery(con, sprintf("
    SELECT QUEUE_ID FROM %s
    WHERE (STATUS = 'PENDING'
           OR (STATUS = 'RUNNING'
               AND LEASE_UNTIL IS NOT NULL
               AND LEASE_UNTIL < CURRENT_TIMESTAMP()))
      %s
    ORDER BY CREATED_AT
    LIMIT 1
  ", queue_fqn, job_filter))

  if (nrow(candidate) == 0) return(NULL)
  cand_id <- candidate$QUEUE_ID[1]

  n_updated <- DBI::dbExecute(con, sprintf("
    UPDATE %s
    SET STATUS      = 'RUNNING',
        WORKER_ID   = '%s',
        CLAIMED_AT  = CURRENT_TIMESTAMP(),
        LEASE_UNTIL = DATEADD('minute', %d, CURRENT_TIMESTAMP()),
        ATTEMPTS    = NVL(ATTEMPTS, 0) + 1
    WHERE QUEUE_ID = '%s'
      AND (STATUS = 'PENDING'
           OR (STATUS = 'RUNNING'
               AND LEASE_UNTIL IS NOT NULL
               AND LEASE_UNTIL < CURRENT_TIMESTAMP()))
  ", queue_fqn, esc(worker_id), lease_min, cand_id))

  if (n_updated == 0) {
    cat(sprintf("[worker] Claim race lost for %s (another worker won)\n", cand_id))
    return("RETRY")
  }

  result <- DBI::dbGetQuery(con, sprintf("
    SELECT QUEUE_ID, JOB_ID, CHUNK_ID, STAGE_PATH
    FROM %s WHERE QUEUE_ID = '%s'
  ", queue_fqn, cand_id))

  if (nrow(result) == 0) return(NULL)
  list(
    queue_id   = result$QUEUE_ID[1],
    job_id     = result$JOB_ID[1],
    chunk_id   = result$CHUNK_ID[1],
    stage_path = result$STAGE_PATH[1]
  )
}

renew_lease <- function(con, queue_id, worker_id, queue_fqn, lease_min) {
  DBI::dbExecute(con, sprintf("
    UPDATE %s SET LEASE_UNTIL = DATEADD('minute', %d, CURRENT_TIMESTAMP())
    WHERE QUEUE_ID = '%s' AND WORKER_ID = '%s' AND STATUS = 'RUNNING'
  ", queue_fqn, lease_min, queue_id, esc(worker_id)))
}

mark_done <- function(con, queue_id, queue_fqn) {
  DBI::dbExecute(con, sprintf("
    UPDATE %s
    SET STATUS = 'DONE', COMPLETED_AT = CURRENT_TIMESTAMP(), LEASE_UNTIL = NULL
    WHERE QUEUE_ID = '%s'
  ", queue_fqn, queue_id))
}

mark_failed <- function(con, queue_id, error_msg, queue_fqn) {
  safe_msg <- gsub("'", "''", substr(error_msg, 1, 1000))
  DBI::dbExecute(con, sprintf("
    UPDATE %s
    SET STATUS = 'FAILED',
        COMPLETED_AT = CURRENT_TIMESTAMP(),
        LEASE_UNTIL = NULL,
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
    shared_cache$manifest <- manifest
    shared_cache$job_id <- claim$job_id
  }

  task_file <- paste0("task_", chunk_id, ".rds")
  stage_get(con, paste0(sp, "/tasks/", task_file), tmp_dir)
  chunk <- readRDS(file.path(tmp_dir, task_file))

  Sys.setenv(OPENBLAS_NUM_THREADS = "1", OMP_NUM_THREADS = "1",
             MKL_NUM_THREADS = "1")
  n_cores <- max(1L, detectCores() - 1L)

  # --- Manifest-driven bulk read (if data_query present) ---
  unit_data_splits <- NULL
  dq <- shared_cache$manifest$data_query
  if (!is.null(dq)) {
    key_arg <- dq$key_arg
    keys <- vapply(chunk$args, function(a) as.character(a[[key_arg]]), character(1))
    sql_in <- paste(sprintf("'%s'", gsub("'", "''", keys)), collapse = ", ")
    cols <- paste(dq$columns, collapse = ", ")
    query <- sprintf("SELECT %s FROM %s WHERE %s IN (%s) ORDER BY %s",
                     cols, dq$table, dq$key_column, sql_in, dq$order_by)

    if (!is.null(dq$warehouse) && nzchar(dq$warehouse)) {
      tryCatch(DBI::dbExecute(con, sprintf("USE WAREHOUSE %s", dq$warehouse)),
               error = function(e) NULL)
    }
    cat(sprintf("[worker] Bulk read: %d keys from %s\n", length(keys), dq$table))
    all_data <- DBI::dbGetQuery(con, query)
    cat(sprintf("[worker] Read %d rows for %d keys\n", nrow(all_data), length(keys)))
    unit_data_splits <- split(all_data, all_data[[dq$key_column]])
  }

  cat(sprintf("[worker] Processing chunk %s (%d iterations, %d cores)\n",
              chunk_id, length(chunk$args), n_cores))

  expr <- shared_cache$expr
  export_list <- shared_cache$export_list
  results <- mclapply(seq_along(chunk$args), function(j) {
    e <- list2env(export_list, parent = .GlobalEnv)
    args <- chunk$args[[j]]
    for (nm in names(args)) assign(nm, args[[nm]], envir = e)

    if (!is.null(unit_data_splits) && !is.null(dq)) {
      key_val <- as.character(args[[dq$key_arg]])
      assign("unit_data", unit_data_splits[[key_val]], envir = e)
    }

    tryCatch(
      eval(expr, envir = e),
      error = function(err) {
        cat(sprintf("[worker] Error in iteration %d: %s\n",
                    chunk$indices[j], conditionMessage(err)))
        err
      }
    )
  }, mc.cores = n_cores)

  # Persist individual model .rds files to a run-specific directory on the stage
  manifest <- shared_cache$manifest
  if (isTRUE(manifest$save_models) && nzchar(manifest$model_run_id %||% "")) {
    if (nzchar(STAGE_MOUNT)) {
      models_dir <- file.path(STAGE_MOUNT, "models", manifest$model_run_id)
    } else {
      models_dir <- file.path(tmp_dir, "models_out")
    }
    dir.create(models_dir, showWarnings = FALSE, recursive = TRUE)
    n_saved <- 0L
    for (j in seq_along(results)) {
      r <- results[[j]]
      if (!inherits(r, "error") && !is.null(r$model_obj)) {
        key <- as.character(chunk$args[[j]][[manifest$model_key_arg]])
        saveRDS(r$model_obj, file.path(models_dir, paste0(key, ".rds")))
        n_saved <- n_saved + 1L
        results[[j]]$model_obj <- NULL
      }
    }
    cat(sprintf("[worker] Saved %d model .rds files to models/%s/\n",
                n_saved, manifest$model_run_id))
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

  if (nzchar(WAREHOUSE)) {
    tryCatch(
      DBI::dbExecute(con, sprintf("USE WAREHOUSE %s", WAREHOUSE)),
      error = function(e) cat(sprintf("[worker] USE WAREHOUSE failed: %s\n", conditionMessage(e)))
    )
  }

  shared_cache <- list(export_list = NULL, expr = NULL, job_id = NULL)
  empty_count <- 0L
  race_losses <- 0L
  backoff_sec <- BACKOFF_BASE_SEC

  running <- TRUE
  tryCatch(
    {
      while (running) {
        claim <- tryCatch(
          claim_work(con, JOB_ID, worker_id, QUEUE_FQN, LEASE_MINUTES),
          error = function(e) {
            cat(sprintf("[worker] Claim error: %s\n", conditionMessage(e)))
            Sys.sleep(1)
            NULL
          }
        )

        if (identical(claim, "RETRY")) {
          race_losses <- race_losses + 1L
          next
        }

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

  cat(sprintf("[worker] Queue worker exiting (race_losses=%d)\n", race_losses))
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
