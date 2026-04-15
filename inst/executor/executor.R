#!/usr/bin/env Rscript
# =============================================================================
# Generic SPCS R Executor
#
# Loads and runs arbitrary R scripts from a mounted Snowflake stage.
# The user's script must define an entry function (default: main(config))
# that receives a config list and returns a data.frame or list.
#
# Environment variables:
#   R_SCRIPT      - Path to the R script to execute (required)
#   R_ENTRY_FN    - Name of the entry function (default: "main")
#   R_CONFIG      - Path to JSON config file (optional)
#   OUTPUT_MODE   - "stage" | "table" | "stdout" | "none" (default: "stage")
#   OUTPUT_FORMAT - "rds" | "parquet" | "csv" | "json"/"ndjson" (default: "rds")
#   OUTPUT_PATH   - Directory for stage output (default: "/stage/results/")
#   STAGE_MOUNT   - Stage mount path (default: "/stage")
#   SNOWFLAKE_WAREHOUSE - Warehouse for SQL operations
#
# SPCS-injected variables (automatic):
#   SNOWFLAKE_JOB_INDEX        - Worker index (EJS)
#   SNOWFLAKE_JOBS_COUNT       - Total replicas (EJS)
#   SNOWFLAKE_SERVICE_INSTANCE_INDEX - Instance index (service)
# =============================================================================

suppressPackageStartupMessages({
  library(jsonlite)
})

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
.worker_id <- Sys.getenv("SNOWFLAKE_JOB_INDEX",
                          Sys.getenv("SNOWFLAKE_SERVICE_INSTANCE_INDEX", "0"))
.n_workers <- as.integer(Sys.getenv("SNOWFLAKE_JOBS_COUNT",
                                     Sys.getenv("REPLICAS", "1")))

log_msg <- function(...) {
  cat(sprintf("[executor-%s %s] %s\n", .worker_id,
              format(Sys.time(), "%H:%M:%S"), paste0(...)))
}

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
r_script    <- Sys.getenv("R_SCRIPT", "")
r_entry_fn  <- Sys.getenv("R_ENTRY_FN", "main")
r_config    <- Sys.getenv("R_CONFIG", "")
output_mode <- Sys.getenv("OUTPUT_MODE", "stage")
output_path <- Sys.getenv("OUTPUT_PATH", "/stage/results/")
stage_mount <- Sys.getenv("STAGE_MOUNT", "/stage")

if (!nzchar(r_script)) {
  stop("R_SCRIPT environment variable is required")
}

# ---------------------------------------------------------------------------
# Load user config
# ---------------------------------------------------------------------------
config <- list()
if (nzchar(r_config) && file.exists(r_config)) {
  log_msg("Loading config: ", r_config)
  config <- fromJSON(r_config, simplifyVector = TRUE)
}

config$.worker_id   <- as.integer(.worker_id)
config$.n_workers   <- .n_workers
config$.stage_mount <- stage_mount
config$.output_path <- output_path

# ---------------------------------------------------------------------------
# Source user script
# ---------------------------------------------------------------------------
if (!file.exists(r_script)) {
  stop(sprintf("R script not found: %s", r_script))
}

log_msg("Sourcing: ", r_script)
source(r_script, local = FALSE)

if (!exists(r_entry_fn, mode = "function")) {
  stop(sprintf("Entry function '%s' not found in %s", r_entry_fn, r_script))
}

entry_fn <- get(r_entry_fn, mode = "function")

# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------
log_msg("Calling ", r_entry_fn, "() -- worker ", .worker_id,
        " of ", .n_workers)
t0 <- proc.time()

result <- tryCatch(
  entry_fn(config),
  error = function(e) {
    log_msg("FATAL: ", conditionMessage(e))
    list(
      status = "FAILED",
      error = conditionMessage(e),
      traceback = paste(capture.output(traceback()), collapse = "\n")
    )
  }
)

elapsed <- round((proc.time() - t0)["elapsed"], 2)
log_msg("Completed in ", elapsed, "s")

# ---------------------------------------------------------------------------
# Output handling
# ---------------------------------------------------------------------------
output_format <- Sys.getenv("OUTPUT_FORMAT", "rds")

.write_result <- function(result, output_path, format, worker_id) {
  dir.create(output_path, recursive = TRUE, showWarnings = FALSE)
  is_df <- is.data.frame(result)

  if (format == "parquet" && is_df) {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      log_msg("arrow not available, falling back to csv")
      format <- "csv"
    }
  }

  out_file <- switch(format,
    parquet = {
      f <- file.path(output_path, sprintf("result_%s.parquet", worker_id))
      arrow::write_parquet(result, f)
      f
    },
    csv = {
      if (!is_df) {
        log_msg("csv format requires a data.frame, falling back to rds")
        f <- file.path(output_path, sprintf("result_%s.rds", worker_id))
        saveRDS(result, f)
        f
      } else {
        f <- file.path(output_path, sprintf("result_%s.csv", worker_id))
        write.csv(result, f, row.names = FALSE)
        f
      }
    },
    json = , ndjson = {
      f <- file.path(output_path, sprintf("result_%s.ndjson", worker_id))
      if (is_df) {
        con <- file(f, open = "w")
        jsonlite::stream_out(result, con, verbose = FALSE)
        close(con)
      } else {
        writeLines(jsonlite::toJSON(result, auto_unbox = TRUE), f)
      }
      f
    },
    {
      # Default: RDS (handles any R object)
      f <- file.path(output_path, sprintf("result_%s.rds", worker_id))
      saveRDS(result, f)
      f
    }
  )
  out_file
}

if (output_mode == "stage") {
  if (is.data.frame(result) || is.list(result)) {
    out_file <- .write_result(result, output_path, output_format, .worker_id)
    log_msg("Result written: ", out_file)
  } else {
    log_msg("Warning: result is not a data.frame or list, skipping stage write")
  }

} else if (output_mode == "table") {
  if (is.data.frame(result)) {
    con <- tryCatch(
      DBI::dbConnect(RSnowflake::Snowflake()),
      error = function(e) {
        log_msg("Cannot connect to Snowflake for table write: ", conditionMessage(e))
        NULL
      }
    )
    if (!is.null(con)) {
      output_table <- Sys.getenv("OUTPUT_TABLE", "")
      if (nzchar(output_table)) {
        DBI::dbWriteTable(con, output_table, result, append = TRUE)
        log_msg("Result appended to: ", output_table)
      }
      DBI::dbDisconnect(con)
    }
  }

} else if (output_mode == "stdout") {
  cat(toJSON(result, auto_unbox = TRUE, pretty = TRUE))
  cat("\n")

} else if (output_mode == "none") {
  log_msg("Output mode: none (result discarded)")
}

log_msg("Executor done. Elapsed: ", elapsed, "s")
