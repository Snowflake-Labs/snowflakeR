#!/usr/bin/env Rscript

# CLI wrapper for parallel_spcs_workflow.R
#
# Examples:
#   Rscript snowflakeR/inst/notebooks/run_parallel_spcs_workflow.R --mode bootstrap_sql
#   Rscript snowflakeR/inst/notebooks/run_parallel_spcs_workflow.R --mode setup --config snowflakeR/inst/notebooks/snowflaker_parallel_spcs_config.yaml
#   Rscript snowflakeR/inst/notebooks/run_parallel_spcs_workflow.R --mode tasks --run
#   Rscript snowflakeR/inst/notebooks/run_parallel_spcs_workflow.R --mode queue --run

args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(argv) {
  out <- list(
    mode = "bootstrap_sql",
    config = "snowflakeR/inst/notebooks/snowflaker_parallel_spcs_config.yaml",
    create_series = TRUE,
    n_units = 120L,
    n_days = 365L,
    run = FALSE,
    load_local = FALSE,
    package_path = "snowflakeR",
    connection_name = NULL,
    private_key_file = NULL,
    run_id = NULL,
    forecast_h = NULL,
    version_name = NULL,
    help = FALSE
  )

  i <- 1L
  while (i <= length(argv)) {
    a <- argv[[i]]
    if (identical(a, "--help") || identical(a, "-h")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }

    if (identical(a, "--mode")) {
      out$mode <- argv[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(a, "--config")) {
      out$config <- argv[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(a, "--create-series")) {
      out$create_series <- tolower(argv[[i + 1L]]) %in% c("1", "true", "yes")
      i <- i + 2L
      next
    }
    if (identical(a, "--n-units")) {
      out$n_units <- as.integer(argv[[i + 1L]])
      i <- i + 2L
      next
    }
    if (identical(a, "--n-days")) {
      out$n_days <- as.integer(argv[[i + 1L]])
      i <- i + 2L
      next
    }
    if (identical(a, "--run")) {
      out$run <- TRUE
      i <- i + 1L
      next
    }
    if (identical(a, "--load-local")) {
      out$load_local <- TRUE
      i <- i + 1L
      next
    }
    if (identical(a, "--package-path")) {
      out$package_path <- argv[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(a, "--connection-name")) {
      out$connection_name <- argv[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(a, "--private-key-file")) {
      out$private_key_file <- argv[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(a, "--run-id")) {
      out$run_id <- argv[[i + 1L]]
      i <- i + 2L
      next
    }
    if (identical(a, "--forecast-h")) {
      out$forecast_h <- as.integer(argv[[i + 1L]])
      i <- i + 2L
      next
    }
    if (identical(a, "--version")) {
      out$version_name <- argv[[i + 1L]]
      i <- i + 2L
      next
    }

    stop(sprintf("Unknown argument: %s", a), call. = FALSE)
  }

  out
}

print_help <- function() {
  cat(
    "run_parallel_spcs_workflow.R\n",
    "\n",
    "Usage:\n",
    "  Rscript run_parallel_spcs_workflow.R [options]\n",
    "\n",
    "Options:\n",
    "  --mode <bootstrap_sql|setup|tasks|queue|benchmark|register|inference>\n",
    "  --config <path>                      YAML config path\n",
    "  --create-series <true|false>         setup only (default: true)\n",
    "  --n-units <int>                      synthetic units (default: 120)\n",
    "  --n-days <int>                       synthetic days (default: 365)\n",
    "  --run                                execute tasks/queue demo\n",
    "  --run-id <id>                        model run ID (for register/inference)\n",
    "  --forecast-h <int>                   forecast horizon for inference\n",
    "  --version <name>                     version name for register mode\n",
    "  --load-local                         load snowflakeR from local source via pkgload\n",
    "  --package-path <path>                local package path (default: snowflakeR)\n",
    "  --connection-name <name>             sfr_connect(name=...)\n",
    "  --private-key-file <path>            explicit key path for key-pair auth\n",
    "  --help, -h                           show this help\n",
    "\n",
    "Notes:\n",
    "  - tasks/queue modes are dry-run unless --run is provided.\n",
    "  - setup/tasks/queue use snowflakeR::sfr_connect() and your current\n",
    "    Snowflake auth environment/profile.\n",
    sep = ""
  )
}

opts <- parse_args(args)
if (isTRUE(opts$help)) {
  print_help()
  quit(status = 0L)
}

script_path <- normalizePath(commandArgs(FALSE)[grep("--file=", commandArgs(FALSE))], mustWork = FALSE)
if (!nzchar(script_path)) {
  script_dir <- getwd()
} else {
  script_path <- sub("^--file=", "", script_path)
  script_dir <- dirname(normalizePath(script_path, mustWork = FALSE))
}

source(file.path(script_dir, "parallel_spcs_workflow.R"))

cfg <- parallel_lab_load_config(opts$config)
parallel_lab_validate_clean_room(
  cfg,
  require_runtime = opts$mode %in% c("setup", "tasks", "queue", "benchmark")
)

`%||%` <- function(x, y) if (is.null(x) || (is.character(x) && !nzchar(x))) y else x

if (identical(opts$mode, "bootstrap_sql")) {
  sql <- parallel_lab_sql_bootstrap(cfg)
  cat(paste(sql, collapse = ";\n"), ";\n", sep = "")
  quit(status = 0L)
}

if (isTRUE(opts$load_local)) {
  if (!requireNamespace("pkgload", quietly = TRUE)) {
    stop("Package 'pkgload' is required for --load-local", call. = FALSE)
  }
  pkgload::load_all(opts$package_path, quiet = TRUE)
}

if (!"snowflakeR" %in% loadedNamespaces() && !requireNamespace("snowflakeR", quietly = TRUE)) {
  stop(
    "Package 'snowflakeR' is required for mode: ", opts$mode,
    " (or pass --load-local).",
    call. = FALSE
  )
}
conn <- snowflakeR::sfr_connect(
  name = opts$connection_name,
  private_key_file = opts$private_key_file
)

if (nzchar(cfg$warehouse %||% "")) {
  snowflakeR::sfr_execute(conn, sprintf("USE WAREHOUSE %s", cfg$warehouse))
}

if (identical(opts$mode, "setup")) {
  parallel_lab_setup(
    conn,
    cfg,
    create_series = isTRUE(opts$create_series),
    n_units = opts$n_units,
    n_days = opts$n_days
  )
  cat("Setup complete.\n")
  quit(status = 0L)
}

if (identical(opts$mode, "tasks")) {
  parallel_lab_run_tasks_demo(conn, cfg, run = isTRUE(opts$run))
  quit(status = 0L)
}

if (identical(opts$mode, "queue")) {
  parallel_lab_run_queue_demo(conn, cfg, run = isTRUE(opts$run))
  quit(status = 0L)
}

if (identical(opts$mode, "benchmark")) {
  parallel_lab_run_benchmark(conn, cfg, run = isTRUE(opts$run))
  quit(status = 0L)
}

if (identical(opts$mode, "register")) {
  run_id <- opts$run_id
  if (is.null(run_id) || !nzchar(run_id)) {
    stop("--run-id is required for register mode", call. = FALSE)
  }
  parallel_lab_register_models(
    conn, cfg,
    model_run_id = run_id,
    version_name = opts$version_name %||% run_id,
    forecast_h   = as.integer(opts$forecast_h %||% 12L)
  )
  quit(status = 0L)
}

if (identical(opts$mode, "inference")) {
  run_id <- opts$run_id
  if (is.null(run_id) || !nzchar(run_id)) {
    stop("--run-id is required for inference mode", call. = FALSE)
  }
  parallel_lab_run_inference(
    conn, cfg,
    model_run_id = run_id,
    version_name = opts$version_name %||% run_id,
    forecast_h   = as.integer(opts$forecast_h %||% 12L)
  )
  quit(status = 0L)
}

stop("Unsupported --mode: ", opts$mode, call. = FALSE)
