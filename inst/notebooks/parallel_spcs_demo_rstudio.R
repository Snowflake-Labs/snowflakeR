# RStudio / Positron entrypoint for the parallel SPCS demo.
#
# This script wraps the lower-level workflow helpers with a more idiomatic
# R API and dbplyr/ggplot monitoring utilities.

.pl_this_file <- tryCatch(
  normalizePath(sys.frame(1)$ofile, winslash = "/", mustWork = TRUE),
  error = function(e) ""
)
.pl_this_dir <- if (nzchar(.pl_this_file)) dirname(.pl_this_file) else getwd()

source(file.path(.pl_this_dir, "parallel_spcs_workflow.R"))
source(file.path(.pl_this_dir, "parallel_spcs_monitor_r.R"))

pl_connect <- function(
    config_path = "snowflaker_parallel_spcs_config.yaml",
    connection_name = NULL,
    private_key_file = NULL
) {
  if (!requireNamespace("snowflakeR", quietly = TRUE)) {
    stop("Package 'snowflakeR' is required.", call. = FALSE)
  }

  cfg <- parallel_lab_load_config(config_path)
  parallel_lab_validate_clean_room(cfg)

  conn <- snowflakeR::sfr_connect(
    name = connection_name,
    private_key_file = private_key_file
  )

  if (nzchar(cfg$warehouse %||% "")) {
    snowflakeR::sfr_execute(conn, sprintf("USE WAREHOUSE %s", cfg$warehouse))
  }
  conn <- snowflakeR::sfr_use(
    conn,
    database = cfg$database,
    schema = cfg$schemas$source_data
  )

  dbi_con <- NULL
  if (
    requireNamespace("DBI", quietly = TRUE) &&
    requireNamespace("RSnowflake", quietly = TRUE) &&
    nzchar(private_key_file %||% "")
  ) {
    dbi_con <- tryCatch(
      DBI::dbConnect(
        RSnowflake::Snowflake(),
        account = conn$account,
        user = conn$user,
        authenticator = "SNOWFLAKE_JWT",
        private_key_path = private_key_file,
        role = conn$role,
        warehouse = cfg$warehouse,
        dbname = cfg$database,
        schema = cfg$schemas$source_data
      ),
      error = function(e) NULL
    )
  }

  list(conn = conn, cfg = cfg, dbi_con = dbi_con)
}

pl_setup <- function(env, create_series = TRUE, n_units = 120L, n_days = 365L) {
  parallel_lab_setup(
    conn = env$conn,
    cfg = env$cfg,
    create_series = isTRUE(create_series),
    n_units = as.integer(n_units),
    n_days = as.integer(n_days)
  )
  invisible(env)
}

pl_run_tasks <- function(env, run = FALSE) {
  parallel_lab_run_tasks_demo(
    conn = env$conn,
    cfg = env$cfg,
    run = isTRUE(run)
  )
}

pl_run_queue <- function(env, run = FALSE) {
  parallel_lab_run_queue_demo(
    conn = env$conn,
    cfg = env$cfg,
    run = isTRUE(run)
  )
}

pl_monitor_snapshot <- function(env, limit_manifest = 200L, limit_training = 500L) {
  list(
    queue_status = pl_queue_status(env$conn, env$cfg, dbi_con = env$dbi_con),
    recent_manifest = pl_recent_manifest(
      env$conn, env$cfg, limit = limit_manifest, dbi_con = env$dbi_con
    ),
    recent_training = pl_recent_training(
      env$conn, env$cfg, limit = limit_training, dbi_con = env$dbi_con
    )
  )
}

pl_monitor_plots <- function(snapshot) {
  list(
    queue_status = pl_plot_queue_status(snapshot$queue_status),
    training_latency = pl_plot_training_latency(snapshot$recent_training),
    model_quality = pl_plot_model_quality(snapshot$recent_training)
  )
}

# Minimal demo flow (copy/paste in RStudio):
# env <- pl_connect(
#   config_path = "snowflaker_parallel_spcs_config.yaml",
#   connection_name = "<your_connections.toml_profile>",
#   private_key_file = "~/.snowflake/keys/<your_rsa_key>.p8"
# )
# pl_setup(env, create_series = TRUE)
# out <- pl_run_tasks(env, run = TRUE)
# snap <- pl_monitor_snapshot(env)
# plots <- pl_monitor_plots(snap)
# print(plots$queue_status)
