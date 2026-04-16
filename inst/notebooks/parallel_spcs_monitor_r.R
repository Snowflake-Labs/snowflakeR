# R-native monitoring helpers for the parallel SPCS demo.
#
# Designed for RStudio / Positron workflows:
# - pulls monitoring data with dbplyr
# - produces ggplot visuals suitable for live demos

`%||%` <- function(x, y) {
  if (is.null(x) || (is.character(x) && !nzchar(x))) y else x
}

pl_require_monitor_packages <- function() {
  pkgs <- c("dplyr", "dbplyr", "ggplot2", "tibble")
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    stop(
      "Missing required packages: ",
      paste(missing, collapse = ", "),
      ". Install before running monitor helpers.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

pl_monitor_tables <- function(conn, cfg, dbi_con = NULL) {
  pl_require_monitor_packages()
  if (is.null(dbi_con)) {
    dbi_con <- conn$dbi_con
  }
  if (is.null(dbi_con)) {
    stop(
      "No DBI connection available for dbplyr monitoring. ",
      "Provide dbi_con explicitly (or use pl_connect(), which can attach one).",
      call. = FALSE
    )
  }
  db <- cfg$database
  sch_cfg <- cfg$schemas$config
  sch_models <- cfg$schemas$models

  qtbl <- cfg$queue_table
  list(
    queue = dplyr::tbl(dbi_con, dbplyr::in_catalog(db, sch_cfg, qtbl)),
    manifest = dplyr::tbl(dbi_con, dbplyr::in_catalog(db, sch_cfg, "JOB_MANIFEST")),
    training = dplyr::tbl(dbi_con, dbplyr::in_catalog(db, sch_models, "TRAINING_RESULTS"))
  )
}

pl_queue_status <- function(conn, cfg, dbi_con = NULL) {
  tabs <- pl_monitor_tables(conn, cfg, dbi_con = dbi_con)
  tabs$queue |>
    dplyr::count(STATUS, name = "n") |>
    dplyr::arrange(dplyr::desc(n)) |>
    dplyr::collect()
}

pl_recent_manifest <- function(conn, cfg, limit = 200L, dbi_con = NULL) {
  tabs <- pl_monitor_tables(conn, cfg, dbi_con = dbi_con)
  tabs$manifest |>
    dplyr::slice_max(order_by = UPDATED_AT, n = as.integer(limit), with_ties = FALSE) |>
    dplyr::collect()
}

pl_recent_training <- function(conn, cfg, limit = 500L, dbi_con = NULL) {
  tabs <- pl_monitor_tables(conn, cfg, dbi_con = dbi_con)
  tabs$training |>
    dplyr::slice_max(order_by = COMPLETED_AT, n = as.integer(limit), with_ties = FALSE) |>
    dplyr::collect()
}

pl_plot_queue_status <- function(queue_status_df) {
  pl_require_monitor_packages()
  if (nrow(queue_status_df) == 0L) {
    return(ggplot2::ggplot() + ggplot2::ggtitle("Queue status (no rows yet)"))
  }
  ggplot2::ggplot(queue_status_df, ggplot2::aes(x = STATUS, y = n, fill = STATUS)) +
    ggplot2::geom_col(width = 0.7) +
    ggplot2::geom_text(ggplot2::aes(label = n), vjust = -0.2, size = 3.5) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::labs(
      title = "Queue status",
      x = NULL,
      y = "Chunks"
    ) +
    ggplot2::guides(fill = "none")
}

pl_plot_training_latency <- function(training_df) {
  pl_require_monitor_packages()
  if (nrow(training_df) == 0L || !"TRAINING_SECS" %in% names(training_df)) {
    return(ggplot2::ggplot() + ggplot2::ggtitle("Training latency (no rows yet)"))
  }
  ggplot2::ggplot(training_df, ggplot2::aes(x = TRAINING_SECS)) +
    ggplot2::geom_histogram(bins = 20, fill = "#2E86C1", alpha = 0.85) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::labs(
      title = "Training time distribution",
      x = "Seconds per unit",
      y = "Count"
    )
}

pl_plot_model_quality <- function(training_df) {
  pl_require_monitor_packages()
  needed <- c("RMSE", "MAE")
  if (nrow(training_df) == 0L || !all(needed %in% names(training_df))) {
    return(ggplot2::ggplot() + ggplot2::ggtitle("Model quality scatter (no rows yet)"))
  }
  ggplot2::ggplot(training_df, ggplot2::aes(x = RMSE, y = MAE)) +
    ggplot2::geom_point(alpha = 0.65, color = "#7D3C98") +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::labs(
      title = "Model quality (RMSE vs MAE)",
      x = "RMSE",
      y = "MAE"
    )
}
