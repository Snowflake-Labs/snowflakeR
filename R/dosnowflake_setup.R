# doSnowflake: Setup and Infrastructure Helpers
# =============================================================================
# Functions for creating the Snowflake objects and Docker images needed
# for remote doSnowflake execution (modes: tasks, spcs, queue).


#' Set up Snowflake infrastructure for doSnowflake
#'
#' Creates the internal stage (if it doesn't exist) and validates that
#' the compute pool and image repository are available. Call this once
#' before using `registerDoSnowflake(mode = "tasks")`.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param stage Character. Name of the internal stage to create
#'   (default `"DOSNOWFLAKE_STAGE"`).
#' @param compute_pool Character. Name of the compute pool to validate.
#'   If `NULL`, validation is skipped.
#' @param image_repo Character. Name of the image repository to validate.
#'   If `NULL`, validation is skipped.
#'
#' @returns A list with `stage`, `compute_pool`, and `image_repo` names
#'   (invisibly).
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' sfr_dosnowflake_setup(conn,
#'   compute_pool = "MY_POOL",
#'   image_repo   = "MY_REPO"
#' )
#' }
#'
#' @export
sfr_dosnowflake_setup <- function(conn,
                                  stage = "DOSNOWFLAKE_STAGE",
                                  compute_pool = NULL,
                                  image_repo = NULL) {
  validate_connection(conn)

  # Create stage
  cli::cli_inform("Creating stage {.val {stage}} (if not exists)...")
  sfr_execute(conn, sprintf(
    "CREATE STAGE IF NOT EXISTS %s ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')",
    stage
  ))
  cli::cli_inform(c("v" = "Stage {.val {stage}} ready."))

  # Validate compute pool
  if (!is.null(compute_pool)) {
    cli::cli_inform("Validating compute pool {.val {compute_pool}}...")
    tryCatch({
      sfr_query(conn, sprintf("DESCRIBE COMPUTE POOL %s", compute_pool))
      cli::cli_inform(c("v" = "Compute pool {.val {compute_pool}} exists."))
    }, error = function(e) {
      cli::cli_abort(c(
        "Compute pool {.val {compute_pool}} not found or not accessible.",
        "i" = "Create it with {.fn sfr_create_compute_pool} or ask your admin.",
        "x" = conditionMessage(e)
      ))
    })
  }

  # Validate image repo
  if (!is.null(image_repo)) {
    cli::cli_inform("Validating image repository {.val {image_repo}}...")
    tryCatch({
      sfr_query(conn, sprintf("DESCRIBE IMAGE REPOSITORY %s", image_repo))
      cli::cli_inform(c("v" = "Image repository {.val {image_repo}} exists."))
    }, error = function(e) {
      cli::cli_abort(c(
        "Image repository {.val {image_repo}} not found or not accessible.",
        "i" = "Create it with {.fn sfr_create_image_repo} or ask your admin.",
        "x" = conditionMessage(e)
      ))
    })
  }

  config <- list(
    stage        = stage,
    compute_pool = compute_pool,
    image_repo   = image_repo
  )
  cli::cli_inform(c("v" = "doSnowflake infrastructure setup complete."))
  invisible(config)
}


#' Build and push a doSnowflake worker Docker image
#'
#' Generates a Dockerfile from the built-in template, optionally adding
#' user-specified packages, then builds and pushes the image to a
#' Snowflake image repository.
#'
#' Requires Docker to be installed and running locally.
#'
#' @param conn An `sfr_connection` object from [sfr_connect()].
#' @param image_repo Character. Name of the Snowflake image repository
#'   (e.g. `"MY_REPO"`).
#' @param tag Character. Image tag (default `"latest"`).
#' @param variant Character. `"lean"` (R only, ~500 MB) or
#'   `"full"` (R + Python + reticulate, ~2 GB).
#' @param packages Named list of additional packages to install:
#'   * `conda` -- character vector of conda-forge package names
#'   * `cran` -- character vector of CRAN package names
#'   * `pip` -- character vector of pip package names
#' @param docker_context Character. Path to the directory containing the
#'   Dockerfiles and worker.R. If `NULL`, uses the built-in templates
#'   from `internal/doSnowflake/docker/`.
#'
#' @returns The full image URI (invisibly).
#'
#' @examples
#' \dontrun{
#' conn <- sfr_connect()
#' uri <- sfr_dosnowflake_build_image(
#'   conn, "MY_REPO",
#'   variant = "lean",
#'   packages = list(cran = c("randomForest", "glmnet"))
#' )
#' # uri is now ready to pass to registerDoSnowflake(image_uri = uri)
#' }
#'
#' @export
sfr_dosnowflake_build_image <- function(conn,
                                        image_repo,
                                        tag = "latest",
                                        variant = c("lean", "full"),
                                        packages = NULL,
                                        docker_context = NULL) {
  validate_connection(conn)
  variant <- match.arg(variant)

  # Resolve image repository URL
  repo_info <- tryCatch(
    sfr_query(conn, sprintf("SHOW IMAGE REPOSITORIES LIKE '%s'", image_repo)),
    error = function(e) {
      cli::cli_abort(c(
        "Cannot find image repository {.val {image_repo}}.",
        "i" = "Create it with {.fn sfr_create_image_repo}."
      ))
    }
  )

  repo_url_col <- if ("repository_url" %in% names(repo_info)) {
    "repository_url"
  } else {
    "REPOSITORY_URL"
  }
  repo_url <- repo_info[[repo_url_col]][1]
  image_name <- "dosnowflake-worker"
  full_uri <- paste0(repo_url, "/", image_name, ":", tag)

  # Locate Dockerfile
  if (is.null(docker_context)) {
    docker_context <- .find_docker_context()
  }
  dockerfile <- file.path(docker_context,
                          paste0("Dockerfile.", variant))
  if (!file.exists(dockerfile)) {
    cli::cli_abort("Dockerfile not found at {.path {dockerfile}}")
  }

  # Build with optional extra packages
  build_dir <- tempfile(pattern = "dosnowflake_build_")
  dir.create(build_dir, recursive = TRUE)
  on.exit(unlink(build_dir, recursive = TRUE), add = TRUE)

  file.copy(
    list.files(docker_context, full.names = TRUE),
    build_dir, recursive = TRUE
  )

  # Append extra package installs to Dockerfile
  if (!is.null(packages)) {
    extra_lines <- .generate_package_install_lines(packages)
    if (nzchar(extra_lines)) {
      cat("\n", extra_lines, "\n",
          file = file.path(build_dir, paste0("Dockerfile.", variant)),
          append = TRUE)
    }
  }

  # Docker build + push
  cli::cli_inform("Building Docker image {.val {full_uri}} (variant = {.val {variant}})...")
  build_cmd <- sprintf(
    "docker build -t %s -f %s %s",
    shQuote(full_uri),
    shQuote(paste0("Dockerfile.", variant)),
    shQuote(build_dir)
  )
  build_result <- system(build_cmd, intern = FALSE)
  if (build_result != 0L) {
    cli::cli_abort("Docker build failed. Check Docker output above.")
  }

  cli::cli_inform("Pushing image to {.val {repo_url}}...")
  push_cmd <- sprintf("docker push %s", shQuote(full_uri))
  push_result <- system(push_cmd, intern = FALSE)
  if (push_result != 0L) {
    cli::cli_abort("Docker push failed. Ensure you are logged into the Snowflake image registry.")
  }

  cli::cli_inform(c("v" = "Image pushed: {.val {full_uri}}"))
  invisible(full_uri)
}


#' Locate the built-in Docker context directory
#' @returns Character path.
#' @noRd
.find_docker_context <- function() {
  # Check relative to workspace (development) or installed package
  candidates <- c(
    system.file("docker", package = "snowflakeR"),
    file.path(system.file(package = "snowflakeR"), "..", "..", "..",
              "internal", "doSnowflake", "docker")
  )

  for (path in candidates) {
    if (nzchar(path) && dir.exists(path)) return(path)
  }

  cli::cli_abort(c(
    "Cannot find doSnowflake Docker context directory.",
    "i" = "Pass the {.arg docker_context} argument explicitly."
  ))
}


#' Generate Dockerfile lines for extra packages
#' @param packages Named list with conda, cran, pip entries.
#' @returns Character string of Dockerfile RUN commands.
#' @noRd
.generate_package_install_lines <- function(packages) {
  lines <- character(0)

  if (!is.null(packages$cran) && length(packages$cran) > 0L) {
    pkg_str <- paste0("'", packages$cran, "'", collapse = ", ")
    lines <- c(lines, sprintf(
      "RUN R -e \"install.packages(c(%s), repos = 'https://cloud.r-project.org')\"",
      pkg_str
    ))
  }

  if (!is.null(packages$pip) && length(packages$pip) > 0L) {
    pkg_str <- paste(packages$pip, collapse = " ")
    lines <- c(lines, sprintf("RUN pip3 install %s", pkg_str))
  }

  if (!is.null(packages$conda) && length(packages$conda) > 0L) {
    pkg_str <- paste(packages$conda, collapse = " ")
    lines <- c(lines, sprintf(
      "RUN if command -v conda >/dev/null 2>&1; then conda install -y -c conda-forge %s; fi",
      pkg_str
    ))
  }

  paste(lines, collapse = "\n")
}
