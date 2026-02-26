# REST Inference -- Direct HTTP calls to SPCS Model Serving
# =============================================================================
#
# Bypasses the Python bridge (reticulate / rpy2) entirely by calling the
# SPCS REST endpoint directly from R.  This eliminates the
# basic_string::substr C++ crash in Workspace Notebooks and gives much
# lower latency since there are zero serialisation hops through Python.
#
# Architecture:
#   R  ->  httr2 POST  ->  SPCS HTTP endpoint  ->  JSON response  ->  R
#
# Auth:  Snowflake Programmatic Access Token (PAT) in the
#        Authorization: Snowflake Token="..." header.
#
# Data format:  Snowflake's dataframe_split (compact, index/columns/data).


# =============================================================================
# Internal helpers
# =============================================================================

#' Convert an R data.frame to Snowflake dataframe_split payload
#'
#' Matches the format produced by `pandas_df.to_json(orient="split")`,
#' which is the recommended input format for Snowflake model serving.
#'
#' @param df A data.frame.
#' @returns A named list ready for `jsonlite::toJSON()`.
#' @noRd
.df_to_split_payload <- function(df) {
  nr <- nrow(df)
  nc <- ncol(df)

  # Build row-oriented data: list of lists
  data_rows <- vector("list", nr)
  for (i in seq_len(nr)) {
    row <- vector("list", nc)
    for (j in seq_len(nc)) {
      val <- df[[j]][i]
      # Ensure clean scalar (no names, no attributes)
      row[[j]] <- unname(val)
    }
    data_rows[[i]] <- row
  }

  # Use I() to protect single-element vectors from auto_unbox collapsing
  # them to scalars.  index, columns, and each data row must be JSON arrays.
  list(
    dataframe_split = list(
      index   = I(seq_len(nr) - 1L),
      columns = I(names(df)),
      data    = data_rows
    )
  )
}


#' Parse the JSON response from Snowflake Model Serving into a data.frame
#'
#' Handles the response formats documented by Snowflake.
#'
#' @param body Parsed JSON (list) from the response.
#' @returns A data.frame.
#' @noRd
.parse_rest_response <- function(body) {
  # The response uses the same dataframe_split / dataframe_records
  # formats as the request, but only one format is returned regardless
  # of which format was sent.

  # Format: top-level list with numeric vectors per column
  # e.g. {"PREDICTION": [22.5, 18.3, 15.1]}
  if (is.list(body) && !is.data.frame(body) &&
      length(body) > 0 && is.atomic(body[[1]])) {
    df <- as.data.frame(body, stringsAsFactors = FALSE)
    names(df) <- tolower(names(df))
    return(df)
  }

  # Format: array of records [{col: val}, ...]
  if (is.data.frame(body)) {
    names(body) <- tolower(names(body))
    return(body)
  }

  if (is.list(body) && length(body) > 0 && is.list(body[[1]]) &&
      !is.null(names(body[[1]]))) {
    df <- do.call(rbind, lapply(body, as.data.frame, stringsAsFactors = FALSE))
    names(df) <- tolower(names(df))
    return(df)
  }

  # Format: dataframe_split response wrapper
  if (!is.null(body$dataframe_split)) {
    split <- body$dataframe_split
    mat <- do.call(rbind, split$data)
    df <- as.data.frame(mat, stringsAsFactors = FALSE)
    names(df) <- tolower(split$columns)
    # Coerce to numeric where possible
    for (col in names(df)) {
      num <- suppressWarnings(as.numeric(df[[col]]))
      if (!any(is.na(num))) df[[col]] <- num
    }
    return(df)
  }

  # Format: {"data": [[idx, {col: val}], [idx, {col: val}], ...]}
  # This is the standard SPCS inference response format.
  if (!is.null(body$data) && is.list(body$data) && length(body$data) > 0) {
    rows <- lapply(body$data, function(row) {
      # Each row is [index, {col1: val1, col2: val2, ...}]
      if (is.list(row) && length(row) >= 2 && is.list(row[[2]])) {
        row[[2]]
      } else if (is.list(row) && !is.null(names(row))) {
        row
      } else {
        row
      }
    })
    df <- do.call(rbind, lapply(rows, as.data.frame, stringsAsFactors = FALSE))
    names(df) <- tolower(names(df))
    return(df)
  }

  cli::cli_abort(c(
    "x" = "Unexpected response format from SPCS endpoint.",
    "i" = "Keys: {.val {paste(names(body), collapse = ', ')}}"
  ))
}


# =============================================================================
# Endpoint Discovery
# =============================================================================

#' Discover the REST endpoint URL for an SPCS model service
#'
#' Queries Snowflake for the endpoint URL of a deployed SPCS service.
#' The returned object can be passed directly to [sfr_predict_rest()].
#'
#' @param conn An `sfr_connection` object.
#' @param service_name Character. Name of the SPCS service.
#'
#' @returns An `sfr_endpoint` object containing the URL and metadata.
#'
#' @examples
#' \dontrun{
#' ep <- sfr_service_endpoint(conn, "mpg_service")
#' ep
#' # <sfr_endpoint>
#' # url:     https://xyz-myorg123.snowflakecomputing.app
#' # service: MYDB.MYSCHEMA.MPG_SERVICE
#' }
#'
#' @export
sfr_service_endpoint <- function(conn, service_name) {
  validate_connection(conn)
  stopifnot(is.character(service_name), length(service_name) == 1L)

  fqn <- sfr_fqn(conn, toupper(service_name))

  # Query endpoints -- returns ingress_url for public access
  endpoints <- sfr_query(
    conn,
    paste("SHOW ENDPOINTS IN SERVICE", fqn),
    .keep_case = FALSE
  )

  if (nrow(endpoints) == 0L) {
    cli::cli_abort(c(
      "x" = "No endpoints found for service {.val {fqn}}.",
      "i" = "Was the service deployed with {.code ingress_enabled = TRUE}?"
    ))
  }

  # Look for the 'inference' endpoint (the default name)
  ingress_url <- NULL
  if ("ingress_url" %in% names(endpoints)) {
    # Prefer the 'inference' endpoint if multiple exist
    inf_row <- endpoints[tolower(endpoints$name) == "inference", , drop = FALSE]
    if (nrow(inf_row) > 0) {
      ingress_url <- inf_row$ingress_url[1]
    } else {
      ingress_url <- endpoints$ingress_url[1]
    }
  }

  if (is.null(ingress_url) || is.na(ingress_url) || !nzchar(ingress_url)) {
    cli::cli_abort(c(
      "x" = "No ingress URL found for service {.val {fqn}}.",
      "i" = "Was the service deployed with {.code ingress_enabled = TRUE}?"
    ))
  }

  # Detect endpoints still provisioning
  if (grepl("provisioning", ingress_url, ignore.case = TRUE)) {
    cli::cli_abort(c(
      "x" = "Endpoints are still provisioning for service {.val {fqn}}.",
      "i" = "Wait a few minutes and retry."
    ))
  }

  # Ensure URL has https:// prefix
  url <- ingress_url
  if (!grepl("^https?://", url)) {
    url <- paste0("https://", url)
  }

  ep <- structure(
    list(
      url          = url,
      service_name = service_name,
      fqn          = fqn,
      account      = conn$account,
      endpoints_df = endpoints
    ),
    class = c("sfr_endpoint", "list")
  )

  cli::cli_inform(c(
    "v" = "Endpoint discovered for {.val {fqn}}",
    "i" = "URL: {.url {url}}"
  ))

  ep
}


#' @export
print.sfr_endpoint <- function(x, ...) {
  cli::cli_text("<{.cls sfr_endpoint}>")
  cli::cli_dl(list(
    url     = cli::format_inline("{.url {x$url}}"),
    service = cli::format_inline("{.val {x$fqn}}"),
    account = cli::format_inline("{.val {x$account}}")
  ))
  invisible(x)
}


# =============================================================================
# PAT Token Resolution
# =============================================================================

#' Get a Snowflake authentication token for REST inference
#'
#' Resolves an auth token from (in order):
#' 1. The `token` argument (if provided)
#' 2. The `SNOWFLAKE_PAT` environment variable
#' 3. The `SNOWFLAKE_TOKEN` environment variable
#' 4. A JWT generated from the connection's key-pair auth (if `conn` provided)
#'
#' @param token Character or NULL. An explicit token (PAT or JWT).
#' @param conn An `sfr_connection` object, or NULL. When provided and no
#'   explicit token is available, generates a JWT from the session's key-pair
#'   credentials.
#'
#' @returns Character. The token string.
#'
#' @examples
#' \dontrun{
#' Sys.setenv(SNOWFLAKE_PAT = "ver:1-hint:1234-ETMs...")
#' sfr_pat()
#' }
#'
#' @export
sfr_pat <- function(token = NULL, conn = NULL) {
  if (!is.null(token) && nzchar(token)) return(token)

  pat <- Sys.getenv("SNOWFLAKE_PAT", "")
  if (nzchar(pat)) return(pat)

  pat <- Sys.getenv("SNOWFLAKE_TOKEN", "")
  if (nzchar(pat)) return(pat)

  # Auto-generate JWT from connection's key-pair auth
  if (!is.null(conn) && is_sfr_connection(conn)) {
    jwt <- .generate_jwt_from_session(conn)
    if (!is.null(jwt)) return(jwt)
  }

  cli::cli_abort(c(
    "x" = "No Snowflake auth token found.",
    "i" = "Set a PAT: {.code Sys.setenv(SNOWFLAKE_PAT = \"your_token\")}",
    "i" = "Or pass directly: {.code sfr_predict_rest(..., token = \"your_token\")}",
    "i" = "Or pass {.arg conn} to auto-generate a JWT from key-pair auth.",
    "i" = "Create a PAT in Snowsight: Account > Programmatic Access Tokens."
  ))
}


#' Generate a JWT from the Snowpark session's key-pair credentials
#'
#' @param conn An `sfr_connection` object.
#' @returns Character JWT string, or NULL if generation fails.
#' @noRd
.generate_jwt_from_session <- function(conn) {
  tryCatch(
    {
      bridge <- get_bridge_module("sfr_connect_bridge")
      bridge$generate_jwt_token(conn$session)
    },
    error = function(e) NULL
  )
}


# =============================================================================
# Direct REST Inference
# =============================================================================

#' Run model inference directly via the SPCS REST endpoint
#'
#' Sends a data.frame to the model's HTTP endpoint and returns predictions.
#' This is the fastest inference path -- pure R, no Python bridge.
#'
#' @param endpoint An `sfr_endpoint` object from [sfr_service_endpoint()],
#'   or a character URL string.
#' @param new_data A data.frame with input data.
#' @param token Character or NULL. Auth token (PAT or JWT). If NULL, resolved
#'   via [sfr_pat()] (reads `SNOWFLAKE_PAT` env var, or auto-generates a JWT
#'   from `conn`).
#' @param conn An `sfr_connection` object, or NULL. Used for auto-generating
#'   a JWT when no explicit token or PAT env var is available.
#' @param method Character. Model method name. Default: `"predict"`.
#'   Underscores are converted to dashes in the URL per Snowflake convention.
#' @param timeout_sec Numeric. Request timeout in seconds. Default: 30.
#'
#' @returns A data.frame with predictions.
#'
#' @examples
#' \dontrun{
#' ep <- sfr_service_endpoint(conn, "mpg_service")
#' preds <- sfr_predict_rest(ep, new_data, conn = conn)
#' }
#'
#' @export
sfr_predict_rest <- function(endpoint,
                             new_data,
                             token = NULL,
                             conn = NULL,
                             method = "predict",
                             timeout_sec = 30) {
  if (!requireNamespace("httr2", quietly = TRUE)) {
    cli::cli_abort(c(
      "Package {.pkg httr2} is required for REST inference.",
      "i" = "Install with: {.code install.packages(\"httr2\")}"
    ))
  }

  stopifnot(is.data.frame(new_data), nrow(new_data) > 0)

  # Resolve URL
  base_url <- if (inherits(endpoint, "sfr_endpoint")) endpoint$url else endpoint

  # Build request URL: underscores -> dashes per Snowflake convention
  method_path <- gsub("_", "-", method)
  request_url <- paste0(base_url, "/", method_path)

  # Resolve auth token (PAT or JWT)
  auth_token <- sfr_pat(token, conn = conn)

  # Build payload
  payload <- .df_to_split_payload(new_data)

  # Make the request
  # Use Snowflake Token auth format which works for both PAT and JWT
  req <- httr2::request(request_url) |>
    httr2::req_headers(
      "Authorization" = paste0('Snowflake Token="', auth_token, '"'),
      "Content-Type"  = "application/json",
      "Accept"        = "application/json"
    ) |>
    httr2::req_body_json(payload, auto_unbox = TRUE) |>
    httr2::req_timeout(timeout_sec)

  resp <- tryCatch(
    httr2::req_perform(req),
    error = function(e) {
      cli::cli_abort(c(
        "x" = "REST inference request failed.",
        "i" = "URL: {.url {request_url}}",
        "i" = "Error: {conditionMessage(e)}"
      ))
    }
  )

  # Check HTTP status
  status <- httr2::resp_status(resp)
  if (status != 200L) {
    body_text <- tryCatch(
      httr2::resp_body_string(resp),
      error = function(e) "(no body)"
    )
    # Detect known server-side bugs
    if (grepl("recarray has no attribute fillna", body_text, fixed = TRUE)) {
      cli::cli_abort(c(
        "x" = "SPCS inference server bug: numpy recarray / pandas incompatibility.",
        "i" = "This is a known issue in the model serving container (numpy 2.x).",
        "i" = "Workaround: redeploy the model or contact Snowflake support.",
        "i" = "See: {.url {request_url}}"
      ))
    }
    cli::cli_abort(c(
      "x" = "SPCS endpoint returned HTTP {status}.",
      "i" = "URL: {.url {request_url}}",
      "i" = "Body: {body_text}"
    ))
  }

  # Parse response -- handle both JSON and unexpected content types
  content_type <- httr2::resp_content_type(resp)
  if (!grepl("json", content_type, ignore.case = TRUE)) {
    body_text <- httr2::resp_body_string(resp)
    # Could be the Snowflake login page (auth failure)
    if (grepl("<html", body_text, ignore.case = TRUE)) {
      cli::cli_abort(c(
        "x" = "Authentication failed -- received login page instead of JSON.",
        "i" = "Check your PAT or JWT token.",
        "i" = "URL: {.url {request_url}}"
      ))
    }
    cli::cli_abort(c(
      "x" = "Unexpected content type: {.val {content_type}}",
      "i" = "URL: {.url {request_url}}"
    ))
  }

  body <- httr2::resp_body_json(resp)
  .parse_rest_response(body)
}


#' Benchmark SPCS inference via REST
#'
#' Runs `n` inference requests directly via the REST endpoint and reports
#' timing statistics.  Measures true service latency without Python bridge
#' overhead.
#'
#' @param endpoint An `sfr_endpoint` object or URL string.
#' @param new_data A data.frame with input data.
#' @param n Integer. Number of iterations. Default: 10.
#' @param token Character or NULL. Snowflake PAT.
#' @param method Character. Model method name. Default: `"predict"`.
#' @param verbose Logical. Print per-iteration results? Default: `TRUE`.
#'
#' @returns A list (invisibly) with timing stats (same structure as
#'   [sfr_benchmark_inference()]).
#'
#' @examples
#' \dontrun{
#' ep <- sfr_service_endpoint(conn, "mpg_service")
#' bench <- sfr_benchmark_rest(ep, new_data, n = 20)
#' }
#'
#' @export
sfr_benchmark_rest <- function(endpoint,
                               new_data,
                               n = 10L,
                               token = NULL,
                               conn = NULL,
                               method = "predict",
                               verbose = TRUE) {
  stopifnot(is.data.frame(new_data), is.numeric(n), n >= 1)
  n <- as.integer(n)

  svc_label <- if (inherits(endpoint, "sfr_endpoint")) endpoint$fqn else endpoint

  if (verbose) {
    cli::cli_inform(c(
      "i" = "Benchmarking {n} REST inference calls to {.val {svc_label}} ..."
    ))
  }

  # Resolve token once (don't re-resolve each iteration)
  pat <- sfr_pat(token, conn = conn)

  timings <- numeric(n)
  last_result <- NULL

  for (i in seq_len(n)) {
    t0 <- proc.time()["elapsed"]

    result <- tryCatch(
      sfr_predict_rest(
        endpoint,
        new_data = new_data,
        token = pat,
        method = method
      ),
      error = function(e) {
        cli::cli_warn("Iteration {i}/{n} failed: {conditionMessage(e)}")
        NULL
      }
    )

    t1 <- proc.time()["elapsed"]
    timings[i] <- t1 - t0

    if (!is.null(result)) {
      last_result <- result
    }

    if (verbose) {
      status_label <- if (!is.null(result)) "OK" else "FAIL"
      cli::cli_inform(
        "  [{i}/{n}] {status_label} -- {round(timings[i], 3)}s"
      )
    }
  }

  # Compute stats
  successful <- timings[timings > 0]
  bench_stats <- list(
    timings     = timings,
    mean_sec    = mean(successful),
    median_sec  = stats::median(successful),
    min_sec     = min(successful),
    max_sec     = max(successful),
    total_sec   = sum(timings),
    n           = n,
    last_result = last_result,
    method      = "rest"
  )

  if (verbose) {
    cli::cli_rule("REST Benchmark Results")
    cli::cli_inform(c(
      "i" = "Iterations: {n}",
      "i" = "Mean:   {round(bench_stats$mean_sec, 3)}s",
      "i" = "Median: {round(bench_stats$median_sec, 3)}s",
      "i" = "Min:    {round(bench_stats$min_sec, 3)}s",
      "i" = "Max:    {round(bench_stats$max_sec, 3)}s",
      "i" = "Total:  {round(bench_stats$total_sec, 1)}s"
    ))
  }

  invisible(bench_stats)
}
