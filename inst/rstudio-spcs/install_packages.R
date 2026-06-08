# Install RSnowflake + snowflakeR from tarballs (preferred) or GitHub fallback.
options(repos = c(CRAN = "https://packagemanager.posit.co/cran/__linux__/noble/latest"))

cran_deps <- c(
  "remotes", "httr2", "cli", "rlang", "DBI", "dplyr", "dbplyr", "tidyr",
  "jsonlite", "reticulate"
)

message("Installing CRAN dependencies...")
install.packages(cran_deps, Ncpus = 4, quiet = TRUE)

pkg_dir <- "/tmp/pkg"
tarballs <- list.files(pkg_dir, pattern = "\\.tar\\.gz$", full.names = TRUE)

install_tarball <- function(pattern) {
  hit <- tarballs[grep(pattern, basename(tarballs), ignore.case = TRUE)]
  if (length(hit) == 0) return(FALSE)
  message("Installing from tarball: ", basename(hit[1]))
  install.packages(hit[1], repos = NULL, type = "source", quiet = TRUE)
  TRUE
}

if (!install_tarball("^RSnowflake")) {
  message("No RSnowflake tarball — installing from GitHub...")
  remotes::install_github("Snowflake-Labs/RSnowflake", upgrade = "never", quiet = TRUE)
}

if (!install_tarball("^snowflakeR")) {
  message("No snowflakeR tarball — installing from GitHub...")
  remotes::install_github("Snowflake-Labs/snowflakeR", upgrade = "never", quiet = TRUE)
}

message("Verifying packages...")
stopifnot(
  requireNamespace("RSnowflake", quietly = TRUE),
  requireNamespace("snowflakeR", quietly = TRUE)
)
message("Package install OK")
