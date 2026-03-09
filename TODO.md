# snowflakeR -- Roadmap / TODO

Items tracked here are longer-term enhancements that are not blocking the
current release.

## Performance

- [ ] **Arrow-based data transfer between Python and R**
  Currently, all pandas DataFrames returned from Snowpark are converted to
  column-oriented Python dicts via `Series.tolist()` before being passed to R
  through reticulate.  This avoids a NumPy 1.x/2.x ABI incompatibility that
  causes reticulate to crash when converting numpy-backed arrays directly.

  `tolist()` is efficient for moderate result sets but involves copying every
  value from numpy arrays into Python lists, then into R vectors.  For large
  datasets (100K+ rows) an Apache Arrow IPC transfer would be significantly
  faster:

  - **Python side**: `pyarrow.Table.from_pandas(df)` -> write Arrow IPC stream
    to a temp file (or in-memory buffer).
  - **R side**: `arrow::read_ipc_stream()` -> `as.data.frame()`.
  - `pyarrow` is already available in Snowflake environments (Snowpark uses it
    internally).  The R `arrow` package would become a suggested dependency.
  - This could be gated behind `requireNamespace("arrow")` so that users
    without the R `arrow` package fall back to the current `tolist()` path.

- [ ] **Arrow IPC for R→Python grid display (large data.frames)**
  The `%%R` magic's grid viewer currently converts R data.frames to pandas via
  rpy2's `pandas2ri` converter (column-by-column copy through rpy2's C bridge).
  For large results (10K+ rows), an Arrow IPC path would be faster:

  - **R side**: `nanoarrow::as_nanoarrow_array_stream(df)` → serialize to IPC.
  - **Python side**: `pyarrow.ipc.read_stream()` → `.to_pandas()` (zero-copy
    for many dtypes).
  - Both `nanoarrow` (R) and `pyarrow` (Python) are already available in the
    Workspace environment.
  - Main challenge: passing the Arrow buffer across the rpy2 boundary
    efficiently (temp file vs shared memory via C-level pointers).
  - Consider implementing when users report grid display performance issues
    on large result sets.

## DBI / Database Connectivity

Standard DBI-compliant database access (dbGetQuery, dbWriteTable, dbplyr,
RStudio Connections Pane) is now provided by the companion **RSnowflake**
package. `snowflakeR` retains lightweight SQL helpers (`sfr_query()`,
`sfr_execute()`, `sfr_read_table()`, `sfr_write_table()`) that route
through the Snowpark session for ML workflows. When `RSnowflake` is
installed, `sfr_query()` and `sfr_execute()` delegate to it automatically.

- [x] Extract DBI methods to RSnowflake package
- [x] Add `sfr_dbi_connection()` bridge function
- [ ] Consider deprecating `sfr_list_tables()`, `sfr_read_table()`, etc.
  in favour of the DBI equivalents via `sfr_dbi_connection()`

## Testing

- [ ] Expand testthat suite with integration tests that run against a live
  Snowflake account (behind an env-var gate).

## Vignettes

- [x] Getting started
- [x] Setup & prerequisites
- [x] Model Registry
- [x] Feature Store
- [x] Workspace Notebooks
