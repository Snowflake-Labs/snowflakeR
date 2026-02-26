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

## Feature Store API

- [ ] Additional methods: `list_feature_views`, `get_feature_view`,
  `generate_training_set` -- partially implemented, may need further testing
  and edge-case handling.

## Vignettes

- [ ] Write comprehensive vignettes for Feature Store, Model Registry, and
  Datasets workflows.

## Testing

- [ ] Expand testthat suite with integration tests that run against a live
  Snowflake account (behind an env-var gate).
