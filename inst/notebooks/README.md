# snowflakeR Notebooks

Interactive Jupyter notebooks demonstrating the `snowflakeR` package.

## Contents

### Notebooks

| File | Environment | Purpose |
|---|---|---|
| `workspace_quickstart.ipynb` | Workspace | Connection, config, queries, ggplot2 |
| `local_quickstart.ipynb` | Local | Quickstart for RStudio, Jupyter, etc. |
| `workspace_model_registry.ipynb` | Workspace | Model Registry: log, deploy, serve R models |
| `local_model_registry.ipynb` | Local | Model Registry for local environments |
| `workspace_feature_store.ipynb` | Workspace | Feature Store: entities, views, training data |
| `local_feature_store.ipynb` | Local | Feature Store for local environments |
| `workspace_forecasting_demo.ipynb` | Workspace | Time series forecasting (ARIMA) with custom predict logic |
| `local_forecasting_demo.ipynb` | Local | Forecasting demo for local environments |
| `workspace_credit_risk_setup.ipynb` | Workspace | Credit risk demo: data preparation and setup |
| `workspace_credit_risk_demo.ipynb` | Workspace | Credit risk demo: model training and registry |
| `workspace_parallel_spcs_setup.ipynb` | Workspace | Parallel SPCS lab: synthetic data + monitoring shells (`SFLAB_EP_DEMO`) |
| `workspace_parallel_spcs_demo.ipynb` | Workspace | Parallel SPCS lab: driver (tasks/queue/registry — extend as needed) |
| `workspace_parallel_spcs_monitor.ipynb` | Workspace | Parallel SPCS lab: SQL-only monitoring in a **second tab** while the driver runs |

### Supporting Files

| File | Purpose |
|---|---|
| `sfnb_setup.py` | All-in-one bootstrap: EAI, R runtime, packages, session context (Workspace) |
| `snowflaker_config.yaml` | Per-notebook config for quickstart, model registry, feature store |
| `snowflaker_forecast_config.yaml` | Per-notebook config for forecasting demo |
| `snowflaker_feature_store_config.yaml` | Per-notebook config for feature store demo |
| `snowflaker_credit_risk_config.yaml` | Per-notebook config for credit risk demo |
| `snowflaker_parallel_spcs_config.yaml` | Parallel SPCS / doSnowflake / forecast lab |
| `PARALLEL_SPCS_DEMO.md` | Design notes: monitoring pattern, bundled Registry inference |
| `parallel_lab_config.py` | Loads `parallel_lab` from `snowflaker_parallel_spcs_config.yaml` (shared by 3 notebooks + Streamlit) |
| `streamlit_parallel_demo_monitor.py` | Streamlit monitor (FQNs from the same YAML as the notebooks) |

## Quick Start

### 1. Configure your environment (optional)

Edit the appropriate `_config.yaml` for your notebook. All sections are
optional -- if omitted, `setup_notebook()` uses the Snowpark session's
current database, schema, and warehouse as defaults:

```yaml
# snowflaker_config.yaml (example)
context:
  warehouse: "MY_WAREHOUSE"
  database: "MY_DATABASE"
  schema: "MY_SCHEMA"

# eai:
#   managed: "MY_EAI"

languages:
  r:
    enabled: true
    tarballs:
      snowflakeR: "https://github.com/Snowflake-Labs/snowflakeR/releases/download/v0.1.0/snowflakeR_0.1.0.tar.gz"
```

### 2. Choose your environment

**Workspace Notebooks** (Python kernel + `%%R` magic):

1. Upload this folder to your Workspace
2. Open a workspace notebook (e.g. `workspace_quickstart.ipynb`)
3. Run the first cell -- `setup_notebook()` handles everything:
   - Validates/creates the EAI (with all required domains)
   - Installs R via [sfnb-multilang](https://github.com/Snowflake-Labs/snowflake-notebook-multilang)
   - Installs R packages (from tarballs or GitHub)
   - Sets session context (USE WAREHOUSE/DATABASE/SCHEMA)
   - Exports SPCS OAuth env vars for RSnowflake DBI connectivity
4. If this is a first-time setup and no EAI is attached yet, follow the
   printed instructions to attach it via the Snowsight UI (one-time step)

**Local R environments** (RStudio, Posit Workbench, JupyterLab with R kernel):

1. Open `local_quickstart.ipynb` (or copy cells to an R script)
2. Ensure `snowflakeR` is installed (`pak::pak("Snowflake-Labs/snowflakeR")`)
3. Configure `connections.toml` or pass credentials to `sfr_connect()`

## Accessing notebooks from an installed package

After installing `snowflakeR`, find the notebooks with:

```r
system.file("notebooks", package = "snowflakeR")
```

Or copy them to your working directory:

```r
nb_dir <- system.file("notebooks", package = "snowflakeR")
file.copy(list.files(nb_dir, full.names = TRUE), ".", recursive = TRUE)
```

## DBI / dbplyr

These notebooks use `sfr_query()` and `sfr_execute()` for SQL. For full
DBI compliance and `dbplyr` integration, install the companion
[RSnowflake](https://github.com/Snowflake-Labs/RSnowflake) package and use
`sfr_dbi_connection()` to bridge from an `sfr_connection`. See the
`local_quickstart.ipynb` Section 4 for examples, or the standalone
`RSnowflake/inst/notebooks/workspace_rsnowflake_test.ipynb`.

## RSnowflake Test Notebook

A standalone test notebook for the **RSnowflake** DBI package is available at
`RSnowflake/inst/notebooks/workspace_rsnowflake_test.ipynb`.

## Troubleshooting: Model Registry & SPCS Inference

### `hardhat::forge()` error with empty message

If SPCS inference fails with `Error in hardhat::forge(new_data, blueprint = ...):`
followed by an empty message, this is almost always a **column name case mismatch**.

`snowflakeR` preserves column names as-is from Snowflake (UPPER case for
unquoted identifiers). Column names are consistent throughout the entire pipeline
(training, registration, inference) so this error should not occur with default
settings. If it does, verify that `names(new_data)` matches the columns the
model was trained on.

If you use `options(snowflakeR.lowercase_columns = TRUE)`, ensure this setting
is consistent between training and inference environments.

The empty error message occurs because `rlang`/`cli` error formatting uses ANSI
codes that get stripped during JSON serialization in the SPCS HTTP response.

### `basic_string::substr` crash

This C++ error from rpy2 hides the real R error. Run a Python diagnostic cell
to call the model's predict function directly and see the actual error message.

### Package not found in SPCS container

SPCS containers install R packages **from conda-forge only**. Packages
installed from CRAN or GitHub in Workspace will not be available at inference
time. Ensure all `predict_pkgs` have conda-forge counterparts (`r-<pkgname>`).

### Version pinning

`sfr_log_model()` auto-pins R and package versions by default (`pin_versions = TRUE`).
This prevents version drift between training and inference environments.
Version pins use `==` (PEP 440 syntax), not `=` (conda syntax).
