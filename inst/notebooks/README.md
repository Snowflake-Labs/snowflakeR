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

### Supporting Files

| File | Purpose |
|---|---|
| `notebook_config.yaml.template` | Configuration template (warehouse, database, schema) |
| `r_helpers.py` | Python helpers for rpy2/%%R magic setup (Workspace only) |

## Quick Start

### 1. Configure your environment

Copy the config template and edit with your values:

```bash
cp notebook_config.yaml.template notebook_config.yaml
```

Edit `notebook_config.yaml` -- at minimum, set:

```yaml
context:
  warehouse: "MY_WAREHOUSE"
  database: "MY_DATABASE"
  schema: "MY_SCHEMA"
```

All notebooks read this file to set execution context.
Table references use fully qualified names (`DATABASE.SCHEMA.TABLE`) as
[recommended by Snowflake](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks-in-workspaces/notebooks-in-workspaces-edit-run#set-the-execution-context).

### 2. Choose your environment

**Workspace Notebooks** (Python kernel + `%%R` magic):

1. Upload this folder to your Workspace
2. Attach an **External Access Integration (EAI)** that allows outbound HTTPS
   to `micro.mamba.pm`, `conda.anaconda.org`, and `repo.anaconda.com` (plus
   `cloud.r-project.org` if installing CRAN packages). See the full host table
   and example SQL in `internal/prd_eng/workspace_notebooks_eai_requirements.md`.
3. Open `workspace_quickstart.ipynb`
4. Run the setup cells -- the first cell installs R via the
   [`sfnb-multilang`](https://github.com/Snowflake-Labs/snowflake-notebook-multilang)
   toolkit (micromamba + conda-forge, no root required)
5. The notebook handles `USE WAREHOUSE/DATABASE/SCHEMA` via `sfr_load_notebook_config()`

**Public standalone install** (when `sfnb-multilang` is not bundled):

```python
!pip install "sfnb-multilang @ https://github.com/Snowflake-Labs/snowflake-notebook-multilang/archive/refs/heads/main.zip"
from sfnb_multilang import install
install(languages=["r"])
```

**Local R environments** (RStudio, Posit Workbench, JupyterLab with R kernel):

1. Open `local_quickstart.ipynb` (or copy cells to an R script)
2. Ensure `snowflakeR` is installed (`pak::pak("Snowflake-Labs/snowflakeR")`)
3. Configure `connections.toml` or set `connection:` section in `notebook_config.yaml`

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
`RSnowflake/inst/notebooks/demo_rsnowflake.ipynb`.

## RSnowflake Test Notebook

A standalone test notebook for the **RSnowflake** DBI package is available at
`RSnowflake/inst/notebooks/workspace_rsnowflake_test.ipynb`.

## Troubleshooting: Model Registry & SPCS Inference

### `hardhat::forge()` error with empty message

If SPCS inference fails with `Error in hardhat::forge(new_data, blueprint = ...):`
followed by an empty message, this is almost always a **column name case mismatch**.

`snowflakeR` lowercases column names from Snowflake (R convention), so the model
blueprint stores lowercase names. SPCS sends UPPER-case columns (Snowflake
convention). The built-in predict templates handle this automatically with
`tolower()`. If you use custom `predict_body`, add `names({{INPUT}}) <- tolower(names({{INPUT}}))`
at the top.

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
