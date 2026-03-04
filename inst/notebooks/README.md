# snowflakeR Notebooks

Interactive Jupyter notebooks demonstrating the `snowflakeR` package.
This directory is **self-contained** -- everything you need is included.

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

### Supporting Files

| File | Purpose |
|---|---|
| `notebook_config.yaml.template` | Configuration template (warehouse, database, schema) |
| `setup_r_environment.sh` | Installs R + packages via micromamba (Workspace only) |
| `r_packages.yaml` | R package list for `setup_r_environment.sh` |
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

1. Upload this entire folder to your Workspace
2. Attach an **External Access Integration (EAI)** that allows outbound HTTPS
   to `micro.mamba.pm`, `conda.anaconda.org`, and `repo.anaconda.com` (plus
   `cloud.r-project.org` if installing CRAN packages). See the full host table
   and example SQL in `internal/prd_eng/workspace_notebooks_eai_requirements.md`.
3. Open `workspace_quickstart.ipynb`
4. Run the setup cells to install R and snowflakeR
5. The notebook handles `USE WAREHOUSE/DATABASE/SCHEMA` via `sfr_load_notebook_config()`

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
`sfr_dbi_connection()` to bridge from an `sfr_connection`.

## RSnowflake Test Notebook

A standalone test notebook for the **RSnowflake** DBI package is available at
`RSnowflake/inst/notebooks/workspace_rsnowflake_test.ipynb`.
