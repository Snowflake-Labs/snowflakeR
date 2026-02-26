# snowflakeR Notebooks

Interactive Jupyter notebooks demonstrating the `snowflakeR` package.
This directory is **self-contained** -- everything you need is included.

## Contents

| File | Purpose |
|---|---|
| `workspace_quickstart.ipynb` | Quickstart for **Snowflake Workspace Notebooks** |
| `local_quickstart.ipynb` | Quickstart for **local R environments** (RStudio, Jupyter, etc.) |
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
2. Open `workspace_quickstart.ipynb`
3. Run the setup cells to install R and snowflakeR
4. The notebook handles `USE WAREHOUSE/DATABASE/SCHEMA` via `sfr_load_notebook_config()`

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
