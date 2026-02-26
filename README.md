# snowflakeR

R interface to the Snowflake ML platform -- Model Registry, Feature Store, Datasets, and data connectivity. Works in **local R environments** (RStudio, VS Code, terminal) and **Snowflake Workspace Notebooks**.

## Overview

**snowflakeR** provides idiomatic R functions for the full Snowflake ML lifecycle, whether you're working locally or directly inside Snowflake:

| Module | What it does |
|---|---|
| **Connect** | One-line connection via `connections.toml`, keypair, or Workspace auto-detect |
| **Query & DBI** | Run SQL, read/write tables, integrate with `dplyr`/`dbplyr` |
| **Model Registry** | Log R models (lm, glm, randomForest, xgboost, ...), deploy to SPCS, run inference |
| **Feature Store** | Create entities & feature views, generate training data, retrieve features at inference |
| **Datasets** | Versioned, immutable snapshots of query results for reproducible ML |
| **Admin** | Manage compute pools, image repos, and external access integrations |
| **Workspace Notebooks** | First-class support for Snowflake Workspace Notebooks with zero-config auth and `%%R` magic cells |

Under the hood, `snowflakeR` uses [`reticulate`](https://rstudio.github.io/reticulate/) to bridge to the [`snowflake-ml-python`](https://docs.snowflake.com/en/developer-guide/snowpark-ml/index) SDK while exposing a native R API with `snake_case` naming, S3 classes, and `cli` messaging. The same code runs identically in local R sessions and Snowflake Workspace Notebooks.

## Installation

```r
# Install from GitHub
# install.packages("pak")
pak::pak("Snowflake-Labs/snowflakeR")
```

### Python dependencies

snowflakeR requires Python >= 3.9 with the Snowflake ML packages:

```r
# Let snowflakeR install them into a dedicated virtualenv
snowflakeR::sfr_install_python_deps()
```

Or install manually:

```bash
pip install snowflake-ml-python snowflake-snowpark-python
```

## Quick start

```r
library(snowflakeR)

# Connect (reads ~/.snowflake/connections.toml by default)
conn <- sfr_connect()

# Run a query
df <- sfr_query(conn, "SELECT * FROM my_table LIMIT 10")

# Log an R model to the Model Registry
model <- lm(mpg ~ wt + hp, data = mtcars)
reg   <- sfr_model_registry(conn)
sfr_log_model(reg, model, model_name = "MPG_MODEL", version = "V1")

# Create a Feature Store entity and feature view
fs <- sfr_feature_store(conn)
sfr_create_entity(fs, name = "CUSTOMER", join_keys = "CUSTOMER_ID")
fv <- sfr_create_feature_view(
  fs,
  name       = "CUSTOMER_FEATURES",
  entity     = "CUSTOMER",
  source_sql = "SELECT customer_id, avg_spend, tenure FROM feature_table"
)
sfr_register_feature_view(fs, fv, version = "V1")
```

## Snowflake Workspace Notebooks

snowflakeR is designed to work seamlessly inside [Snowflake Workspace Notebooks](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks). It auto-detects the Workspace environment, connects via the active session token (no credentials needed), and supports the `%%R` magic cell pattern for mixing Python and R:

```r
# In a Workspace Notebook %%R cell -- zero-config connection
library(snowflakeR)
conn <- sfr_connect()   # auto-detects Workspace session

# All snowflakeR functions work identically
df <- sfr_query(conn, "SELECT * FROM my_table LIMIT 10")
reg <- sfr_model_registry(conn)
fs  <- sfr_feature_store(conn)
```

The package also provides `rprint()`, `rview()`, `rglimpse()`, and `rcat()` helpers for rich output rendering in Workspace cells. See `vignette("workspace-notebooks")` for setup instructions, PAT management, and tips for the dual local/Workspace workflow.

> **Note:** Workspace Notebooks do [not auto-set database or schema](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks-in-workspaces/notebooks-in-workspaces-edit-run#set-the-execution-context).
> `snowflakeR` provides `sfr_load_notebook_config()` to set execution context
> from a single YAML file, and `sfr_fqn()` to build fully qualified table
> names (`DATABASE.SCHEMA.TABLE`). See the example notebooks below.

## Example Notebooks

`snowflakeR` ships with a **self-contained `notebooks/` directory** that has
everything you need to get started, including setup scripts, config template,
and Python helpers:

```r
# Find the notebooks directory in the installed package
system.file("notebooks", package = "snowflakeR")

# Or copy the entire folder to your working directory
nb_dir <- system.file("notebooks", package = "snowflakeR")
file.copy(list.files(nb_dir, full.names = TRUE), ".", recursive = TRUE)
```

For **Workspace Notebooks**, upload the folder contents to your Workspace and
open `workspace_quickstart.ipynb`. For **local** environments, open
`local_quickstart.ipynb`.

| File | Purpose |
|---|---|
| `workspace_quickstart.ipynb` | Quickstart for Snowflake Workspace Notebooks |
| `local_quickstart.ipynb` | Quickstart for local R environments |
| `notebook_config.yaml.template` | Single config file for warehouse, database, schema |
| `setup_r_environment.sh` | Installs R + packages via micromamba (Workspace only) |
| `r_packages.yaml` | R package list for the setup script |
| `r_helpers.py` | Python helpers for rpy2 / `%%R` magic (Workspace only) |

Copy `notebook_config.yaml.template` to `notebook_config.yaml` and set your
context before running any notebook.

## Vignettes

| Vignette | Topic |
|---|---|
| `vignette("setup")` | **Prerequisites, Python setup, auth, and environment-specific config** (RStudio, VS Code, Jupyter, Workspace, Docker/CI) |
| `vignette("getting-started")` | Connecting, queries, table ops, DBI/dbplyr |
| `vignette("model-registry")` | Training, logging, deploying, and serving R models |
| `vignette("feature-store")` | Entities, feature views, training data, inference |
| `vignette("workspace-notebooks")` | Full guide for Snowflake Workspace Notebooks |

## Requirements

- R >= 4.1.0
- Python >= 3.9
- `snowflake-ml-python` >= 1.5.0
- `snowflake-snowpark-python`

Optional: [`snowflakeauth`](https://github.com/Snowflake-Labs/snowflakeauth) for `connections.toml` credential management.

## License

Apache License 2.0
